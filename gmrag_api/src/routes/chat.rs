use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, Query, State, rejection::JsonRejection},
    http::{HeaderValue, StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use chrono::NaiveDateTime;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::api_error::ApiError;
use crate::auth::authz::{Authz, AuthzClient, Object, Relation};
use crate::auth::hrm::HrmChatPermission;
use crate::chat::deepseek::{DeepseekTokenParser, deepseek_stream_idle_timeout, next_stream_token};
use crate::chat::retrieval::{StoredChatMessagePage, fetch_session_chat_messages};
use crate::chat::{
    ChatPipelineError, SessionError, build_chat_context, delete_chat_session, ensure_chat_session,
    filter_citation_ids_for_user, filter_citations_for_allowed_ids, filter_citations_for_user,
    insert_chat_message, insert_user_chat_message_and_reserve_turn, list_user_chat_sessions,
    prepare_deepseek_stream, resolve_chunk_index_citations, truncate_session_title,
    update_chat_session_title, verify_chat_session_owner,
};
use crate::state::AppState;

const MAX_CITATION_IDS: usize = 64;
const CITATION_SNIPPET_CHARS: usize = 280;
const DEFAULT_CHAT_PAGE_LIMIT: i64 = 20;
const MAX_CHAT_PAGE_LIMIT: i64 = 100;
const MAX_SESSION_TITLE_CHARS: usize = 200;

#[derive(Deserialize)]
pub struct ChatHistoryQuery {
    pub session_id: Uuid,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Deserialize)]
pub struct ChatPaginationQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Deserialize)]
pub struct WorkspaceChatSessionPath {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Serialize)]
pub struct ChatHistoryMessageResponse {
    pub id: Uuid,
    pub role: String,
    pub content: String,
    pub citations: Vec<HistoryCitation>,
    #[serde(serialize_with = "crate::utc_timestamp::serialize")]
    pub created_at: NaiveDateTime,
}

#[derive(Serialize)]
pub struct ChatHistoryPageResponse {
    pub messages: Vec<ChatHistoryMessageResponse>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Deserialize)]
pub struct ChatRequest {
    pub session_id: Uuid,
    pub message: String,
}

#[derive(Deserialize)]
pub struct UpdateChatSessionTitleRequest {
    pub title: String,
}

/// Danh sách chunk cần phân giải thành thông tin nguồn hiển thị.
#[derive(Deserialize)]
pub struct ResolveCitationsRequest {
    pub chunk_ids: Vec<Uuid>,
}

/// Kết quả phân giải chỉ gồm các citation caller được phép xem.
#[derive(Serialize)]
pub struct ResolveCitationsResponse {
    pub citations: Vec<ResolvedCitation>,
}

/// Thông tin nguồn đã được kiểm tra ACL cho một chunk.
#[derive(Serialize)]
pub struct ResolvedCitation {
    pub chunk_id: Uuid,
    pub document_id: Uuid,
    pub document_name: String,
    pub snippet: String,
    pub chunk_index: i32,
}

#[derive(Serialize)]
pub struct HistoryCitation {
    pub chunk_id: Uuid,
    pub document_id: Uuid,
    pub document_name: String,
    pub snippet: String,
}

#[derive(Serialize)]
struct StreamCitationsResponse {
    citations: Vec<StreamCitation>,
}

#[derive(Serialize)]
struct StreamCitation {
    index: usize,
    chunk_id: Uuid,
    document_id: Uuid,
    document_name: String,
    snippet: String,
}

struct SseEventPayload {
    event: &'static str,
    data: String,
}

#[derive(sqlx::FromRow)]
struct CitationHydrationRow {
    chunk_id: Uuid,
    document_id: Uuid,
    document_name: String,
    original_text: String,
    chunk_index: i32,
}

#[derive(Clone)]
struct HydratedCitation {
    chunk_id: Uuid,
    document_id: Uuid,
    document_name: String,
    original_text: String,
    snippet: String,
    chunk_index: i32,
}

impl HydratedCitation {
    fn into_resolved(self) -> ResolvedCitation {
        ResolvedCitation {
            chunk_id: self.chunk_id,
            document_id: self.document_id,
            document_name: self.document_name,
            snippet: self.snippet,
            chunk_index: self.chunk_index,
        }
    }

    fn resolve_for_query(&self, query: Option<&str>) -> HistoryCitation {
        HistoryCitation {
            chunk_id: self.chunk_id,
            document_id: self.document_id,
            document_name: self.document_name.clone(),
            snippet: select_citation_snippet(&self.original_text, query),
        }
    }
}

/// Phân giải citation theo lô và loại bỏ im lặng các chunk không thể xem.
pub async fn resolve_workspace_citations(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
    payload: Result<Json<ResolveCitationsRequest>, JsonRejection>,
) -> Result<Json<ResolveCitationsResponse>, ApiError> {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(workspace_id))
        .await
    {
        if err.status.is_server_error() {
            return Err(ApiError::from_status(StatusCode::INTERNAL_SERVER_ERROR));
        }
        return Err(err);
    }

    let chunk_ids = parse_citation_ids(payload)?;
    if chunk_ids.is_empty() {
        return Ok(Json(ResolveCitationsResponse {
            citations: Vec::new(),
        }));
    }

    let allowed_ids =
        authorize_citation_ids(&state, workspace_id, &authz.user_id, &chunk_ids).await?;
    if allowed_ids.is_empty() {
        return Ok(Json(ResolveCitationsResponse {
            citations: Vec::new(),
        }));
    }

    let citations = hydrate_citations(&state, workspace_id, &authz.user_id, allowed_ids, None)
        .await?
        .into_iter()
        .map(HydratedCitation::into_resolved)
        .collect();
    Ok(Json(ResolveCitationsResponse { citations }))
}

fn parse_citation_ids(
    payload: Result<Json<ResolveCitationsRequest>, JsonRejection>,
) -> Result<Vec<Uuid>, ApiError> {
    let Json(request) = payload.map_err(|_| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
            "Malformed citation resolve request",
        )
    })?;

    if request.chunk_ids.len() > MAX_CITATION_IDS {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
            format!("chunk_ids must contain at most {MAX_CITATION_IDS} items"),
        ));
    }

    let mut seen = HashSet::with_capacity(request.chunk_ids.len());
    Ok(request
        .chunk_ids
        .into_iter()
        .filter(|chunk_id| seen.insert(*chunk_id))
        .collect())
}

async fn authorize_citation_ids(
    state: &AppState,
    workspace_id: Uuid,
    user_id: &str,
    chunk_ids: &[Uuid],
) -> Result<Vec<Uuid>, ApiError> {
    match filter_citation_ids_for_user(
        &state.pool,
        &state.authz_client,
        workspace_id,
        user_id,
        chunk_ids,
    )
    .await
    {
        Ok(ids) => Ok(ids),
        Err(err) => {
            tracing::error!(
                error = %err,
                user_id = %user_id,
                workspace_id = %workspace_id,
                "Failed to authorize citation resolution"
            );
            Err(ApiError::from_status(StatusCode::INTERNAL_SERVER_ERROR))
        }
    }
}

async fn hydrate_citations(
    state: &AppState,
    workspace_id: Uuid,
    user_id: &str,
    allowed_ids: Vec<Uuid>,
    query: Option<&str>,
) -> Result<Vec<HydratedCitation>, ApiError> {
    let rows: Vec<CitationHydrationRow> = sqlx::query_as(
        r#"
        SELECT
            dc.id AS chunk_id,
            dc.document_id,
            d.filename AS document_name,
            dc.original_text,
            dc.chunk_index
        FROM document_chunks dc
        INNER JOIN documents d
            ON d.id = dc.document_id
           AND d.workspace_id = dc.workspace_id
        WHERE dc.workspace_id = $1
          AND dc.id = ANY($2)
        "#,
    )
    .bind(workspace_id)
    .bind(&allowed_ids)
    .fetch_all(&state.pool)
    .await
    .map_err(|err| {
        tracing::error!(
            error = %err,
            user_id = %user_id,
            workspace_id = %workspace_id,
            "Failed to hydrate citations"
        );
        ApiError::from_status(StatusCode::INTERNAL_SERVER_ERROR)
    })?;

    let mut row_by_chunk = rows
        .into_iter()
        .map(|row| (row.chunk_id, row))
        .collect::<HashMap<_, _>>();
    Ok(allowed_ids
        .into_iter()
        .filter_map(|chunk_id| row_by_chunk.remove(&chunk_id))
        .map(|row| HydratedCitation {
            chunk_id: row.chunk_id,
            document_id: row.document_id,
            document_name: row.document_name,
            original_text: row.original_text.clone(),
            snippet: select_citation_snippet(&row.original_text, query),
            chunk_index: row.chunk_index,
        })
        .collect())
}

/// Keeps the pre-Phase-14 behavior for callers that do not have the question.
fn truncate_citation_snippet(text: &str) -> String {
    let mut prefix = text
        .chars()
        .take(CITATION_SNIPPET_CHARS + 1)
        .collect::<Vec<_>>();
    if prefix.len() <= CITATION_SNIPPET_CHARS {
        return prefix.into_iter().collect();
    }

    prefix.truncate(CITATION_SNIPPET_CHARS);
    while prefix
        .last()
        .is_some_and(|character| character.is_whitespace())
    {
        prefix.pop();
    }

    if let Some(boundary) = prefix
        .iter()
        .rposition(|character| character.is_whitespace())
        .filter(|boundary| *boundary >= CITATION_SNIPPET_CHARS / 2)
    {
        prefix.truncate(boundary);
        while prefix
            .last()
            .is_some_and(|character| character.is_whitespace())
        {
            prefix.pop();
        }
    }

    let mut snippet = prefix.into_iter().collect::<String>();
    snippet.push('…');
    snippet
}

#[derive(Debug, Clone, Copy)]
struct SnippetRange {
    start: usize,
    end: usize,
}

/// Chooses a sentence/line around the strongest query match without adding a
/// second retrieval or model round-trip. A tie deliberately falls back to the
/// old prefix behavior: an ambiguous match is worse than a stable snippet.
fn select_citation_snippet(text: &str, query: Option<&str>) -> String {
    if text.trim().is_empty() {
        return String::new();
    }
    if text.chars().count() <= CITATION_SNIPPET_CHARS {
        return text.to_string();
    }

    let Some(query) = query else {
        return truncate_citation_snippet(text);
    };

    let query_terms = snippet_keywords(query);
    if query_terms.len() < 2 {
        return truncate_citation_snippet(text);
    }

    let segments = split_snippet_segments(text);
    if segments.is_empty() {
        return truncate_citation_snippet(text);
    }

    let scores = segments
        .iter()
        .map(|segment| {
            snippet_keywords(&text[segment.start..segment.end])
                .intersection(&query_terms)
                .count()
        })
        .collect::<Vec<_>>();

    let Some((best_index, &best_score)) = scores.iter().enumerate().max_by_key(|(_, score)| *score)
    else {
        return truncate_citation_snippet(text);
    };
    if best_score == 0
        || scores
            .iter()
            .enumerate()
            .any(|(index, score)| index != best_index && *score == best_score)
    {
        return truncate_citation_snippet(text);
    }

    let (start, end) = expand_snippet_window(text, &segments, best_index);
    let (start, end) = trim_snippet_range(text, start, end);
    if start >= end {
        return truncate_citation_snippet(text);
    }

    render_snippet_window(text, start, end)
}

fn snippet_keywords(text: &str) -> HashSet<String> {
    let mut keywords = HashSet::new();
    let mut word = String::new();

    for character in text.chars() {
        if character.is_alphanumeric() {
            for lowered in character.to_lowercase() {
                word.push(fold_vietnamese_character(lowered));
            }
        } else if !word.is_empty() {
            keywords.insert(std::mem::take(&mut word));
        }
    }
    if !word.is_empty() {
        keywords.insert(word);
    }

    keywords
}

fn fold_vietnamese_character(character: char) -> char {
    match character {
        'à' | 'á' | 'ả' | 'ã' | 'ạ' | 'ă' | 'ằ' | 'ắ' | 'ẳ' | 'ẵ' | 'ặ' | 'â' | 'ầ' | 'ấ' | 'ẩ'
        | 'ẫ' | 'ậ' => 'a',
        'è' | 'é' | 'ẻ' | 'ẽ' | 'ẹ' | 'ê' | 'ề' | 'ế' | 'ể' | 'ễ' | 'ệ' => 'e',
        'ì' | 'í' | 'ỉ' | 'ĩ' | 'ị' => 'i',
        'ò' | 'ó' | 'ỏ' | 'õ' | 'ọ' | 'ô' | 'ồ' | 'ố' | 'ổ' | 'ỗ' | 'ộ' | 'ơ' | 'ờ' | 'ớ' | 'ở'
        | 'ỡ' | 'ợ' => 'o',
        'ù' | 'ú' | 'ủ' | 'ũ' | 'ụ' | 'ư' | 'ừ' | 'ứ' | 'ử' | 'ữ' | 'ự' => 'u',
        'ỳ' | 'ý' | 'ỷ' | 'ỹ' | 'ỵ' => 'y',
        'đ' => 'd',
        character => character,
    }
}

fn split_snippet_segments(text: &str) -> Vec<SnippetRange> {
    let mut ranges = Vec::new();
    let mut start = 0;

    for (index, character) in text.char_indices() {
        let end = index + character.len_utf8();
        if character == '\n' || matches!(character, '.' | '!' | '?' | '。' | '！' | '？') {
            if !text[start..end].trim().is_empty() {
                ranges.push(SnippetRange { start, end });
            }
            start = end;
        }
    }

    if start < text.len() && !text[start..].trim().is_empty() {
        ranges.push(SnippetRange {
            start,
            end: text.len(),
        });
    }

    ranges
}

fn expand_snippet_window(
    text: &str,
    segments: &[SnippetRange],
    best_index: usize,
) -> (usize, usize) {
    let mut left = best_index;
    let mut right = best_index;
    let mut prefer_right = best_index > 0 && best_index + 1 < segments.len();

    loop {
        let current_start = segments[left].start;
        let current_end = segments[right].end;
        let prefix_chars = usize::from(!text[..current_start].trim().is_empty());
        let suffix_chars = usize::from(!text[current_end..].trim().is_empty());
        let available = CITATION_SNIPPET_CHARS.saturating_sub(prefix_chars + suffix_chars);

        let left_candidate = (left > 0).then(|| {
            let range = SnippetRange {
                start: segments[left - 1].start,
                end: current_end,
            };
            (range, snippet_range_char_count(text, range))
        });
        let right_candidate = (right + 1 < segments.len()).then(|| {
            let range = SnippetRange {
                start: current_start,
                end: segments[right + 1].end,
            };
            (range, snippet_range_char_count(text, range))
        });

        let right_fits = right_candidate
            .as_ref()
            .is_some_and(|(_, length)| *length <= available);
        let left_fits = left_candidate.as_ref().is_some_and(|(range, length)| {
            *length <= available && !(range.start == 0 && best_index > 0)
        });

        if prefer_right && right_fits {
            right += 1;
            prefer_right = false;
        } else if !prefer_right && left_fits {
            left -= 1;
            prefer_right = true;
        } else if right_fits {
            right += 1;
            prefer_right = false;
        } else if left_fits {
            left -= 1;
            prefer_right = true;
        } else {
            return (current_start, current_end);
        }
    }
}

fn snippet_range_char_count(text: &str, range: SnippetRange) -> usize {
    text[range.start..range.end].chars().count()
}

fn trim_snippet_range(text: &str, mut start: usize, mut end: usize) -> (usize, usize) {
    while start < end {
        let character = text[start..end].chars().next().expect("range is non-empty");
        if !character.is_whitespace() {
            break;
        }
        start += character.len_utf8();
    }
    while start < end {
        let character = text[..end].chars().next_back().expect("range is non-empty");
        if !character.is_whitespace() {
            break;
        }
        end -= character.len_utf8();
    }
    (start, end)
}

fn render_snippet_window(text: &str, start: usize, end: usize) -> String {
    let (start, end) = trim_snippet_range(text, start, end);
    let has_prefix = !text[..start].trim().is_empty();
    let has_suffix = !text[end..].trim().is_empty();
    let ellipsis_count = usize::from(has_prefix) + usize::from(has_suffix);
    let content_limit = CITATION_SNIPPET_CHARS.saturating_sub(ellipsis_count);
    let content = text[start..end].chars().collect::<Vec<_>>();

    let mut visible = if content.len() <= content_limit {
        content
    } else {
        let mut cut = content_limit;
        while cut > 0 && !content[cut - 1].is_whitespace() {
            cut -= 1;
        }
        if cut == 0 {
            cut = content
                .iter()
                .enumerate()
                .skip(content_limit)
                .find(|(_, character)| character.is_whitespace())
                .map(|(index, _)| index)
                .unwrap_or(content_limit);
        }
        while cut > 0 && content[cut - 1].is_whitespace() {
            cut -= 1;
        }
        content[..cut].to_vec()
    };

    let mut snippet = String::new();
    if has_prefix {
        snippet.push('…');
    }
    snippet.extend(visible.drain(..));
    if has_suffix {
        snippet.push('…');
    }
    snippet
}

fn build_stream_citations(
    chunk_ids: &[Uuid],
    citations: Vec<ResolvedCitation>,
) -> Vec<StreamCitation> {
    let mut citation_by_chunk = citations
        .into_iter()
        .map(|citation| (citation.chunk_id, citation))
        .collect::<HashMap<_, _>>();

    chunk_ids
        .iter()
        .enumerate()
        .filter_map(|(position, chunk_id)| {
            citation_by_chunk
                .remove(chunk_id)
                .map(|citation| StreamCitation {
                    index: position + 1,
                    chunk_id: citation.chunk_id,
                    document_id: citation.document_id,
                    document_name: citation.document_name,
                    snippet: citation.snippet,
                })
        })
        .collect()
}

fn terminal_sse_events(citations: Vec<StreamCitation>, session_id: Uuid) -> [SseEventPayload; 2] {
    let citation_data = serde_json::to_string(&StreamCitationsResponse { citations })
        .expect("stream citation payload contains only infallible JSON values");

    [
        SseEventPayload {
            event: "citations",
            data: citation_data,
        },
        SseEventPayload {
            event: "done",
            data: session_id.to_string(),
        },
    ]
}

pub async fn list_workspace_chat_sessions(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
    Query(query): Query<ChatPaginationQuery>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    let (limit, offset) = match chat_page_window(query.limit, query.offset) {
        Ok(window) => window,
        Err(err) => return err.into_response(),
    };

    match list_user_chat_sessions(&state.pool, workspace_id, &authz.user_id, limit, offset).await {
        Ok(sessions) => Json(sessions).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "Failed to list chat sessions");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn get_workspace_chat_session_messages(
    State(state): State<AppState>,
    authz: Authz,
    Path(path): Path<WorkspaceChatSessionPath>,
    Query(query): Query<ChatPaginationQuery>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(path.workspace_id))
        .await
    {
        return err.into_response();
    }

    let (limit, offset) = match chat_page_window(query.limit, query.offset) {
        Ok(window) => window,
        Err(err) => return err.into_response(),
    };

    match verify_chat_session_owner(
        &state.pool,
        path.session_id,
        path.workspace_id,
        &authz.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => return Json(empty_chat_history_page(limit, offset)).into_response(),
        Err(SessionError::Forbidden) => {
            return (StatusCode::FORBIDDEN, "Chat session not accessible").into_response();
        }
        Err(SessionError::Database(err)) => {
            tracing::error!(error = %err, "Failed to verify chat session");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    match fetch_session_chat_messages(
        &state.pool,
        path.session_id,
        path.workspace_id,
        &authz.user_id,
        limit,
        offset,
    )
    .await
    {
        Ok(page) => {
            match build_history_page_with_acl(&state, path.workspace_id, &authz.user_id, page).await
            {
                Ok(page) => Json(page).into_response(),
                Err(err) => err.into_response(),
            }
        }
        Err(err) => {
            tracing::error!(error = %err, "Failed to fetch session messages");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn delete_workspace_chat_session(
    State(state): State<AppState>,
    authz: Authz,
    Path(path): Path<WorkspaceChatSessionPath>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(path.workspace_id))
        .await
    {
        return err.into_response();
    }

    match delete_chat_session(
        &state.pool,
        path.session_id,
        path.workspace_id,
        &authz.user_id,
    )
    .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(SessionError::Forbidden) => {
            (StatusCode::FORBIDDEN, "Chat session not accessible").into_response()
        }
        Err(SessionError::Database(err)) => {
            tracing::error!(error = %err, "Failed to delete chat session");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn patch_workspace_chat_session(
    State(state): State<AppState>,
    authz: Authz,
    Path(path): Path<WorkspaceChatSessionPath>,
    payload: Result<Json<UpdateChatSessionTitleRequest>, JsonRejection>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(path.workspace_id))
        .await
    {
        return err.into_response();
    }

    let Json(request) = match payload {
        Ok(payload) => payload,
        Err(_) => {
            return ApiError::new(
                StatusCode::BAD_REQUEST,
                "INVALID_REQUEST",
                "title is required and must be a string",
            )
            .into_response();
        }
    };

    let title = match validate_session_title(&request.title) {
        Ok(title) => title,
        Err(message) => {
            return ApiError::new(StatusCode::BAD_REQUEST, "INVALID_REQUEST", message)
                .into_response();
        }
    };

    match update_chat_session_title(
        &state.pool,
        path.session_id,
        path.workspace_id,
        &authz.user_id,
        &title,
    )
    .await
    {
        Ok(Some(session)) => Json(session).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(SessionError::Forbidden) => {
            (StatusCode::FORBIDDEN, "Chat session not accessible").into_response()
        }
        Err(SessionError::Database(err)) => {
            tracing::error!(error = %err, "Failed to update chat session title");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn validate_session_title(title: &str) -> Result<String, &'static str> {
    let trimmed = title.trim();
    if trimmed.is_empty() {
        return Err("title must not be empty");
    }
    if trimmed.chars().any(char::is_control) {
        return Err("title must not contain control characters");
    }
    if trimmed.chars().count() > MAX_SESSION_TITLE_CHARS {
        return Err("title must contain at most 200 Unicode characters");
    }

    // HTML escaping is deliberately left to the client that renders this
    // user-entered value; the API stores the trimmed Unicode string as-is.
    Ok(trimmed.to_string())
}

pub async fn workspace_chat_history(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
    Query(query): Query<ChatHistoryQuery>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    let (limit, offset) = match chat_page_window(query.limit, query.offset) {
        Ok(window) => window,
        Err(err) => return err.into_response(),
    };

    match verify_chat_session_owner(&state.pool, query.session_id, workspace_id, &authz.user_id)
        .await
    {
        Ok(true) => {}
        Ok(false) => return Json(empty_chat_history_page(limit, offset)).into_response(),
        Err(SessionError::Forbidden) => {
            return (StatusCode::FORBIDDEN, "Chat session not accessible").into_response();
        }
        Err(SessionError::Database(err)) => {
            tracing::error!(error = %err, "Failed to look up chat session for history");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    match fetch_session_chat_messages(
        &state.pool,
        query.session_id,
        workspace_id,
        &authz.user_id,
        limit,
        offset,
    )
    .await
    {
        Ok(page) => {
            match build_history_page_with_acl(&state, workspace_id, &authz.user_id, page).await {
                Ok(page) => Json(page).into_response(),
                Err(err) => err.into_response(),
            }
        }
        Err(err) => {
            tracing::error!(error = %err, "Failed to fetch chat history");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Đảm bảo assistant message được persist ở cả hai đường: stream chạy xong bình thường
/// và client ngắt kết nối giữa chừng.
///
/// Khi client disconnect, axum drop SSE body -> generator bị drop ngay tại `yield`, nên
/// mọi code đặt sau vòng lặp stream sẽ không bao giờ chạy. `Drop` thì luôn chạy, đúng
/// một lần, ở cả hai đường — nên persistence phải nằm ở đây.
struct PersistAssistantOnDrop {
    buffer: Arc<Mutex<String>>,
    chunk_ids: Vec<Uuid>,
    pool: PgPool,
    authz_client: AuthzClient,
    workspace_id: Uuid,
    user_id: String,
    session_id: Uuid,
    message_sequence: i64,
}

impl Drop for PersistAssistantOnDrop {
    fn drop(&mut self) {
        let assistant_text = match self.buffer.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };

        if assistant_text.is_empty() {
            return;
        }

        let (content, citations) = resolve_chunk_index_citations(&assistant_text, &self.chunk_ids);

        let pool = self.pool.clone();
        let authz_client = self.authz_client.clone();
        let workspace_id = self.workspace_id;
        let user_id = self.user_id.clone();
        let session_id = self.session_id;
        let message_sequence = self.message_sequence;

        // Drop không async: cần Handle để spawn. Ngoài runtime thì bỏ qua, không panic.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::error!(
                %session_id,
                "No tokio runtime available while persisting assistant message"
            );
            return;
        };

        handle.spawn(async move {
            let filtered = filter_citations_for_user(
                &pool,
                &authz_client,
                workspace_id,
                &user_id,
                &content,
                &citations,
            )
            .await;

            let (content, citations) = match filtered {
                Ok(value) => value,
                Err(err) => {
                    tracing::error!(
                        %session_id,
                        error = %err,
                        "Failed to re-check citations before persisting assistant message"
                    );
                    return;
                }
            };

            if let Err(err) = insert_chat_message(
                &pool,
                session_id,
                message_sequence,
                "assistant",
                &content,
                &citations,
            )
            .await
            {
                tracing::error!(
                    %session_id,
                    error = %err,
                    "Failed to persist assistant chat message"
                );
            }
        });
    }
}

pub async fn workspace_chat(
    State(state): State<AppState>,
    authz: Authz,
    _chat_permission: HrmChatPermission,
    Path(workspace_id): Path<Uuid>,
    Json(body): Json<ChatRequest>,
) -> impl IntoResponse {
    tracing::info!(
        %workspace_id,
        user_id = %authz.user_id,
        session_id = %body.session_id,
        "Chat request received"
    );

    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(workspace_id))
        .await
    {
        tracing::warn!(
            %workspace_id,
            user_id = %authz.user_id,
            "Chat request denied: user is not a workspace member"
        );
        return err.into_response();
    }

    let message = body.message.trim().to_string();
    if message.is_empty() {
        return (StatusCode::BAD_REQUEST, "Message is required").into_response();
    }

    // Xin suất xử lý TRƯỚC mọi thao tác ghi. Nếu từ chối ở đây thì request không
    // để lại dấu vết nào — không có session rác, không có câu hỏi treo trong
    // lịch sử mà chẳng bao giờ được trả lời.
    let admission_permit = match state.chat_admission.acquire().await {
        Ok(permit) => permit,
        Err(rejection) => {
            tracing::warn!(
                %workspace_id,
                user_id = %authz.user_id,
                reason = rejection.as_str(),
                "Chat request shed: server at capacity"
            );
            return chat_busy_response(state.chat_admission.retry_after_secs());
        }
    };

    let session_title = truncate_session_title(&message);
    match ensure_chat_session(
        &state.pool,
        body.session_id,
        workspace_id,
        &authz.user_id,
        &session_title,
    )
    .await
    {
        Ok(()) => {}
        Err(SessionError::Forbidden) => {
            return (StatusCode::FORBIDDEN, "Chat session not accessible").into_response();
        }
        Err(SessionError::Database(err)) => {
            tracing::error!(error = %err, "Failed to ensure chat session");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    let turn = match insert_user_chat_message_and_reserve_turn(
        &state.pool,
        body.session_id,
        &message,
    )
    .await
    {
        Ok(turn) => turn,
        Err(err) => {
            tracing::error!(error = %err, "Failed to insert user chat message and reserve turn");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let client = Client::new();
    let context = match build_chat_context(
        &state.pool,
        &state.retrieval,
        &state.authz_client,
        &client,
        workspace_id,
        body.session_id,
        &authz.user_id,
        &message,
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(err) => {
            tracing::error!(error = %err, "Chat context preparation failed");
            return chat_pipeline_error_response(err);
        }
    };

    let deepseek_response = match prepare_deepseek_stream(&client, &context).await {
        Ok(response) => response,
        Err(err) => {
            tracing::error!(error = %err, "DeepSeek stream request failed");
            return chat_pipeline_error_response(err);
        }
    };

    let pool = state.pool.clone();
    let authz_client = state.authz_client.clone();
    let user_id_for_acl = authz.user_id.clone();
    let workspace_id_for_acl = workspace_id;
    let session_id = body.session_id;
    let chunk_ids = context.chunk_ids;
    let citation_chunk_ids = chunk_ids.clone();
    let citation_query = message.clone();
    let citation_state = state.clone();
    let citation_user_id = authz.user_id.clone();
    let byte_stream = deepseek_response.bytes_stream();
    let idle_timeout = deepseek_stream_idle_timeout();

    let event_stream = async_stream::stream! {
        // Giữ suất xử lý tới khi stream kết thúc (hoặc client ngắt kết nối, lúc
        // đó stream bị drop và permit được trả lại). Thả permit sớm hơn — ví dụ
        // ngay sau `build_chat_context` — sẽ cho phép số stream chạy song song
        // vượt xa giới hạn, đúng thứ mà cơ chế này sinh ra để ngăn.
        // Cùng lý do đặt tên như `_persist_guard`: `let _ = ...` drop tức thì.
        let _admission_permit = admission_permit;

        let mut byte_stream = byte_stream;
        let mut parser = DeepseekTokenParser::new();
        let assistant_buffer = Arc::new(Mutex::new(String::new()));

        // Tên binding quan trọng: `let _ = ...` sẽ drop ngay lập tức, persist một
        // message rỗng rồi không bao giờ chạy lại nữa.
        let _persist_guard = PersistAssistantOnDrop {
            buffer: Arc::clone(&assistant_buffer),
            chunk_ids,
            pool,
            authz_client,
            workspace_id: workspace_id_for_acl,
            user_id: user_id_for_acl,
            session_id,
            message_sequence: turn.assistant_sequence,
        };

        while let Some(token_result) = next_stream_token(&mut byte_stream, &mut parser, idle_timeout).await {
            match token_result {
                Ok(token) => {
                    if let Ok(mut buffer) = assistant_buffer.lock() {
                        buffer.push_str(&token);
                    }
                    yield Ok::<Event, Infallible>(Event::default().data(token));
                }
                Err(err) => {
                    tracing::error!(error = %err, "DeepSeek stream parse failed");
                    yield Ok::<Event, Infallible>(
                        Event::default().event("error").data(err.client_message()),
                    );
                    break;
                }
            }
        }

        // Stage 1a observability: log provider-returned generation metadata only.
        // No prompt/chunk/answer/reasoning text and no credentials are logged;
        // reasoning_deltas is a count (>0 = served in thinking mode).
        let generation_metadata = parser.metadata();
        tracing::info!(
            %session_id,
            provider_model = generation_metadata.model.as_deref().unwrap_or("(none)"),
            system_fingerprint = generation_metadata
                .system_fingerprint
                .as_deref()
                .unwrap_or("(none)"),
            finish_reason = generation_metadata
                .finish_reason
                .as_deref()
                .unwrap_or("(none)"),
            reasoning_deltas = generation_metadata.reasoning_delta_count,
            "DeepSeek chat generation metadata"
        );

        // Persistence nằm trong Drop của `_persist_guard` (xem PersistAssistantOnDrop):
        // đặt ở đây thì đường client-disconnect không bao giờ chạy tới.

        // `citation_chunk_ids` comes from the ACL-rechecked chat context.
        let hydrated_citations = match hydrate_citations(
            &citation_state,
            workspace_id_for_acl,
            &citation_user_id,
            citation_chunk_ids.clone(),
            Some(&citation_query),
        )
        .await
        {
            Ok(citations) => citations,
            Err(_) => {
                tracing::warn!(
                    %session_id,
                    "Failed to hydrate SSE citations; sending an empty citation list"
                );
                Vec::new()
            }
        };
        let stream_citations = build_stream_citations(
            &citation_chunk_ids,
            hydrated_citations
                .into_iter()
                .map(HydratedCitation::into_resolved)
                .collect(),
        );
        for payload in terminal_sse_events(stream_citations, session_id) {
            yield Ok::<Event, Infallible>(
                Event::default().event(payload.event).data(payload.data),
            );
        }
    };

    Sse::new(event_stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

fn chat_page_window(limit: Option<i64>, offset: Option<i64>) -> Result<(i64, i64), ApiError> {
    let limit = limit.unwrap_or(DEFAULT_CHAT_PAGE_LIMIT);
    let offset = offset.unwrap_or(0);
    if !(0..=MAX_CHAT_PAGE_LIMIT).contains(&limit) || offset < 0 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
            "limit must be between 0 and 100 and offset must be non-negative",
        ));
    }
    Ok((limit, offset))
}

fn empty_chat_history_page(limit: i64, offset: i64) -> ChatHistoryPageResponse {
    ChatHistoryPageResponse {
        messages: Vec::new(),
        total: 0,
        limit,
        offset,
    }
}

async fn build_history_page_with_acl(
    state: &AppState,
    workspace_id: Uuid,
    user_id: &str,
    page: StoredChatMessagePage,
) -> Result<ChatHistoryPageResponse, ApiError> {
    // Read-time ACL is intentionally performed once for the union of all IDs
    // on this page. The hydrated rows are then indexed and projected back to
    // each message, so a page of 100 messages does not cause 100 hydration
    // queries (or one query per citation).
    let (page_chunk_ids, query_by_message_id) = history_citation_plan(&page.messages);

    let allowed_ids = filter_citation_ids_for_user(
        &state.pool,
        &state.authz_client,
        workspace_id,
        user_id,
        &page_chunk_ids,
    )
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "Failed to re-check history citation ACL");
        ApiError::from_status(StatusCode::INTERNAL_SERVER_ERROR)
    })?;

    // `hydrate_citations` remains the single source of document metadata and
    // applies the same missing-row behavior as SSE/resolve. Passing `None`
    // here gives us the raw prefix as a fallback while retaining the original
    // text internally so each assistant message can select its own snippet.
    let hydrated = if allowed_ids.is_empty() {
        Vec::new()
    } else {
        hydrate_citations(state, workspace_id, user_id, allowed_ids, None).await?
    };
    let hydrated_by_chunk = hydrated
        .into_iter()
        .map(|citation| (citation.chunk_id, citation))
        .collect::<HashMap<_, _>>();
    let hydrated_ids = hydrated_by_chunk.keys().copied().collect::<Vec<_>>();

    let mut messages = Vec::with_capacity(page.messages.len());
    for row in page.messages {
        let (content, visible_ids) = if row.role == "assistant" {
            filter_citations_for_allowed_ids(&row.content, &row.citations.0, &hydrated_ids)
        } else {
            (row.content.clone(), Vec::new())
        };
        let query = query_by_message_id.get(&row.id).map(String::as_str);
        let citations = if row.role == "assistant" {
            visible_ids
                .into_iter()
                .filter_map(|chunk_id| {
                    hydrated_by_chunk
                        .get(&chunk_id)
                        .map(|citation| citation.resolve_for_query(query))
                })
                .collect()
        } else {
            Vec::new()
        };

        messages.push(ChatHistoryMessageResponse {
            id: row.id,
            role: row.role,
            content,
            citations,
            created_at: row.created_at,
        });
    }

    Ok(ChatHistoryPageResponse {
        messages,
        total: page.total,
        limit: page.limit,
        offset: page.offset,
    })
}

fn history_citation_plan(
    rows: &[crate::chat::retrieval::StoredChatMessage],
) -> (Vec<Uuid>, HashMap<Uuid, String>) {
    let mut page_chunk_ids = Vec::new();
    let mut seen_page_chunk_ids = HashSet::new();
    let mut query_by_message_id = HashMap::new();

    for (index, row) in rows.iter().enumerate() {
        for chunk_id in &row.citations.0 {
            if seen_page_chunk_ids.insert(*chunk_id) {
                page_chunk_ids.push(*chunk_id);
            }
        }

        if row.role == "assistant" && index > 0 && rows[index - 1].role == "user" {
            query_by_message_id.insert(row.id, rows[index - 1].content.clone());
        }
    }

    (page_chunk_ids, query_by_message_id)
}

/// 503 kèm `Retry-After` khi server đang quá tải.
///
/// Cố ý KHÔNG dùng 429 `RATE_LIMITED`: 429 nghĩa là "bạn gửi quá nhiều", còn ở
/// đây caller không làm gì sai — server đang bận. Client nên thử lại nguyên văn
/// request sau `Retry-After` giây, không cần backoff theo user.
fn chat_busy_response(retry_after_secs: u64) -> Response {
    let mut response = ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "CHAT_BUSY",
        "Server is at capacity. Retry after the number of seconds in the Retry-After header.",
    )
    .into_response();

    response.headers_mut().insert(
        header::RETRY_AFTER,
        HeaderValue::from_str(&retry_after_secs.to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("15")),
    );
    response
}

fn chat_pipeline_error_response(err: ChatPipelineError) -> axum::response::Response {
    let status = match &err {
        ChatPipelineError::Embed(_)
        | ChatPipelineError::Generation(_)
        | ChatPipelineError::Retrieval(_) => StatusCode::BAD_GATEWAY,
        ChatPipelineError::Database(_) | ChatPipelineError::AccessControl(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };

    let message = match &err {
        ChatPipelineError::Generation(error) => error.client_message(),
        ChatPipelineError::Embed(_) => "Embedding service unavailable",
        ChatPipelineError::Retrieval(_) => "Retrieval service unavailable",
        ChatPipelineError::Database(_) | ChatPipelineError::AccessControl(_) => {
            "Chat service unavailable"
        }
    };

    (status, message).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::StreamExt;
    use serde_json::Value;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn chat_pagination_uses_document_directory_convention() {
        assert_eq!(chat_page_window(None, None).unwrap(), (20, 0));
        assert_eq!(chat_page_window(Some(0), Some(7)).unwrap(), (0, 7));
        assert_eq!(chat_page_window(Some(100), Some(0)).unwrap(), (100, 0));
    }

    #[test]
    fn chat_pagination_rejects_out_of_range_values() {
        for (limit, offset) in [(Some(-1), None), (Some(101), None), (None, Some(-1))] {
            let err = chat_page_window(limit, offset).unwrap_err();
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
            assert_eq!(err.code, "INVALID_REQUEST");
        }
    }

    #[test]
    fn session_title_validation_trims_unicode_and_counts_characters() {
        assert_eq!(
            validate_session_title("  Hỏi về nghỉ phép  ").unwrap(),
            "Hỏi về nghỉ phép"
        );
        assert!(validate_session_title(&"đ".repeat(MAX_SESSION_TITLE_CHARS)).is_ok());
        assert!(validate_session_title(&"đ".repeat(MAX_SESSION_TITLE_CHARS + 1)).is_err());
    }

    #[test]
    fn session_title_validation_rejects_empty_and_control_characters() {
        for title in ["", "   ", "một\ndòng", "một\tdòng", "một\rdòng"] {
            let error = validate_session_title(title).unwrap_err();
            assert!(!error.is_empty());
        }
    }

    #[test]
    fn history_citation_plan_deduplicates_ids_and_uses_immediately_previous_user_query() {
        let first_chunk = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let second_chunk = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let rows = vec![
            crate::chat::retrieval::StoredChatMessage {
                id: Uuid::new_v4(),
                role: "user".to_string(),
                content: "Câu hỏi thứ nhất".to_string(),
                citations: sqlx::types::Json(Vec::new()),
                created_at: chrono::NaiveDateTime::MIN,
            },
            crate::chat::retrieval::StoredChatMessage {
                id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
                role: "assistant".to_string(),
                content: "Trả lời thứ nhất".to_string(),
                citations: sqlx::types::Json(vec![first_chunk]),
                created_at: chrono::NaiveDateTime::MIN,
            },
            crate::chat::retrieval::StoredChatMessage {
                id: Uuid::new_v4(),
                role: "user".to_string(),
                content: "Câu hỏi thứ hai".to_string(),
                citations: sqlx::types::Json(Vec::new()),
                created_at: chrono::NaiveDateTime::MIN,
            },
            crate::chat::retrieval::StoredChatMessage {
                id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
                role: "assistant".to_string(),
                content: "Trả lời thứ hai".to_string(),
                citations: sqlx::types::Json(vec![first_chunk, second_chunk, first_chunk]),
                created_at: chrono::NaiveDateTime::MIN,
            },
        ];

        let (chunk_ids, queries) = history_citation_plan(&rows);
        assert_eq!(chunk_ids, vec![first_chunk, second_chunk]);
        assert_eq!(queries[&rows[1].id], "Câu hỏi thứ nhất");
        assert_eq!(queries[&rows[3].id], "Câu hỏi thứ hai");
    }

    #[test]
    fn history_citation_plan_has_no_query_for_an_assistant_at_page_start() {
        let row = crate::chat::retrieval::StoredChatMessage {
            id: Uuid::new_v4(),
            role: "assistant".to_string(),
            content: "Trả lời ở đầu trang".to_string(),
            citations: sqlx::types::Json(Vec::new()),
            created_at: chrono::NaiveDateTime::MIN,
        };

        let (_, queries) = history_citation_plan(&[row]);
        assert!(queries.is_empty());
    }

    #[test]
    fn history_snippet_queries_follow_distinct_user_turns() {
        let first_chunk = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let second_chunk = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let rows = vec![
            crate::chat::retrieval::StoredChatMessage {
                id: Uuid::new_v4(),
                role: "user".to_string(),
                content: "Chính sách làm thêm giờ".to_string(),
                citations: sqlx::types::Json(Vec::new()),
                created_at: chrono::NaiveDateTime::MIN,
            },
            crate::chat::retrieval::StoredChatMessage {
                id: Uuid::new_v4(),
                role: "assistant".to_string(),
                content: "Trả lời làm thêm giờ".to_string(),
                citations: sqlx::types::Json(vec![first_chunk]),
                created_at: chrono::NaiveDateTime::MIN,
            },
            crate::chat::retrieval::StoredChatMessage {
                id: Uuid::new_v4(),
                role: "user".to_string(),
                content: "Nghỉ phép năm".to_string(),
                citations: sqlx::types::Json(Vec::new()),
                created_at: chrono::NaiveDateTime::MIN,
            },
            crate::chat::retrieval::StoredChatMessage {
                id: Uuid::new_v4(),
                role: "assistant".to_string(),
                content: "Trả lời nghỉ phép năm".to_string(),
                citations: sqlx::types::Json(vec![second_chunk]),
                created_at: chrono::NaiveDateTime::MIN,
            },
        ];

        let (_, queries) = history_citation_plan(&rows);
        assert_eq!(queries[&rows[1].id], "Chính sách làm thêm giờ");
        assert_eq!(queries[&rows[3].id], "Nghỉ phép năm");
        assert_eq!(
            select_citation_snippet(
                "Làm thêm giờ được phê duyệt trước.",
                queries.get(&rows[1].id).map(String::as_str),
            ),
            "Làm thêm giờ được phê duyệt trước."
        );
        assert_eq!(
            select_citation_snippet(
                "Nghỉ phép năm là 12 ngày.",
                queries.get(&rows[3].id).map(String::as_str),
            ),
            "Nghỉ phép năm là 12 ngày."
        );
    }

    #[test]
    fn citation_snippet_selects_a_matching_segment_in_the_middle() {
        let chunk = concat!(
            "### Điều 12. An toàn và sức khỏe nghề nghiệp.\n",
            "Nhân viên tuân thủ biển báo và quy trình sơ tán.\n",
            "### Điều 16. Hỗ trợ bữa trưa.\n",
            "Mức hỗ trợ bữa trưa là 35.000 đồng nếu nhân viên làm đủ sáu giờ trong ngày.\n",
            "### Điều 18. Chăm sóc sức khỏe.\n",
            "Nhân viên được khám sức khỏe định kỳ mỗi năm.\n",
            "### Điều 20. Hỗ trợ ăn tối.\n",
            "Ca làm thêm từ ba giờ được hỗ trợ một bữa ăn tối."
        );

        let query = "Mức hỗ trợ bữa trưa 35.000 đồng";
        let snippet = select_citation_snippet(chunk, Some(query));

        assert!(snippet.contains("Điều 16"));
        assert!(snippet.contains("35.000 đồng"));
        assert!(!snippet.starts_with("### Điều 12"));
        assert!(snippet.starts_with('…'));
    }

    #[test]
    fn citation_snippet_selects_a_matching_segment_at_the_end() {
        let chunk = concat!(
            "### Điều 18. Chăm sóc sức khỏe.\n",
            "Nhân viên được khám sức khỏe định kỳ mỗi năm.\n",
            "Hồ sơ sức khỏe được quản lý riêng, chỉ người có nhiệm vụ mới được tiếp cận và mọi thay đổi phải được ghi nhận theo quy trình nội bộ.\n",
            "Các đầu mối liên quan phải bảo đảm thông tin được cập nhật đầy đủ, đúng thời hạn và không dùng hồ sơ cho mục đích ngoài phạm vi công việc.\n",
            "### Điều 20. Hỗ trợ ăn tối.\n",
            "Ca làm thêm vào cuối tuần từ ba giờ được hỗ trợ một bữa ăn tối 80.000 đồng."
        );

        let snippet = select_citation_snippet(
            chunk,
            Some("Điều kiện nhận hỗ trợ ăn tối 80.000 đồng cho ca làm thêm là gì?"),
        );

        assert!(snippet.contains("80.000 đồng"));
        assert!(snippet.contains("Điều 20"));
        assert!(!snippet.starts_with("### Điều 18"));
        assert!(snippet.starts_with('…'));
        assert!(!snippet.ends_with('…'));
    }

    #[test]
    fn citation_snippet_falls_back_when_match_is_ambiguous() {
        let chunk = "Nhân viên làm việc tại văn phòng.\nNhân viên làm việc từ xa.\n";
        let expected = truncate_citation_snippet(chunk);

        assert_eq!(
            select_citation_snippet(chunk, Some("nhân viên làm việc")),
            expected
        );
        assert_eq!(
            select_citation_snippet(chunk, Some("đi")),
            truncate_citation_snippet(chunk)
        );
    }

    #[test]
    fn citation_snippet_preserves_short_chunks_and_unicode_words() {
        let short = "Điều 16. Hỗ trợ bữa trưa là 35.000 đồng.";
        assert_eq!(
            select_citation_snippet(short, Some("hỗ trợ bữa trưa")),
            short
        );

        let chunk = concat!(
            "### Điều 16. Hỗ trợ bữa trưa.\n",
            "Quy định cũ về thời gian.\n",
            "Nhân viên đủ điều kiện nhận hỗ trợ bữa trưa 35.000 đồng mỗi ngày.\n",
            "Quy định kết thúc."
        );
        let snippet = select_citation_snippet(chunk, Some("hỗ trợ bữa trưa 35.000 đồng"));
        assert!(snippet.contains("hỗ trợ bữa trưa 35.000 đồng"));
        assert!(snippet.contains("Nhân viên"));
        assert!(!snippet.contains("bữa trư…"));
        assert!(snippet.chars().count() <= CITATION_SNIPPET_CHARS);
    }

    #[test]
    fn citation_snippet_handles_empty_and_whitespace_chunks() {
        assert_eq!(select_citation_snippet("", Some("câu hỏi bất kỳ")), "");
        assert_eq!(
            select_citation_snippet("   \n\t  ", Some("câu hỏi bất kỳ")),
            ""
        );
    }

    /// Bảo vệ giả định của `PersistAssistantOnDrop`: generator bị drop giữa chừng
    /// (client disconnect) vẫn chạy `Drop` của giá trị nó sở hữu.
    #[tokio::test]
    async fn guard_owned_by_stream_drops_when_stream_is_abandoned_midway() {
        let fired = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&fired);

        // Box::pin để test thực sự SỞ HỮU generator. `futures::pin_mut!` chỉ tạo
        // Pin<&mut _>, nên `drop()` lên nó chỉ bỏ một mutable reference — generator
        // vẫn sống tới hết scope và test sẽ fail nhầm, che mất thứ cần đo.
        let mut stream = Box::pin(async_stream::stream! {
            let _guard = DropFlag(flag);
            for value in 0..10u8 {
                yield value;
            }
        });

        // Chỉ đọc 1 item rồi bỏ stream — mô phỏng client ngắt kết nối giữa chừng.
        let first = stream.next().await;
        assert_eq!(first, Some(0));
        assert!(
            !fired.load(Ordering::SeqCst),
            "guard must still be alive mid-stream"
        );

        drop(stream);
        assert!(
            fired.load(Ordering::SeqCst),
            "Drop phải chạy khi generator bị huỷ giữa chừng — nếu fail, cơ chế persist đã hỏng"
        );
    }

    #[test]
    fn citations_event_precedes_done_and_uses_prompt_marker_index() {
        let first_chunk = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let second_chunk = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let document_id = Uuid::parse_str("99999999-8888-7777-6666-555555555555").unwrap();
        let session_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let citations = vec![ResolvedCitation {
            chunk_id: second_chunk,
            document_id,
            document_name: "policy.pdf".to_string(),
            snippet: "Second retrieved passage".to_string(),
            chunk_index: 8,
        }];

        let stream_citations = build_stream_citations(&[first_chunk, second_chunk], citations);
        let events = terminal_sse_events(stream_citations, session_id);

        assert_eq!(events[0].event, "citations");
        assert_eq!(events[1].event, "done");
        let payload: Value = serde_json::from_str(&events[0].data).unwrap();
        assert_eq!(payload["citations"][0]["index"], 2);
        assert_eq!(
            payload["citations"][0]["chunk_id"],
            second_chunk.to_string()
        );
        assert_eq!(events[1].data, session_id.to_string());
    }

    #[test]
    fn citations_event_is_present_with_an_empty_array_when_no_chunks_exist() {
        let session_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let events = terminal_sse_events(build_stream_citations(&[], Vec::new()), session_id);

        assert_eq!(events[0].event, "citations");
        assert_eq!(events[0].data, r#"{"citations":[]}"#);
        assert_eq!(events[1].event, "done");
    }
}
