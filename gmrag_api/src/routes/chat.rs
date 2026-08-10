use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, Query, State, rejection::JsonRejection},
    http::StatusCode,
    response::{
        IntoResponse,
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
use crate::chat::retrieval::{StoredChatMessage, fetch_session_chat_messages};
use crate::chat::{
    ChatPipelineError, SessionError, build_chat_context, delete_chat_session, ensure_chat_session,
    filter_citation_ids_for_user, filter_citations_for_user, insert_chat_message,
    list_user_chat_sessions, prepare_deepseek_stream, resolve_chunk_index_citations,
    truncate_session_title, verify_chat_session_owner,
};
use crate::state::AppState;

const MAX_CITATION_IDS: usize = 64;
const CITATION_SNIPPET_CHARS: usize = 280;

#[derive(Deserialize)]
pub struct ChatHistoryQuery {
    pub session_id: Uuid,
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
    pub citations: Vec<Uuid>,
    pub created_at: NaiveDateTime,
}

#[derive(Deserialize)]
pub struct ChatRequest {
    pub session_id: Uuid,
    pub message: String,
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

    let citations = hydrate_citations(&state, workspace_id, &authz.user_id, allowed_ids).await?;
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
) -> Result<Vec<ResolvedCitation>, ApiError> {
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
        .map(|row| ResolvedCitation {
            chunk_id: row.chunk_id,
            document_id: row.document_id,
            document_name: row.document_name,
            snippet: truncate_citation_snippet(&row.original_text),
            chunk_index: row.chunk_index,
        })
        .collect())
}

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
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    match list_user_chat_sessions(&state.pool, workspace_id, &authz.user_id).await {
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
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(path.workspace_id))
        .await
    {
        return err.into_response();
    }

    match verify_chat_session_owner(
        &state.pool,
        path.session_id,
        path.workspace_id,
        &authz.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => return Json(Vec::<ChatHistoryMessageResponse>::new()).into_response(),
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
    )
    .await
    {
        Ok(rows) => {
            match build_history_messages_with_acl(&state, path.workspace_id, &authz.user_id, rows)
                .await
            {
                Ok(messages) => Json(messages).into_response(),
                Err(err) => {
                    tracing::error!(error = %err, "Failed to filter citation ACL for session messages");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
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

    match verify_chat_session_owner(&state.pool, query.session_id, workspace_id, &authz.user_id)
        .await
    {
        Ok(true) => {}
        Ok(false) => return Json(Vec::<ChatHistoryMessageResponse>::new()).into_response(),
        Err(SessionError::Forbidden) => {
            return (StatusCode::FORBIDDEN, "Chat session not accessible").into_response();
        }
        Err(SessionError::Database(err)) => {
            tracing::error!(error = %err, "Failed to look up chat session for history");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    match fetch_session_chat_messages(&state.pool, query.session_id, workspace_id, &authz.user_id)
        .await
    {
        Ok(rows) => {
            match build_history_messages_with_acl(&state, workspace_id, &authz.user_id, rows).await
            {
                Ok(messages) => Json(messages).into_response(),
                Err(err) => {
                    tracing::error!(error = %err, "Failed to filter citation ACL for chat history");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
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

            if let Err(err) =
                insert_chat_message(&pool, session_id, "assistant", &content, &citations).await
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

    if let Err(err) = insert_chat_message(&state.pool, body.session_id, "user", &message, &[]).await
    {
        tracing::error!(error = %err, "Failed to insert user chat message");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

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
    let citation_state = state.clone();
    let citation_user_id = authz.user_id.clone();
    let byte_stream = deepseek_response.bytes_stream();
    let idle_timeout = deepseek_stream_idle_timeout();

    let event_stream = async_stream::stream! {
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
        let stream_citations = build_stream_citations(&citation_chunk_ids, hydrated_citations);
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

async fn build_history_messages_with_acl(
    state: &AppState,
    workspace_id: Uuid,
    user_id: &str,
    rows: Vec<StoredChatMessage>,
) -> Result<Vec<ChatHistoryMessageResponse>, ChatPipelineError> {
    let mut messages = Vec::with_capacity(rows.len());

    for row in rows {
        let (content, citations) = filter_citations_for_user(
            &state.pool,
            &state.authz_client,
            workspace_id,
            user_id,
            &row.content,
            &row.citations.0,
        )
        .await?;

        messages.push(ChatHistoryMessageResponse {
            id: row.id,
            role: row.role,
            content,
            citations,
            created_at: row.created_at,
        });
    }

    Ok(messages)
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
        assert_eq!(payload["citations"][0]["chunk_id"], second_chunk.to_string());
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
