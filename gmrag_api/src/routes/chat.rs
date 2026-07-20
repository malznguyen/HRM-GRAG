use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
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
use uuid::Uuid;

use crate::api_error::ApiError;
use crate::auth::authz::{Authz, Object, Relation};
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

pub async fn workspace_chat(
    State(state): State<AppState>,
    authz: Authz,
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
    let byte_stream = deepseek_response.bytes_stream();
    let idle_timeout = deepseek_stream_idle_timeout();

    let event_stream = async_stream::stream! {
        let mut byte_stream = byte_stream;
        let mut parser = DeepseekTokenParser::new();
        let mut assistant_buffer = String::new();

        while let Some(token_result) = next_stream_token(&mut byte_stream, &mut parser, idle_timeout).await {
            match token_result {
                Ok(token) => {
                    assistant_buffer.push_str(&token);
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

        if !assistant_buffer.is_empty() {
            let (content, citations) =
                resolve_chunk_index_citations(&assistant_buffer, &chunk_ids);
            tokio::spawn(async move {
                let filtered = filter_citations_for_user(
                    &pool,
                    &authz_client,
                    workspace_id_for_acl,
                    &user_id_for_acl,
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

        yield Ok::<Event, Infallible>(
            Event::default().event("done").data(session_id.to_string()),
        );
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
