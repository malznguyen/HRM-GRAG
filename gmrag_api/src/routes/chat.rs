use std::convert::Infallible;
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, Query, State},
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

use crate::auth::extractor::AuthUser;
use crate::auth::rbac::require_workspace_member;
use crate::chat::deepseek::{DeepseekTokenParser, next_stream_token};
use crate::chat::retrieval::fetch_session_chat_messages;
use crate::chat::{
    ChatPipelineError, SessionError, build_chat_context, delete_chat_session,
    ensure_chat_session, extract_chunk_citations, insert_chat_message, list_user_chat_sessions,
    prepare_deepseek_stream, truncate_session_title, verify_chat_session_owner,
};
use crate::state::AppState;

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

pub async fn list_workspace_chat_sessions(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(status) = require_workspace_member(&state.pool, workspace_id, &auth.user_id).await {
        return status.into_response();
    }

    match list_user_chat_sessions(&state.pool, workspace_id, &auth.user_id).await {
        Ok(sessions) => Json(sessions).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "Failed to list chat sessions");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn get_workspace_chat_session_messages(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(path): Path<WorkspaceChatSessionPath>,
) -> impl IntoResponse {
    if let Err(status) =
        require_workspace_member(&state.pool, path.workspace_id, &auth.user_id).await
    {
        return status.into_response();
    }

    match verify_chat_session_owner(
        &state.pool,
        path.session_id,
        path.workspace_id,
        &auth.user_id,
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
        &auth.user_id,
    )
    .await
    {
        Ok(rows) => {
            let messages: Vec<ChatHistoryMessageResponse> = rows
                .into_iter()
                .map(|row| ChatHistoryMessageResponse {
                    id: row.id,
                    role: row.role,
                    content: row.content,
                    citations: row.citations.0,
                    created_at: row.created_at,
                })
                .collect();
            Json(messages).into_response()
        }
        Err(err) => {
            tracing::error!(error = %err, "Failed to fetch session messages");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn delete_workspace_chat_session(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(path): Path<WorkspaceChatSessionPath>,
) -> impl IntoResponse {
    if let Err(status) =
        require_workspace_member(&state.pool, path.workspace_id, &auth.user_id).await
    {
        return status.into_response();
    }

    match delete_chat_session(
        &state.pool,
        path.session_id,
        path.workspace_id,
        &auth.user_id,
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
    auth: AuthUser,
    Path(workspace_id): Path<Uuid>,
    Query(query): Query<ChatHistoryQuery>,
) -> impl IntoResponse {
    if let Err(status) = require_workspace_member(&state.pool, workspace_id, &auth.user_id).await {
        return status.into_response();
    }

    match verify_chat_session_owner(
        &state.pool,
        query.session_id,
        workspace_id,
        &auth.user_id,
    )
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

    match fetch_session_chat_messages(
        &state.pool,
        query.session_id,
        workspace_id,
        &auth.user_id,
    )
    .await
    {
        Ok(rows) => {
            let messages: Vec<ChatHistoryMessageResponse> = rows
                .into_iter()
                .map(|row| ChatHistoryMessageResponse {
                    id: row.id,
                    role: row.role,
                    content: row.content,
                    citations: row.citations.0,
                    created_at: row.created_at,
                })
                .collect();
            Json(messages).into_response()
        }
        Err(err) => {
            tracing::error!(error = %err, "Failed to fetch chat history");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn workspace_chat(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(workspace_id): Path<Uuid>,
    Json(body): Json<ChatRequest>,
) -> impl IntoResponse {
    tracing::info!(
        %workspace_id,
        user_id = %auth.user_id,
        session_id = %body.session_id,
        "Chat request received"
    );

    if let Err(status) = require_workspace_member(&state.pool, workspace_id, &auth.user_id).await {
        tracing::warn!(
            %workspace_id,
            user_id = %auth.user_id,
            "Chat request denied: user is not a workspace member"
        );
        return status.into_response();
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
        &auth.user_id,
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

    if let Err(err) =
        insert_chat_message(&state.pool, body.session_id, "user", &message, &[]).await
    {
        tracing::error!(error = %err, "Failed to insert user chat message");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let client = Client::new();
    let context = match build_chat_context(
        &state.pool,
        &client,
        workspace_id,
        body.session_id,
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
    let session_id = body.session_id;
    let byte_stream = deepseek_response.bytes_stream();

    let event_stream = async_stream::stream! {
        let mut byte_stream = byte_stream;
        let mut parser = DeepseekTokenParser::new();
        let mut assistant_buffer = String::new();

        while let Some(token_result) = next_stream_token(&mut byte_stream, &mut parser).await {
            match token_result {
                Ok(token) => {
                    assistant_buffer.push_str(&token);
                    yield Ok::<Event, Infallible>(Event::default().data(token));
                }
                Err(err) => {
                    tracing::error!(error = %err, "DeepSeek stream parse failed");
                    yield Ok::<Event, Infallible>(
                        Event::default().event("error").data(err.to_string()),
                    );
                    break;
                }
            }
        }

        if !assistant_buffer.is_empty() {
            let content = assistant_buffer.clone();
            let citations = extract_chunk_citations(&content);
            tokio::spawn(async move {
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

fn chat_pipeline_error_response(err: ChatPipelineError) -> axum::response::Response {
    let status = match &err {
        ChatPipelineError::Embed(_) | ChatPipelineError::Generation(_) => StatusCode::BAD_GATEWAY,
        ChatPipelineError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };

    (status, err.to_string()).into_response()
}
