use std::convert::Infallible;
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
};
use reqwest::Client;
use serde::Deserialize;
use uuid::Uuid;

use crate::auth::extractor::AuthUser;
use crate::auth::rbac::require_workspace_member;
use crate::chat::deepseek::{DeepseekTokenParser, next_stream_token};
use crate::chat::{
    ChatPipelineError, build_chat_context, ensure_chat_session, insert_chat_message,
    prepare_deepseek_stream,
};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct ChatRequest {
    pub session_id: Uuid,
    pub message: String,
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

    match ensure_chat_session(&state.pool, body.session_id, workspace_id, &auth.user_id).await {
        Ok(()) => {}
        Err(crate::chat::SessionError::Forbidden) => {
            return (StatusCode::FORBIDDEN, "Chat session not accessible").into_response();
        }
        Err(crate::chat::SessionError::Database(err)) => {
            tracing::error!(error = %err, "Failed to ensure chat session");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    if let Err(err) = insert_chat_message(&state.pool, body.session_id, "user", &message).await {
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
            tokio::spawn(async move {
                if let Err(err) =
                    insert_chat_message(&pool, session_id, "assistant", &content).await
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
