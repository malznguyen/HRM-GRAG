pub mod audit;
pub mod auth;
pub mod chat;
pub mod identity_report;
pub mod ingestion;
pub mod invite;
pub mod invite_cleanup;
pub mod outbox;
pub mod retrieval;
pub mod routes;
pub mod state;
pub mod storage;
pub mod tenant_cleanup;

use axum::{
    Router,
    extract::{DefaultBodyLimit, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Json},
    routing::{delete, get, post},
};
use routes::chat::{
    delete_workspace_chat_session, get_workspace_chat_session_messages,
    list_workspace_chat_sessions, workspace_chat, workspace_chat_history,
};
use routes::documents::{
    delete_document, get_document_chunk, get_document_preview, list_documents,
    patch_document_access_mode, retry_document_ingestion, revoke_document_share, share_document,
    upload_document,
};
use routes::graph::get_workspace_graph;
use routes::members::{
    add_workspace_member, list_workspace_members, remove_workspace_member,
    update_workspace_member_role,
};
use routes::users::{get_current_user, sync_current_user};
use routes::workspaces::{
    add_tenant_owner, create_tenant, create_workspace, delete_workspace, list_workspaces,
};
use serde::Serialize;
use state::AppState;
use tower_http::cors::{AllowOrigin, CorsLayer};

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    db: &'static str,
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await
    {
        Ok(_) => (
            StatusCode::OK,
            Json(HealthResponse {
                status: "ok",
                db: "connected",
            }),
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub fn app_router(state: AppState) -> Router {
    const MAX_UPLOAD_BYTES: usize = 50 * 1024 * 1024;
    let cors = cors_layer();

    Router::new()
        .route("/health", get(health))
        .route("/users/me", get(get_current_user))
        .route("/users/sync", post(sync_current_user))
        .route("/tenants", post(create_tenant))
        .route("/tenants/{tenant_id}/owners", post(add_tenant_owner))
        .route("/tenants/{tenant_id}/workspaces", post(create_workspace))
        .route("/workspaces", get(list_workspaces))
        .route("/workspaces/{workspace_id}", delete(delete_workspace))
        .route("/workspaces/{workspace_id}/documents", get(list_documents))
        .route(
            "/workspaces/{workspace_id}/documents/upload",
            post(upload_document),
        )
        .route(
            "/workspaces/{workspace_id}/documents/{document_id}",
            delete(delete_document),
        )
        .route(
            "/workspaces/{workspace_id}/documents/{document_id}/retry",
            post(retry_document_ingestion),
        )
        .route(
            "/workspaces/{workspace_id}/documents/{document_id}/access-mode",
            axum::routing::patch(patch_document_access_mode),
        )
        .route(
            "/workspaces/{workspace_id}/documents/{document_id}/shares/{user_id}",
            post(share_document).delete(revoke_document_share),
        )
        .route(
            "/workspaces/{workspace_id}/documents/{document_id}/preview",
            get(get_document_preview),
        )
        .route(
            "/workspaces/{workspace_id}/chunks/{chunk_id}",
            get(get_document_chunk),
        )
        .route("/workspaces/{workspace_id}/chat", post(workspace_chat))
        .route(
            "/workspaces/{workspace_id}/chat/history",
            get(workspace_chat_history),
        )
        .route(
            "/workspaces/{workspace_id}/chat/sessions",
            get(list_workspace_chat_sessions),
        )
        .route(
            "/workspaces/{workspace_id}/chat/sessions/{session_id}/messages",
            get(get_workspace_chat_session_messages),
        )
        .route(
            "/workspaces/{workspace_id}/chat/sessions/{session_id}",
            delete(delete_workspace_chat_session),
        )
        .route("/workspaces/{workspace_id}/graph", get(get_workspace_graph))
        .route(
            "/workspaces/{workspace_id}/members",
            get(list_workspace_members).post(add_workspace_member),
        )
        .route(
            "/workspaces/{workspace_id}/members/{member_id}",
            delete(remove_workspace_member).patch(update_workspace_member_role),
        )
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(cors)
        .with_state(state)
}

fn cors_layer() -> CorsLayer {
    let allowed_origins = std::env::var("CORS_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| "http://localhost:3000,http://127.0.0.1:3000".to_string());

    let origins: Vec<_> = allowed_origins
        .split(',')
        .filter_map(|origin| origin.trim().parse().ok())
        .collect();

    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
            Method::HEAD,
        ])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
}
