mod auth;
mod chat;
mod ingestion;
mod invite;
mod routes;
mod state;
mod webhooks;

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
use routes::documents::{delete_document, get_document_chunk, list_documents, upload_document};
use routes::graph::get_workspace_graph;
use routes::users::{get_current_user, sync_current_user};
use routes::members::{
    add_workspace_member, list_workspace_members, remove_workspace_member,
};
use routes::workspaces::{create_workspace, delete_workspace, list_workspaces};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use state::AppState;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tower_http::cors::{AllowOrigin, CorsLayer};
use webhooks::clerk::handle_clerk_webhook;

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

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,lopdf=error")),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let max_connections = std::env::var("DATABASE_POOL_SIZE")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(32)
        .max(1);

    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    let jwt =
        auth::jwt::JwtValidator::from_env().expect("CLERK_ISSUER must be set for JWT validation");

    let upload_dir = AppState::upload_dir_from_env();
    tokio::fs::create_dir_all(&upload_dir)
        .await
        .expect("Failed to create upload directory");

    let state = AppState {
        pool,
        jwt,
        upload_dir,
        ingestion_limiter: Arc::new(Semaphore::new(AppState::ingestion_limit_from_env())),
    };

    const MAX_UPLOAD_BYTES: usize = 50 * 1024 * 1024;

    let cors = cors_layer();

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/webhooks/clerk", post(handle_clerk_webhook))
        .route("/users/me", get(get_current_user))
        .route("/users/sync", post(sync_current_user))
        .route("/workspaces", get(list_workspaces).post(create_workspace))
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
            delete(remove_workspace_member),
        )
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind TCP listener");

    println!("gmrag_api listening on {addr}");

    axum::serve(listener, app).await.expect("Server failed");
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
