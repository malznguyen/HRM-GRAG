mod api_docs;
pub mod api_error;
pub mod audit;
pub mod auth;
pub mod authz_orphan_report;
pub mod chat;
pub mod document_directory;
pub mod document_format;
pub mod identity_report;
pub mod ingestion;
pub mod invite;
pub mod invite_cleanup;
pub mod outbox;
pub mod rate_limit;
pub mod retention_report;
pub mod retrieval;
pub mod routes;
pub mod shutdown;
pub mod state;
pub mod storage;
pub mod telemetry;
pub mod tenant_cleanup;
pub mod tenant_directory;
pub mod workspace_admin_recovery;

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod test_isolation;

use auth::authz::{Object, Relation};
use axum::{
    Router,
    extract::{DefaultBodyLimit, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Json},
    routing::{delete, get, post},
};
use routes::chat::{
    delete_workspace_chat_session, get_workspace_chat_session_messages,
    list_workspace_chat_sessions, resolve_workspace_citations, workspace_chat,
    workspace_chat_history,
};
use routes::documents::{
    delete_document, get_document_chunk, get_document_preview, get_document_shares,
    get_document_status, list_documents, patch_document_access_mode, put_document_permissions,
    retry_document_ingestion, revoke_document_share, share_document, upload_document,
};
use routes::graph::get_workspace_graph;
use routes::members::{
    add_workspace_member, list_workspace_members, remove_workspace_member,
    update_workspace_member_role,
};
use routes::users::{get_current_user, sync_current_user};
use routes::workspaces::{
    add_tenant_owner, create_tenant, create_workspace, delete_workspace, list_my_tenants,
    list_tenants, list_workspaces,
};
use serde::Serialize;
use state::AppState;
use tower_http::cors::{AllowOrigin, CorsLayer};
use uuid::Uuid;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    db: &'static str,
}

#[derive(Clone, Copy)]
enum RuntimeRole {
    Api,
    IngestionWorker,
    AuthzOutboxWorker,
    QdrantOutboxWorker,
    StorageWorker,
}

impl RuntimeRole {
    fn from_env() -> Self {
        let value = std::env::var("APP_RUNTIME_ROLE")
            .unwrap_or_else(|_| "api".to_string())
            .to_ascii_lowercase();

        match value.as_str() {
            "ingestion-worker" | "ingestion_worker" => Self::IngestionWorker,
            "process-authz-outbox" | "authz-outbox-worker" | "authz_outbox_worker" => {
                Self::AuthzOutboxWorker
            }
            "process-qdrant-outbox" | "qdrant-outbox-worker" | "qdrant_outbox_worker" => {
                Self::QdrantOutboxWorker
            }
            "process-storage-outbox"
            | "storage-orphan-scan"
            | "storage-worker"
            | "storage_worker" => Self::StorageWorker,
            _ => Self::Api,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::IngestionWorker => "ingestion-worker",
            Self::AuthzOutboxWorker => "process-authz-outbox",
            Self::QdrantOutboxWorker => "process-qdrant-outbox",
            Self::StorageWorker => "storage-worker",
        }
    }

    fn required_dependencies(self) -> &'static [DependencyName] {
        match self {
            Self::Api => &[DependencyName::Postgres, DependencyName::OpenFga],
            Self::IngestionWorker => &[
                DependencyName::Postgres,
                DependencyName::Qdrant,
                DependencyName::ObjectStorage,
            ],
            Self::AuthzOutboxWorker => &[DependencyName::Postgres, DependencyName::OpenFga],
            Self::QdrantOutboxWorker => &[DependencyName::Postgres, DependencyName::Qdrant],
            Self::StorageWorker => &[DependencyName::Postgres, DependencyName::ObjectStorage],
        }
    }
}

#[derive(Clone, Copy)]
enum DependencyName {
    Postgres,
    OpenFga,
    Qdrant,
    ObjectStorage,
}

impl DependencyName {
    fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::OpenFga => "openfga",
            Self::Qdrant => "qdrant",
            Self::ObjectStorage => "object_storage",
        }
    }
}

#[derive(Serialize)]
struct ReadyDependencyStatus {
    name: &'static str,
    healthy: bool,
}

#[derive(Serialize)]
struct ReadyResponse {
    status: &'static str,
    role: &'static str,
    dependencies: Vec<ReadyDependencyStatus>,
    failed_dependencies: Vec<&'static str>,
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

async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let role = RuntimeRole::from_env();
    let mut dependencies = Vec::new();
    let mut failed_dependencies = Vec::new();

    for dependency in role.required_dependencies() {
        let result = check_dependency(state.clone(), *dependency).await;
        if let Err(error) = &result {
            tracing::warn!(
                role = role.as_str(),
                dependency = dependency.as_str(),
                error = %error,
                "Readiness dependency check failed"
            );
            failed_dependencies.push(dependency.as_str());
        }

        dependencies.push(ReadyDependencyStatus {
            name: dependency.as_str(),
            healthy: result.is_ok(),
        });
    }

    let ready = failed_dependencies.is_empty();
    let response = ReadyResponse {
        status: if ready { "ready" } else { "not_ready" },
        role: role.as_str(),
        dependencies,
        failed_dependencies,
    };

    if ready {
        (StatusCode::OK, Json(response)).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(response)).into_response()
    }
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    if let Err(error) = telemetry::refresh_operational_metrics(&state.pool).await {
        tracing::warn!(error = %error, "Failed to refresh operational metrics before scrape");
    }

    let body = telemetry::metrics_handle().render();
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

async fn check_dependency(
    state: AppState,
    dependency: DependencyName,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match dependency {
        DependencyName::Postgres => {
            sqlx::query_scalar::<_, i32>("SELECT 1")
                .fetch_one(&state.pool)
                .await
                .map(|_| ())?;
            Ok(())
        }
        DependencyName::OpenFga => {
            state
                .authz_client
                .check_fga(
                    "user:readiness-probe",
                    Relation::Member,
                    &Object::Workspace(Uuid::nil()),
                )
                .await
                .map(|_| ())?;
            Ok(())
        }
        DependencyName::Qdrant => {
            state.retrieval.readiness_probe().await?;
            Ok(())
        }
        DependencyName::ObjectStorage => {
            state.storage.readiness_probe().await?;
            Ok(())
        }
    }
}

/// Resolve alias `hrm` ở vị trí `{workspace_id}` **trước khi routing**, một lần cho
/// toàn bộ router.
///
/// Đặt ở đây thay vì trong từng handler là có chủ đích: alias chạy ở route này mà
/// không chạy ở route kia là thứ khó hiểu nhất đối với người tích hợp, và mọi handler
/// vẫn giữ nguyên `Path<Uuid>` — chúng không cần biết alias tồn tại. Vì URI được viết
/// lại trước khi match route, `ensure_scope`, rate limit, metrics và log đều nhìn thấy
/// UUID thật.
///
/// Khi `HRM_MODE=false` thì `hrm_config()` là `None` và request đi qua nguyên trạng:
/// `hrm` lại chỉ là một chuỗi không parse được thành UUID, đúng như hành vi cũ.
async fn resolve_hrm_workspace_alias(
    State(state): State<AppState>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Some(config) = state.jwt.hrm_config() {
        if let Some(path) = auth::hrm::resolve_workspace_alias_path(request.uri().path(), config) {
            if let Some(uri) = replace_uri_path(request.uri(), &path) {
                *request.uri_mut() = uri;
            }
        }
    }

    next.run(request).await
}

/// Thay path của URI, giữ nguyên query. Trả `None` nếu ghép lại không ra URI hợp lệ —
/// khi đó request đi tiếp với URI gốc thay vì bị từ chối bởi một lỗi nội bộ.
fn replace_uri_path(uri: &axum::http::Uri, path: &str) -> Option<axum::http::Uri> {
    let path_and_query = match uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_string(),
    };
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(path_and_query.parse().ok()?);
    axum::http::Uri::from_parts(parts).ok()
}

pub fn app_router(state: AppState) -> Router {
    let hrm_alias_layer =
        axum::middleware::from_fn_with_state(state.clone(), resolve_hrm_workspace_alias);

    // `Router::layer` chạy **sau** khi axum đã match route và bóc `Path<Uuid>`, nên
    // viết lại URI ở đó là quá muộn — alias đã fail parse từ trước. Router ngoài này
    // không có route nào, chỉ có fallback_service, nên request luôn đi qua middleware
    // rồi mới tới bước routing thật bên trong.
    Router::new()
        .fallback_service(routed_app(state))
        .layer(hrm_alias_layer)
}

fn routed_app(state: AppState) -> Router {
    let cors = cors_layer();
    let rate_limit_layer = axum::middleware::from_fn(rate_limit::enforce_rate_limits);
    telemetry::init_metrics_recorder();

    Router::new()
        .merge(api_docs::router(api_docs::enabled_from_env()))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/users/me", get(get_current_user))
        .route("/users/sync", post(sync_current_user))
        .route("/me/tenants", get(list_my_tenants))
        .route("/tenants", get(list_tenants).post(create_tenant))
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
            get(get_document_status).delete(delete_document),
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
            "/workspaces/{workspace_id}/documents/{document_id}/shares",
            get(get_document_shares),
        )
        .route(
            "/workspaces/{workspace_id}/documents/{document_id}/permissions",
            axum::routing::put(put_document_permissions),
        )
        .route(
            "/workspaces/{workspace_id}/documents/{document_id}/preview",
            get(get_document_preview),
        )
        .route(
            "/workspaces/{workspace_id}/chunks/{chunk_id}",
            get(get_document_chunk),
        )
        .route(
            "/workspaces/{workspace_id}/citations/resolve",
            post(resolve_workspace_citations),
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
        .fallback(|| async { crate::api_error::not_found().await })
        .method_not_allowed_fallback(|| async { crate::api_error::method_not_allowed().await })
        .layer(axum::middleware::from_fn(
            telemetry::http_metrics_middleware,
        ))
        .layer(rate_limit_layer)
        .layer(axum::middleware::from_fn(
            api_error::normalize_error_responses,
        ))
        .layer(DefaultBodyLimit::max(
            document_format::document_max_upload_bytes(),
        ))
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
