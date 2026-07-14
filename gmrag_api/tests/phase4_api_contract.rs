use std::sync::Arc;

use gmrag_api::{
    auth::{authz::AuthzClient, jwt::JwtValidator, keycloak::KeycloakClient},
    ingestion::embedding::EXPECTED_EMBEDDING_DIM,
    retrieval::{RetrievalClient, RetrievalConfig},
    state::AppState,
    storage::{StorageClient, StorageConfig},
};
use reqwest::{Client, Method, StatusCode};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Semaphore;

const PROTECTED_ROUTES: &[(&str, &str)] = &[
    ("GET", "/users/me"),
    ("POST", "/users/sync"),
    ("GET", "/me/tenants"),
    ("GET", "/tenants"),
    ("POST", "/tenants"),
    (
        "POST",
        "/tenants/00000000-0000-0000-0000-000000000001/owners",
    ),
    (
        "POST",
        "/tenants/00000000-0000-0000-0000-000000000001/workspaces",
    ),
    ("GET", "/workspaces"),
    ("DELETE", "/workspaces/00000000-0000-0000-0000-000000000001"),
    (
        "GET",
        "/workspaces/00000000-0000-0000-0000-000000000001/documents",
    ),
    (
        "POST",
        "/workspaces/00000000-0000-0000-0000-000000000001/documents/upload",
    ),
    (
        "DELETE",
        "/workspaces/00000000-0000-0000-0000-000000000001/documents/00000000-0000-0000-0000-000000000002",
    ),
    (
        "POST",
        "/workspaces/00000000-0000-0000-0000-000000000001/documents/00000000-0000-0000-0000-000000000002/retry",
    ),
    (
        "PATCH",
        "/workspaces/00000000-0000-0000-0000-000000000001/documents/00000000-0000-0000-0000-000000000002/access-mode",
    ),
    (
        "POST",
        "/workspaces/00000000-0000-0000-0000-000000000001/documents/00000000-0000-0000-0000-000000000002/shares/user-1",
    ),
    (
        "DELETE",
        "/workspaces/00000000-0000-0000-0000-000000000001/documents/00000000-0000-0000-0000-000000000002/shares/user-1",
    ),
    (
        "GET",
        "/workspaces/00000000-0000-0000-0000-000000000001/documents/00000000-0000-0000-0000-000000000002/shares",
    ),
    (
        "PUT",
        "/workspaces/00000000-0000-0000-0000-000000000001/documents/00000000-0000-0000-0000-000000000002/permissions",
    ),
    (
        "GET",
        "/workspaces/00000000-0000-0000-0000-000000000001/documents/00000000-0000-0000-0000-000000000002/preview",
    ),
    (
        "GET",
        "/workspaces/00000000-0000-0000-0000-000000000001/chunks/00000000-0000-0000-0000-000000000002",
    ),
    (
        "POST",
        "/workspaces/00000000-0000-0000-0000-000000000001/chat",
    ),
    (
        "GET",
        "/workspaces/00000000-0000-0000-0000-000000000001/chat/history?session_id=00000000-0000-0000-0000-000000000002",
    ),
    (
        "GET",
        "/workspaces/00000000-0000-0000-0000-000000000001/chat/sessions",
    ),
    (
        "GET",
        "/workspaces/00000000-0000-0000-0000-000000000001/chat/sessions/00000000-0000-0000-0000-000000000002/messages",
    ),
    (
        "DELETE",
        "/workspaces/00000000-0000-0000-0000-000000000001/chat/sessions/00000000-0000-0000-0000-000000000002",
    ),
    (
        "GET",
        "/workspaces/00000000-0000-0000-0000-000000000001/graph",
    ),
    (
        "GET",
        "/workspaces/00000000-0000-0000-0000-000000000001/members",
    ),
    (
        "POST",
        "/workspaces/00000000-0000-0000-0000-000000000001/members",
    ),
    (
        "DELETE",
        "/workspaces/00000000-0000-0000-0000-000000000001/members/user-1",
    ),
    (
        "PATCH",
        "/workspaces/00000000-0000-0000-0000-000000000001/members/user-1",
    ),
];

const PRIMARY_ID: &str = "00000000-0000-0000-0000-000000000001";
const SECONDARY_ID: &str = "00000000-0000-0000-0000-000000000002";

#[tokio::test]
async fn route_inventory_is_registered_and_protected_with_json_errors() {
    init_test_env();
    let address = spawn_api().await;
    let client = Client::new();

    for (method, path) in PROTECTED_ROUTES {
        let response = client
            .request(
                Method::from_bytes(method.as_bytes()).expect("valid method"),
                format!("{address}{path}"),
            )
            .send()
            .await
            .expect("route probe must respond");
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {path}"
        );
        assert_json_error(response, "UNAUTHORIZED").await;
    }
}

#[tokio::test]
async fn fallback_and_method_mismatch_are_json_error_envelopes() {
    init_test_env();
    let address = spawn_api().await;
    let client = Client::new();

    let missing = client
        .get(format!("{address}/removed-clerk-webhook"))
        .send()
        .await
        .expect("unknown route must respond");
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_json_error(missing, "RESOURCE_NOT_FOUND").await;

    let mismatch = client
        .post(format!("{address}/health"))
        .send()
        .await
        .expect("method mismatch must respond");
    assert_eq!(mismatch.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_json_error(mismatch, "METHOD_NOT_ALLOWED").await;
}

#[tokio::test]
async fn public_route_inventory_has_expected_protocol_contracts() {
    init_test_env();
    let address = spawn_api().await;
    let client = Client::new();

    let health = client
        .get(format!("{address}/health"))
        .send()
        .await
        .expect("health route");
    assert_eq!(health.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_json_error(health, "INTERNAL_ERROR").await;

    let ready = client
        .get(format!("{address}/ready"))
        .send()
        .await
        .expect("ready route");
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let ready_payload: serde_json::Value = ready.json().await.expect("ready JSON envelope");
    assert_eq!(ready_payload["error"]["code"], "SERVICE_UNAVAILABLE");
    assert!(ready_payload["error"]["details"]["failed_dependencies"].is_array());

    let metrics = client
        .get(format!("{address}/metrics"))
        .send()
        .await
        .expect("metrics route");
    assert_eq!(metrics.status(), StatusCode::OK);
    assert!(
        metrics
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/plain"))
    );
}

#[test]
fn api_contract_documents_the_registered_surface() {
    let contract = include_str!("../../docs/CURRENT_API_CONTRACT.md");
    for (_, path) in PROTECTED_ROUTES {
        let documented_path = documented_path(path);
        assert!(
            contract.contains(&documented_path),
            "CURRENT_API_CONTRACT.md is missing {documented_path}"
        );
    }
    for path in ["/health", "/ready", "/metrics"] {
        assert!(
            contract.contains(path),
            "CURRENT_API_CONTRACT.md is missing {path}"
        );
    }
    assert!(!contract.contains("/api/webhooks/clerk"));
}

async fn assert_json_error(response: reqwest::Response, code: &str) {
    assert!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    );
    let body: serde_json::Value = response.json().await.expect("error body must be JSON");
    assert_eq!(body["error"]["code"], code);
    assert!(body["error"]["message"].is_string());
}

fn documented_path(probe_path: &str) -> String {
    let path = probe_path.split('?').next().expect("path before query");
    if path.starts_with("/tenants/") {
        return path.replace(PRIMARY_ID, "{tenant_id}");
    }
    path.replace(PRIMARY_ID, "{workspace_id}")
        .replace(SECONDARY_ID, "{document_id}")
        .replace("/chunks/{document_id}", "/chunks/{chunk_id}")
        .replace(
            "/chat/sessions/{document_id}",
            "/chat/sessions/{session_id}",
        )
        .replace("/shares/user-1", "/shares/{user_id}")
        .replace("/members/user-1", "/members/{member_id}")
}

fn init_test_env() {
    unsafe {
        std::env::set_var("APP_ENV", "test");
        std::env::set_var("TEST_BYPASS_JWT", "1");
        std::env::set_var("TEST_BYPASS_KEYCLOAK", "1");
        std::env::set_var("S3_ENDPOINT_URL", "http://127.0.0.1:9000");
        std::env::set_var("S3_REGION", "us-east-1");
        if std::env::var_os("S3_BUCKET").is_none() {
            std::env::set_var("S3_BUCKET", "gmrag-documents");
        }
        std::env::set_var("S3_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("S3_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("S3_FORCE_PATH_STYLE", "true");
        std::env::set_var("S3_PRESIGN_EXPIRY_SECS", "900");
    }
}

async fn spawn_api() -> String {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:5432/unused")
        .expect("lazy pool");
    let storage_config = StorageConfig::from_env().expect("storage config");
    let state = AppState {
        pool,
        jwt: JwtValidator::from_env().expect("bypass JWT"),
        storage: StorageClient::from_config(storage_config).await,
        retrieval: RetrievalClient::from_config(RetrievalConfig {
            qdrant_url: "http://127.0.0.1:6333".to_string(),
            collection_name: "gmrag_document_chunks".to_string(),
            vector_size: EXPECTED_EMBEDDING_DIM,
            top_k: 5,
            api_key: None,
            delete_request_timeout_secs: 5,
            delete_worker_timeout_secs: 60,
        }),
        ingestion_limiter: Arc::new(Semaphore::new(1)),
        authz_client: AuthzClient::new(
            "http://127.0.0.1:8081".to_string(),
            "unused".to_string(),
            None,
        ),
        keycloak_client: KeycloakClient::from_env().expect("bypass Keycloak"),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(listener, gmrag_api::app_router(state))
            .await
            .expect("contract test server");
    });
    format!("http://{address}")
}
