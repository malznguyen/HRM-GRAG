mod support;

use std::sync::{Arc, OnceLock};

use axum::{Json, Router, routing::post};
use reqwest::Client;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{Mutex, Semaphore};

use gmrag_api::auth::authz::AuthzClient;
use gmrag_api::retrieval::{RetrievalClient, RetrievalConfig};
use gmrag_api::state::AppState;
use gmrag_api::storage::{StorageClient, StorageConfig};

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();
static READY_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn ready_test_lock() -> &'static Mutex<()> {
    READY_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
async fn ready_returns_200_when_required_dependencies_are_healthy() {
    let _guard = ready_test_lock().lock().await;
    init_test_env();
    unsafe {
        std::env::set_var("APP_RUNTIME_ROLE", "api");
    }

    let pool = setup_pool().await;
    let openfga_url = spawn_openfga_stub().await;
    let authz_client = AuthzClient::new(openfga_url, "test-store".to_string(), None);
    let state = setup_state(pool, authz_client).await;
    let addr = spawn_api(state).await;

    let response = Client::new()
        .get(format!("{addr}/ready"))
        .send()
        .await
        .expect("/ready request must succeed");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("/ready payload as JSON");
    assert_eq!(payload["status"], "ready");
    assert_eq!(payload["role"], "api");
    assert_eq!(dependency_health(&payload, "postgres"), Some(true));
    assert_eq!(dependency_health(&payload, "openfga"), Some(true));
    assert!(
        payload["failed_dependencies"]
            .as_array()
            .expect("failed_dependencies array")
            .is_empty()
    );
}

#[tokio::test]
async fn ready_returns_503_when_postgres_is_down() {
    let _guard = ready_test_lock().lock().await;
    init_test_env();
    unsafe {
        std::env::set_var("APP_RUNTIME_ROLE", "api");
    }

    let pool = setup_pool().await;
    pool.close().await;

    let openfga_url = spawn_openfga_stub().await;
    let authz_client = AuthzClient::new(openfga_url, "test-store".to_string(), None);
    let state = setup_state(pool, authz_client).await;
    let addr = spawn_api(state).await;

    let response = Client::new()
        .get(format!("{addr}/ready"))
        .send()
        .await
        .expect("/ready request must succeed");

    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("/ready payload as JSON");
    assert_eq!(payload["error"]["code"], "SERVICE_UNAVAILABLE");
    let details = &payload["error"]["details"];
    assert_eq!(details["status"], "not_ready");
    assert_eq!(dependency_health(details, "postgres"), Some(false));
    assert_eq!(dependency_health(details, "openfga"), Some(true));
    assert!(failed_dependencies(details).contains(&"postgres"));
}

#[tokio::test]
async fn ready_returns_503_when_openfga_is_down() {
    let _guard = ready_test_lock().lock().await;
    init_test_env();
    unsafe {
        std::env::set_var("APP_RUNTIME_ROLE", "api");
    }

    let pool = setup_pool().await;
    let authz_client = AuthzClient::new(
        "http://127.0.0.1:1".to_string(),
        "test-store".to_string(),
        None,
    );
    let state = setup_state(pool, authz_client).await;
    let addr = spawn_api(state).await;

    let response = Client::new()
        .get(format!("{addr}/ready"))
        .send()
        .await
        .expect("/ready request must succeed");

    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("/ready payload as JSON");
    assert_eq!(payload["error"]["code"], "SERVICE_UNAVAILABLE");
    let details = &payload["error"]["details"];
    assert_eq!(details["status"], "not_ready");
    assert_eq!(dependency_health(details, "postgres"), Some(true));
    assert_eq!(dependency_health(details, "openfga"), Some(false));
    assert!(failed_dependencies(details).contains(&"openfga"));
}

async fn openfga_check_stub() -> Json<serde_json::Value> {
    Json(json!({ "allowed": false }))
}

async fn spawn_openfga_stub() -> String {
    let app = Router::new().route("/stores/{store_id}/check", post(openfga_check_stub));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind OpenFGA stub");
    let addr = listener.local_addr().expect("OpenFGA stub address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("OpenFGA stub server should stay healthy");
    });
    format!("http://{addr}")
}

fn init_test_env() {
    TEST_ENV_INIT.get_or_init(|| unsafe {
        std::env::set_var("APP_ENV", "test");
        std::env::set_var("TEST_BYPASS_JWT", "1");
        std::env::set_var("TEST_BYPASS_KEYCLOAK", "1");
        std::env::set_var("S3_ENDPOINT_URL", "http://localhost:9000");
        std::env::set_var("S3_REGION", "us-east-1");
        if std::env::var_os("S3_BUCKET").is_none() {
            std::env::set_var("S3_BUCKET", "gmrag-documents");
        }
        std::env::set_var("S3_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("S3_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("S3_FORCE_PATH_STYLE", "true");
        std::env::set_var("S3_PRESIGN_EXPIRY_SECS", "900");
    });
}

async fn setup_pool() -> sqlx::PgPool {
    dotenvy::dotenv().ok();

    let database_url = support::database_url().expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("connect postgres for readiness tests");

    match sqlx::migrate!("./migrations").run(&pool).await {
        Ok(_) | Err(sqlx::migrate::MigrateError::VersionMismatch(_)) => {}
        Err(err) => panic!("Failed to run migrations: {err}"),
    }

    pool
}

async fn setup_state(pool: sqlx::PgPool, authz_client: AuthzClient) -> AppState {
    let jwt = gmrag_api::auth::jwt::JwtValidator::from_env().expect("test bypass JWT validator");
    let keycloak_client =
        gmrag_api::auth::keycloak::KeycloakClient::from_env().expect("test bypass keycloak");
    let storage = setup_storage().await;

    let retrieval = RetrievalClient::from_config(RetrievalConfig {
        qdrant_url: "http://127.0.0.1:6333".to_string(),
        collection_name: "gmrag_document_chunks".to_string(),
        vector_size: gmrag_api::ingestion::embedding::DEFAULT_EMBEDDING_DIM,
        top_k: 5,
        api_key: None,
        delete_request_timeout_secs: 5,
        delete_worker_timeout_secs: 60,
    });

    AppState {
        pool,
        jwt,
        storage,
        retrieval,
        ingestion_limiter: Arc::new(Semaphore::new(1)),
        authz_client,
        keycloak_client,
    }
}

async fn setup_storage() -> StorageClient {
    let config = StorageConfig::from_env().expect("storage env must exist for readiness tests");
    StorageClient::from_config(config).await
}

async fn spawn_api(state: AppState) -> String {
    let app = gmrag_api::app_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test API listener");
    let addr = listener.local_addr().expect("test API address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test API server should stay healthy");
    });
    format!("http://{addr}")
}

fn dependency_health(payload: &serde_json::Value, name: &str) -> Option<bool> {
    payload["dependencies"]
        .as_array()?
        .iter()
        .find_map(|dependency| {
            (dependency["name"] == name).then(|| dependency["healthy"].as_bool())
        })
        .flatten()
}

fn failed_dependencies(payload: &serde_json::Value) -> Vec<&str> {
    payload["failed_dependencies"]
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}
