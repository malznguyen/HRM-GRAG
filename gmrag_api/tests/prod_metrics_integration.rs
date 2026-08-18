mod support;

use std::sync::OnceLock;
use std::time::Duration;

use reqwest::Client;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Mutex;

use gmrag_api::auth::authz::AuthzClient;
use gmrag_api::retrieval::{RetrievalClient, RetrievalConfig};
use gmrag_api::state::AppState;
use gmrag_api::storage::{StorageClient, StorageConfig};

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();
static METRICS_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn metrics_test_lock() -> &'static Mutex<()> {
    METRICS_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
async fn metrics_endpoint_exposes_http_model_and_operational_metrics() {
    let _guard = metrics_test_lock().lock().await;
    init_test_env();

    let pool = setup_pool().await;
    let state = setup_state(pool).await;
    let addr = spawn_api(state).await;
    let client = Client::new();

    let health = client
        .get(format!("{addr}/health"))
        .send()
        .await
        .expect("health request must succeed");
    assert_eq!(health.status(), reqwest::StatusCode::OK);

    gmrag_api::telemetry::record_model_latency(
        "deepseek",
        "chat_stream_request",
        Duration::from_millis(25),
    );

    let metrics = client
        .get(format!("{addr}/metrics"))
        .send()
        .await
        .expect("metrics request must succeed");
    assert_eq!(metrics.status(), reqwest::StatusCode::OK);
    let body = metrics.text().await.expect("metrics body as text");

    assert!(
        body.contains("gmrag_http_requests_total"),
        "expected HTTP counter in payload"
    );
    assert!(
        body.contains("route=\"/health\""),
        "expected /health route label in HTTP metric"
    );
    assert!(
        body.contains("gmrag_model_latency_seconds"),
        "expected model latency histogram in payload"
    );
    assert!(
        body.contains("gmrag_ingestion_failure_count"),
        "expected ingestion failure metric in payload"
    );
    assert!(
        body.contains("gmrag_outbox_depth"),
        "expected outbox depth metrics in payload"
    );
}

fn init_test_env() {
    TEST_ENV_INIT.get_or_init(|| unsafe {
        std::env::set_var("APP_ENV", "test");
        std::env::set_var("TEST_BYPASS_JWT", "1");
        std::env::set_var("TEST_BYPASS_KEYCLOAK", "1");
        std::env::set_var("APP_RUNTIME_ROLE", "api");
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
        .expect("connect postgres for metrics tests");

    match sqlx::migrate!("./migrations").run(&pool).await {
        Ok(_) | Err(sqlx::migrate::MigrateError::VersionMismatch(_)) => {}
        Err(err) => panic!("Failed to run migrations: {err}"),
    }

    pool
}

async fn setup_state(pool: sqlx::PgPool) -> AppState {
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
        chat_admission: Default::default(),
        authz_client: AuthzClient::new(
            "http://127.0.0.1:8081".to_string(),
            "unused-test-store".to_string(),
            None,
        ),
        keycloak_client,
    }
}

async fn setup_storage() -> StorageClient {
    let config = StorageConfig::from_env().expect("storage env must exist for metrics tests");
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
