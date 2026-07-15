mod support;

use std::sync::{Arc, OnceLock};

use reqwest::Client;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{Mutex, Semaphore};

use gmrag_api::auth::authz::AuthzClient;
use gmrag_api::retrieval::{RetrievalClient, RetrievalConfig};
use gmrag_api::state::AppState;
use gmrag_api::storage::{StorageClient, StorageConfig};

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();
static RATE_LIMIT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn rate_limit_test_lock() -> &'static Mutex<()> {
    RATE_LIMIT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
async fn auth_sensitive_burst_returns_429_with_rate_limited_envelope() {
    let _guard = rate_limit_test_lock().lock().await;
    init_test_env();

    let pool = setup_pool().await;
    let state = setup_state(pool).await;
    let addr = spawn_api(state).await;
    let client = Client::new();

    let under_limit_response = client
        .post(format!("{addr}/users/sync"))
        .bearer_auth("rate-limit-under-threshold")
        .send()
        .await
        .expect("under-limit request must succeed");
    assert_ne!(
        under_limit_response.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS
    );

    for _ in 0..2 {
        let response = client
            .post(format!("{addr}/users/sync"))
            .bearer_auth("rate-limit-burst-user")
            .send()
            .await
            .expect("request within configured burst window must return a response");
        assert_ne!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    }

    let blocked = client
        .post(format!("{addr}/users/sync"))
        .bearer_auth("rate-limit-burst-user")
        .send()
        .await
        .expect("burst request must return a response");
    assert_eq!(blocked.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    let payload: serde_json::Value = blocked
        .json()
        .await
        .expect("429 response should be JSON envelope");
    assert_eq!(payload["error"]["code"], "RATE_LIMITED");
    assert_eq!(
        payload["error"]["message"],
        "Too many requests. Please retry later."
    );
}

fn init_test_env() {
    TEST_ENV_INIT.get_or_init(|| unsafe {
        std::env::set_var("APP_ENV", "test");
        std::env::set_var("TEST_BYPASS_JWT", "1");
        std::env::set_var("TEST_BYPASS_KEYCLOAK", "1");
        std::env::set_var("APP_RUNTIME_ROLE", "api");
        std::env::set_var("RATE_LIMIT_WINDOW_SECS", "60");
        std::env::set_var("RATE_LIMIT_AUTH_PER_WINDOW", "2");
        std::env::set_var("RATE_LIMIT_CHAT_PER_WINDOW", "30");
        std::env::set_var("RATE_LIMIT_UPLOAD_PER_WINDOW", "10");
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
        .expect("connect postgres for rate-limit tests");

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
        ingestion_limiter: Arc::new(Semaphore::new(1)),
        authz_client: AuthzClient::new(
            "http://127.0.0.1:8081".to_string(),
            "unused-test-store".to_string(),
            None,
        ),
        keycloak_client,
    }
}

async fn setup_storage() -> StorageClient {
    let config = StorageConfig::from_env().expect("storage env must exist for rate-limit tests");
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
