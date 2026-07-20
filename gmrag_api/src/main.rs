use gmrag_api::auth;
use gmrag_api::retrieval::RetrievalClient;
use gmrag_api::shutdown::{DrainOutcome, Shutdown, drain_with_deadline, shutdown_signal};
use gmrag_api::state::AppState;
use gmrag_api::storage::{StorageClient, StorageConfig};
use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::warn;

const API_DRAIN_TIMEOUT_SECS: u64 = 20;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,lopdf=error")),
        )
        .init();

    let shutdown = Shutdown::install().expect("Failed to install shutdown signal handler");

    if let Err(error) = auth::validate_test_bypass_configuration() {
        eprintln!("Invalid test bypass configuration: {error}");
        std::process::exit(1);
    }

    if let Err(error) = auth::validate_secret_configuration() {
        eprintln!("Invalid secret configuration: {error}");
        std::process::exit(1);
    }

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

    let jwt = auth::jwt::JwtValidator::from_env()
        .expect("JWT_ISSUER, JWT_AUDIENCE, and JWT_JWKS_URL must be configured for JWT validation");

    let authz_client =
        auth::authz::AuthzClient::from_env().expect("Failed to initialize AuthzClient from env");

    let keycloak_client = auth::keycloak::KeycloakClient::from_env()
        .expect("Failed to initialize KeycloakClient from env");

    let storage_config =
        StorageConfig::from_env().expect("Failed to load storage configuration from env");
    let storage = StorageClient::from_config(storage_config).await;
    let retrieval =
        RetrievalClient::from_env().expect("Failed to load retrieval configuration from env");

    // ADR-21: ingestion + chat query share one Ollama embed model (default AITeamVN/Vietnamese_Embedding).
    gmrag_api::ingestion::embedding::log_embedding_config_on_startup();

    let state = AppState {
        pool,
        jwt,
        storage,
        retrieval,
        ingestion_limiter: Arc::new(Semaphore::new(AppState::ingestion_limit_from_env())),
        authz_client,
        keycloak_client,
    };

    let app = gmrag_api::app_router(state);

    let addr = std::env::var("API_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8083".to_string())
        .parse::<SocketAddr>()
        .expect("API_BIND_ADDR must be a valid socket address");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind TCP listener");

    println!("gmrag_api listening on {addr}");

    let server_result = {
        let graceful_shutdown_listener = shutdown.clone();
        let graceful_shutdown = async move {
            shutdown_signal(graceful_shutdown_listener).await;
        };
        let server = axum::serve(listener, app).with_graceful_shutdown(graceful_shutdown);
        let server = async move { server.await };
        tokio::pin!(server);

        tokio::select! {
            biased;
            signal = shutdown_signal(shutdown.clone()) => {
                warn!(signal = signal.as_str(), "API shutdown signal received");
                warn!(
                    drain_timeout_secs = API_DRAIN_TIMEOUT_SECS,
                    "API connection draining started"
                );

                match drain_with_deadline(
                    &mut server,
                    shutdown,
                    std::time::Duration::from_secs(API_DRAIN_TIMEOUT_SECS),
                )
                .await
                {
                    DrainOutcome::Completed(result) => Some(result),
                    DrainOutcome::DeadlineElapsed => {
                        warn!(
                            drain_timeout_secs = API_DRAIN_TIMEOUT_SECS,
                            "API drain deadline elapsed; forcing remaining connections closed"
                        );
                        None
                    }
                    DrainOutcome::SecondSignal(signal) => {
                        warn!(
                            signal = signal.as_str(),
                            "Second shutdown signal received; forcing API connections closed"
                        );
                        None
                    }
                }
            }
            result = &mut server => Some(result),
        }
    };

    if let Some(result) = server_result {
        result.expect("Server failed");
    }
    warn!("gmrag_api stopped");
}
