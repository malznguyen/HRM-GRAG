use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::auth::authz::AuthzClient;
use crate::auth::jwt::JwtValidator;
use crate::auth::keycloak::KeycloakClient;
use crate::retrieval::RetrievalClient;
use crate::storage::StorageClient;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub jwt: Arc<JwtValidator>,
    pub storage: StorageClient,
    pub retrieval: RetrievalClient,
    pub ingestion_limiter: Arc<Semaphore>,
    pub authz_client: AuthzClient,
    pub keycloak_client: KeycloakClient,
}

impl AppState {
    pub fn ingestion_limit_from_env() -> usize {
        std::env::var("GMRAG_INGESTION_DOCUMENT_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(2)
            .max(1)
    }
}
