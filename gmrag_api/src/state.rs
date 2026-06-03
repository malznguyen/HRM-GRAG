use sqlx::PgPool;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::auth::jwt::JwtValidator;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub jwt: Arc<JwtValidator>,
    pub upload_dir: PathBuf,
    pub ingestion_limiter: Arc<Semaphore>,
}

impl AppState {
    pub fn upload_dir_from_env() -> PathBuf {
        std::env::var("UPLOAD_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/uploads"))
    }

    pub fn ingestion_limit_from_env() -> usize {
        std::env::var("GMRAG_INGESTION_DOCUMENT_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(2)
            .max(1)
    }
}
