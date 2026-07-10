use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::retrieval::RetrievalClient;
use gmrag_api::retrieval::outbox::{QdrantOutboxProcessorConfig, process_qdrant_outbox};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    let retrieval = RetrievalClient::from_env().expect("Failed to load retrieval configuration");
    let config = QdrantOutboxProcessorConfig::from_env();

    let _ = insert_audit_event(
        &pool,
        AuditEventRecord::new(AuditEventType::QdrantOutboxProcessingStarted).with_metadata(
            json!({
                "batch_size": config.batch_size,
                "max_retries": config.max_retries,
                "base_backoff_secs": config.backoff.base_backoff_secs,
                "max_backoff_secs": config.backoff.max_backoff_secs,
                "claim_lease_secs": config.backoff.claim_lease_secs
            }),
        ),
    )
    .await;

    match process_qdrant_outbox(&pool, &retrieval, config).await {
        Ok(result) => {
            println!(
                "Qdrant outbox complete: batches={}, fetched_rows={}, processed_rows={}, failed_rows={}, dead_rows={}, skipped_max_retry_rows={}",
                result.batches,
                result.fetched_rows,
                result.processed_rows,
                result.failed_rows,
                result.dead_rows,
                result.skipped_max_retry_rows
            );

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(AuditEventType::QdrantOutboxProcessingCompleted)
                    .with_metadata(json!({
                        "batch_size": config.batch_size,
                        "max_retries": config.max_retries,
                        "base_backoff_secs": config.backoff.base_backoff_secs,
                        "max_backoff_secs": config.backoff.max_backoff_secs,
                        "claim_lease_secs": config.backoff.claim_lease_secs,
                        "batches": result.batches,
                        "fetched_rows": result.fetched_rows,
                        "processed_rows": result.processed_rows,
                        "failed_rows": result.failed_rows,
                        "dead_rows": result.dead_rows,
                        "skipped_max_retry_rows": result.skipped_max_retry_rows
                    })),
            )
            .await;
        }
        Err(err) => {
            eprintln!("Qdrant outbox processing failed: {err}");

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(AuditEventType::QdrantOutboxProcessingFailed).with_metadata(
                    json!({
                        "batch_size": config.batch_size,
                        "max_retries": config.max_retries,
                        "base_backoff_secs": config.backoff.base_backoff_secs,
                        "max_backoff_secs": config.backoff.max_backoff_secs,
                        "claim_lease_secs": config.backoff.claim_lease_secs,
                        "error_code": sanitize_error_code(&err.to_string())
                    }),
                ),
            )
            .await;

            std::process::exit(1);
        }
    }
}
