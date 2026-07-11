use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::storage::outbox::{StorageOutboxProcessorConfig, process_storage_outbox};
use gmrag_api::storage::{StorageClient, StorageConfig};
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

    let storage_config = StorageConfig::from_env().expect("Failed to load storage config");
    let storage = StorageClient::from_config(storage_config).await;
    let config = StorageOutboxProcessorConfig::from_env();

    let _ = insert_audit_event(
        &pool,
        AuditEventRecord::new(AuditEventType::StorageOutboxProcessingStarted).with_metadata(
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

    match process_storage_outbox(&pool, &storage, config).await {
        Ok(result) => {
            println!(
                "Storage outbox complete: batches={}, fetched_rows={}, processed_rows={}, failed_rows={}, dead_rows={}, skipped_max_retry_rows={}",
                result.batches,
                result.fetched_rows,
                result.processed_rows,
                result.failed_rows,
                result.dead_rows,
                result.skipped_max_retry_rows
            );

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(AuditEventType::StorageOutboxProcessingCompleted)
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
            eprintln!("Storage outbox processing failed: {err}");

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(AuditEventType::StorageOutboxProcessingFailed).with_metadata(
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
