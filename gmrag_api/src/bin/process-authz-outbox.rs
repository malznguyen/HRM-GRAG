use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::auth::authz::AuthzClient;
use gmrag_api::auth::outbox::{AuthzOutboxProcessorConfig, process_authz_outbox};
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

    let authz_client = AuthzClient::from_env().expect("Failed to initialize AuthzClient");
    let config = AuthzOutboxProcessorConfig::from_env();

    let _ = insert_audit_event(
        &pool,
        AuditEventRecord::new(AuditEventType::AuthzOutboxProcessingStarted).with_metadata(json!({
            "batch_size": config.batch_size,
            "max_retries": config.max_retries
        })),
    )
    .await;

    match process_authz_outbox(&pool, &authz_client, config).await {
        Ok(result) => {
            println!(
                "Authz outbox complete: batches={}, fetched_rows={}, processed_rows={}, failed_rows={}, skipped_max_retry_rows={}",
                result.batches,
                result.fetched_rows,
                result.processed_rows,
                result.failed_rows,
                result.skipped_max_retry_rows
            );

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(AuditEventType::AuthzOutboxProcessingCompleted)
                    .with_metadata(json!({
                        "batch_size": config.batch_size,
                        "max_retries": config.max_retries,
                        "batches": result.batches,
                        "fetched_rows": result.fetched_rows,
                        "processed_rows": result.processed_rows,
                        "failed_rows": result.failed_rows,
                        "skipped_max_retry_rows": result.skipped_max_retry_rows
                    })),
            )
            .await;
        }
        Err(err) => {
            eprintln!("Authz outbox processing failed: {err}");

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(AuditEventType::AuthzOutboxProcessingFailed).with_metadata(
                    json!({
                        "batch_size": config.batch_size,
                        "max_retries": config.max_retries,
                        "error_code": sanitize_error_code(&err.to_string())
                    }),
                ),
            )
            .await;

            std::process::exit(1);
        }
    }
}
