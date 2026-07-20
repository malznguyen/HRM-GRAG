use std::time::Duration;

use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::retrieval::RetrievalClient;
use gmrag_api::retrieval::outbox::{
    QDRANT_OUTBOX_EXIT_FAILURE, QdrantOutboxProcessorConfig, QdrantOutboxRunMode,
    process_qdrant_outbox_until, qdrant_outbox_exit_code,
};
use gmrag_api::shutdown::{Shutdown, drain_or_second_signal, shutdown_signal};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use tokio::time::sleep;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let shutdown = Shutdown::install().expect("Failed to install shutdown signal handler");

    let cli_args: Vec<String> = std::env::args().skip(1).collect();
    if cli_args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        std::process::exit(0);
    }

    let run_mode = QdrantOutboxRunMode::from_args_and_env(&cli_args);
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

    info!(
        loop_mode = run_mode.loop_mode,
        interval_secs = run_mode.interval_secs,
        batch_size = config.batch_size,
        max_retries = config.max_retries,
        claim_lease_secs = config.backoff.claim_lease_secs,
        "Starting process-qdrant-outbox"
    );

    if run_mode.loop_mode {
        run_loop(&pool, &retrieval, config, run_mode, shutdown).await;
    } else {
        let code = run_once(&pool, &retrieval, config, &shutdown).await;
        std::process::exit(code);
    }
}

async fn run_loop(
    pool: &sqlx::PgPool,
    retrieval: &RetrievalClient,
    config: QdrantOutboxProcessorConfig,
    run_mode: QdrantOutboxRunMode,
    shutdown: Shutdown,
) {
    let interval = Duration::from_secs(run_mode.interval_secs);
    loop {
        if shutdown.received() {
            let signal = shutdown_signal(shutdown.clone()).await;
            log_shutdown_start(signal);
            break;
        }

        let run = run_once(pool, retrieval, config, &shutdown);
        tokio::pin!(run);
        let mut draining = false;
        let code = tokio::select! {
            biased;
            signal = shutdown_signal(shutdown.clone()) => {
                log_shutdown_start(signal);
                draining = true;
                match drain_or_second_signal(&mut run, shutdown.clone()).await {
                    Ok(code) => code,
                    Err(signal) => {
                        warn!(
                            signal = signal.as_str(),
                            "Second shutdown signal received; stopping process-qdrant-outbox immediately"
                        );
                        warn!("process-qdrant-outbox stopped");
                        return;
                    }
                }
            }
            code = &mut run => code,
        };

        if code != 0 {
            // Thoát non-zero để Compose restart — không nuốt lỗi trong loop.
            error!(
                exit_code = code,
                "Qdrant outbox drain failed; exiting for supervisor restart"
            );
            std::process::exit(code);
        }

        if draining {
            break;
        }

        tokio::select! {
            signal = shutdown_signal(shutdown.clone()) => {
                log_shutdown_start(signal);
                break;
            }
            _ = sleep(interval) => {}
        }
    }

    warn!("process-qdrant-outbox stopped");
}

async fn run_once(
    pool: &sqlx::PgPool,
    retrieval: &RetrievalClient,
    config: QdrantOutboxProcessorConfig,
    shutdown: &Shutdown,
) -> i32 {
    let _ = insert_audit_event(
        pool,
        AuditEventRecord::new(AuditEventType::QdrantOutboxProcessingStarted).with_metadata(json!({
            "batch_size": config.batch_size,
            "max_retries": config.max_retries,
            "base_backoff_secs": config.backoff.base_backoff_secs,
            "max_backoff_secs": config.backoff.max_backoff_secs,
            "claim_lease_secs": config.backoff.claim_lease_secs
        })),
    )
    .await;

    let result = process_qdrant_outbox_until(pool, retrieval, config, || shutdown.received()).await;
    let exit_code = qdrant_outbox_exit_code(&result);

    match result {
        Ok(stats) => {
            println!(
                "Qdrant outbox complete: batches={}, fetched_rows={}, processed_rows={}, failed_rows={}, dead_rows={}, skipped_max_retry_rows={}",
                stats.batches,
                stats.fetched_rows,
                stats.processed_rows,
                stats.failed_rows,
                stats.dead_rows,
                stats.skipped_max_retry_rows
            );

            let _ = insert_audit_event(
                pool,
                AuditEventRecord::new(AuditEventType::QdrantOutboxProcessingCompleted)
                    .with_metadata(json!({
                        "batch_size": config.batch_size,
                        "max_retries": config.max_retries,
                        "base_backoff_secs": config.backoff.base_backoff_secs,
                        "max_backoff_secs": config.backoff.max_backoff_secs,
                        "claim_lease_secs": config.backoff.claim_lease_secs,
                        "batches": stats.batches,
                        "fetched_rows": stats.fetched_rows,
                        "processed_rows": stats.processed_rows,
                        "failed_rows": stats.failed_rows,
                        "dead_rows": stats.dead_rows,
                        "skipped_max_retry_rows": stats.skipped_max_retry_rows
                    })),
            )
            .await;
        }
        Err(err) => {
            eprintln!("Qdrant outbox processing failed: {err}");

            let _ = insert_audit_event(
                pool,
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

            debug_assert_eq!(exit_code, QDRANT_OUTBOX_EXIT_FAILURE);
        }
    }

    exit_code
}

fn log_shutdown_start(signal: gmrag_api::shutdown::ShutdownSignal) {
    warn!(
        signal = signal.as_str(),
        "Qdrant outbox shutdown signal received"
    );
    warn!("Qdrant outbox draining started");
}

fn print_usage() {
    println!(
        "Usage: process-qdrant-outbox [--once|--loop] [--interval-secs <n>]\n\n\
         Drains qdrant_outbox via the shared process_qdrant_outbox library\n\
         (FOR UPDATE SKIP LOCKED + claim lease — multi-replica safe).\n\
         Default is --once (manual/debug). --loop sleeps between drains;\n\
         hard failures exit non-zero so Docker Compose can restart the process.\n\n\
         Env: DATABASE_URL, QDRANT_URL, QDRANT_COLLECTION, QDRANT_API_KEY,\n\
              QDRANT_OUTBOX_BATCH_SIZE, QDRANT_OUTBOX_MAX_RETRIES,\n\
              QDRANT_OUTBOX_BASE_BACKOFF_SECS, QDRANT_OUTBOX_MAX_BACKOFF_SECS,\n\
              QDRANT_OUTBOX_CLAIM_LEASE_SECS, QDRANT_OUTBOX_POLL_INTERVAL_SECS,\n\
              QDRANT_DELETE_WORKER_TIMEOUT_SECS, RUST_LOG"
    );
}
