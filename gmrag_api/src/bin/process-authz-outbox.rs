use std::time::Duration;

use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::auth::authz::AuthzClient;
use gmrag_api::auth::outbox::{
    AUTHZ_OUTBOX_EXIT_FAILURE, AuthzOutboxProcessorConfig, AuthzOutboxRunMode,
    authz_outbox_exit_code, process_authz_outbox,
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

    let run_mode = AuthzOutboxRunMode::from_args_and_env(&cli_args);
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");

    let authz_client = AuthzClient::from_env().expect("Failed to initialize AuthzClient");
    let config = AuthzOutboxProcessorConfig::from_env();

    info!(
        loop_mode = run_mode.loop_mode,
        interval_secs = run_mode.interval_secs,
        batch_size = config.batch_size,
        max_retries = config.max_retries,
        "Starting process-authz-outbox"
    );

    if run_mode.loop_mode {
        run_loop(&pool, &authz_client, config, run_mode, shutdown).await;
    } else {
        let code = run_once(&pool, &authz_client, config).await;
        std::process::exit(code);
    }
}

async fn run_loop(
    pool: &sqlx::PgPool,
    authz_client: &AuthzClient,
    config: AuthzOutboxProcessorConfig,
    run_mode: AuthzOutboxRunMode,
    shutdown: Shutdown,
) {
    let interval = Duration::from_secs(run_mode.interval_secs);
    loop {
        if shutdown.received() {
            let signal = shutdown_signal(shutdown.clone()).await;
            log_shutdown_start(signal);
            break;
        }

        let run = run_once(pool, authz_client, config);
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
                            "Second shutdown signal received; stopping process-authz-outbox immediately"
                        );
                        warn!("process-authz-outbox stopped");
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
                "Authz outbox drain failed; exiting for supervisor restart"
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

    warn!("process-authz-outbox stopped");
}

async fn run_once(
    pool: &sqlx::PgPool,
    authz_client: &AuthzClient,
    config: AuthzOutboxProcessorConfig,
) -> i32 {
    let _ = insert_audit_event(
        pool,
        AuditEventRecord::new(AuditEventType::AuthzOutboxProcessingStarted).with_metadata(json!({
            "batch_size": config.batch_size,
            "max_retries": config.max_retries
        })),
    )
    .await;

    let result = process_authz_outbox(pool, authz_client, config).await;
    let exit_code = authz_outbox_exit_code(&result);

    match result {
        Ok(stats) => {
            println!(
                "Authz outbox complete: batches={}, fetched_rows={}, processed_rows={}, failed_rows={}, skipped_max_retry_rows={}",
                stats.batches,
                stats.fetched_rows,
                stats.processed_rows,
                stats.failed_rows,
                stats.skipped_max_retry_rows
            );

            let _ = insert_audit_event(
                pool,
                AuditEventRecord::new(AuditEventType::AuthzOutboxProcessingCompleted)
                    .with_metadata(json!({
                        "batch_size": config.batch_size,
                        "max_retries": config.max_retries,
                        "batches": stats.batches,
                        "fetched_rows": stats.fetched_rows,
                        "processed_rows": stats.processed_rows,
                        "failed_rows": stats.failed_rows,
                        "skipped_max_retry_rows": stats.skipped_max_retry_rows
                    })),
            )
            .await;
        }
        Err(err) => {
            eprintln!("Authz outbox processing failed: {err}");

            let _ = insert_audit_event(
                pool,
                AuditEventRecord::new(AuditEventType::AuthzOutboxProcessingFailed).with_metadata(
                    json!({
                        "batch_size": config.batch_size,
                        "max_retries": config.max_retries,
                        "error_code": sanitize_error_code(&err.to_string())
                    }),
                ),
            )
            .await;

            debug_assert_eq!(exit_code, AUTHZ_OUTBOX_EXIT_FAILURE);
        }
    }

    exit_code
}

fn log_shutdown_start(signal: gmrag_api::shutdown::ShutdownSignal) {
    warn!(
        signal = signal.as_str(),
        "Authz outbox shutdown signal received"
    );
    warn!("Authz outbox draining started");
}

fn print_usage() {
    println!(
        "Usage: process-authz-outbox [--once|--loop] [--interval-secs <n>]\n\n\
         Drains authz_outbox via the shared process_authz_outbox library.\n\
         Default is --once (manual/debug). --loop sleeps between drains;\n\
         hard failures exit non-zero so Docker Compose can restart the process.\n\n\
         Env: DATABASE_URL, OPENFGA_API_URL, OPENFGA_STORE_ID, OPENFGA_MODEL_ID,\n\
              AUTHZ_OUTBOX_BATCH_SIZE, AUTHZ_OUTBOX_MAX_RETRIES,\n\
              AUTHZ_OUTBOX_POLL_INTERVAL_SECS, RUST_LOG"
    );
}
