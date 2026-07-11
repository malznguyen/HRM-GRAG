use std::time::Duration;

use gmrag_api::ingestion::jobs::{
    IngestionWorkerConfig, JobFailure, claim_jobs, extend_lease, finish_job_failure,
};
use gmrag_api::ingestion::processor::process_claimed_job;
use gmrag_api::retrieval::RetrievalClient;
use gmrag_api::storage::{StorageClient, StorageConfig};
use sqlx::postgres::PgPoolOptions;
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Default)]
struct RunSummary {
    claimed: u64,
    succeeded: u64,
    retried: u64,
    dead: u64,
    lease_conflicts: u64,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,lopdf=error")),
        )
        .init();

    let options = WorkerOptions::parse();
    let config = IngestionWorkerConfig::from_env();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    let storage = StorageClient::from_config(
        StorageConfig::from_env().expect("Failed to load storage configuration"),
    )
    .await;
    let retrieval = RetrievalClient::from_env().expect("Failed to load retrieval configuration");

    let poll_interval =
        Duration::from_millis(env_u64("INGESTION_WORKER_POLL_INTERVAL_MS", 1_000, 100));
    let mut summary = RunSummary::default();
    loop {
        let claimed = match claim_jobs(&pool, &options.worker_id, config).await {
            Ok(rows) => rows,
            Err(err) => {
                error!(error = %err, "Failed to claim ingestion jobs");
                if options.once {
                    std::process::exit(1);
                }
                sleep(poll_interval).await;
                continue;
            }
        };
        if claimed.is_empty() {
            if options.once {
                break;
            }
            tokio::select! {
                _ = tokio::signal::ctrl_c() => break,
                _ = sleep(poll_interval) => {}
            }
            continue;
        }
        for job in claimed {
            summary.claimed += 1;
            let outcome = process_one(
                pool.clone(),
                storage.clone(),
                retrieval.clone(),
                job.clone(),
                options.worker_id.clone(),
                config,
            )
            .await;
            match outcome {
                ProcessOutcome::Succeeded => summary.succeeded += 1,
                ProcessOutcome::Retried => summary.retried += 1,
                ProcessOutcome::Dead => summary.dead += 1,
                ProcessOutcome::LeaseConflict => summary.lease_conflicts += 1,
            }
        }
        if options.once {
            break;
        }
    }
    info!(
        claimed = summary.claimed,
        succeeded = summary.succeeded,
        retried = summary.retried,
        dead = summary.dead,
        lease_conflicts = summary.lease_conflicts,
        "Ingestion worker stopped"
    );
}

enum ProcessOutcome {
    Succeeded,
    Retried,
    Dead,
    LeaseConflict,
}

async fn process_one(
    pool: sqlx::PgPool,
    storage: StorageClient,
    retrieval: RetrievalClient,
    job: gmrag_api::ingestion::jobs::ClaimedJob,
    worker_id: String,
    config: IngestionWorkerConfig,
) -> ProcessOutcome {
    let task_pool = pool.clone();
    let task_job = job.clone();
    let task_worker_id = worker_id.clone();
    let mut task = tokio::spawn(async move {
        process_claimed_job(task_pool, storage, retrieval, &task_job, &task_worker_id).await
    });
    let mut heartbeat = interval(Duration::from_secs((config.lease_secs / 3).max(1) as u64));
    heartbeat.tick().await;
    let result = loop {
        tokio::select! {
            result = &mut task => break result,
            _ = heartbeat.tick() => {
                match extend_lease(&pool, &job, &worker_id, config.lease_secs).await {
                    Ok(true) => {}
                    Ok(false) => {
                        task.abort();
                        warn!(job_id = %job.id, document_id = %job.document_id, "Ingestion lease ownership was lost");
                        return ProcessOutcome::LeaseConflict;
                    }
                    Err(err) => {
                        warn!(job_id = %job.id, error = %err, "Ingestion lease heartbeat failed; preserving job for recovery");
                    }
                }
            }
        }
    };
    match result {
        Ok(Ok(())) => ProcessOutcome::Succeeded,
        Ok(Err(err)) if err.is_lease_lost() => ProcessOutcome::LeaseConflict,
        Ok(Err(err)) => {
            let (code, message, retryable) = err.failure_kind();
            error!(job_id = %job.id, document_id = %job.document_id, workspace_id = %job.workspace_id, attempt = job.attempt_count, failure_code = code, "Document ingestion failed");
            match finish_job_failure(
                &pool,
                &job,
                &worker_id,
                JobFailure {
                    code,
                    message,
                    retryable,
                },
                config,
            )
            .await
            {
                Ok(Some(true)) => ProcessOutcome::Dead,
                Ok(Some(false)) => ProcessOutcome::Retried,
                Ok(None) => ProcessOutcome::LeaseConflict,
                Err(update_err) => {
                    error!(job_id = %job.id, error = %update_err, "Failed to persist ingestion job failure");
                    ProcessOutcome::LeaseConflict
                }
            }
        }
        Err(join_err) => {
            error!(job_id = %job.id, error = %join_err, "Ingestion task stopped unexpectedly");
            match finish_job_failure(
                &pool,
                &job,
                &worker_id,
                JobFailure {
                    code: "INTERNAL_INGESTION_ERROR",
                    message: "Ingestion worker stopped unexpectedly",
                    retryable: true,
                },
                config,
            )
            .await
            {
                Ok(Some(true)) => ProcessOutcome::Dead,
                Ok(Some(false)) => ProcessOutcome::Retried,
                _ => ProcessOutcome::LeaseConflict,
            }
        }
    }
}

struct WorkerOptions {
    once: bool,
    worker_id: String,
}

impl WorkerOptions {
    fn parse() -> Self {
        let mut once = false;
        let mut worker_id = format!("ingestion-worker-{}", Uuid::new_v4());
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--once" => once = true,
                "--help" | "-h" => {
                    println!(
                        "Usage: ingestion-worker [--once] [--worker-id <id>]\n\nPolls durable PostgreSQL ingestion jobs and processes claimed documents."
                    );
                    std::process::exit(0);
                }
                "--worker-id" => {
                    worker_id = arguments.next().expect("--worker-id requires a value");
                }
                _ => panic!("Unknown argument: {argument}"),
            }
        }
        Self { once, worker_id }
    }
}

fn env_u64(name: &str, default: u64, min: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(min)
}
