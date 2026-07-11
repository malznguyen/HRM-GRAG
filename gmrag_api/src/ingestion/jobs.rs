//! Hàng đợi ingestion durable trên PostgreSQL.

use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::outbox::{
    FailureDisposition, OutboxBackoffConfig, disposition_after_failure, parse_env_i32,
    parse_env_i64,
};

pub const JOB_QUEUED: &str = "QUEUED";
pub const JOB_PROCESSING: &str = "PROCESSING";
pub const JOB_SUCCEEDED: &str = "SUCCEEDED";
pub const JOB_DEAD: &str = "DEAD";

pub const DOCUMENT_PROCESSING: &str = "PROCESSING";
pub const DOCUMENT_COMPLETED: &str = "COMPLETED";
pub const DOCUMENT_FAILED: &str = "FAILED";

pub const STAGE_QUEUED: &str = "QUEUED";
pub const STAGE_PARSING: &str = "PARSING";
pub const STAGE_EMBEDDING: &str = "EMBEDDING";
pub const STAGE_GRAPH_EXTRACTION: &str = "GRAPH_EXTRACTION";
pub const STAGE_SAVING: &str = "SAVING";
pub const STAGE_INDEXING: &str = "INDEXING";
pub const STAGE_DONE: &str = "DONE";
pub const STAGE_FAILED: &str = "FAILED";

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClaimedJob {
    pub id: Uuid,
    pub document_id: Uuid,
    pub workspace_id: Uuid,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub claim_token: Uuid,
}

#[derive(Debug, Clone, Copy)]
pub struct IngestionWorkerConfig {
    pub batch_size: i64,
    pub max_attempts: i32,
    pub lease_secs: i64,
    pub base_backoff_secs: i64,
    pub max_backoff_secs: i64,
}

impl IngestionWorkerConfig {
    pub fn from_env() -> Self {
        Self {
            batch_size: parse_env_i64("INGESTION_JOB_BATCH_SIZE", 1, 1, 32),
            max_attempts: parse_env_i32("INGESTION_JOB_MAX_ATTEMPTS", 5, 1, 100),
            lease_secs: parse_env_i64("INGESTION_JOB_LEASE_SECS", 300, 10, 86_400),
            base_backoff_secs: parse_env_i64("INGESTION_JOB_BASE_BACKOFF_SECS", 5, 1, 3_600),
            max_backoff_secs: parse_env_i64("INGESTION_JOB_MAX_BACKOFF_SECS", 300, 1, 86_400),
        }
    }

    fn backoff(self) -> OutboxBackoffConfig {
        OutboxBackoffConfig {
            base_backoff_secs: self.base_backoff_secs,
            max_backoff_secs: self.max_backoff_secs,
            claim_lease_secs: self.lease_secs,
        }
    }
}

/// Tạo một job ngay trong transaction upload; document và job cùng commit hoặc cùng rollback.
pub async fn enqueue_job_tx(
    tx: &mut Transaction<'_, Postgres>,
    document_id: Uuid,
    workspace_id: Uuid,
    max_attempts: i32,
) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        INSERT INTO ingestion_jobs (document_id, workspace_id, max_attempts)
        VALUES ($1, $2, $3)
        RETURNING id
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .bind(max_attempts)
    .fetch_one(&mut **tx)
    .await
}

/// Claim job với lease token. Token ngăn worker cũ ghi đè sau khi lease bị reclaim.
pub async fn claim_jobs(
    pool: &PgPool,
    worker_id: &str,
    config: IngestionWorkerConfig,
) -> Result<Vec<ClaimedJob>, sqlx::Error> {
    claim_jobs_filtered(pool, worker_id, config, None).await
}

/// Claim một document cụ thể cho integration recovery và operator diagnostics.
/// Production worker luôn dùng `claim_jobs` để poll toàn queue.
pub async fn claim_document_job(
    pool: &PgPool,
    worker_id: &str,
    config: IngestionWorkerConfig,
    document_id: Uuid,
) -> Result<Vec<ClaimedJob>, sqlx::Error> {
    claim_jobs_filtered(pool, worker_id, config, Some(document_id)).await
}

async fn claim_jobs_filtered(
    pool: &PgPool,
    worker_id: &str,
    config: IngestionWorkerConfig,
    document_id: Option<Uuid>,
) -> Result<Vec<ClaimedJob>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query_as(
        r#"
        WITH candidates AS (
            SELECT id
            FROM ingestion_jobs
            WHERE ($4::uuid IS NULL OR document_id = $4)
              AND ((status = 'QUEUED' AND available_at <= CURRENT_TIMESTAMP)
               OR (status = 'PROCESSING' AND lease_expires_at < CURRENT_TIMESTAMP)
              )
            ORDER BY available_at, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT $1
        )
        UPDATE ingestion_jobs AS job
        SET status = 'PROCESSING',
            claimed_by = $2,
            claim_token = gen_random_uuid(),
            lease_expires_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision),
            attempt_count = attempt_count + 1,
            started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
            updated_at = CURRENT_TIMESTAMP
        FROM candidates
        WHERE job.id = candidates.id
          AND (
            job.status = 'QUEUED'
            OR (job.status = 'PROCESSING' AND job.lease_expires_at < CURRENT_TIMESTAMP)
          )
        RETURNING job.id, job.document_id, job.workspace_id, job.attempt_count,
                  job.max_attempts, job.claim_token
        "#,
    )
    .bind(config.batch_size)
    .bind(worker_id)
    .bind(config.lease_secs as f64)
    .bind(document_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
}

pub async fn extend_lease(
    pool: &PgPool,
    job: &ClaimedJob,
    worker_id: &str,
    lease_secs: i64,
) -> Result<bool, sqlx::Error> {
    let updated = sqlx::query(
        r#"
        UPDATE ingestion_jobs
        SET lease_expires_at = CURRENT_TIMESTAMP + make_interval(secs => $4::double precision),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1 AND status = 'PROCESSING' AND claimed_by = $2 AND claim_token = $3
        "#,
    )
    .bind(job.id)
    .bind(worker_id)
    .bind(job.claim_token)
    .bind(lease_secs as f64)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(updated == 1)
}

pub async fn set_stage_for_owner(
    pool: &PgPool,
    job: &ClaimedJob,
    worker_id: &str,
    stage: &'static str,
) -> Result<bool, sqlx::Error> {
    let changed = sqlx::query(
        r#"
        UPDATE documents AS document
        SET processing_stage = $4
        WHERE document.id = $1 AND document.workspace_id = $2
          AND EXISTS (
            SELECT 1 FROM ingestion_jobs job
            WHERE job.id = $3 AND job.status = 'PROCESSING'
              AND job.claimed_by = $5 AND job.claim_token = $6
              AND job.lease_expires_at > CURRENT_TIMESTAMP
          )
        "#,
    )
    .bind(job.document_id)
    .bind(job.workspace_id)
    .bind(job.id)
    .bind(stage)
    .bind(worker_id)
    .bind(job.claim_token)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(changed == 1)
}

pub async fn complete_job_and_document(
    pool: &PgPool,
    job: &ClaimedJob,
    worker_id: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let claimed = sqlx::query(
        r#"
        UPDATE ingestion_jobs
        SET status = 'SUCCEEDED', lease_expires_at = NULL, completed_at = CURRENT_TIMESTAMP,
            failure_code = NULL, failure_message = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1 AND status = 'PROCESSING' AND claimed_by = $2 AND claim_token = $3
          AND lease_expires_at > CURRENT_TIMESTAMP
        "#,
    )
    .bind(job.id)
    .bind(worker_id)
    .bind(job.claim_token)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if claimed != 1 {
        tx.rollback().await?;
        return Ok(false);
    }
    sqlx::query(
        r#"
        UPDATE documents
        SET status = 'COMPLETED', processing_stage = 'DONE', failure_code = NULL,
            failure_message = NULL, failed_at = NULL
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(job.document_id)
    .bind(job.workspace_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

#[derive(Debug, Clone, Copy)]
pub struct JobFailure<'a> {
    pub code: &'a str,
    pub message: &'a str,
    pub retryable: bool,
}

/// Ghi retry backoff hoặc terminal failure, chỉ khi lease hiện tại còn thuộc worker.
pub async fn finish_job_failure(
    pool: &PgPool,
    job: &ClaimedJob,
    worker_id: &str,
    failure: JobFailure<'_>,
    config: IngestionWorkerConfig,
) -> Result<Option<bool>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let current: Option<(i32, i32)> = sqlx::query_as(
        r#"
        SELECT attempt_count, max_attempts
        FROM ingestion_jobs
        WHERE id = $1 AND status = 'PROCESSING' AND claimed_by = $2 AND claim_token = $3
          AND lease_expires_at > CURRENT_TIMESTAMP
        FOR UPDATE
        "#,
    )
    .bind(job.id)
    .bind(worker_id)
    .bind(job.claim_token)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((attempt_count, max_attempts)) = current else {
        tx.rollback().await?;
        return Ok(None);
    };
    let permanent = !failure.retryable;
    let disposition = disposition_after_failure(
        attempt_count.saturating_sub(1),
        max_attempts.min(config.max_attempts),
        permanent,
        config.backoff(),
    );
    match disposition {
        FailureDisposition::Retryable { backoff_secs, .. } => {
            sqlx::query(
                r#"
                UPDATE ingestion_jobs
                SET status = 'QUEUED', available_at = CURRENT_TIMESTAMP + make_interval(secs => $2::double precision),
                    lease_expires_at = NULL, claimed_by = NULL, claim_token = NULL,
                    failure_code = $3, failure_message = $4, updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                "#,
            )
            .bind(job.id)
            .bind(backoff_secs as f64)
            .bind(failure.code)
            .bind(sanitize_message(failure.message))
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE documents SET status = 'PROCESSING', processing_stage = 'QUEUED' WHERE id = $1 AND workspace_id = $2",
            )
            .bind(job.document_id)
            .bind(job.workspace_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(Some(false))
        }
        FailureDisposition::Dead { .. } => {
            let code = if failure.retryable {
                "INGESTION_MAX_ATTEMPTS_EXCEEDED"
            } else {
                failure.code
            };
            let message = if failure.retryable {
                "Ingestion could not be completed after the configured retry limit"
            } else {
                failure.message
            };
            sqlx::query(
                r#"
                UPDATE ingestion_jobs
                SET status = 'DEAD', lease_expires_at = NULL, completed_at = CURRENT_TIMESTAMP,
                    failure_code = $2, failure_message = $3, updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                "#,
            )
            .bind(job.id)
            .bind(code)
            .bind(sanitize_message(message))
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                UPDATE documents
                SET status = 'FAILED', processing_stage = 'FAILED', failure_code = $3,
                    failure_message = $4, failed_at = CURRENT_TIMESTAMP
                WHERE id = $1 AND workspace_id = $2
                "#,
            )
            .bind(job.document_id)
            .bind(job.workspace_id)
            .bind(code)
            .bind(sanitize_message(message))
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(Some(true))
        }
    }
}

pub async fn retry_failed_document(
    pool: &PgPool,
    document_id: Uuid,
    workspace_id: Uuid,
    max_attempts: i32,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let changed = sqlx::query(
        r#"
        UPDATE documents
        SET status = 'PROCESSING', processing_stage = 'QUEUED', failure_code = NULL,
            failure_message = NULL, failed_at = NULL
        WHERE id = $1 AND workspace_id = $2 AND status = 'FAILED'
          AND NOT EXISTS (
            SELECT 1 FROM ingestion_jobs
            WHERE document_id = $1 AND status IN ('QUEUED', 'PROCESSING')
          )
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if changed != 1 {
        tx.rollback().await?;
        return Ok(false);
    }
    enqueue_job_tx(&mut tx, document_id, workspace_id, max_attempts).await?;
    tx.commit().await?;
    Ok(true)
}

pub async fn recover_legacy_processing_documents(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        r#"
        UPDATE documents AS document
        SET status = 'FAILED', processing_stage = 'FAILED', failure_code = 'INGESTION_JOB_MISSING',
            failure_message = 'No durable ingestion job exists for this legacy processing document',
            failed_at = CURRENT_TIMESTAMP
        WHERE document.status = 'PROCESSING'
          AND NOT EXISTS (
            SELECT 1 FROM ingestion_jobs job
            WHERE job.document_id = document.id AND job.status IN ('QUEUED', 'PROCESSING')
          )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

fn sanitize_message(message: &str) -> String {
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(240)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stages_are_canonical() {
        assert_eq!(
            [
                STAGE_QUEUED,
                STAGE_PARSING,
                STAGE_EMBEDDING,
                STAGE_GRAPH_EXTRACTION,
                STAGE_SAVING,
                STAGE_INDEXING,
                STAGE_DONE,
                STAGE_FAILED
            ]
            .len(),
            8
        );
    }

    #[test]
    fn failure_message_is_sanitized() {
        assert_eq!(sanitize_message("safe\u{0000} message"), "safe message");
    }
}
