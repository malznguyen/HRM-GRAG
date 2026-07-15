mod support;

use std::{collections::HashMap, sync::Arc};

use futures::future::join_all;
use gmrag_api::auth::document_acl::fetch_completed_workspace_document_acl_rows;
use gmrag_api::chat::retrieval::fetch_chunks_by_ids;
use gmrag_api::ingestion::embedding::DEFAULT_EMBEDDING_DIM;
use gmrag_api::ingestion::jobs::{
    IngestionWorkerConfig, JobFailure, STAGE_PARSING, claim_document_job, enqueue_job_tx,
    finish_job_failure, retry_failed_document, set_stage_for_owner,
};
use gmrag_api::ingestion::processor::{ChunkIndexer, ProcessError, index_document_outputs};
use gmrag_api::retrieval::{ChunkPoint, RetrievalError};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::Mutex;
use uuid::Uuid;

async fn pool_or_skip() -> Option<PgPool> {
    dotenvy::dotenv().ok();
    let database_url = support::database_url().ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(12)
        .connect(&database_url)
        .await
        .ok()?;
    sqlx::migrate!("./migrations").run(&pool).await.ok()?;
    // Dọn fixture từ run trước bị panic để các claim test không đụng job cũ.
    sqlx::query(
        "DELETE FROM documents WHERE filename = 'test.pdf' AND object_key = 'test/object.pdf'",
    )
    .execute(&pool)
    .await
    .ok()?;
    sqlx::query("DELETE FROM users WHERE id LIKE 'ingestion-test-%'")
        .execute(&pool)
        .await
        .ok()?;
    Some(pool)
}

async fn seed_document(pool: &PgPool, status: &str) -> (Uuid, Uuid, String) {
    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let user_id = format!("ingestion-test-{}", Uuid::new_v4());
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&user_id)
        .bind(format!("{user_id}@test.local"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("tenant-{tenant_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("workspace-{workspace_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO documents (id, workspace_id, owner_id, filename, status, processing_stage, object_key, bucket, uploaded_by) VALUES ($1, $2, $3, 'test.pdf', $4, 'QUEUED', 'test/object.pdf', 'test', $3)",
    )
    .bind(document_id).bind(workspace_id).bind(&user_id).bind(status).execute(pool).await.unwrap();
    (workspace_id, document_id, user_id)
}

async fn cleanup(pool: &PgPool, workspace_id: Uuid, user_id: &str) {
    let _ = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await;
}

#[derive(Clone, Copy)]
enum FailureMode {
    FailBeforeFirstBatch,
    FailAfterFirstBatch,
    TimeoutAfterAccept,
    AlwaysFail,
}

#[derive(Debug, Clone)]
struct BatchObservation {
    attempt_count: i32,
    batch_number: usize,
    point_ids: Vec<Uuid>,
    accepted: bool,
}

#[derive(Default)]
struct FakeQdrantState {
    delete_calls: usize,
    upsert_calls: usize,
    batches: Vec<BatchObservation>,
    points: HashMap<Uuid, ChunkPoint>,
}

#[derive(Clone)]
struct FakeQdrant {
    mode: FailureMode,
    batch_size: usize,
    state: Arc<Mutex<FakeQdrantState>>,
}

impl FakeQdrant {
    fn new(mode: FailureMode, batch_size: usize) -> Self {
        Self {
            mode,
            batch_size,
            state: Arc::new(Mutex::new(FakeQdrantState::default())),
        }
    }

    async fn snapshot(&self) -> FakeQdrantStateSnapshot {
        let state = self.state.lock().await;
        FakeQdrantStateSnapshot {
            delete_calls: state.delete_calls,
            upsert_calls: state.upsert_calls,
            batches: state.batches.clone(),
            point_ids: state.points.keys().copied().collect(),
        }
    }
}

#[derive(Debug)]
struct FakeQdrantStateSnapshot {
    delete_calls: usize,
    upsert_calls: usize,
    batches: Vec<BatchObservation>,
    point_ids: Vec<Uuid>,
}

impl ChunkIndexer for FakeQdrant {
    fn delete_points_by_document<'a>(
        &'a self,
        _workspace_id: Uuid,
        _document_id: Uuid,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), RetrievalError>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            state.delete_calls += 1;
            state.points.clear();
            Ok(())
        })
    }

    fn upsert_chunk_points<'a>(
        &'a self,
        points: &'a [ChunkPoint],
        attempt_count: i32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), RetrievalError>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            state.upsert_calls += 1;

            for (batch_index, batch) in points.chunks(self.batch_size).enumerate() {
                let batch_number = batch_index + 1;
                let point_ids = batch.iter().map(|point| point.chunk_id).collect();
                let should_fail = match self.mode {
                    FailureMode::FailBeforeFirstBatch => attempt_count == 1 && batch_number == 1,
                    FailureMode::FailAfterFirstBatch => attempt_count == 1 && batch_number == 2,
                    FailureMode::TimeoutAfterAccept => attempt_count == 1 && batch_number == 1,
                    FailureMode::AlwaysFail => true,
                };

                if should_fail && matches!(self.mode, FailureMode::TimeoutAfterAccept) {
                    for point in batch {
                        state.points.insert(point.chunk_id, point.clone());
                    }
                    state.batches.push(BatchObservation {
                        attempt_count,
                        batch_number,
                        point_ids,
                        accepted: true,
                    });
                    return Err(RetrievalError::Timeout {
                        operation: "points_upsert",
                        timeout_secs: 1,
                    });
                }

                if should_fail {
                    state.batches.push(BatchObservation {
                        attempt_count,
                        batch_number,
                        point_ids,
                        accepted: false,
                    });
                    return Err(RetrievalError::Api {
                        status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                        body: "injected failure".to_string(),
                        operation: "points_upsert",
                    });
                }

                for point in batch {
                    state.points.insert(point.chunk_id, point.clone());
                }
                state.batches.push(BatchObservation {
                    attempt_count,
                    batch_number,
                    point_ids,
                    accepted: true,
                });
            }

            Ok(())
        })
    }
}

fn test_worker_config(max_attempts: i32) -> IngestionWorkerConfig {
    IngestionWorkerConfig {
        batch_size: 1,
        max_attempts,
        lease_secs: 60,
        base_backoff_secs: 1,
        max_backoff_secs: 2,
    }
}

async fn seed_points(pool: &PgPool, workspace_id: Uuid, document_id: Uuid) -> Vec<ChunkPoint> {
    let embedding = vec![0.01_f32; DEFAULT_EMBEDDING_DIM];
    let embedding_literal = format!(
        "[{}]",
        embedding
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut points = Vec::new();

    for chunk_index in 0..4 {
        let chunk_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO document_chunks (id, document_id, workspace_id, chunk_index, original_text, embedding) VALUES ($1, $2, $3, $4, $5, $6::vector)",
        )
        .bind(chunk_id)
        .bind(document_id)
        .bind(workspace_id)
        .bind(chunk_index)
        .bind(format!("chunk-{chunk_index}"))
        .bind(&embedding_literal)
        .execute(pool)
        .await
        .unwrap();
        points.push(ChunkPoint {
            chunk_id,
            workspace_id,
            document_id,
            chunk_index,
            embedding: embedding.clone(),
        });
    }

    points
}

async fn insert_document_with_status(
    pool: &PgPool,
    workspace_id: Uuid,
    user_id: &str,
    status: &str,
    processing_stage: &str,
) -> Uuid {
    let document_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO documents (id, workspace_id, owner_id, filename, status, processing_stage, object_key, bucket, uploaded_by) VALUES ($1, $2, $3, $4, $5, $6, $7, 'test', $3)",
    )
    .bind(document_id)
    .bind(workspace_id)
    .bind(user_id)
    .bind(format!("{document_id}.pdf"))
    .bind(status)
    .bind(processing_stage)
    .bind(format!("test/{document_id}.pdf"))
    .execute(pool)
    .await
    .unwrap();
    document_id
}

async fn enqueue_and_claim(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
    worker_id: &str,
    config: IngestionWorkerConfig,
) -> gmrag_api::ingestion::jobs::ClaimedJob {
    let mut tx = pool.begin().await.unwrap();
    enqueue_job_tx(&mut tx, document_id, workspace_id, config.max_attempts)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    claim_document_job(pool, worker_id, config, document_id)
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn finish_retryable_failure(
    pool: &PgPool,
    job: &gmrag_api::ingestion::jobs::ClaimedJob,
    worker_id: &str,
    config: IngestionWorkerConfig,
    error: ProcessError,
) {
    let (code, message, retryable) = error.failure_kind();
    assert!(retryable);
    assert_eq!(
        finish_job_failure(
            pool,
            job,
            worker_id,
            JobFailure {
                code,
                message,
                retryable,
            },
            config,
        )
        .await
        .unwrap(),
        Some(false)
    );
}

#[tokio::test]
async fn qdrant_partial_batch_failure_retries_and_converges() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let (workspace_id, document_id, user_id) = seed_document(&pool, "PROCESSING").await;
    let points = seed_points(&pool, workspace_id, document_id).await;
    let config = test_worker_config(3);
    let first = enqueue_and_claim(&pool, workspace_id, document_id, "worker-a", config).await;
    let fake = FakeQdrant::new(FailureMode::FailAfterFirstBatch, 2);

    let first_error = index_document_outputs(&pool, &fake, &first, "worker-a", &points)
        .await
        .expect_err("second batch must fail deterministically");
    assert!(!matches!(first_error, ProcessError::LeaseLost));
    finish_retryable_failure(&pool, &first, "worker-a", config, first_error).await;

    let retry_state: (String, String, String, i32, bool) = sqlx::query_as(
        "SELECT job.status, document.status, document.processing_stage, job.attempt_count, job.available_at > CURRENT_TIMESTAMP FROM ingestion_jobs job JOIN documents document ON document.id = job.document_id WHERE job.id = $1",
    )
    .bind(first.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retry_state.0, "QUEUED");
    assert_eq!(retry_state.1, "PROCESSING");
    assert_eq!(retry_state.2, "QUEUED");
    assert_eq!(retry_state.3, 1);
    assert!(retry_state.4);

    sqlx::query("UPDATE ingestion_jobs SET available_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = $1")
        .bind(first.id)
        .execute(&pool)
        .await
        .unwrap();
    let second = claim_document_job(&pool, "worker-b", config, document_id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    index_document_outputs(&pool, &fake, &second, "worker-b", &points)
        .await
        .unwrap();

    let final_state: (String, String, String, i32) = sqlx::query_as(
        "SELECT job.status, document.status, document.processing_stage, job.attempt_count FROM ingestion_jobs job JOIN documents document ON document.id = job.document_id WHERE job.id = $1",
    )
    .bind(first.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        final_state,
        ("SUCCEEDED".into(), "COMPLETED".into(), "DONE".into(), 2)
    );

    let snapshot = fake.snapshot().await;
    assert_eq!(snapshot.delete_calls, 2);
    assert_eq!(snapshot.upsert_calls, 2);
    assert_eq!(snapshot.point_ids.len(), points.len());
    assert_eq!(
        snapshot
            .batches
            .iter()
            .filter(|batch| batch.accepted)
            .count(),
        3
    );
    assert!(
        snapshot
            .batches
            .iter()
            .any(|batch| !batch.accepted && batch.batch_number == 2)
    );
    assert_eq!(snapshot.batches[0].attempt_count, 1);
    assert_eq!(snapshot.batches.last().unwrap().attempt_count, 2);
    assert_eq!(snapshot.batches[0].point_ids, snapshot.batches[2].point_ids);

    let chunk_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM document_chunks WHERE document_id = $1")
            .bind(document_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(chunk_count, points.len() as i64);
    cleanup(&pool, workspace_id, &user_id).await;
}

#[tokio::test]
async fn qdrant_timeout_after_accept_retries_without_duplicate_points() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let (workspace_id, document_id, user_id) = seed_document(&pool, "PROCESSING").await;
    let points = seed_points(&pool, workspace_id, document_id).await;
    let config = test_worker_config(3);
    let first =
        enqueue_and_claim(&pool, workspace_id, document_id, "worker-timeout-a", config).await;
    let fake = FakeQdrant::new(FailureMode::TimeoutAfterAccept, 2);

    let first_error = index_document_outputs(&pool, &fake, &first, "worker-timeout-a", &points)
        .await
        .expect_err("accepted timeout must remain unconfirmed");
    assert!(matches!(
        first_error,
        ProcessError::Retrieval(RetrievalError::Timeout { .. })
    ));
    finish_retryable_failure(&pool, &first, "worker-timeout-a", config, first_error).await;
    sqlx::query("UPDATE ingestion_jobs SET available_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = $1")
        .bind(first.id)
        .execute(&pool)
        .await
        .unwrap();
    let second = claim_document_job(&pool, "worker-timeout-b", config, document_id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    index_document_outputs(&pool, &fake, &second, "worker-timeout-b", &points)
        .await
        .unwrap();

    let snapshot = fake.snapshot().await;
    assert_eq!(snapshot.point_ids.len(), points.len());
    assert_eq!(snapshot.batches[0].point_ids, snapshot.batches[1].point_ids);
    assert!(snapshot.batches[0].accepted);
    assert_eq!(snapshot.upsert_calls, 2);
    let status: (String, String) = sqlx::query_as(
        "SELECT job.status, document.status FROM ingestion_jobs job JOIN documents document ON document.id = job.document_id WHERE job.id = $1",
    )
    .bind(first.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, ("SUCCEEDED".into(), "COMPLETED".into()));
    cleanup(&pool, workspace_id, &user_id).await;
}

#[tokio::test]
async fn abandoned_worker_lease_is_reclaimed_and_pipeline_completes() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let (workspace_id, document_id, user_id) = seed_document(&pool, "PROCESSING").await;
    let points = seed_points(&pool, workspace_id, document_id).await;
    let config = test_worker_config(3);
    let first = enqueue_and_claim(&pool, workspace_id, document_id, "worker-dead", config).await;
    sqlx::query("UPDATE ingestion_jobs SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = $1")
        .bind(first.id)
        .execute(&pool)
        .await
        .unwrap();
    let second = claim_document_job(&pool, "worker-restarted", config, document_id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let fake = FakeQdrant::new(FailureMode::FailBeforeFirstBatch, 2);
    index_document_outputs(&pool, &fake, &second, "worker-restarted", &points)
        .await
        .unwrap();

    assert!(
        !gmrag_api::ingestion::jobs::complete_job_and_document(&pool, &first, "worker-dead")
            .await
            .unwrap()
    );
    let final_state: (String, String, String, Option<String>) = sqlx::query_as(
        "SELECT job.status, document.status, document.processing_stage, job.claimed_by FROM ingestion_jobs job JOIN documents document ON document.id = job.document_id WHERE job.id = $1",
    )
    .bind(first.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(final_state.0, "SUCCEEDED");
    assert_eq!(final_state.1, "COMPLETED");
    assert_eq!(final_state.2, "DONE");
    assert_eq!(final_state.3.as_deref(), Some("worker-restarted"));
    cleanup(&pool, workspace_id, &user_id).await;
}

#[tokio::test]
async fn qdrant_failure_exhausts_attempts_and_marks_document_failed() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let (workspace_id, document_id, user_id) = seed_document(&pool, "PROCESSING").await;
    let points = seed_points(&pool, workspace_id, document_id).await;
    let config = test_worker_config(2);
    let fake = FakeQdrant::new(FailureMode::AlwaysFail, 2);

    let first = enqueue_and_claim(&pool, workspace_id, document_id, "worker-fail-a", config).await;
    let first_error = index_document_outputs(&pool, &fake, &first, "worker-fail-a", &points)
        .await
        .expect_err("always-fail Qdrant must fail attempt one");
    finish_retryable_failure(&pool, &first, "worker-fail-a", config, first_error).await;
    sqlx::query("UPDATE ingestion_jobs SET available_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = $1")
        .bind(first.id)
        .execute(&pool)
        .await
        .unwrap();

    let second = claim_document_job(&pool, "worker-fail-b", config, document_id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let second_error = index_document_outputs(&pool, &fake, &second, "worker-fail-b", &points)
        .await
        .expect_err("always-fail Qdrant must fail final attempt");
    let (code, message, retryable) = second_error.failure_kind();
    assert!(retryable);
    assert_eq!(
        finish_job_failure(
            &pool,
            &second,
            "worker-fail-b",
            JobFailure {
                code,
                message,
                retryable,
            },
            config,
        )
        .await
        .unwrap(),
        Some(true)
    );

    let terminal: (String, String, String, String, Option<String>) = sqlx::query_as(
        "SELECT job.status, document.status, document.processing_stage, document.failure_code, document.failure_message FROM ingestion_jobs job JOIN documents document ON document.id = job.document_id WHERE job.id = $1",
    )
    .bind(first.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(terminal.0, "DEAD");
    assert_eq!(terminal.1, "FAILED");
    assert_eq!(terminal.2, "FAILED");
    assert_eq!(terminal.3, "INGESTION_MAX_ATTEMPTS_EXCEEDED");
    assert_eq!(
        terminal.4.as_deref(),
        Some("Ingestion could not be completed after the configured retry limit")
    );
    assert!(
        claim_document_job(&pool, "worker-fail-c", config, document_id)
            .await
            .unwrap()
            .is_empty()
    );
    cleanup(&pool, workspace_id, &user_id).await;
}

#[tokio::test]
async fn partial_and_failed_documents_are_excluded_from_retrieval() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let (workspace_id, processing_document_id, user_id) = seed_document(&pool, "PROCESSING").await;
    let failed_document_id =
        insert_document_with_status(&pool, workspace_id, &user_id, "FAILED", "FAILED").await;
    let completed_document_id =
        insert_document_with_status(&pool, workspace_id, &user_id, "COMPLETED", "DONE").await;

    for document_id in [
        processing_document_id,
        failed_document_id,
        completed_document_id,
    ] {
        sqlx::query(
            "INSERT INTO document_chunks (document_id, workspace_id, chunk_index, original_text) VALUES ($1, $2, 0, $3)",
        )
        .bind(document_id)
        .bind(workspace_id)
        .bind(format!("chunk-{document_id}"))
        .execute(&pool)
        .await
        .unwrap();
    }

    let acl_rows = fetch_completed_workspace_document_acl_rows(&pool, workspace_id)
        .await
        .unwrap();
    assert_eq!(acl_rows.len(), 1);
    assert_eq!(acl_rows[0].document_id, completed_document_id);

    let chunk_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM document_chunks WHERE workspace_id = $1 ORDER BY document_id",
    )
    .bind(workspace_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    let retrieved = fetch_chunks_by_ids(&pool, workspace_id, &chunk_ids)
        .await
        .unwrap();
    assert_eq!(retrieved.len(), 1);
    assert_eq!(retrieved[0].document_id, completed_document_id);
    assert_ne!(retrieved[0].document_id, processing_document_id);
    assert_ne!(retrieved[0].document_id, failed_document_id);
    cleanup(&pool, workspace_id, &user_id).await;
}

#[tokio::test]
async fn non_retryable_failure_marks_document_dead_immediately() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let (workspace_id, document_id, user_id) = seed_document(&pool, "PROCESSING").await;
    let config = test_worker_config(5);
    let job = enqueue_and_claim(&pool, workspace_id, document_id, "worker-permanent", config).await;
    assert_eq!(
        finish_job_failure(
            &pool,
            &job,
            "worker-permanent",
            JobFailure {
                code: "DOCUMENT_OBJECT_MISSING",
                message: "Original document object is missing",
                retryable: false,
            },
            config,
        )
        .await
        .unwrap(),
        Some(true)
    );
    let terminal: (String, String, String, String, Option<String>) = sqlx::query_as(
        "SELECT job.status, document.status, document.processing_stage, document.failure_code, document.failure_message FROM ingestion_jobs job JOIN documents document ON document.id = job.document_id WHERE job.id = $1",
    )
    .bind(job.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(terminal.0, "DEAD");
    assert_eq!(terminal.1, "FAILED");
    assert_eq!(terminal.2, "FAILED");
    assert_eq!(terminal.3, "DOCUMENT_OBJECT_MISSING");
    assert_eq!(
        terminal.4.as_deref(),
        Some("Original document object is missing")
    );
    assert!(
        claim_document_job(&pool, "worker-never-retries", config, document_id)
            .await
            .unwrap()
            .is_empty()
    );
    cleanup(&pool, workspace_id, &user_id).await;
}

#[tokio::test]
async fn needs_ocr_failure_persists_job_dead_and_document_failed() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let (workspace_id, document_id, user_id) = seed_document(&pool, "PROCESSING").await;
    let config = test_worker_config(5);
    let job = enqueue_and_claim(&pool, workspace_id, document_id, "worker-needs-ocr", config).await;

    let (code, message, retryable) = ProcessError::NeedsOcr.failure_kind();
    assert_eq!(code, "NEEDS_OCR");
    assert!(!retryable);

    assert_eq!(
        finish_job_failure(
            &pool,
            &job,
            "worker-needs-ocr",
            JobFailure {
                code,
                message,
                retryable,
            },
            config,
        )
        .await
        .unwrap(),
        Some(true)
    );

    let terminal: (String, String, String, String, Option<String>) = sqlx::query_as(
        "SELECT job.status, document.status, document.processing_stage, document.failure_code, document.failure_message FROM ingestion_jobs job JOIN documents document ON document.id = job.document_id WHERE job.id = $1",
    )
    .bind(job.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(terminal.0, "DEAD");
    assert_eq!(terminal.1, "FAILED");
    assert_eq!(terminal.2, "FAILED");
    assert_eq!(terminal.3, "NEEDS_OCR");
    assert_eq!(
        terminal.4.as_deref(),
        Some("Document requires OCR and no OCR provider is available")
    );
    assert!(
        claim_document_job(&pool, "worker-no-auto-retry", config, document_id)
            .await
            .unwrap()
            .is_empty(),
        "NEEDS_OCR must not re-enter the worker claim loop"
    );
    cleanup(&pool, workspace_id, &user_id).await;
}

#[tokio::test]
async fn two_workers_claim_one_job_and_stale_owner_cannot_update() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let (workspace_id, document_id, user_id) = seed_document(&pool, "PROCESSING").await;
    let mut tx = pool.begin().await.unwrap();
    enqueue_job_tx(&mut tx, document_id, workspace_id, 5)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let config = IngestionWorkerConfig {
        batch_size: 1,
        max_attempts: 5,
        lease_secs: 60,
        base_backoff_secs: 1,
        max_backoff_secs: 2,
    };
    let claims = join_all([
        claim_document_job(&pool, "worker-a", config, document_id),
        claim_document_job(&pool, "worker-b", config, document_id),
    ])
    .await;
    let mut jobs = claims
        .into_iter()
        .flat_map(Result::unwrap)
        .collect::<Vec<_>>();
    assert_eq!(
        jobs.len(),
        1,
        "exactly one worker owns a queued job; claimed={jobs:?}"
    );
    let first = jobs.pop().unwrap();
    sqlx::query("UPDATE ingestion_jobs SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = $1")
        .bind(first.id).execute(&pool).await.unwrap();
    let second = claim_document_job(&pool, "worker-c", config, document_id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_ne!(
        first.claim_token, second.claim_token,
        "reclaim gets a new ownership token"
    );
    assert!(
        !set_stage_for_owner(&pool, &first, "worker-a", STAGE_PARSING)
            .await
            .unwrap()
    );
    assert!(
        set_stage_for_owner(&pool, &second, "worker-c", STAGE_PARSING)
            .await
            .unwrap()
    );
    cleanup(&pool, workspace_id, &user_id).await;
}

#[tokio::test]
async fn concurrent_manual_retry_has_one_winner_and_one_active_job() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let (workspace_id, document_id, user_id) = seed_document(&pool, "FAILED").await;
    let winners =
        join_all((0..3).map(|_| retry_failed_document(&pool, document_id, workspace_id, 5))).await;
    assert_eq!(
        winners
            .into_iter()
            .filter_map(Result::ok)
            .filter(|won| *won)
            .count(),
        1
    );
    let active: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM ingestion_jobs WHERE document_id = $1 AND status IN ('QUEUED', 'PROCESSING')")
        .bind(document_id).fetch_one(&pool).await.unwrap();
    assert_eq!(active, 1);
    cleanup(&pool, workspace_id, &user_id).await;
}

#[tokio::test]
async fn retryable_and_exhausted_failures_follow_document_policy() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let (workspace_id, document_id, user_id) = seed_document(&pool, "PROCESSING").await;
    let mut tx = pool.begin().await.unwrap();
    enqueue_job_tx(&mut tx, document_id, workspace_id, 2)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let initial_attempts: i32 =
        sqlx::query_scalar("SELECT attempt_count FROM ingestion_jobs WHERE document_id = $1")
            .bind(document_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(initial_attempts, 0);
    let config = IngestionWorkerConfig {
        batch_size: 1,
        max_attempts: 2,
        lease_secs: 60,
        base_backoff_secs: 1,
        max_backoff_secs: 2,
    };
    let first = claim_document_job(&pool, "worker-a", config, document_id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        first.document_id, document_id,
        "claimed an unrelated job: {first:?}"
    );
    assert_eq!(first.attempt_count, 1);
    assert_eq!(
        finish_job_failure(
            &pool,
            &first,
            "worker-a",
            JobFailure {
                code: "QDRANT_INDEX_FAILED",
                message: "Vector indexing could not be completed",
                retryable: true,
            },
            config,
        )
        .await
        .unwrap(),
        Some(false)
    );
    let document: (String, String, Option<String>) = sqlx::query_as(
        "SELECT status, processing_stage, failure_code FROM documents WHERE id = $1",
    )
    .bind(document_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        document,
        ("PROCESSING".to_string(), "QUEUED".to_string(), None)
    );
    sqlx::query("UPDATE ingestion_jobs SET available_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = $1")
        .bind(first.id).execute(&pool).await.unwrap();
    let second = claim_document_job(&pool, "worker-b", config, document_id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        finish_job_failure(
            &pool,
            &second,
            "worker-b",
            JobFailure {
                code: "QDRANT_INDEX_FAILED",
                message: "Vector indexing could not be completed",
                retryable: true
            },
            config,
        )
        .await
        .unwrap(),
        Some(true)
    );
    let terminal: (String, String, String) = sqlx::query_as(
        "SELECT status, processing_stage, failure_code FROM documents WHERE id = $1",
    )
    .bind(document_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        terminal,
        (
            "FAILED".to_string(),
            "FAILED".to_string(),
            "INGESTION_MAX_ATTEMPTS_EXCEEDED".to_string()
        )
    );
    cleanup(&pool, workspace_id, &user_id).await;
}
