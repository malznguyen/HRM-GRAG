//! Qdrant point-delete outbox — recovery sau best-effort delete fail/timeout.
//!
//! Tách khỏi `authz_outbox`: domain khác (OpenFGA tuples vs vector points),
//! processor và failure mode khác nhau.
//!
//! Production processor:
//! - Claim batch bằng `FOR UPDATE SKIP LOCKED` + lease `next_attempt_at`
//! - Exponential backoff khi FAILED
//! - Poison (`DEAD`) khi payload hỏng hoặc hết max retries

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashSet;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::{RetrievalClient, RetrievalError};
use crate::outbox::{
    FailureDisposition, OutboxBackoffConfig, STATUS_DEAD, STATUS_FAILED, STATUS_PROCESSED,
    disposition_after_failure, parse_env_i32, parse_env_i64,
};

const DEFAULT_QDRANT_OUTBOX_BATCH_SIZE: i64 = 50;
const DEFAULT_QDRANT_OUTBOX_MAX_RETRIES: i32 = 5;
const DEFAULT_QDRANT_OUTBOX_BASE_BACKOFF_SECS: i64 = 2;
const DEFAULT_QDRANT_OUTBOX_MAX_BACKOFF_SECS: i64 = 300;
const DEFAULT_QDRANT_OUTBOX_CLAIM_LEASE_SECS: i64 = 120;
const MAX_ERROR_MESSAGE_LEN: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QdrantOutboxEventType {
    DeleteByDocument,
    DeleteByWorkspace,
    DeleteByWorkspaces,
}

impl QdrantOutboxEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            QdrantOutboxEventType::DeleteByDocument => "delete_by_document",
            QdrantOutboxEventType::DeleteByWorkspace => "delete_by_workspace",
            QdrantOutboxEventType::DeleteByWorkspaces => "delete_by_workspaces",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "delete_by_document" => Some(Self::DeleteByDocument),
            "delete_by_workspace" => Some(Self::DeleteByWorkspace),
            "delete_by_workspaces" => Some(Self::DeleteByWorkspaces),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteByDocumentPayload {
    pub workspace_id: Uuid,
    pub document_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteByWorkspacePayload {
    pub workspace_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteByWorkspacesPayload {
    pub workspace_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Copy)]
pub struct QdrantOutboxProcessorConfig {
    pub batch_size: i64,
    pub max_retries: i32,
    pub backoff: OutboxBackoffConfig,
}

impl Default for QdrantOutboxProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_QDRANT_OUTBOX_BATCH_SIZE,
            max_retries: DEFAULT_QDRANT_OUTBOX_MAX_RETRIES,
            backoff: OutboxBackoffConfig {
                base_backoff_secs: DEFAULT_QDRANT_OUTBOX_BASE_BACKOFF_SECS,
                max_backoff_secs: DEFAULT_QDRANT_OUTBOX_MAX_BACKOFF_SECS,
                claim_lease_secs: DEFAULT_QDRANT_OUTBOX_CLAIM_LEASE_SECS,
            },
        }
    }
}

impl QdrantOutboxProcessorConfig {
    pub fn from_env() -> Self {
        Self {
            batch_size: parse_env_i64(
                "QDRANT_OUTBOX_BATCH_SIZE",
                DEFAULT_QDRANT_OUTBOX_BATCH_SIZE,
                1,
                500,
            ),
            max_retries: parse_env_i32(
                "QDRANT_OUTBOX_MAX_RETRIES",
                DEFAULT_QDRANT_OUTBOX_MAX_RETRIES,
                1,
                100,
            ),
            backoff: OutboxBackoffConfig {
                base_backoff_secs: parse_env_i64(
                    "QDRANT_OUTBOX_BASE_BACKOFF_SECS",
                    DEFAULT_QDRANT_OUTBOX_BASE_BACKOFF_SECS,
                    0,
                    3600,
                ),
                max_backoff_secs: parse_env_i64(
                    "QDRANT_OUTBOX_MAX_BACKOFF_SECS",
                    DEFAULT_QDRANT_OUTBOX_MAX_BACKOFF_SECS,
                    0,
                    86_400,
                ),
                claim_lease_secs: parse_env_i64(
                    "QDRANT_OUTBOX_CLAIM_LEASE_SECS",
                    DEFAULT_QDRANT_OUTBOX_CLAIM_LEASE_SECS,
                    10,
                    3600,
                ),
            },
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct QdrantOutboxRunResult {
    pub batches: usize,
    pub fetched_rows: usize,
    pub processed_rows: usize,
    pub failed_rows: usize,
    pub dead_rows: usize,
    pub skipped_max_retry_rows: i64,
}

#[derive(sqlx::FromRow)]
struct QdrantOutboxRow {
    id: Uuid,
    event_type: String,
    payload: Value,
    retry_count: i32,
}

enum RowOutcome {
    Processed,
    Failed,
    Dead,
}

pub async fn enqueue_delete_by_document(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let id = enqueue_delete_by_document_tx(&mut tx, workspace_id, document_id).await?;
    tx.commit().await?;
    Ok(id)
}

/// Insert `delete_by_document` trong transaction lifecycle (cùng commit với SQL delete).
pub async fn enqueue_delete_by_document_tx(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let payload = json!({
        "workspace_id": workspace_id,
        "document_id": document_id,
    });
    enqueue_event_tx(tx, QdrantOutboxEventType::DeleteByDocument, payload).await
}

pub async fn enqueue_delete_by_workspace(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let id = enqueue_delete_by_workspace_tx(&mut tx, workspace_id).await?;
    tx.commit().await?;
    Ok(id)
}

/// Insert `delete_by_workspace` trong transaction lifecycle (cùng commit với SQL delete).
pub async fn enqueue_delete_by_workspace_tx(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let payload = json!({
        "workspace_id": workspace_id,
    });
    enqueue_event_tx(tx, QdrantOutboxEventType::DeleteByWorkspace, payload).await
}

pub async fn enqueue_delete_by_workspaces(
    pool: &PgPool,
    workspace_ids: &[Uuid],
) -> Result<Uuid, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let id = enqueue_delete_by_workspaces_tx(&mut tx, workspace_ids).await?;
    tx.commit().await?;
    Ok(id)
}

/// Insert `delete_by_workspaces` trong transaction lifecycle (tenant cascade: LIFE-005).
///
/// `workspace_ids` có thể rỗng — payload vẫn ghi `[]` tường minh (không silent omit).
/// Caller phải capture ids **trước** SQL cascade; sau cascade không resolve lại được.
pub async fn enqueue_delete_by_workspaces_tx(
    tx: &mut Transaction<'_, Postgres>,
    workspace_ids: &[Uuid],
) -> Result<Uuid, sqlx::Error> {
    let payload = json!({
        "workspace_ids": workspace_ids,
    });
    enqueue_event_tx(tx, QdrantOutboxEventType::DeleteByWorkspaces, payload).await
}

/// Ghi outbox row trong transaction đang mở — rollback theo transaction cha.
async fn enqueue_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    event_type: QdrantOutboxEventType,
    payload: Value,
) -> Result<Uuid, sqlx::Error> {
    // next_attempt_at = now → worker claim được ngay (không delay enqueue).
    sqlx::query_scalar(
        r#"
        INSERT INTO qdrant_outbox (event_type, payload, status, retry_count, next_attempt_at)
        VALUES ($1, $2, 'PENDING', 0, CURRENT_TIMESTAMP)
        RETURNING id
        "#,
    )
    .bind(event_type.as_str())
    .bind(payload)
    .fetch_one(&mut **tx)
    .await
}

/// Chạy processor: claim theo batch → gọi Qdrant → mark PROCESSED / FAILED / DEAD.
pub async fn process_qdrant_outbox(
    pool: &PgPool,
    retrieval: &RetrievalClient,
    config: QdrantOutboxProcessorConfig,
) -> Result<QdrantOutboxRunResult, sqlx::Error> {
    let mut result = QdrantOutboxRunResult::default();
    // Loại id đã xử lý trong run này khỏi claim — tránh vòng lặp khi backoff/lease = 0.
    let mut seen_ids: HashSet<Uuid> = HashSet::new();

    loop {
        let exclude: Vec<Uuid> = seen_ids.iter().copied().collect();
        let rows = claim_qdrant_outbox_batch(pool, &config, &exclude).await?;
        if rows.is_empty() {
            break;
        }

        result.batches += 1;
        result.fetched_rows += rows.len();

        for row in rows {
            seen_ids.insert(row.id);
            match process_single_row(pool, retrieval, &config, row).await {
                Ok(RowOutcome::Processed) => result.processed_rows += 1,
                Ok(RowOutcome::Failed) => result.failed_rows += 1,
                Ok(RowOutcome::Dead) => result.dead_rows += 1,
                Err(err) => {
                    error!(error = %err, "Failed to update qdrant_outbox row state");
                    return Err(err);
                }
            }
        }
    }

    result.skipped_max_retry_rows = count_dead_rows(pool).await?;

    info!(
        batches = result.batches,
        fetched_rows = result.fetched_rows,
        processed_rows = result.processed_rows,
        failed_rows = result.failed_rows,
        dead_rows = result.dead_rows,
        skipped_max_retry_rows = result.skipped_max_retry_rows,
        "Qdrant outbox processing completed"
    );

    Ok(result)
}

/// Claim batch an toàn multi-worker:
/// 1. `FOR UPDATE SKIP LOCKED` — worker khác bỏ qua row đang bị lock
/// 2. Đẩy `next_attempt_at` ra xa = lease tạm — nếu worker crash giữa chừng,
///    sau lease row sẽ claim lại được (không kẹt vĩnh viễn)
async fn claim_qdrant_outbox_batch(
    pool: &PgPool,
    config: &QdrantOutboxProcessorConfig,
    exclude_ids: &[Uuid],
) -> Result<Vec<QdrantOutboxRow>, sqlx::Error> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    // Chỉ claim row đủ điều kiện: PENDING/FAILED, chưa hết retry, đến giờ retry.
    // exclude_ids: id đã xử lý trong run hiện tại (tránh re-claim khi delay=0).
    let rows: Vec<QdrantOutboxRow> = sqlx::query_as(
        r#"
        WITH candidates AS (
            SELECT id
            FROM qdrant_outbox
            WHERE status IN ('PENDING', 'FAILED')
              AND retry_count < $1
              AND next_attempt_at <= CURRENT_TIMESTAMP
              AND NOT (id = ANY($4::uuid[]))
            ORDER BY created_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE qdrant_outbox AS o
        SET next_attempt_at = CURRENT_TIMESTAMP + make_interval(secs => $3::double precision),
            updated_at = CURRENT_TIMESTAMP
        FROM candidates
        WHERE o.id = candidates.id
        RETURNING o.id, o.event_type, o.payload, o.retry_count
        "#,
    )
    .bind(config.max_retries)
    .bind(config.batch_size)
    .bind(config.backoff.claim_lease_secs as f64)
    .bind(exclude_ids)
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(rows)
}

async fn process_single_row(
    pool: &PgPool,
    retrieval: &RetrievalClient,
    config: &QdrantOutboxProcessorConfig,
    row: QdrantOutboxRow,
) -> Result<RowOutcome, sqlx::Error> {
    let event_type = match QdrantOutboxEventType::parse(&row.event_type) {
        Some(event_type) => event_type,
        None => {
            // Event type lạ không tự hết → poison ngay, không đốt retry.
            return mark_outbox_row_failure(
                pool,
                row.id,
                row.retry_count,
                "unsupported_event_type".to_string(),
                true,
                config,
            )
            .await;
        }
    };

    let delete_result = match event_type {
        QdrantOutboxEventType::DeleteByDocument => {
            match serde_json::from_value::<DeleteByDocumentPayload>(row.payload.clone()) {
                Ok(payload) => {
                    retrieval
                        .delete_points_by_document(payload.workspace_id, payload.document_id)
                        .await
                }
                Err(_) => {
                    return mark_outbox_row_failure(
                        pool,
                        row.id,
                        row.retry_count,
                        "invalid_payload".to_string(),
                        true,
                        config,
                    )
                    .await;
                }
            }
        }
        QdrantOutboxEventType::DeleteByWorkspace => {
            match serde_json::from_value::<DeleteByWorkspacePayload>(row.payload.clone()) {
                Ok(payload) => {
                    retrieval
                        .delete_points_by_workspace(payload.workspace_id)
                        .await
                }
                Err(_) => {
                    return mark_outbox_row_failure(
                        pool,
                        row.id,
                        row.retry_count,
                        "invalid_payload".to_string(),
                        true,
                        config,
                    )
                    .await;
                }
            }
        }
        QdrantOutboxEventType::DeleteByWorkspaces => {
            match serde_json::from_value::<DeleteByWorkspacesPayload>(row.payload.clone()) {
                Ok(payload) => {
                    retrieval
                        .delete_points_by_workspaces(&payload.workspace_ids)
                        .await
                }
                Err(_) => {
                    return mark_outbox_row_failure(
                        pool,
                        row.id,
                        row.retry_count,
                        "invalid_payload".to_string(),
                        true,
                        config,
                    )
                    .await;
                }
            }
        }
    };

    match delete_result {
        Ok(()) => {
            mark_outbox_row_processed(pool, row.id).await?;
            Ok(RowOutcome::Processed)
        }
        // Filter-delete không match point nào vẫn Ok từ client — idempotent sẵn.
        Err(err) => {
            mark_outbox_row_failure(
                pool,
                row.id,
                row.retry_count,
                sanitize_error_message(&err),
                false,
                config,
            )
            .await
        }
    }
}

async fn mark_outbox_row_processed(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE qdrant_outbox
        SET status = $2,
            error_message = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(STATUS_PROCESSED)
    .execute(pool)
    .await?;

    Ok(())
}

/// Mark FAILED (kèm backoff) hoặc DEAD (poison / hết retry).
async fn mark_outbox_row_failure(
    pool: &PgPool,
    id: Uuid,
    retry_count: i32,
    error_message: String,
    permanent_error: bool,
    config: &QdrantOutboxProcessorConfig,
) -> Result<RowOutcome, sqlx::Error> {
    let sanitized_message = truncate_error_message(error_message);
    let disposition = disposition_after_failure(
        retry_count,
        config.max_retries,
        permanent_error,
        config.backoff,
    );

    match disposition {
        FailureDisposition::Retryable {
            next_retry_count,
            backoff_secs,
        } => {
            sqlx::query(
                r#"
                UPDATE qdrant_outbox
                SET status = $2,
                    retry_count = $3,
                    error_message = $4,
                    next_attempt_at = CURRENT_TIMESTAMP
                        + make_interval(secs => $5::double precision),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                "#,
            )
            .bind(id)
            .bind(STATUS_FAILED)
            .bind(next_retry_count)
            .bind(&sanitized_message)
            .bind(backoff_secs as f64)
            .execute(pool)
            .await?;

            Ok(RowOutcome::Failed)
        }
        FailureDisposition::Dead { next_retry_count } => {
            // Poison: không còn claim tự động; operator inspect / cleanup-qdrant-orphans.
            warn!(
                outbox_id = %id,
                retry_count = next_retry_count,
                permanent_error,
                error_message = %sanitized_message,
                "qdrant_outbox poison message marked DEAD"
            );

            sqlx::query(
                r#"
                UPDATE qdrant_outbox
                SET status = $2,
                    retry_count = $3,
                    error_message = $4,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                "#,
            )
            .bind(id)
            .bind(STATUS_DEAD)
            .bind(next_retry_count)
            .bind(&sanitized_message)
            .execute(pool)
            .await?;

            Ok(RowOutcome::Dead)
        }
    }
}

async fn count_dead_rows(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM qdrant_outbox
        WHERE status = $1
        "#,
    )
    .bind(STATUS_DEAD)
    .fetch_one(pool)
    .await
}

fn sanitize_error_message(err: &RetrievalError) -> String {
    match err {
        RetrievalError::Http(_) => "qdrant_http_error".to_string(),
        RetrievalError::Timeout { timeout_secs, .. } => {
            format!("qdrant_delete_timeout_{timeout_secs}s")
        }
        RetrievalError::Api { status, .. } => format!("qdrant_status_{}", status.as_u16()),
        RetrievalError::EmptyTenantWorkspaceList { .. } => {
            "empty_tenant_workspace_list".to_string()
        }
        RetrievalError::Database(_) => "qdrant_database_error".to_string(),
        RetrievalError::InvalidPointId { .. } => "invalid_point_id".to_string(),
        RetrievalError::InvalidEmbeddingLiteral { .. } => "invalid_embedding".to_string(),
    }
}

fn truncate_error_message(message: String) -> String {
    if message.len() <= MAX_ERROR_MESSAGE_LEN {
        return message;
    }

    message.chars().take(MAX_ERROR_MESSAGE_LEN).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::compute_backoff_secs;

    #[test]
    fn event_type_roundtrip() {
        for event in [
            QdrantOutboxEventType::DeleteByDocument,
            QdrantOutboxEventType::DeleteByWorkspace,
            QdrantOutboxEventType::DeleteByWorkspaces,
        ] {
            assert_eq!(QdrantOutboxEventType::parse(event.as_str()), Some(event));
        }
        assert_eq!(QdrantOutboxEventType::parse("unknown"), None);
    }

    #[test]
    fn payload_serde_document() {
        let payload = DeleteByDocumentPayload {
            workspace_id: Uuid::nil(),
            document_id: Uuid::nil(),
        };
        let value = serde_json::to_value(&payload).unwrap();
        let back: DeleteByDocumentPayload = serde_json::from_value(value).unwrap();
        assert_eq!(back, payload);
    }

    #[test]
    fn config_default_backoff_matches_constants() {
        let config = QdrantOutboxProcessorConfig::default();
        assert_eq!(config.batch_size, DEFAULT_QDRANT_OUTBOX_BATCH_SIZE);
        assert_eq!(config.max_retries, DEFAULT_QDRANT_OUTBOX_MAX_RETRIES);
        assert_eq!(
            config.backoff.base_backoff_secs,
            DEFAULT_QDRANT_OUTBOX_BASE_BACKOFF_SECS
        );
        assert_eq!(
            config.backoff.max_backoff_secs,
            DEFAULT_QDRANT_OUTBOX_MAX_BACKOFF_SECS
        );
        assert_eq!(
            config.backoff.claim_lease_secs,
            DEFAULT_QDRANT_OUTBOX_CLAIM_LEASE_SECS
        );
    }

    #[test]
    fn disposition_schedules_exponential_backoff() {
        let config = QdrantOutboxProcessorConfig::default();
        let d1 = disposition_after_failure(0, config.max_retries, false, config.backoff);
        let d2 = disposition_after_failure(1, config.max_retries, false, config.backoff);
        match (d1, d2) {
            (
                FailureDisposition::Retryable {
                    backoff_secs: b1, ..
                },
                FailureDisposition::Retryable {
                    backoff_secs: b2, ..
                },
            ) => {
                assert_eq!(b1, compute_backoff_secs(1, 2, 300));
                assert_eq!(b2, compute_backoff_secs(2, 2, 300));
                assert!(b2 > b1);
            }
            other => panic!("expected two Retryable, got {other:?}"),
        }
    }

    #[test]
    fn status_constants_stable() {
        use crate::outbox::STATUS_PENDING;
        assert_eq!(STATUS_PENDING, "PENDING");
        assert_eq!(STATUS_PROCESSED, "PROCESSED");
        assert_eq!(STATUS_FAILED, "FAILED");
        assert_eq!(STATUS_DEAD, "DEAD");
    }
}
