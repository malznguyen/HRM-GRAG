//! Storage object-delete outbox — recovery sau best-effort S3/MinIO delete fail/crash.
//!
//! Tách khỏi `authz_outbox` / `qdrant_outbox`: domain khác (object storage vs FGA/vector).
//!
//! Production processor:
//! - Claim batch bằng `FOR UPDATE SKIP LOCKED` + lease `next_attempt_at`
//! - Exponential backoff khi FAILED
//! - Poison (`DEAD`) khi payload hỏng hoặc hết max retries
//! - Missing object / empty prefix → success idempotent

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashSet;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::cleanup::cleanup_prefix_in_bucket;
use super::{StorageClient, StorageError};
use crate::outbox::{
    FailureDisposition, OutboxBackoffConfig, STATUS_DEAD, STATUS_FAILED, STATUS_PROCESSED,
    disposition_after_failure, parse_env_i32, parse_env_i64,
};

const DEFAULT_STORAGE_OUTBOX_BATCH_SIZE: i64 = 50;
const DEFAULT_STORAGE_OUTBOX_MAX_RETRIES: i32 = 5;
const DEFAULT_STORAGE_OUTBOX_BASE_BACKOFF_SECS: i64 = 2;
const DEFAULT_STORAGE_OUTBOX_MAX_BACKOFF_SECS: i64 = 300;
const DEFAULT_STORAGE_OUTBOX_CLAIM_LEASE_SECS: i64 = 120;
const MAX_ERROR_MESSAGE_LEN: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageOutboxEventType {
    DeleteObject,
    DeletePrefix,
}

impl StorageOutboxEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            StorageOutboxEventType::DeleteObject => "delete_object",
            StorageOutboxEventType::DeletePrefix => "delete_prefix",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "delete_object" => Some(Self::DeleteObject),
            "delete_prefix" => Some(Self::DeletePrefix),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteObjectPayload {
    pub object_key: String,
    pub bucket: String,
    pub workspace_id: Uuid,
    pub document_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletePrefixPayload {
    pub prefix: String,
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy)]
pub struct StorageOutboxProcessorConfig {
    pub batch_size: i64,
    pub max_retries: i32,
    pub backoff: OutboxBackoffConfig,
}

impl Default for StorageOutboxProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_STORAGE_OUTBOX_BATCH_SIZE,
            max_retries: DEFAULT_STORAGE_OUTBOX_MAX_RETRIES,
            backoff: OutboxBackoffConfig {
                base_backoff_secs: DEFAULT_STORAGE_OUTBOX_BASE_BACKOFF_SECS,
                max_backoff_secs: DEFAULT_STORAGE_OUTBOX_MAX_BACKOFF_SECS,
                claim_lease_secs: DEFAULT_STORAGE_OUTBOX_CLAIM_LEASE_SECS,
            },
        }
    }
}

impl StorageOutboxProcessorConfig {
    pub fn from_env() -> Self {
        Self {
            batch_size: parse_env_i64(
                "STORAGE_OUTBOX_BATCH_SIZE",
                DEFAULT_STORAGE_OUTBOX_BATCH_SIZE,
                1,
                500,
            ),
            max_retries: parse_env_i32(
                "STORAGE_OUTBOX_MAX_RETRIES",
                DEFAULT_STORAGE_OUTBOX_MAX_RETRIES,
                1,
                100,
            ),
            backoff: OutboxBackoffConfig {
                base_backoff_secs: parse_env_i64(
                    "STORAGE_OUTBOX_BASE_BACKOFF_SECS",
                    DEFAULT_STORAGE_OUTBOX_BASE_BACKOFF_SECS,
                    0,
                    3600,
                ),
                max_backoff_secs: parse_env_i64(
                    "STORAGE_OUTBOX_MAX_BACKOFF_SECS",
                    DEFAULT_STORAGE_OUTBOX_MAX_BACKOFF_SECS,
                    0,
                    86_400,
                ),
                claim_lease_secs: parse_env_i64(
                    "STORAGE_OUTBOX_CLAIM_LEASE_SECS",
                    DEFAULT_STORAGE_OUTBOX_CLAIM_LEASE_SECS,
                    10,
                    3600,
                ),
            },
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StorageOutboxRunResult {
    pub batches: usize,
    pub fetched_rows: usize,
    pub processed_rows: usize,
    pub failed_rows: usize,
    pub dead_rows: usize,
    pub skipped_max_retry_rows: i64,
}

#[derive(sqlx::FromRow)]
struct StorageOutboxRow {
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

pub async fn enqueue_delete_object(
    pool: &PgPool,
    object_key: &str,
    bucket: &str,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let id =
        enqueue_delete_object_tx(&mut tx, object_key, bucket, workspace_id, document_id).await?;
    tx.commit().await?;
    Ok(id)
}

/// Insert `delete_object` trong transaction lifecycle (cùng commit với SQL delete).
pub async fn enqueue_delete_object_tx(
    tx: &mut Transaction<'_, Postgres>,
    object_key: &str,
    bucket: &str,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let payload = json!({
        "object_key": object_key,
        "bucket": bucket,
        "workspace_id": workspace_id,
        "document_id": document_id,
    });
    enqueue_event_tx(tx, StorageOutboxEventType::DeleteObject, payload).await
}

/// Enqueue `delete_prefix` (dùng sau LIFE-004/LIFE-005; không wire workspace delete ở LIFE-003).
pub async fn enqueue_delete_prefix(
    pool: &PgPool,
    prefix: &str,
    bucket: &str,
    tenant_id: Option<Uuid>,
    workspace_id: Option<Uuid>,
) -> Result<Uuid, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let id = enqueue_delete_prefix_tx(&mut tx, prefix, bucket, tenant_id, workspace_id).await?;
    tx.commit().await?;
    Ok(id)
}

pub async fn enqueue_delete_prefix_tx(
    tx: &mut Transaction<'_, Postgres>,
    prefix: &str,
    bucket: &str,
    tenant_id: Option<Uuid>,
    workspace_id: Option<Uuid>,
) -> Result<Uuid, sqlx::Error> {
    let payload = json!({
        "prefix": prefix,
        "bucket": bucket,
        "tenant_id": tenant_id,
        "workspace_id": workspace_id,
    });
    enqueue_event_tx(tx, StorageOutboxEventType::DeletePrefix, payload).await
}

/// Ghi outbox row trong transaction đang mở — rollback theo transaction cha.
async fn enqueue_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    event_type: StorageOutboxEventType,
    payload: Value,
) -> Result<Uuid, sqlx::Error> {
    // next_attempt_at = now → worker claim được ngay (không delay enqueue).
    sqlx::query_scalar(
        r#"
        INSERT INTO storage_outbox (event_type, payload, status, retry_count, next_attempt_at)
        VALUES ($1, $2, 'PENDING', 0, CURRENT_TIMESTAMP)
        RETURNING id
        "#,
    )
    .bind(event_type.as_str())
    .bind(payload)
    .fetch_one(&mut **tx)
    .await
}

/// Chạy processor: claim theo batch → gọi S3 → mark PROCESSED / FAILED / DEAD.
pub async fn process_storage_outbox(
    pool: &PgPool,
    storage: &StorageClient,
    config: StorageOutboxProcessorConfig,
) -> Result<StorageOutboxRunResult, sqlx::Error> {
    let mut result = StorageOutboxRunResult::default();
    // Loại id đã xử lý trong run này khỏi claim — tránh vòng lặp khi backoff/lease = 0.
    let mut seen_ids: HashSet<Uuid> = HashSet::new();

    loop {
        let exclude: Vec<Uuid> = seen_ids.iter().copied().collect();
        let rows = claim_storage_outbox_batch(pool, &config, &exclude).await?;
        if rows.is_empty() {
            break;
        }

        result.batches += 1;
        result.fetched_rows += rows.len();

        for row in rows {
            seen_ids.insert(row.id);
            match process_single_row(pool, storage, &config, row).await {
                Ok(RowOutcome::Processed) => result.processed_rows += 1,
                Ok(RowOutcome::Failed) => result.failed_rows += 1,
                Ok(RowOutcome::Dead) => result.dead_rows += 1,
                Err(err) => {
                    error!(error = %err, "Failed to update storage_outbox row state");
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
        "Storage outbox processing completed"
    );

    Ok(result)
}

/// Claim batch an toàn multi-worker:
/// 1. `FOR UPDATE SKIP LOCKED` — worker khác bỏ qua row đang bị lock
/// 2. Đẩy `next_attempt_at` ra xa = lease tạm — nếu worker crash giữa chừng,
///    sau lease row sẽ claim lại được (không kẹt vĩnh viễn)
async fn claim_storage_outbox_batch(
    pool: &PgPool,
    config: &StorageOutboxProcessorConfig,
    exclude_ids: &[Uuid],
) -> Result<Vec<StorageOutboxRow>, sqlx::Error> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    let rows: Vec<StorageOutboxRow> = sqlx::query_as(
        r#"
        WITH candidates AS (
            SELECT id
            FROM storage_outbox
            WHERE status IN ('PENDING', 'FAILED')
              AND retry_count < $1
              AND next_attempt_at <= CURRENT_TIMESTAMP
              AND NOT (id = ANY($4::uuid[]))
            ORDER BY created_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE storage_outbox AS o
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
    storage: &StorageClient,
    config: &StorageOutboxProcessorConfig,
    row: StorageOutboxRow,
) -> Result<RowOutcome, sqlx::Error> {
    let event_type = match StorageOutboxEventType::parse(&row.event_type) {
        Some(event_type) => event_type,
        None => {
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

    let operation_result = match event_type {
        StorageOutboxEventType::DeleteObject => {
            match serde_json::from_value::<DeleteObjectPayload>(row.payload.clone()) {
                Ok(payload) => {
                    if let Err(reason) = validate_bucket(&payload.bucket) {
                        return mark_outbox_row_failure(
                            pool,
                            row.id,
                            row.retry_count,
                            reason.to_string(),
                            true,
                            config,
                        )
                        .await;
                    }
                    if let Err(reason) = validate_object_key(&payload.object_key) {
                        return mark_outbox_row_failure(
                            pool,
                            row.id,
                            row.retry_count,
                            reason.to_string(),
                            true,
                            config,
                        )
                        .await;
                    }
                    execute_delete_object(storage, &payload).await
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
        StorageOutboxEventType::DeletePrefix => {
            match serde_json::from_value::<DeletePrefixPayload>(row.payload.clone()) {
                Ok(payload) => {
                    if let Err(reason) = validate_bucket(&payload.bucket) {
                        return mark_outbox_row_failure(
                            pool,
                            row.id,
                            row.retry_count,
                            reason.to_string(),
                            true,
                            config,
                        )
                        .await;
                    }
                    if let Err(reason) = validate_prefix(&payload.prefix) {
                        return mark_outbox_row_failure(
                            pool,
                            row.id,
                            row.retry_count,
                            reason.to_string(),
                            true,
                            config,
                        )
                        .await;
                    }
                    execute_delete_prefix(storage, &payload).await
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

    match operation_result {
        Ok(()) => {
            mark_outbox_row_processed(pool, row.id).await?;
            Ok(RowOutcome::Processed)
        }
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

async fn execute_delete_object(
    storage: &StorageClient,
    payload: &DeleteObjectPayload,
) -> Result<(), StorageError> {
    // Dùng đúng payload.bucket từ SQL — không silent fallback sang S3_BUCKET runtime
    // (tránh xóa nhầm bucket / đánh dấu PROCESSED sau khi đổi cấu hình).
    match storage
        .delete_object_in_bucket(&payload.bucket, &payload.object_key)
        .await
    {
        Ok(()) => Ok(()),
        // Missing object = đã xóa / không tồn tại → success idempotent.
        Err(StorageError::ObjectNotFound { .. }) => Ok(()),
        Err(err) => Err(err),
    }
}

async fn execute_delete_prefix(
    storage: &StorageClient,
    payload: &DeletePrefixPayload,
) -> Result<(), StorageError> {
    cleanup_prefix_in_bucket(storage, &payload.bucket, payload.prefix.clone(), true)
        .await
        .map(|_| ())
        .map_err(|err| match err {
            super::cleanup::StorageCleanupError::Storage(storage_err) => storage_err,
            other => StorageError::OperationFailed {
                message: format!("storage prefix cleanup failed: {other}"),
            },
        })
}

async fn mark_outbox_row_processed(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE storage_outbox
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
    config: &StorageOutboxProcessorConfig,
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
                UPDATE storage_outbox
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
            // Poison: không còn claim tự động; operator inspect / cleanup-storage-objects.
            warn!(
                outbox_id = %id,
                retry_count = next_retry_count,
                permanent_error,
                error_message = %sanitized_message,
                "storage_outbox poison message marked DEAD"
            );

            sqlx::query(
                r#"
                UPDATE storage_outbox
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
        FROM storage_outbox
        WHERE status = $1
        "#,
    )
    .bind(STATUS_DEAD)
    .fetch_one(pool)
    .await
}

/// Bucket tin cậy từ SQL document row — rỗng = poison (không gọi S3).
fn validate_bucket(bucket: &str) -> Result<(), &'static str> {
    if bucket.trim().is_empty() {
        return Err("empty_bucket");
    }
    Ok(())
}

/// Key từ SQL/canonical builder — từ chối rỗng và traversal cơ bản.
fn validate_object_key(object_key: &str) -> Result<(), &'static str> {
    let key = object_key.trim();
    if key.is_empty() {
        return Err("empty_object_key");
    }
    if key.starts_with('/') || key.contains("..") || key.contains('\\') {
        return Err("invalid_object_key");
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<(), &'static str> {
    let prefix = prefix.trim();
    if prefix.is_empty() {
        return Err("empty_prefix");
    }
    if prefix.starts_with('/') || prefix.contains("..") || prefix.contains('\\') {
        return Err("invalid_prefix");
    }
    // Chỉ cho phép prefix canonical tenants/... — tránh xóa rộng ngoài scope.
    if !prefix.starts_with("tenants/") {
        return Err("prefix_not_tenant_scoped");
    }
    Ok(())
}

fn sanitize_error_message(err: &StorageError) -> String {
    match err {
        StorageError::ObjectNotFound { .. } => "object_not_found".to_string(),
        StorageError::OperationFailed { message } => {
            // Không log credentials/body; chỉ rút gọn class lỗi.
            if message.contains("timeout") || message.contains("Timeout") {
                "storage_timeout".to_string()
            } else if message.contains("connect") || message.contains("Connection") {
                "storage_connection_error".to_string()
            } else {
                "storage_operation_failed".to_string()
            }
        }
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
            StorageOutboxEventType::DeleteObject,
            StorageOutboxEventType::DeletePrefix,
        ] {
            assert_eq!(StorageOutboxEventType::parse(event.as_str()), Some(event));
        }
        assert_eq!(StorageOutboxEventType::parse("unknown"), None);
    }

    #[test]
    fn payload_serde_delete_object() {
        let payload = DeleteObjectPayload {
            object_key: "tenants/a/workspaces/b/documents/c/original.pdf".to_string(),
            bucket: "gmrag-documents".to_string(),
            workspace_id: Uuid::nil(),
            document_id: Uuid::nil(),
        };
        let value = serde_json::to_value(&payload).unwrap();
        let back: DeleteObjectPayload = serde_json::from_value(value).unwrap();
        assert_eq!(back, payload);
    }

    #[test]
    fn payload_serde_delete_prefix() {
        let payload = DeletePrefixPayload {
            prefix: "tenants/a/workspaces/b/".to_string(),
            bucket: "gmrag-documents".to_string(),
            tenant_id: Some(Uuid::nil()),
            workspace_id: Some(Uuid::nil()),
        };
        let value = serde_json::to_value(&payload).unwrap();
        let back: DeletePrefixPayload = serde_json::from_value(value).unwrap();
        assert_eq!(back, payload);
    }

    #[test]
    fn validate_bucket_rejects_empty() {
        assert!(validate_bucket("gmrag-documents").is_ok());
        assert!(validate_bucket("").is_err());
        assert!(validate_bucket("   ").is_err());
    }

    #[test]
    fn validate_object_key_rejects_traversal() {
        assert!(validate_object_key("tenants/x/original.pdf").is_ok());
        assert!(validate_object_key("").is_err());
        assert!(validate_object_key("../etc/passwd").is_err());
        assert!(validate_object_key("/absolute").is_err());
    }

    #[test]
    fn validate_prefix_requires_tenant_scope() {
        assert!(validate_prefix("tenants/x/workspaces/y/").is_ok());
        assert!(validate_prefix("").is_err());
        assert!(validate_prefix("other/prefix/").is_err());
        assert!(validate_prefix("tenants/../escape/").is_err());
    }

    #[test]
    fn config_default_backoff_matches_constants() {
        let config = StorageOutboxProcessorConfig::default();
        assert_eq!(config.batch_size, DEFAULT_STORAGE_OUTBOX_BATCH_SIZE);
        assert_eq!(config.max_retries, DEFAULT_STORAGE_OUTBOX_MAX_RETRIES);
        assert_eq!(
            config.backoff.base_backoff_secs,
            DEFAULT_STORAGE_OUTBOX_BASE_BACKOFF_SECS
        );
        assert_eq!(
            config.backoff.max_backoff_secs,
            DEFAULT_STORAGE_OUTBOX_MAX_BACKOFF_SECS
        );
        assert_eq!(
            config.backoff.claim_lease_secs,
            DEFAULT_STORAGE_OUTBOX_CLAIM_LEASE_SECS
        );
    }

    #[test]
    fn disposition_schedules_exponential_backoff() {
        let config = StorageOutboxProcessorConfig::default();
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
