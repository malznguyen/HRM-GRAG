//! Lifecycle xoá tenant (operator/library) — LIFE-005.
//!
//! **Không** có public HTTP route. Chỉ dùng qua library helper / binary drill.
//!
//! Thứ tự bắt buộc trong cùng PostgreSQL transaction:
//! 1. Capture metadata tin cậy từ SQL (tenant name + workspace ids) **trước** cascade
//! 2. `DELETE FROM tenants` (cascade workspace/document SQL)
//! 3. Enqueue `qdrant_outbox` (`delete_by_workspaces` với ids đã capture, kể cả `[]`)
//! 4. Enqueue `storage_outbox` (`delete_prefix` prefix canonical `tenants/{tenant_id}/`)
//!
//! Worker thực thi outbox vẫn manual (`process-qdrant-outbox` / `process-storage-outbox`;
//! scheduling unattended = OPS-003). Không gọi S3/Qdrant trong path này.

use std::fmt;

use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::retrieval::outbox::enqueue_delete_by_workspaces_tx;
use crate::storage::cleanup::build_tenant_prefix;
use crate::storage::outbox::enqueue_delete_prefix_tx;

/// Metadata capture trước cascade — sống sót trong outbox payload sau khi SQL tenant đã mất.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantDeletePlan {
    pub tenant_id: Uuid,
    pub tenant_name: String,
    /// Workspace IDs capture tường minh (có thể rỗng — empty tenant, không silent omit).
    pub workspace_ids: Vec<Uuid>,
    pub storage_prefix: String,
    pub storage_bucket: String,
}

impl TenantDeletePlan {
    pub fn workspace_count(&self) -> usize {
        self.workspace_ids.len()
    }

    /// Danh sách rỗng là trạng thái tường minh (empty tenant), không phải lỗi capture.
    pub fn has_empty_workspace_list(&self) -> bool {
        self.workspace_ids.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantDeleteResult {
    pub plan: TenantDeletePlan,
    pub qdrant_outbox_id: Uuid,
    pub storage_outbox_id: Uuid,
}

#[derive(Debug)]
pub enum TenantCleanupError {
    Database(sqlx::Error),
    TenantNotFound { tenant_id: Uuid },
    EmptyBucket,
}

impl fmt::Display for TenantCleanupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TenantCleanupError::Database(err) => write!(f, "database error: {err}"),
            TenantCleanupError::TenantNotFound { tenant_id } => {
                write!(
                    f,
                    "tenant {tenant_id} not found — refuse silent cleanup success"
                )
            }
            TenantCleanupError::EmptyBucket => {
                write!(f, "storage bucket is empty (must come from runtime config)")
            }
        }
    }
}

impl std::error::Error for TenantCleanupError {}

impl From<sqlx::Error> for TenantCleanupError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

/// Đọc plan từ SQL (pool) — dry-run / preview, không mutate.
///
/// Tenant không tồn tại → `TenantNotFound` (không coi empty workspace list là thành công).
pub async fn capture_tenant_delete_plan(
    pool: &PgPool,
    tenant_id: Uuid,
    storage_bucket: &str,
) -> Result<TenantDeletePlan, TenantCleanupError> {
    let mut tx = pool.begin().await?;
    let plan = capture_tenant_delete_plan_tx(&mut tx, tenant_id, storage_bucket).await?;
    // Chỉ đọc — rollback để không giữ lock.
    tx.rollback().await?;
    Ok(plan)
}

/// Capture trong transaction đang mở (có thể `FOR UPDATE` tenant row).
pub async fn capture_tenant_delete_plan_tx(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    storage_bucket: &str,
) -> Result<TenantDeletePlan, TenantCleanupError> {
    let bucket = storage_bucket.trim();
    if bucket.is_empty() {
        return Err(TenantCleanupError::EmptyBucket);
    }

    // Khoá tenant row trong TX lifecycle — tránh concurrent delete/rename giữa capture và DELETE.
    let tenant_name: Option<String> =
        sqlx::query_scalar("SELECT name FROM tenants WHERE id = $1 FOR UPDATE")
            .bind(tenant_id)
            .fetch_optional(&mut **tx)
            .await?;

    let Some(tenant_name) = tenant_name else {
        return Err(TenantCleanupError::TenantNotFound { tenant_id });
    };

    // Capture workspace ids TRƯỚC cascade — sau DELETE tenants không còn row để resolve.
    let workspace_ids: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT id
        FROM workspaces
        WHERE tenant_id = $1
        ORDER BY id
        "#,
    )
    .bind(tenant_id)
    .fetch_all(&mut **tx)
    .await?;

    Ok(TenantDeletePlan {
        tenant_id,
        tenant_name,
        workspace_ids,
        storage_prefix: build_tenant_prefix(tenant_id),
        storage_bucket: bucket.to_string(),
    })
}

/// Transaction helper tái sử dụng: capture → SQL cascade → enqueue Qdrant + storage outbox.
///
/// Caller chịu trách nhiệm `commit` / `rollback`. Rollback → tenant còn, không có outbox row.
pub async fn delete_tenant_with_cleanup_tx(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    storage_bucket: &str,
) -> Result<TenantDeleteResult, TenantCleanupError> {
    let plan = capture_tenant_delete_plan_tx(tx, tenant_id, storage_bucket).await?;

    let outcome = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(&mut **tx)
        .await?;

    if outcome.rows_affected() == 0 {
        // FOR UPDATE đã thấy row; rows_affected=0 là bất thường — không silent success.
        return Err(TenantCleanupError::TenantNotFound { tenant_id });
    }

    // Payload workspace_ids tường minh kể cả khi rỗng (empty tenant).
    let qdrant_outbox_id = enqueue_delete_by_workspaces_tx(tx, &plan.workspace_ids).await?;

    // Prefix tenant-level + bucket từ config runtime tin cậy — không client/request input.
    let storage_outbox_id = enqueue_delete_prefix_tx(
        tx,
        &plan.storage_prefix,
        &plan.storage_bucket,
        Some(tenant_id),
        None,
    )
    .await?;

    Ok(TenantDeleteResult {
        plan,
        qdrant_outbox_id,
        storage_outbox_id,
    })
}

/// Begin + commit lifecycle đầy đủ (operator/drill). Không gọi S3/Qdrant.
pub async fn commit_tenant_delete_lifecycle(
    pool: &PgPool,
    tenant_id: Uuid,
    storage_bucket: &str,
) -> Result<TenantDeleteResult, TenantCleanupError> {
    let mut tx = pool.begin().await?;
    let result = delete_tenant_with_cleanup_tx(&mut tx, tenant_id, storage_bucket).await?;
    tx.commit().await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_workspace_list_is_explicit_flag() {
        let plan = TenantDeletePlan {
            tenant_id: Uuid::nil(),
            tenant_name: "empty".to_string(),
            workspace_ids: vec![],
            storage_prefix: build_tenant_prefix(Uuid::nil()),
            storage_bucket: "gmrag-documents".to_string(),
        };
        assert!(plan.has_empty_workspace_list());
        assert_eq!(plan.workspace_count(), 0);
        assert_eq!(
            plan.storage_prefix,
            "tenants/00000000-0000-0000-0000-000000000000/"
        );
    }

    #[test]
    fn populated_workspace_list_is_not_empty() {
        let ws = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let plan = TenantDeletePlan {
            tenant_id: Uuid::nil(),
            tenant_name: "populated".to_string(),
            workspace_ids: vec![ws],
            storage_prefix: build_tenant_prefix(Uuid::nil()),
            storage_bucket: "gmrag-documents".to_string(),
        };
        assert!(!plan.has_empty_workspace_list());
        assert_eq!(plan.workspace_count(), 1);
    }
}
