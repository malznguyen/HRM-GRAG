//! Lifecycle xoá tenant (operator/library) — LIFE-005.
//!
//! **Không** có public HTTP route. Chỉ dùng qua library helper / binary drill.
//!
//! Thứ tự outbox trong cùng PostgreSQL transaction:
//! 1. Capture metadata tin cậy từ SQL (tenant name + workspace ids) **trước** cascade
//! 2. Enqueue `qdrant_outbox` (`delete_by_workspaces` với ids đã capture, kể cả `[]`)
//! 3. Enqueue `storage_outbox` (`delete_prefix` prefix canonical `tenants/{tenant_id}/`)
//! 4. `DELETE FROM tenants` (cascade workspace/document SQL)
//!
//! Worker thực thi outbox vẫn manual (`process-qdrant-outbox` / `process-storage-outbox`;
//! scheduling unattended = OPS-003). Không gọi S3/Qdrant trong path này.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use chrono::{NaiveDateTime, Utc};
use serde::Serialize;
use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::audit::{AuditEventRecord, AuditEventType, insert_audit_event_tx};
use crate::auth::authz::{AuthzClient, AuthzError, TupleKey};
use crate::retrieval::RetrievalClient;
use crate::retrieval::outbox::enqueue_delete_by_workspaces_tx;
use crate::storage::StorageClient;
use crate::storage::cleanup::build_tenant_prefix;
use crate::storage::cleanup::cleanup_prefix_in_bucket;
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

    let outcome = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(&mut **tx)
        .await?;

    if outcome.rows_affected() == 0 {
        // FOR UPDATE đã thấy row; rows_affected=0 là bất thường — không silent success.
        return Err(TenantCleanupError::TenantNotFound { tenant_id });
    }

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

const OPENFGA_DELETE_BATCH_SIZE: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantWorkspaceImpact {
    pub id: Uuid,
    pub name: String,
    pub document_count: i64,
}

#[derive(Debug, Clone)]
pub struct TenantDeleteImpact {
    pub plan: TenantDeletePlan,
    pub created_at: NaiveDateTime,
    pub owner_emails: Vec<String>,
    pub workspaces: Vec<TenantWorkspaceImpact>,
    pub document_count: i64,
    pub chunk_count: i64,
    pub graph_node_count: i64,
    pub chat_session_count: i64,
    pub document_ids: Vec<Uuid>,
    pub openfga_tuples: Vec<TupleKey>,
}

impl TenantDeleteImpact {
    pub fn openfga_tuples_by_relation(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for tuple in &self.openfga_tuples {
            *counts.entry(tuple.relation.clone()).or_default() += 1;
        }
        counts
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantNameMatch {
    pub tenant_id: Uuid,
    pub tenant_name: String,
    pub created_at: NaiveDateTime,
}

#[derive(Debug)]
pub enum OperatorTenantDeleteError {
    Database(sqlx::Error),
    Authorization(AuthzError),
    RecoveryFile {
        path: PathBuf,
        source: std::io::Error,
    },
    RecoverySerialization {
        path: PathBuf,
        source: serde_json::Error,
    },
    SqlAfterOpenFga {
        recovery_file: PathBuf,
        source: sqlx::Error,
    },
}

impl fmt::Display for OperatorTenantDeleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(_) => write!(f, "database operation failed before OpenFGA deletion"),
            Self::Authorization(_) => {
                write!(f, "OpenFGA tuple deletion failed; SQL was not changed")
            }
            Self::RecoveryFile { path, .. } | Self::RecoverySerialization { path, .. } => write!(
                f,
                "could not write required recovery file at {}",
                path.display()
            ),
            Self::SqlAfterOpenFga { recovery_file, .. } => write!(
                f,
                "SQL delete did not commit after OpenFGA tuples were removed; recovery file: {}",
                recovery_file.display()
            ),
        }
    }
}

impl std::error::Error for OperatorTenantDeleteError {}

impl From<sqlx::Error> for OperatorTenantDeleteError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

impl From<AuthzError> for OperatorTenantDeleteError {
    fn from(value: AuthzError) -> Self {
        Self::Authorization(value)
    }
}

pub enum OperatorTenantDeleteResult {
    NotFound,
    Deleted {
        impact: TenantDeleteImpact,
        recovery_file: PathBuf,
        qdrant_outbox_id: Uuid,
        storage_outbox_id: Uuid,
    },
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PostCommitCleanupResult {
    pub storage_succeeded: bool,
    pub qdrant_succeeded: bool,
}

#[derive(Serialize)]
struct TenantDeleteRecoveryFile<'a> {
    tenant_id: Uuid,
    tenant_name: &'a str,
    tenant_created_at: NaiveDateTime,
    openfga_tuples: &'a [TupleKey],
}

/// Tìm tenant theo tên chính xác để CLI từ chối khi tên không duy nhất.
pub async fn find_tenants_by_exact_name(
    pool: &PgPool,
    name: &str,
) -> Result<Vec<TenantNameMatch>, sqlx::Error> {
    sqlx::query_as::<_, (Uuid, String, NaiveDateTime)>(
        r#"
        SELECT id, name, created_at
        FROM tenants
        WHERE name = $1
        ORDER BY created_at ASC, id ASC
        "#,
    )
    .bind(name)
    .fetch_all(pool)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|(tenant_id, tenant_name, created_at)| TenantNameMatch {
                tenant_id,
                tenant_name,
                created_at,
            })
            .collect()
    })
}

/// Capture toàn bộ impact operator; dry-run chỉ đọc SQL và OpenFGA.
pub async fn capture_operator_tenant_delete_impact(
    pool: &PgPool,
    authz: &AuthzClient,
    tenant_id: Uuid,
    storage_bucket: &str,
) -> Result<Option<TenantDeleteImpact>, OperatorTenantDeleteError> {
    let mut tx = pool.begin().await?;
    let Some(mut impact) = capture_operator_impact_tx(&mut tx, tenant_id, storage_bucket).await?
    else {
        tx.rollback().await?;
        return Ok(None);
    };

    let all_tuples = authz.list_all_tuples().await?;
    impact.openfga_tuples = filter_tenant_subtree_tuples(&impact, all_tuples);
    tx.rollback().await?;
    Ok(Some(impact))
}

/// Xoá tenant theo operator lifecycle: recovery file → OpenFGA → SQL/outbox cùng transaction.
pub async fn execute_operator_tenant_delete(
    pool: &PgPool,
    authz: &AuthzClient,
    tenant_id: Uuid,
    storage_bucket: &str,
    actor: &str,
    recovery_dir: &Path,
) -> Result<OperatorTenantDeleteResult, OperatorTenantDeleteError> {
    let mut tx = pool.begin().await?;
    let Some(mut impact) = capture_operator_impact_tx(&mut tx, tenant_id, storage_bucket).await?
    else {
        tx.rollback().await?;
        return Ok(OperatorTenantDeleteResult::NotFound);
    };

    let all_tuples = match authz.list_all_tuples().await {
        Ok(tuples) => tuples,
        Err(error) => {
            tx.rollback().await?;
            return Err(error.into());
        }
    };
    impact.openfga_tuples = filter_tenant_subtree_tuples(&impact, all_tuples);

    let recovery_file = write_recovery_file(recovery_dir, &impact)?;

    if let Err(error) = delete_openfga_tuples(authz, &impact.openfga_tuples).await {
        tx.rollback().await?;
        return Err(error.into());
    }

    let sql_result = commit_operator_delete_tx(&mut tx, &impact, actor).await;
    match sql_result {
        Ok((qdrant_outbox_id, storage_outbox_id)) => {
            tx.commit()
                .await
                .map_err(|source| OperatorTenantDeleteError::SqlAfterOpenFga {
                    recovery_file: recovery_file.clone(),
                    source,
                })?;
            Ok(OperatorTenantDeleteResult::Deleted {
                impact,
                recovery_file,
                qdrant_outbox_id,
                storage_outbox_id,
            })
        }
        Err(source) => {
            let _ = tx.rollback().await;
            Err(OperatorTenantDeleteError::SqlAfterOpenFga {
                recovery_file,
                source,
            })
        }
    }
}

/// Sau commit chỉ thử cleanup inline; outbox mới là recovery bền vững.
pub async fn run_post_commit_cleanup(
    impact: &TenantDeleteImpact,
    storage: &StorageClient,
    retrieval: &RetrievalClient,
) -> PostCommitCleanupResult {
    let storage_succeeded = cleanup_prefix_in_bucket(
        storage,
        &impact.plan.storage_bucket,
        impact.plan.storage_prefix.clone(),
        true,
    )
    .await
    .is_ok();
    let qdrant_succeeded = retrieval
        .delete_points_by_workspaces(&impact.plan.workspace_ids)
        .await
        .is_ok();

    PostCommitCleanupResult {
        storage_succeeded,
        qdrant_succeeded,
    }
}

async fn capture_operator_impact_tx(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    storage_bucket: &str,
) -> Result<Option<TenantDeleteImpact>, sqlx::Error> {
    let bucket = storage_bucket.trim();
    if bucket.is_empty() {
        return Err(sqlx::Error::Protocol("storage bucket is empty".to_string()));
    }

    let tenant: Option<(String, NaiveDateTime)> =
        sqlx::query_as("SELECT name, created_at FROM tenants WHERE id = $1 FOR UPDATE")
            .bind(tenant_id)
            .fetch_optional(&mut **tx)
            .await?;
    let Some((tenant_name, created_at)) = tenant else {
        return Ok(None);
    };

    let owner_emails: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT u.email
        FROM tenant_members tm
        JOIN users u ON u.id = tm.user_id
        WHERE tm.tenant_id = $1
        ORDER BY u.email ASC
        "#,
    )
    .bind(tenant_id)
    .fetch_all(&mut **tx)
    .await?;

    let workspaces: Vec<TenantWorkspaceImpact> = sqlx::query_as::<_, (Uuid, String, i64)>(
        r#"
        SELECT w.id, w.name, COUNT(d.id)::bigint
        FROM workspaces w
        LEFT JOIN documents d ON d.workspace_id = w.id
        WHERE w.tenant_id = $1
        GROUP BY w.id, w.name
        ORDER BY w.id ASC
        "#,
    )
    .bind(tenant_id)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(|(id, name, document_count)| TenantWorkspaceImpact {
        id,
        name,
        document_count,
    })
    .collect();
    let workspace_ids: Vec<Uuid> = workspaces.iter().map(|workspace| workspace.id).collect();

    let document_ids: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT d.id
        FROM documents d
        JOIN workspaces w ON w.id = d.workspace_id
        WHERE w.tenant_id = $1
        ORDER BY d.id ASC
        "#,
    )
    .bind(tenant_id)
    .fetch_all(&mut **tx)
    .await?;

    let document_count = document_ids.len() as i64;
    let chunk_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM document_chunks dc
        JOIN workspaces w ON w.id = dc.workspace_id
        WHERE w.tenant_id = $1
        "#,
    )
    .bind(tenant_id)
    .fetch_one(&mut **tx)
    .await?;
    let graph_node_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM graph_nodes WHERE workspace_id = ANY($1::uuid[])",
    )
    .bind(&workspace_ids)
    .fetch_one(&mut **tx)
    .await?;
    let chat_session_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM chat_sessions WHERE workspace_id = ANY($1::uuid[])",
    )
    .bind(&workspace_ids)
    .fetch_one(&mut **tx)
    .await?;

    Ok(Some(TenantDeleteImpact {
        plan: TenantDeletePlan {
            tenant_id,
            tenant_name,
            workspace_ids,
            storage_prefix: build_tenant_prefix(tenant_id),
            storage_bucket: bucket.to_string(),
        },
        created_at,
        owner_emails,
        workspaces,
        document_count,
        chunk_count,
        graph_node_count,
        chat_session_count,
        document_ids,
        openfga_tuples: Vec::new(),
    }))
}

fn filter_tenant_subtree_tuples(
    impact: &TenantDeleteImpact,
    tuples: Vec<TupleKey>,
) -> Vec<TupleKey> {
    let workspace_objects: HashSet<String> = impact
        .plan
        .workspace_ids
        .iter()
        .map(|id| format!("workspace:{id}"))
        .collect();
    let document_objects: HashSet<String> = impact
        .document_ids
        .iter()
        .map(|id| format!("document:{id}"))
        .collect();
    let tenant_object = format!("tenant:{}", impact.plan.tenant_id);

    tuples
        .into_iter()
        .filter(|tuple| {
            tuple.object == tenant_object
                || workspace_objects.contains(&tuple.object)
                || document_objects.contains(&tuple.object)
        })
        .collect()
}

fn write_recovery_file(
    recovery_dir: &Path,
    impact: &TenantDeleteImpact,
) -> Result<PathBuf, OperatorTenantDeleteError> {
    let timestamp = Utc::now().format("%Y%m%dT%H%M%S%.3fZ");
    for suffix in 0..1000 {
        let filename = if suffix == 0 {
            format!(
                "tenant-delete-recovery-{}-{timestamp}.json",
                impact.plan.tenant_id
            )
        } else {
            format!(
                "tenant-delete-recovery-{}-{timestamp}-{suffix}.json",
                impact.plan.tenant_id
            )
        };
        let path = recovery_dir.join(filename);
        let file = match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(OperatorTenantDeleteError::RecoveryFile { path, source }),
        };
        let recovery = TenantDeleteRecoveryFile {
            tenant_id: impact.plan.tenant_id,
            tenant_name: &impact.plan.tenant_name,
            tenant_created_at: impact.created_at,
            openfga_tuples: &impact.openfga_tuples,
        };
        let mut writer = std::io::BufWriter::new(file);
        if let Err(source) = serde_json::to_writer_pretty(&mut writer, &recovery) {
            let _ = fs::remove_file(&path);
            return Err(OperatorTenantDeleteError::RecoverySerialization { path, source });
        }
        if let Err(source) = writer.write_all(b"\n").and_then(|_| writer.flush()) {
            let _ = fs::remove_file(&path);
            return Err(OperatorTenantDeleteError::RecoveryFile { path, source });
        }
        return Ok(path);
    }

    let path = recovery_dir.join(format!(
        "tenant-delete-recovery-{}-collision.json",
        impact.plan.tenant_id
    ));
    Err(OperatorTenantDeleteError::RecoveryFile {
        path,
        source: std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "recovery filename collision limit reached",
        ),
    })
}

async fn delete_openfga_tuples(authz: &AuthzClient, tuples: &[TupleKey]) -> Result<(), AuthzError> {
    for batch in tuples.chunks(OPENFGA_DELETE_BATCH_SIZE) {
        match authz.write_tuples(Vec::new(), batch.to_vec()).await {
            Ok(()) => {}
            Err(error) if is_missing_tuple_error(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn is_missing_tuple_error(error: &AuthzError) -> bool {
    let AuthzError::OpenFga { body, .. } = error else {
        return false;
    };
    let body = body.to_ascii_lowercase();
    body.contains("does not exist") || body.contains("not found")
}

async fn commit_operator_delete_tx(
    tx: &mut Transaction<'_, Postgres>,
    impact: &TenantDeleteImpact,
    actor: &str,
) -> Result<(Uuid, Uuid), sqlx::Error> {
    let qdrant_outbox_id = enqueue_delete_by_workspaces_tx(tx, &impact.plan.workspace_ids).await?;
    let storage_outbox_id = enqueue_delete_prefix_tx(
        tx,
        &impact.plan.storage_prefix,
        &impact.plan.storage_bucket,
        Some(impact.plan.tenant_id),
        None,
    )
    .await?;
    insert_audit_event_tx(
        tx,
        AuditEventRecord::new(AuditEventType::TenantDeleted)
            .with_actor_user_id(actor)
            .with_tenant_id(impact.plan.tenant_id)
            .with_target("tenant", impact.plan.tenant_id.to_string())
            .with_metadata(json!({
                "tenant_name": impact.plan.tenant_name,
                "workspace_count": impact.workspaces.len(),
                "document_count": impact.document_count,
                "openfga_tuple_count": impact.openfga_tuples.len(),
                "public_api": false,
            })),
    )
    .await?;
    let deleted = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(impact.plan.tenant_id)
        .execute(&mut **tx)
        .await?;
    if deleted.rows_affected() != 1 {
        return Err(sqlx::Error::Protocol(
            "tenant disappeared before delete could commit".to_string(),
        ));
    }
    Ok((qdrant_outbox_id, storage_outbox_id))
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
