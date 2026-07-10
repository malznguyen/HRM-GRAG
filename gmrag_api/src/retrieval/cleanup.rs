//! Operator cleanup cho orphan Qdrant points.
//!
//! Ưu tiên: replay từ `qdrant_outbox` PENDING/FAILED/DEAD + audit delete fail.
//! Full collection scroll chỉ khi `--full-scan` (đắt trên collection lớn).

use serde_json::{Value, json};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use super::outbox::{
    QdrantOutboxEventType, enqueue_delete_by_document, enqueue_delete_by_workspace,
    enqueue_delete_by_workspaces,
};
use super::{RetrievalClient, RetrievalError};

#[derive(Debug, Clone)]
pub struct QdrantCleanupOptions {
    pub dry_run: bool,
    pub delete: bool,
    pub workspace_id: Option<Uuid>,
    pub tenant_id: Option<Uuid>,
    /// Bật scroll toàn collection — mặc định tắt (đắt).
    pub full_scan: bool,
    pub scroll_page_size: usize,
    /// Cho phép `--delete` khi workspace/tenant vẫn còn trong SQL (mặc định refuse).
    pub force: bool,
}

impl Default for QdrantCleanupOptions {
    fn default() -> Self {
        Self {
            dry_run: true,
            delete: false,
            workspace_id: None,
            tenant_id: None,
            full_scan: false,
            scroll_page_size: 256,
            force: false,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct QdrantCleanupReport {
    pub mode: String,
    pub dry_run: bool,
    pub candidates_from_outbox: usize,
    pub candidates_from_audit: usize,
    pub candidates_from_full_scan: usize,
    pub unique_delete_targets: usize,
    pub deletes_attempted: usize,
    pub deletes_succeeded: usize,
    pub deletes_failed: usize,
    pub outbox_requeued: usize,
    pub errors: Vec<String>,
    pub sample_targets: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum DeleteTarget {
    Document {
        workspace_id: Uuid,
        document_id: Uuid,
    },
    Workspace {
        workspace_id: Uuid,
    },
    Workspaces {
        workspace_ids: Vec<Uuid>,
    },
}

impl DeleteTarget {
    fn to_json(&self) -> Value {
        match self {
            DeleteTarget::Document {
                workspace_id,
                document_id,
            } => json!({
                "kind": "document",
                "workspace_id": workspace_id,
                "document_id": document_id,
            }),
            DeleteTarget::Workspace { workspace_id } => json!({
                "kind": "workspace",
                "workspace_id": workspace_id,
            }),
            DeleteTarget::Workspaces { workspace_ids } => json!({
                "kind": "workspaces",
                "workspace_ids": workspace_ids,
                "count": workspace_ids.len(),
            }),
        }
    }
}

#[derive(Debug)]
pub enum QdrantCleanupError {
    Retrieval(RetrievalError),
    Database(sqlx::Error),
    InvalidArgs(String),
}

impl std::fmt::Display for QdrantCleanupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QdrantCleanupError::Retrieval(err) => write!(f, "{err}"),
            QdrantCleanupError::Database(err) => write!(f, "database error: {err}"),
            QdrantCleanupError::InvalidArgs(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for QdrantCleanupError {}

impl From<RetrievalError> for QdrantCleanupError {
    fn from(value: RetrievalError) -> Self {
        Self::Retrieval(value)
    }
}

impl From<sqlx::Error> for QdrantCleanupError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

/// Chạy cleanup orphan Qdrant theo options operator.
pub async fn cleanup_qdrant_orphans(
    pool: &PgPool,
    retrieval: &RetrievalClient,
    options: &QdrantCleanupOptions,
) -> Result<QdrantCleanupReport, QdrantCleanupError> {
    if options.delete && options.dry_run {
        return Err(QdrantCleanupError::InvalidArgs(
            "cannot combine --delete with dry-run".to_string(),
        ));
    }

    let mut report = QdrantCleanupReport {
        dry_run: !options.delete,
        ..Default::default()
    };

    // Scope tường minh: workspace / tenant prefix delete (không cần scan).
    if let Some(workspace_id) = options.workspace_id {
        report.mode = "workspace".to_string();
        // --delete trên workspace còn sống sẽ xoá vectors live → refuse trừ --force.
        if options.delete && !options.force {
            let live = workspace_exists(pool, workspace_id).await?;
            if live {
                return Err(QdrantCleanupError::InvalidArgs(format!(
                    "workspace_id={workspace_id} still exists in SQL; refusing --delete \
                     (would wipe live vectors). Re-run after SQL delete, use outbox/audit mode, \
                     or pass --force if intentional"
                )));
            }
        }
        let target = DeleteTarget::Workspace { workspace_id };
        apply_targets(pool, retrieval, options, &mut report, vec![target]).await?;
        return Ok(report);
    }

    if let Some(tenant_id) = options.tenant_id {
        report.mode = "tenant".to_string();
        let workspace_ids = load_workspace_ids_for_tenant(pool, tenant_id).await?;
        // Empty = tenant đã cascade / id sai: không silent no-op (delete_points_by_workspaces([]) = Ok).
        if workspace_ids.is_empty() {
            return Err(QdrantCleanupError::InvalidArgs(format!(
                "no workspaces found for tenant_id={tenant_id} (already cascaded or unknown). \
                 Capture workspace ids before SQL cascade, use --workspace-id, or rely on \
                 outbox/audit / --full-scan — do not treat empty tenant delete as success"
            )));
        }
        // Workspace list non-empty ⇒ tenant/workspace rows còn sống → wipe live vectors nếu --delete.
        if options.delete && !options.force {
            return Err(QdrantCleanupError::InvalidArgs(format!(
                "tenant_id={tenant_id} still has {} workspace(s) in SQL; refusing --delete \
                 (would wipe live vectors). Delete/cascade workspaces first, then re-run with \
                 captured ids, or pass --force if intentional",
                workspace_ids.len()
            )));
        }
        let target = DeleteTarget::Workspaces { workspace_ids };
        apply_targets(pool, retrieval, options, &mut report, vec![target]).await?;
        return Ok(report);
    }

    report.mode = if options.full_scan {
        "outbox_audit_full_scan".to_string()
    } else {
        "outbox_audit".to_string()
    };

    let mut targets: HashMap<DeleteTarget, ()> = HashMap::new();

    let from_outbox = collect_targets_from_outbox(pool).await?;
    report.candidates_from_outbox = from_outbox.len();
    for target in from_outbox {
        targets.insert(target, ());
    }

    let from_audit = collect_targets_from_audit(pool).await?;
    report.candidates_from_audit = from_audit.len();
    for target in from_audit {
        targets.insert(target, ());
    }

    if options.full_scan {
        let from_scan =
            collect_targets_from_full_scan(pool, retrieval, options.scroll_page_size).await?;
        report.candidates_from_full_scan = from_scan.len();
        for target in from_scan {
            targets.insert(target, ());
        }
    }

    let target_list: Vec<DeleteTarget> = targets.into_keys().collect();
    apply_targets(pool, retrieval, options, &mut report, target_list).await?;
    Ok(report)
}

async fn apply_targets(
    pool: &PgPool,
    retrieval: &RetrievalClient,
    options: &QdrantCleanupOptions,
    report: &mut QdrantCleanupReport,
    targets: Vec<DeleteTarget>,
) -> Result<(), QdrantCleanupError> {
    report.unique_delete_targets = targets.len();

    for target in targets.iter().take(20) {
        report.sample_targets.push(target.to_json());
    }

    if !options.delete {
        return Ok(());
    }

    for target in targets {
        report.deletes_attempted += 1;
        let result = match &target {
            DeleteTarget::Document {
                workspace_id,
                document_id,
            } => {
                retrieval
                    .delete_points_by_document(*workspace_id, *document_id)
                    .await
            }
            DeleteTarget::Workspace { workspace_id } => {
                retrieval.delete_points_by_workspace(*workspace_id).await
            }
            DeleteTarget::Workspaces { workspace_ids } => {
                retrieval.delete_points_by_workspaces(workspace_ids).await
            }
        };

        match result {
            Ok(()) => {
                report.deletes_succeeded += 1;
            }
            Err(err) => {
                report.deletes_failed += 1;
                report.errors.push(format!("{}: {err}", target.to_json()));
                // Re-queue để process-qdrant-outbox retry sau — tránh mất track.
                let enqueue_result = match &target {
                    DeleteTarget::Document {
                        workspace_id,
                        document_id,
                    } => enqueue_delete_by_document(pool, *workspace_id, *document_id).await,
                    DeleteTarget::Workspace { workspace_id } => {
                        enqueue_delete_by_workspace(pool, *workspace_id).await
                    }
                    DeleteTarget::Workspaces { workspace_ids } => {
                        enqueue_delete_by_workspaces(pool, workspace_ids).await
                    }
                };
                match enqueue_result {
                    Ok(_) => report.outbox_requeued += 1,
                    Err(enqueue_err) => {
                        report.errors.push(format!("enqueue_failed: {enqueue_err}"));
                    }
                }
            }
        }
    }

    Ok(())
}

async fn collect_targets_from_outbox(pool: &PgPool) -> Result<Vec<DeleteTarget>, sqlx::Error> {
    let rows: Vec<(String, Value)> = sqlx::query_as(
        r#"
        SELECT event_type, payload
        FROM qdrant_outbox
        WHERE status IN ('PENDING', 'FAILED', 'DEAD')
        ORDER BY created_at ASC
        LIMIT 5000
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut targets = Vec::new();
    for (event_type, payload) in rows {
        if let Some(target) = target_from_outbox_row(&event_type, &payload) {
            targets.push(target);
        }
    }
    Ok(targets)
}

fn target_from_outbox_row(event_type: &str, payload: &Value) -> Option<DeleteTarget> {
    match QdrantOutboxEventType::parse(event_type)? {
        QdrantOutboxEventType::DeleteByDocument => {
            let workspace_id = parse_uuid_field(payload, "workspace_id")?;
            let document_id = parse_uuid_field(payload, "document_id")?;
            Some(DeleteTarget::Document {
                workspace_id,
                document_id,
            })
        }
        QdrantOutboxEventType::DeleteByWorkspace => {
            let workspace_id = parse_uuid_field(payload, "workspace_id")?;
            Some(DeleteTarget::Workspace { workspace_id })
        }
        QdrantOutboxEventType::DeleteByWorkspaces => {
            let workspace_ids = payload
                .get("workspace_ids")?
                .as_array()?
                .iter()
                .filter_map(|v| serde_json::from_value::<Uuid>(v.clone()).ok())
                .collect::<Vec<_>>();
            Some(DeleteTarget::Workspaces { workspace_ids })
        }
    }
}

async fn collect_targets_from_audit(pool: &PgPool) -> Result<Vec<DeleteTarget>, sqlx::Error> {
    let rows: Vec<(String, Option<Uuid>, Option<Uuid>, Value)> = sqlx::query_as(
        r#"
        SELECT event_type, workspace_id, document_id, metadata
        FROM audit_events
        WHERE event_type IN ('document_deleted', 'workspace_deleted')
          AND (
            metadata->>'qdrant_delete_succeeded' = 'false'
            OR metadata->>'qdrant_workspace_delete_succeeded' = 'false'
          )
        ORDER BY created_at DESC
        LIMIT 2000
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut targets = Vec::new();
    for (event_type, workspace_id, document_id, _metadata) in rows {
        match event_type.as_str() {
            "document_deleted" => {
                if let (Some(workspace_id), Some(document_id)) = (workspace_id, document_id) {
                    targets.push(DeleteTarget::Document {
                        workspace_id,
                        document_id,
                    });
                }
            }
            "workspace_deleted" => {
                if let Some(workspace_id) = workspace_id {
                    targets.push(DeleteTarget::Workspace { workspace_id });
                }
            }
            _ => {}
        }
    }
    Ok(targets)
}

async fn collect_targets_from_full_scan(
    pool: &PgPool,
    retrieval: &RetrievalClient,
    page_size: usize,
) -> Result<Vec<DeleteTarget>, QdrantCleanupError> {
    let live_workspaces: HashSet<Uuid> = sqlx::query_scalar("SELECT id FROM workspaces")
        .fetch_all(pool)
        .await?
        .into_iter()
        .collect();

    let live_documents: HashSet<(Uuid, Uuid)> = sqlx::query_as(
        r#"
        SELECT workspace_id, id
        FROM documents
        WHERE workspace_id IS NOT NULL
        "#,
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();

    let mut orphan_workspaces: HashSet<Uuid> = HashSet::new();
    let mut orphan_documents: HashSet<(Uuid, Uuid)> = HashSet::new();

    let mut offset: Option<Value> = None;
    loop {
        let page = retrieval
            .scroll_points_page(page_size, offset.clone())
            .await?;
        if page.points.is_empty() {
            break;
        }

        for point in &page.points {
            if !live_workspaces.contains(&point.workspace_id) {
                orphan_workspaces.insert(point.workspace_id);
                continue;
            }
            if !live_documents.contains(&(point.workspace_id, point.document_id)) {
                orphan_documents.insert((point.workspace_id, point.document_id));
            }
        }

        match page.next_offset {
            Some(next) => offset = Some(next),
            None => break,
        }
    }

    let mut targets = Vec::new();
    for workspace_id in orphan_workspaces {
        targets.push(DeleteTarget::Workspace { workspace_id });
    }
    for (workspace_id, document_id) in orphan_documents {
        targets.push(DeleteTarget::Document {
            workspace_id,
            document_id,
        });
    }
    Ok(targets)
}

async fn load_workspace_ids_for_tenant(
    pool: &PgPool,
    tenant_id: Uuid,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT id
        FROM workspaces
        WHERE tenant_id = $1
        "#,
    )
    .bind(tenant_id)
    .fetch_all(pool)
    .await
}

async fn workspace_exists(pool: &PgPool, workspace_id: Uuid) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM workspaces WHERE id = $1
        )
        "#,
    )
    .bind(workspace_id)
    .fetch_one(pool)
    .await
}

fn parse_uuid_field(payload: &Value, key: &str) -> Option<Uuid> {
    let value = payload.get(key)?;
    if let Some(s) = value.as_str() {
        return Uuid::parse_str(s).ok();
    }
    serde_json::from_value(value.clone()).ok()
}

pub fn report_to_metadata(report: &QdrantCleanupReport) -> Value {
    json!({
        "mode": report.mode,
        "dry_run": report.dry_run,
        "candidates_from_outbox": report.candidates_from_outbox,
        "candidates_from_audit": report.candidates_from_audit,
        "candidates_from_full_scan": report.candidates_from_full_scan,
        "unique_delete_targets": report.unique_delete_targets,
        "deletes_attempted": report.deletes_attempted,
        "deletes_succeeded": report.deletes_succeeded,
        "deletes_failed": report.deletes_failed,
        "outbox_requeued": report.outbox_requeued,
        "error_count": report.errors.len(),
        "sample_targets": report.sample_targets,
    })
}
