use serde_json::{Map, Value};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditEventType {
    TenantCreated,
    TenantOwnerAdded,
    WorkspaceCreated,
    WorkspaceDeleted,
    MemberAdded,
    MemberRoleChanged,
    MemberRemoved,
    WorkspaceAdminRecovered,
    DocumentUploaded,
    DocumentAccessModeChanged,
    DocumentShared,
    DocumentShareRevoked,
    DocumentDeleted,
    DocumentRetryStarted,
    BackfillDocumentWorkspaceTuplesStarted,
    BackfillDocumentWorkspaceTuplesCompleted,
    BackfillDocumentWorkspaceTuplesFailed,
    AuthzOutboxProcessingStarted,
    AuthzOutboxProcessingCompleted,
    AuthzOutboxProcessingFailed,
    QdrantOutboxProcessingStarted,
    QdrantOutboxProcessingCompleted,
    QdrantOutboxProcessingFailed,
    QdrantCleanupDryRun,
    QdrantCleanupCompleted,
    QdrantCleanupFailed,
    StorageCleanupDryRun,
    StorageCleanupCompleted,
    StorageCleanupFailed,
    StorageOrphanScanReport,
    StorageOutboxProcessingStarted,
    StorageOutboxProcessingCompleted,
    StorageOutboxProcessingFailed,
    InvitePlaceholderCleanupDryRun,
    InvitePlaceholderCleanupCompleted,
    InvitePlaceholderCleanupFailed,
    /// Operator drill LIFE-005 — không phải public API tenant delete.
    TenantDeleteDrillDryRun,
    TenantDeleteDrillCompleted,
    TenantDeleteDrillFailed,
    TenantDeleted,
    GraphNodeEmbeddingBackfillCompleted,
    GraphNodeEmbeddingBackfillFailed,
    /// OCR-004 apply bị refuse (OCR capability đóng); metadata-only.
    OcrAffectedDocumentsApplyRefused,
    /// OCR-004 bounded requeue đã chạy (khi OCR available); metadata-only.
    OcrAffectedDocumentsApplyCompleted,
}

impl AuditEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            AuditEventType::TenantCreated => "tenant_created",
            AuditEventType::TenantOwnerAdded => "tenant_owner_added",
            AuditEventType::WorkspaceCreated => "workspace_created",
            AuditEventType::WorkspaceDeleted => "workspace_deleted",
            AuditEventType::MemberAdded => "member_added",
            AuditEventType::MemberRoleChanged => "member_role_changed",
            AuditEventType::MemberRemoved => "member_removed",
            AuditEventType::WorkspaceAdminRecovered => "workspace_admin_recovered",
            AuditEventType::DocumentUploaded => "document_uploaded",
            AuditEventType::DocumentAccessModeChanged => "document_access_mode_changed",
            AuditEventType::DocumentShared => "document_shared",
            AuditEventType::DocumentShareRevoked => "document_share_revoked",
            AuditEventType::DocumentDeleted => "document_deleted",
            AuditEventType::DocumentRetryStarted => "document_retry_started",
            AuditEventType::BackfillDocumentWorkspaceTuplesStarted => {
                "backfill_document_workspace_tuples_started"
            }
            AuditEventType::BackfillDocumentWorkspaceTuplesCompleted => {
                "backfill_document_workspace_tuples_completed"
            }
            AuditEventType::BackfillDocumentWorkspaceTuplesFailed => {
                "backfill_document_workspace_tuples_failed"
            }
            AuditEventType::AuthzOutboxProcessingStarted => "authz_outbox_processing_started",
            AuditEventType::AuthzOutboxProcessingCompleted => "authz_outbox_processing_completed",
            AuditEventType::AuthzOutboxProcessingFailed => "authz_outbox_processing_failed",
            AuditEventType::QdrantOutboxProcessingStarted => "qdrant_outbox_processing_started",
            AuditEventType::QdrantOutboxProcessingCompleted => "qdrant_outbox_processing_completed",
            AuditEventType::QdrantOutboxProcessingFailed => "qdrant_outbox_processing_failed",
            AuditEventType::QdrantCleanupDryRun => "qdrant_cleanup_dry_run",
            AuditEventType::QdrantCleanupCompleted => "qdrant_cleanup_completed",
            AuditEventType::QdrantCleanupFailed => "qdrant_cleanup_failed",
            AuditEventType::StorageCleanupDryRun => "storage_cleanup_dry_run",
            AuditEventType::StorageCleanupCompleted => "storage_cleanup_completed",
            AuditEventType::StorageCleanupFailed => "storage_cleanup_failed",
            AuditEventType::StorageOrphanScanReport => "storage_orphan_scan_report",
            AuditEventType::StorageOutboxProcessingStarted => "storage_outbox_processing_started",
            AuditEventType::StorageOutboxProcessingCompleted => {
                "storage_outbox_processing_completed"
            }
            AuditEventType::StorageOutboxProcessingFailed => "storage_outbox_processing_failed",
            AuditEventType::InvitePlaceholderCleanupDryRun => "invite_placeholder_cleanup_dry_run",
            AuditEventType::InvitePlaceholderCleanupCompleted => {
                "invite_placeholder_cleanup_completed"
            }
            AuditEventType::InvitePlaceholderCleanupFailed => "invite_placeholder_cleanup_failed",
            AuditEventType::TenantDeleteDrillDryRun => "tenant_delete_drill_dry_run",
            AuditEventType::TenantDeleteDrillCompleted => "tenant_delete_drill_completed",
            AuditEventType::TenantDeleteDrillFailed => "tenant_delete_drill_failed",
            AuditEventType::TenantDeleted => "tenant_deleted",
            AuditEventType::GraphNodeEmbeddingBackfillCompleted => {
                "graph_node_embedding_backfill_completed"
            }
            AuditEventType::GraphNodeEmbeddingBackfillFailed => {
                "graph_node_embedding_backfill_failed"
            }
            AuditEventType::OcrAffectedDocumentsApplyRefused => {
                "ocr_affected_documents_apply_refused"
            }
            AuditEventType::OcrAffectedDocumentsApplyCompleted => {
                "ocr_affected_documents_apply_completed"
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuditEventRecord {
    actor_user_id: Option<String>,
    tenant_id: Option<Uuid>,
    workspace_id: Option<Uuid>,
    document_id: Option<Uuid>,
    event_type: AuditEventType,
    target_type: Option<String>,
    target_id: Option<String>,
    metadata: Value,
}

impl AuditEventRecord {
    pub fn new(event_type: AuditEventType) -> Self {
        Self {
            actor_user_id: None,
            tenant_id: None,
            workspace_id: None,
            document_id: None,
            event_type,
            target_type: None,
            target_id: None,
            metadata: Value::Object(Map::new()),
        }
    }

    pub fn with_actor_user_id(mut self, actor_user_id: impl Into<String>) -> Self {
        self.actor_user_id = Some(actor_user_id.into());
        self
    }

    pub fn with_tenant_id(mut self, tenant_id: Uuid) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }

    pub fn with_workspace_id(mut self, workspace_id: Uuid) -> Self {
        self.workspace_id = Some(workspace_id);
        self
    }

    pub fn with_document_id(mut self, document_id: Uuid) -> Self {
        self.document_id = Some(document_id);
        self
    }

    pub fn with_target(
        mut self,
        target_type: impl Into<String>,
        target_id: impl Into<String>,
    ) -> Self {
        self.target_type = Some(target_type.into());
        self.target_id = Some(target_id.into());
        self
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

pub async fn insert_audit_event(pool: &PgPool, event: AuditEventRecord) -> Result<(), sqlx::Error> {
    let metadata = sanitize_metadata(event.metadata);

    sqlx::query(
        r#"
        INSERT INTO audit_events (
            actor_user_id,
            tenant_id,
            workspace_id,
            document_id,
            event_type,
            target_type,
            target_id,
            metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(event.actor_user_id)
    .bind(event.tenant_id)
    .bind(event.workspace_id)
    .bind(event.document_id)
    .bind(event.event_type.as_str())
    .bind(event.target_type)
    .bind(event.target_id)
    .bind(metadata)
    .execute(pool)
    .await?;

    Ok(())
}

/// Ghi audit event trong transaction lifecycle khi event phải commit cùng thay đổi nghiệp vụ.
pub async fn insert_audit_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    event: AuditEventRecord,
) -> Result<(), sqlx::Error> {
    let metadata = sanitize_metadata(event.metadata);

    sqlx::query(
        r#"
        INSERT INTO audit_events (
            actor_user_id,
            tenant_id,
            workspace_id,
            document_id,
            event_type,
            target_type,
            target_id,
            metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(event.actor_user_id)
    .bind(event.tenant_id)
    .bind(event.workspace_id)
    .bind(event.document_id)
    .bind(event.event_type.as_str())
    .bind(event.target_type)
    .bind(event.target_id)
    .bind(metadata)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub fn sanitize_metadata(metadata: Value) -> Value {
    sanitize_metadata_value(metadata)
}

pub fn sanitize_error_code(raw: &str) -> String {
    let mut normalized = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        } else if !normalized.ends_with('_') {
            normalized.push('_');
        }
        if normalized.len() >= 64 {
            break;
        }
    }

    normalized.trim_matches('_').to_string()
}

fn sanitize_metadata_value(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut sanitized = Map::with_capacity(object.len());
            for (key, nested) in object {
                if is_sensitive_metadata_key(&key) {
                    sanitized.insert(key, Value::String("[REDACTED]".to_string()));
                } else {
                    sanitized.insert(key, sanitize_metadata_value(nested));
                }
            }
            Value::Object(sanitized)
        }
        Value::Array(items) => {
            Value::Array(items.into_iter().map(sanitize_metadata_value).collect())
        }
        Value::String(content) => Value::String(truncate_string(content, 512)),
        other => other,
    }
}

fn truncate_string(value: String, max_len: usize) -> String {
    if value.len() <= max_len {
        return value;
    }

    let mut truncated = value.chars().take(max_len).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn is_sensitive_metadata_key(key: &str) -> bool {
    let lowered = key.to_ascii_lowercase();
    const SENSITIVE_KEYS: &[&str] = &[
        "content",
        "chunk_text",
        "original_text",
        "extracted_text",
        "prompt",
        "api_key",
        "secret",
        "credential",
        "access_key",
        "token",
        "object_bytes",
        "pdf_bytes",
    ];

    SENSITIVE_KEYS
        .iter()
        .any(|needle| lowered == *needle || lowered.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sanitize_metadata_redacts_sensitive_fields() {
        let metadata = json!({
            "status": "ok",
            "prompt": "secret prompt",
            "nested": {
                "chunk_text": "hidden",
                "relation": "explicit_viewer"
            }
        });

        let sanitized = sanitize_metadata(metadata);

        assert_eq!(sanitized["status"], json!("ok"));
        assert_eq!(sanitized["prompt"], json!("[REDACTED]"));
        assert_eq!(sanitized["nested"]["chunk_text"], json!("[REDACTED]"));
        assert_eq!(sanitized["nested"]["relation"], json!("explicit_viewer"));
    }

    #[test]
    fn sanitize_error_code_normalizes_value() {
        let code = sanitize_error_code("OpenFGA status=500: connection timeout");
        assert_eq!(code, "openfga_status_500_connection_timeout");
    }
}
