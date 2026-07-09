use sqlx::PgPool;
use uuid::Uuid;

use super::StorageClient;

#[derive(Debug)]
pub enum StorageCleanupError {
    Database(sqlx::Error),
    Storage(super::StorageError),
    WorkspaceTenantNotFound { workspace_id: Uuid },
}

impl std::fmt::Display for StorageCleanupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageCleanupError::Database(err) => write!(f, "database error: {err}"),
            StorageCleanupError::Storage(err) => write!(f, "storage error: {err}"),
            StorageCleanupError::WorkspaceTenantNotFound { workspace_id } => {
                write!(
                    f,
                    "workspace tenant_id not found for workspace {workspace_id}"
                )
            }
        }
    }
}

impl std::error::Error for StorageCleanupError {}

impl From<sqlx::Error> for StorageCleanupError {
    fn from(value: sqlx::Error) -> Self {
        StorageCleanupError::Database(value)
    }
}

impl From<super::StorageError> for StorageCleanupError {
    fn from(value: super::StorageError) -> Self {
        StorageCleanupError::Storage(value)
    }
}

#[derive(Debug, Clone)]
pub struct MissingDocumentObject {
    pub document_id: Uuid,
    pub workspace_id: Uuid,
    pub object_key: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct StorageCleanupReport {
    pub checked_documents: usize,
    pub missing_document_objects: Vec<MissingDocumentObject>,
    pub marked_failed_documents: usize,
    pub listed_objects: usize,
    pub orphan_object_keys: Vec<String>,
    pub deleted_orphan_objects: usize,
}

#[derive(Debug, Clone)]
pub struct PrefixCleanupReport {
    pub prefix: String,
    pub listed_objects: usize,
    pub object_keys: Vec<String>,
    pub deleted_objects: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct StorageCleanupOptions {
    pub allow_delete: bool,
    pub delete_orphans: bool,
    pub mark_missing_documents_failed: bool,
}

impl Default for StorageCleanupOptions {
    fn default() -> Self {
        Self {
            allow_delete: false,
            delete_orphans: false,
            mark_missing_documents_failed: false,
        }
    }
}

#[derive(sqlx::FromRow)]
struct DocumentObjectRow {
    id: Uuid,
    workspace_id: Uuid,
    object_key: String,
    status: String,
}

pub async fn scan_documents_and_orphans(
    pool: &PgPool,
    storage: &StorageClient,
    options: StorageCleanupOptions,
) -> Result<StorageCleanupReport, StorageCleanupError> {
    let document_rows: Vec<DocumentObjectRow> = sqlx::query_as(
        r#"
        SELECT id, workspace_id, object_key, status
        FROM documents
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut missing_document_objects = Vec::new();
    for row in &document_rows {
        let exists = storage.object_exists(&row.object_key).await?;
        if !exists {
            missing_document_objects.push(MissingDocumentObject {
                document_id: row.id,
                workspace_id: row.workspace_id,
                object_key: row.object_key.clone(),
                status: row.status.clone(),
            });
        }
    }

    let marked_failed_documents = if options.mark_missing_documents_failed {
        mark_missing_documents_as_failed(pool, &missing_document_objects).await?
    } else {
        0
    };

    let listed_objects = storage.list_objects(None).await?;
    let listed_object_keys: Vec<String> = listed_objects
        .iter()
        .map(|object| object.key.clone())
        .collect();
    let orphan_object_keys = find_orphan_object_keys(&document_rows, &listed_object_keys);

    let keys_to_delete = build_delete_candidate_keys(
        &orphan_object_keys,
        options.allow_delete && options.delete_orphans,
    );

    let deleted_orphan_objects = if keys_to_delete.is_empty() {
        0
    } else {
        storage.delete_objects(&keys_to_delete).await?
    };

    Ok(StorageCleanupReport {
        checked_documents: document_rows.len(),
        missing_document_objects,
        marked_failed_documents,
        listed_objects: listed_object_keys.len(),
        orphan_object_keys,
        deleted_orphan_objects,
    })
}

pub async fn cleanup_prefix(
    storage: &StorageClient,
    prefix: String,
    allow_delete: bool,
) -> Result<PrefixCleanupReport, StorageCleanupError> {
    let listed_objects = storage.list_objects(Some(&prefix)).await?;
    let object_keys: Vec<String> = listed_objects
        .iter()
        .map(|object| object.key.clone())
        .collect();
    let keys_to_delete = build_delete_candidate_keys(&object_keys, allow_delete);

    let deleted_objects = if keys_to_delete.is_empty() {
        0
    } else {
        storage.delete_objects(&keys_to_delete).await?
    };

    Ok(PrefixCleanupReport {
        prefix,
        listed_objects: object_keys.len(),
        object_keys,
        deleted_objects,
    })
}

pub async fn resolve_workspace_prefix(
    pool: &PgPool,
    workspace_id: Uuid,
    tenant_id_override: Option<Uuid>,
) -> Result<String, StorageCleanupError> {
    if let Some(tenant_id) = tenant_id_override {
        return Ok(build_workspace_prefix(tenant_id, workspace_id));
    }

    let tenant_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        SELECT tenant_id
        FROM workspaces
        WHERE id = $1
        "#,
    )
    .bind(workspace_id)
    .fetch_optional(pool)
    .await?;

    let Some(tenant_id) = tenant_id else {
        return Err(StorageCleanupError::WorkspaceTenantNotFound { workspace_id });
    };

    Ok(build_workspace_prefix(tenant_id, workspace_id))
}

pub fn build_workspace_prefix(tenant_id: Uuid, workspace_id: Uuid) -> String {
    format!("tenants/{tenant_id}/workspaces/{workspace_id}/")
}

pub fn build_tenant_prefix(tenant_id: Uuid) -> String {
    format!("tenants/{tenant_id}/")
}

async fn mark_missing_documents_as_failed(
    pool: &PgPool,
    missing_documents: &[MissingDocumentObject],
) -> Result<usize, StorageCleanupError> {
    let mut marked = 0usize;

    for missing in missing_documents {
        let result = sqlx::query(
            r#"
            UPDATE documents
            SET status = 'FAILED',
                processing_stage = 'DONE'
            WHERE id = $1
              AND workspace_id = $2
              AND status = 'PROCESSING'
            "#,
        )
        .bind(missing.document_id)
        .bind(missing.workspace_id)
        .execute(pool)
        .await?;

        marked += result.rows_affected() as usize;
    }

    Ok(marked)
}

fn find_orphan_object_keys(
    document_rows: &[DocumentObjectRow],
    listed_object_keys: &[String],
) -> Vec<String> {
    let known_object_keys = document_rows
        .iter()
        .map(|row| row.object_key.as_str())
        .collect::<std::collections::HashSet<&str>>();

    listed_object_keys
        .iter()
        .filter(|object_key| !known_object_keys.contains(object_key.as_str()))
        .cloned()
        .collect()
}

fn build_delete_candidate_keys(object_keys: &[String], allow_delete: bool) -> Vec<String> {
    if !allow_delete {
        return Vec::new();
    }

    object_keys.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_prefix_builder_uses_canonical_format() {
        let tenant_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let workspace_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();

        let prefix = build_workspace_prefix(tenant_id, workspace_id);

        assert_eq!(
            prefix,
            "tenants/11111111-1111-1111-1111-111111111111/workspaces/22222222-2222-2222-2222-222222222222/"
        );
    }

    #[test]
    fn dry_run_does_not_mark_any_object_for_deletion() {
        let object_keys = vec!["a".to_string(), "b".to_string()];
        let delete_candidates = build_delete_candidate_keys(&object_keys, false);

        assert!(delete_candidates.is_empty());
    }
}
