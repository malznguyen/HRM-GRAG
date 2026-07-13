use std::collections::HashSet;

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::authz::{AuthzClient, AuthzError, Object, Relation, TupleKey, delete_tuples_fga_first};
use crate::audit::{AuditEventRecord, AuditEventType, insert_audit_event_tx};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentAccessMode {
    WorkspaceDefault,
    Restricted,
}

impl DocumentAccessMode {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "workspace_default" => Some(Self::WorkspaceDefault),
            "restricted" => Some(Self::Restricted),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceDefault => "workspace_default",
            Self::Restricted => "restricted",
        }
    }

    pub fn is_restricted(self) -> bool {
        matches!(self, Self::Restricted)
    }
}

#[derive(Debug, Clone)]
pub struct DocumentAclRow {
    pub document_id: Uuid,
    pub access_mode: DocumentAccessMode,
}

#[derive(Debug, Clone, Copy)]
pub struct WorkspaceDocumentBackfillResult {
    pub total_documents: usize,
    pub inserted_relations: usize,
    pub existing_relations: usize,
}

#[derive(Debug)]
pub enum DocumentAclError {
    Database(sqlx::Error),
    Authz(AuthzError),
    InvalidAccessMode { document_id: Uuid, raw_mode: String },
    InvalidObjectFormat { raw_object: String },
    InvalidObjectId { raw_object: String },
}

impl std::fmt::Display for DocumentAclError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DocumentAclError::Database(err) => write!(f, "database error: {err}"),
            DocumentAclError::Authz(err) => write!(f, "authz error: {err}"),
            DocumentAclError::InvalidAccessMode {
                document_id,
                raw_mode,
            } => {
                write!(
                    f,
                    "invalid access_mode '{raw_mode}' for document {document_id}"
                )
            }
            DocumentAclError::InvalidObjectFormat { raw_object } => {
                write!(f, "invalid OpenFGA object format: {raw_object}")
            }
            DocumentAclError::InvalidObjectId { raw_object } => {
                write!(f, "invalid OpenFGA object id: {raw_object}")
            }
        }
    }
}

impl std::error::Error for DocumentAclError {}

impl From<sqlx::Error> for DocumentAclError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

impl From<AuthzError> for DocumentAclError {
    fn from(value: AuthzError) -> Self {
        Self::Authz(value)
    }
}

#[derive(sqlx::FromRow)]
struct DocumentAclRawRow {
    id: Uuid,
    access_mode: String,
}

#[derive(sqlx::FromRow)]
struct DocumentWorkspaceRow {
    document_id: Uuid,
    workspace_id: Uuid,
}

pub async fn fetch_workspace_document_acl_rows(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<Vec<DocumentAclRow>, DocumentAclError> {
    let rows: Vec<DocumentAclRawRow> = sqlx::query_as(
        r#"
        SELECT id, access_mode
        FROM documents
        WHERE workspace_id = $1
        "#,
    )
    .bind(workspace_id)
    .fetch_all(pool)
    .await?;

    let mut acl_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let access_mode = DocumentAccessMode::parse(&row.access_mode).ok_or(
            DocumentAclError::InvalidAccessMode {
                document_id: row.id,
                raw_mode: row.access_mode,
            },
        )?;
        acl_rows.push(DocumentAclRow {
            document_id: row.id,
            access_mode,
        });
    }

    Ok(acl_rows)
}

/// Chỉ trả tài liệu đã hoàn tất cho retrieval; Qdrant có thể còn point partial của job retry.
pub async fn fetch_completed_workspace_document_acl_rows(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<Vec<DocumentAclRow>, DocumentAclError> {
    let rows: Vec<DocumentAclRawRow> = sqlx::query_as(
        r#"
        SELECT id, access_mode
        FROM documents
        WHERE workspace_id = $1 AND status = 'COMPLETED' AND processing_stage = 'DONE'
        "#,
    )
    .bind(workspace_id)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let access_mode = DocumentAccessMode::parse(&row.access_mode).ok_or(
                DocumentAclError::InvalidAccessMode {
                    document_id: row.id,
                    raw_mode: row.access_mode,
                },
            )?;
            Ok(DocumentAclRow {
                document_id: row.id,
                access_mode,
            })
        })
        .collect()
}

pub async fn collect_viewable_document_ids(
    authz_client: &AuthzClient,
    user_id: &str,
    documents: &[DocumentAclRow],
) -> Result<HashSet<Uuid>, DocumentAclError> {
    let mut visible_ids: HashSet<Uuid> = documents
        .iter()
        .filter(|doc| !doc.access_mode.is_restricted())
        .map(|doc| doc.document_id)
        .collect();

    let restricted_ids: HashSet<Uuid> = documents
        .iter()
        .filter(|doc| doc.access_mode.is_restricted())
        .map(|doc| doc.document_id)
        .collect();

    if restricted_ids.is_empty() {
        return Ok(visible_ids);
    }

    let explicit_ids =
        list_document_relation_ids(authz_client, user_id, Relation::ExplicitViewer).await?;
    let bypass_ids =
        list_document_relation_ids(authz_client, user_id, Relation::BypassViewer).await?;

    for doc_id in explicit_ids
        .into_iter()
        .chain(bypass_ids.into_iter())
        .filter(|doc_id| restricted_ids.contains(doc_id))
    {
        visible_ids.insert(doc_id);
    }

    Ok(visible_ids)
}

pub async fn can_user_view_document(
    authz_client: &AuthzClient,
    user_id: &str,
    document_id: Uuid,
    access_mode: DocumentAccessMode,
) -> Result<bool, DocumentAclError> {
    if !access_mode.is_restricted() {
        return Ok(true);
    }

    let user = format_user(user_id);
    let object = Object::Document(document_id);

    if authz_client
        .check_fga(&user, Relation::ExplicitViewer, &object)
        .await?
    {
        return Ok(true);
    }

    authz_client
        .check_fga(&user, Relation::BypassViewer, &object)
        .await
        .map_err(DocumentAclError::Authz)
}

pub async fn ensure_document_workspace_relation(
    authz_client: &AuthzClient,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<(), DocumentAclError> {
    authz_client
        .write_tuple(
            &format!("workspace:{workspace_id}"),
            Relation::Workspace,
            &Object::Document(document_id),
        )
        .await?;
    Ok(())
}

pub async fn remove_document_workspace_relation(
    authz_client: &AuthzClient,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<(), DocumentAclError> {
    authz_client
        .delete_tuple(
            &format!("workspace:{workspace_id}"),
            Relation::Workspace,
            &Object::Document(document_id),
        )
        .await?;
    Ok(())
}

pub async fn backfill_document_workspace_relations(
    pool: &PgPool,
    authz_client: &AuthzClient,
) -> Result<WorkspaceDocumentBackfillResult, DocumentAclError> {
    let rows: Vec<DocumentWorkspaceRow> = sqlx::query_as(
        r#"
        SELECT id AS document_id, workspace_id
        FROM documents
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut inserted_relations = 0usize;
    let mut existing_relations = 0usize;

    for row in &rows {
        let workspace_user = format!("workspace:{}", row.workspace_id);
        let document_object = Object::Document(row.document_id);

        if authz_client
            .check_fga(&workspace_user, Relation::Workspace, &document_object)
            .await?
        {
            existing_relations += 1;
            continue;
        }

        authz_client
            .write_tuple(&workspace_user, Relation::Workspace, &document_object)
            .await?;
        inserted_relations += 1;
    }

    Ok(WorkspaceDocumentBackfillResult {
        total_documents: rows.len(),
        inserted_relations,
        existing_relations,
    })
}

/// Đổi access_mode của document; trả về `None` nếu document không tồn tại trong workspace.
///
/// Khi target là `workspace_default`, dọn `document_shares` + tuple `explicit_viewer`
/// (residue inert nếu để lại; dọn cả khi re-apply để retry an toàn).
pub async fn set_document_access_mode(
    pool: &PgPool,
    authz_client: &AuthzClient,
    document_id: Uuid,
    workspace_id: Uuid,
    access_mode: DocumentAccessMode,
    actor_user_id: &str,
) -> Result<Option<()>, DocumentAclError> {
    let mut tx = pool.begin().await?;
    let current_raw: Option<String> = sqlx::query_scalar(
        r#"
        SELECT access_mode
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        FOR UPDATE
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(current_raw) = current_raw else {
        return Ok(None);
    };

    let previous_mode =
        DocumentAccessMode::parse(&current_raw).ok_or(DocumentAclError::InvalidAccessMode {
            document_id,
            raw_mode: current_raw,
        })?;

    let share_user_ids: Vec<String> = if access_mode.is_restricted() {
        Vec::new()
    } else {
        sqlx::query_scalar("SELECT user_id FROM document_shares WHERE document_id = $1")
            .bind(document_id)
            .fetch_all(&mut *tx)
            .await?
    };
    let tuples: Vec<TupleKey> = share_user_ids
        .iter()
        .map(|user_id| TupleKey {
            user: format_user(user_id),
            relation: Relation::ExplicitViewer.as_str().to_string(),
            object: Object::Document(document_id).to_string(),
        })
        .collect();

    // Revoke OpenFGA trước để SQL không thể commit khi quyền cũ vẫn còn sống.
    delete_tuples_fga_first(authz_client, &tuples).await?;

    sqlx::query(
        r#"
        UPDATE documents
        SET access_mode = $1
        WHERE id = $2 AND workspace_id = $3
        "#,
    )
    .bind(access_mode.as_str())
    .bind(document_id)
    .bind(workspace_id)
    .execute(&mut *tx)
    .await?;

    if !access_mode.is_restricted() {
        sqlx::query("DELETE FROM document_shares WHERE document_id = $1")
            .bind(document_id)
            .execute(&mut *tx)
            .await?;
    }

    insert_audit_event_tx(
        &mut tx,
        AuditEventRecord::new(AuditEventType::DocumentAccessModeChanged)
            .with_actor_user_id(actor_user_id.to_string())
            .with_workspace_id(workspace_id)
            .with_document_id(document_id)
            .with_target("document", document_id.to_string())
            .with_metadata(json!({
                "previous_access_mode": previous_mode.as_str(),
                "access_mode": access_mode.as_str(),
                "shares_cleaned": share_user_ids.len(),
            })),
    )
    .await?;
    tx.commit().await?;

    Ok(Some(()))
}

pub async fn grant_document_explicit_viewer(
    pool: &PgPool,
    authz_client: &AuthzClient,
    document_id: Uuid,
    user_id: &str,
) -> Result<(), DocumentAclError> {
    sqlx::query(
        r#"
        INSERT INTO document_shares (document_id, user_id)
        VALUES ($1, $2)
        ON CONFLICT (document_id, user_id) DO NOTHING
        "#,
    )
    .bind(document_id)
    .bind(user_id)
    .execute(pool)
    .await?;

    if let Err(err) = authz_client
        .write_tuple(
            &format_user(user_id),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
    {
        let _ = sqlx::query(
            r#"
            DELETE FROM document_shares
            WHERE document_id = $1 AND user_id = $2
            "#,
        )
        .bind(document_id)
        .bind(user_id)
        .execute(pool)
        .await;
        return Err(DocumentAclError::Authz(err));
    }

    Ok(())
}

pub async fn revoke_document_explicit_viewer(
    pool: &PgPool,
    authz_client: &AuthzClient,
    document_id: Uuid,
    user_id: &str,
) -> Result<(), DocumentAclError> {
    // Xoa tuple OpenFGA truoc de tranh over-grant neu co loi giua hai buoc.
    authz_client
        .delete_tuple(
            &format_user(user_id),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await?;

    sqlx::query(
        r#"
        DELETE FROM document_shares
        WHERE document_id = $1 AND user_id = $2
        "#,
    )
    .bind(document_id)
    .bind(user_id)
    .execute(pool)
    .await?;

    Ok(())
}

fn format_user(user_id: &str) -> String {
    format!("user:{user_id}")
}

async fn list_document_relation_ids(
    authz_client: &AuthzClient,
    user_id: &str,
    relation: Relation,
) -> Result<HashSet<Uuid>, DocumentAclError> {
    let raw_objects = authz_client
        .list_objects(&format_user(user_id), relation, "document")
        .await?;

    raw_objects
        .into_iter()
        .map(parse_document_object)
        .collect::<Result<HashSet<_>, _>>()
}

fn parse_document_object(raw_object: String) -> Result<Uuid, DocumentAclError> {
    let Some(raw_id) = raw_object.strip_prefix("document:") else {
        return Err(DocumentAclError::InvalidObjectFormat { raw_object });
    };

    Uuid::parse_str(raw_id).map_err(|_| DocumentAclError::InvalidObjectId { raw_object })
}
