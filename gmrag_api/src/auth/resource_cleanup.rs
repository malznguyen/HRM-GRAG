use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{
    authz::{Object, Relation, TupleKey},
    workspace_role::WorkspaceMemberRole,
};

#[derive(Debug, Clone)]
pub struct WorkspaceDeletePlan {
    pub tenant_id: Uuid,
    pub tuples: Vec<TupleKey>,
}

#[derive(Debug, Clone)]
pub struct DocumentDeletePlan {
    pub object_key: String,
    pub bucket: String,
    pub tuples: Vec<TupleKey>,
}

/// Capture các tuple của workspace còn có thể suy ra từ SQL trước cascade delete.
pub async fn capture_workspace_delete_plan(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
) -> Result<Option<WorkspaceDeletePlan>, sqlx::Error> {
    let tenant_id: Option<Uuid> =
        sqlx::query_scalar("SELECT tenant_id FROM workspaces WHERE id = $1 FOR UPDATE")
            .bind(workspace_id)
            .fetch_optional(&mut **tx)
            .await?;
    let Some(tenant_id) = tenant_id else {
        return Ok(None);
    };

    let members: Vec<(String, String)> =
        sqlx::query_as("SELECT user_id, role FROM workspace_members WHERE workspace_id = $1")
            .bind(workspace_id)
            .fetch_all(&mut **tx)
            .await?;
    let document_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM documents WHERE workspace_id = $1")
            .bind(workspace_id)
            .fetch_all(&mut **tx)
            .await?;
    let shares: Vec<(Uuid, String)> = sqlx::query_as(
        r#"
        SELECT ds.document_id, ds.user_id
        FROM document_shares ds
        JOIN documents d ON d.id = ds.document_id
        WHERE d.workspace_id = $1
        "#,
    )
    .bind(workspace_id)
    .fetch_all(&mut **tx)
    .await?;

    let mut tuples = Vec::with_capacity(1 + members.len() + document_ids.len() + shares.len());
    tuples.push(TupleKey {
        user: format!("tenant:{tenant_id}"),
        relation: Relation::Tenant.as_str().to_string(),
        object: Object::Workspace(workspace_id).to_string(),
    });
    for (user_id, role) in members {
        let role = WorkspaceMemberRole::from_sql(&role).ok_or_else(|| {
            sqlx::Error::Protocol(format!("invalid workspace member role: {role}"))
        })?;
        tuples.push(TupleKey {
            user: format!("user:{user_id}"),
            relation: role.as_fga_relation().as_str().to_string(),
            object: Object::Workspace(workspace_id).to_string(),
        });
    }
    for document_id in document_ids {
        tuples.push(TupleKey {
            user: format!("workspace:{workspace_id}"),
            relation: Relation::Workspace.as_str().to_string(),
            object: Object::Document(document_id).to_string(),
        });
    }
    for (document_id, user_id) in shares {
        tuples.push(TupleKey {
            user: format!("user:{user_id}"),
            relation: Relation::ExplicitViewer.as_str().to_string(),
            object: Object::Document(document_id).to_string(),
        });
    }

    Ok(Some(WorkspaceDeletePlan { tenant_id, tuples }))
}

/// Capture object metadata và toàn bộ relation document/share trước document delete.
pub async fn capture_document_delete_plan(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<Option<DocumentDeletePlan>, sqlx::Error> {
    let target: Option<(String, String)> = sqlx::query_as(
        r#"
        SELECT object_key, bucket
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        FOR UPDATE
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((object_key, bucket)) = target else {
        return Ok(None);
    };
    let share_user_ids: Vec<String> =
        sqlx::query_scalar("SELECT user_id FROM document_shares WHERE document_id = $1")
            .bind(document_id)
            .fetch_all(&mut **tx)
            .await?;

    let mut tuples = Vec::with_capacity(1 + share_user_ids.len());
    tuples.push(TupleKey {
        user: format!("workspace:{workspace_id}"),
        relation: Relation::Workspace.as_str().to_string(),
        object: Object::Document(document_id).to_string(),
    });
    for user_id in share_user_ids {
        tuples.push(TupleKey {
            user: format!("user:{user_id}"),
            relation: Relation::ExplicitViewer.as_str().to_string(),
            object: Object::Document(document_id).to_string(),
        });
    }

    Ok(Some(DocumentDeletePlan {
        object_key,
        bucket,
        tuples,
    }))
}
