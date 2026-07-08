use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Serialize;
use sqlx::PgPool;
use tracing::error;
use uuid::Uuid;

use crate::auth::authz::{Authz, Relation, Object};
use crate::auth::document_acl::{collect_viewable_document_ids, fetch_workspace_document_acl_rows};
use crate::state::AppState;

#[derive(Serialize, sqlx::FromRow)]
pub struct GraphNodeResponse {
    pub id: Uuid,
    pub entity_name: String,
    pub entity_type: Option<String>,
    pub description: Option<String>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct GraphLinkResponse {
    pub id: Uuid,
    pub source: Uuid,
    pub target: Uuid,
    pub relationship: String,
    pub description: Option<String>,
}

#[derive(Serialize)]
pub struct GraphResponse {
    pub nodes: Vec<GraphNodeResponse>,
    pub links: Vec<GraphLinkResponse>,
}

pub async fn get_workspace_graph(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(err) = authz.require_relation(Relation::Member, &Object::Workspace(workspace_id)).await {
        return err.into_response();
    }

    let acl_rows = match fetch_workspace_document_acl_rows(&state.pool, workspace_id).await {
        Ok(rows) => rows,
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                "Failed to load document ACL data for graph"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let visible_doc_ids = match collect_viewable_document_ids(
        &state.authz_client,
        &authz.user_id,
        &acl_rows,
    )
    .await
    {
        Ok(ids) => ids,
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                "Failed to filter graph sources by document ACL"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    match fetch_workspace_graph(&state.pool, workspace_id, visible_doc_ids.into_iter().collect()).await {
        Ok(graph) => Json(graph).into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                "Failed to fetch workspace graph"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn fetch_workspace_graph(
    pool: &PgPool,
    workspace_id: Uuid,
    visible_document_ids: Vec<Uuid>,
) -> Result<GraphResponse, sqlx::Error> {
    if visible_document_ids.is_empty() {
        return Ok(GraphResponse {
            nodes: Vec::new(),
            links: Vec::new(),
        });
    }

    let nodes = sqlx::query_as::<_, GraphNodeResponse>(
        r#"
        SELECT DISTINCT
            node.id,
            node.entity_name,
            node.entity_type,
            node.description
        FROM graph_nodes node
        INNER JOIN graph_node_sources source
            ON source.graph_node_id = node.id
        WHERE node.workspace_id = $1
          AND source.workspace_id = $1
          AND source.document_id = ANY($2)
        ORDER BY node.entity_name ASC
        "#,
    )
    .bind(workspace_id)
    .bind(&visible_document_ids)
    .fetch_all(pool)
    .await?;

    let links = sqlx::query_as::<_, GraphLinkResponse>(
        r#"
        SELECT DISTINCT
            edge.id,
            edge.source_node_id AS source,
            edge.target_node_id AS target,
            edge.relationship,
            edge.description
        FROM graph_edges edge
        INNER JOIN graph_edge_sources source
            ON source.graph_edge_id = edge.id
        WHERE edge.workspace_id = $1
          AND source.workspace_id = $1
          AND source.document_id = ANY($2)
        "#,
    )
    .bind(workspace_id)
    .bind(&visible_document_ids)
    .fetch_all(pool)
    .await?;

    Ok(GraphResponse { nodes, links })
}
