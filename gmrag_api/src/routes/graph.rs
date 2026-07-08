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

    match fetch_workspace_graph(&state.pool, workspace_id).await {
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
) -> Result<GraphResponse, sqlx::Error> {
    let nodes = sqlx::query_as::<_, GraphNodeResponse>(
        r#"
        SELECT id, entity_name, entity_type, description
        FROM graph_nodes
        WHERE workspace_id = $1
          AND EXISTS (
            SELECT 1
            FROM graph_node_sources source
            WHERE source.graph_node_id = graph_nodes.id
          )
        ORDER BY entity_name ASC
        "#,
    )
    .bind(workspace_id)
    .fetch_all(pool)
    .await?;

    let links = sqlx::query_as::<_, GraphLinkResponse>(
        r#"
        SELECT id, source_node_id AS source, target_node_id AS target, relationship, description
        FROM graph_edges
        WHERE workspace_id = $1
          AND EXISTS (
            SELECT 1
            FROM graph_edge_sources source
            WHERE source.graph_edge_id = graph_edges.id
          )
        "#,
    )
    .bind(workspace_id)
    .fetch_all(pool)
    .await?;

    Ok(GraphResponse { nodes, links })
}
