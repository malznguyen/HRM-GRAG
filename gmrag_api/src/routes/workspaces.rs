use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::extractor::AuthUser;
use crate::state::AppState;
use tracing::error;

#[derive(Deserialize)]
pub struct CreateWorkspaceRequest {
    pub name: String,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct WorkspaceResponse {
    pub id: Uuid,
    pub name: String,
    pub created_at: NaiveDateTime,
}

pub async fn create_workspace(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<CreateWorkspaceRequest>,
) -> impl IntoResponse {
    let name = body.name.trim();
    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, "Workspace name is required").into_response();
    }

    let is_super_admin: Option<bool> =
        sqlx::query_scalar("SELECT is_super_admin FROM users WHERE id = $1")
            .bind(&auth.user_id)
            .fetch_optional(&state.pool)
            .await
            .ok()
            .flatten();

    if !is_super_admin.unwrap_or(false) {
        return (StatusCode::FORBIDDEN, "Super admin access required").into_response();
    }

    match insert_workspace_with_admin(&state.pool, name, &auth.user_id).await {
        Ok(workspace) => (StatusCode::CREATED, Json(workspace)).into_response(),
        Err(err) => {
            error!(error = %err, user_id = %auth.user_id, "Failed to create workspace");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn list_workspaces(State(state): State<AppState>, auth: AuthUser) -> impl IntoResponse {
    let rows: Result<Vec<WorkspaceResponse>, _> = sqlx::query_as(
        r#"
        SELECT w.id, w.name, w.created_at
        FROM workspaces w
        INNER JOIN workspace_members wm ON wm.workspace_id = w.id
        WHERE wm.user_id = $1
        ORDER BY w.created_at DESC
        "#,
    )
    .bind(&auth.user_id)
    .fetch_all(&state.pool)
    .await;

    match rows {
        Ok(workspaces) => Json(workspaces).into_response(),
        Err(err) => {
            error!(error = %err, user_id = %auth.user_id, "Failed to list workspaces");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn insert_workspace_with_admin(
    pool: &PgPool,
    name: &str,
    user_id: &str,
) -> Result<WorkspaceResponse, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let workspace: WorkspaceResponse = sqlx::query_as(
        r#"
        INSERT INTO workspaces (name)
        VALUES ($1)
        RETURNING id, name, created_at
        "#,
    )
    .bind(name)
    .fetch_one(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO workspace_members (workspace_id, user_id, role)
        VALUES ($1, $2, 'ADMIN')
        "#,
    )
    .bind(workspace.id)
    .bind(user_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(workspace)
}

pub async fn delete_workspace(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    let is_super_admin: Option<bool> =
        sqlx::query_scalar("SELECT is_super_admin FROM users WHERE id = $1")
            .bind(&auth.user_id)
            .fetch_optional(&state.pool)
            .await
            .ok()
            .flatten();

    if !is_super_admin.unwrap_or(false) {
        return (StatusCode::FORBIDDEN, "Super admin access required").into_response();
    }

    let result = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(&state.pool)
        .await;

    match result {
        Ok(outcome) if outcome.rows_affected() == 0 => StatusCode::NOT_FOUND.into_response(),
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => {
            error!(error = %err, user_id = %auth.user_id, workspace_id = %workspace_id, "Failed to delete workspace");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
