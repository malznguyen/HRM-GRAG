use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::error;
use uuid::Uuid;

use crate::auth::extractor::AuthUser;
use crate::auth::rbac::{require_workspace_admin, require_workspace_member};
use crate::invite::{invite_placeholder_user_id, normalize_email};
use crate::state::AppState;

#[derive(Serialize, sqlx::FromRow)]
pub struct WorkspaceMemberResponse {
    pub id: String,
    pub email: String,
    pub role: String,
    pub joined_at: NaiveDateTime,
}

#[derive(Deserialize)]
pub struct AddWorkspaceMemberRequest {
    pub email: String,
    pub role: String,
}

pub async fn list_workspace_members(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(status) =
        require_workspace_member(&state.pool, workspace_id, &auth.user_id).await
    {
        return status.into_response();
    }

    match fetch_workspace_members(&state.pool, workspace_id).await {
        Ok(members) => Json(members).into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %auth.user_id,
                workspace_id = %workspace_id,
                "Failed to list workspace members"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn add_workspace_member(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(workspace_id): Path<Uuid>,
    Json(body): Json<AddWorkspaceMemberRequest>,
) -> impl IntoResponse {
    if let Err((status, message)) =
        require_workspace_admin(&state.pool, workspace_id, &auth.user_id).await
    {
        return (status, message).into_response();
    }

    let email = normalize_email(&body.email);
    if email.is_empty() || !email.contains('@') {
        return (StatusCode::BAD_REQUEST, "Valid email is required").into_response();
    }

    let role = match normalize_member_role(&body.role) {
        Some(role) => role,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                "Role must be owner or member (admin/user also accepted)",
            )
                .into_response();
        }
    };

    match insert_workspace_member(&state.pool, workspace_id, &email, role).await {
        Ok(member) => (StatusCode::CREATED, Json(member)).into_response(),
        Err(MemberInsertError::AlreadyMember) => {
            (StatusCode::CONFLICT, "User is already a workspace member").into_response()
        }
        Err(MemberInsertError::Database(err)) => {
            error!(
                error = %err,
                user_id = %auth.user_id,
                workspace_id = %workspace_id,
                email = %email,
                "Failed to add workspace member"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn remove_workspace_member(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((workspace_id, member_id)): Path<(Uuid, String)>,
) -> impl IntoResponse {
    if let Err((status, message)) =
        require_workspace_admin(&state.pool, workspace_id, &auth.user_id).await
    {
        return (status, message).into_response();
    }

    if member_id == auth.user_id {
        return (
            StatusCode::BAD_REQUEST,
            "You cannot remove yourself from the workspace",
        )
            .into_response();
    }

    let result = sqlx::query(
        r#"
        DELETE FROM workspace_members
        WHERE workspace_id = $1 AND user_id = $2
        "#,
    )
    .bind(workspace_id)
    .bind(&member_id)
    .execute(&state.pool)
    .await;

    match result {
        Ok(outcome) if outcome.rows_affected() == 0 => StatusCode::NOT_FOUND.into_response(),
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %auth.user_id,
                workspace_id = %workspace_id,
                member_id = %member_id,
                "Failed to remove workspace member"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn fetch_workspace_members(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<Vec<WorkspaceMemberResponse>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT wm.user_id AS id, u.email, wm.role, wm.joined_at
        FROM workspace_members wm
        INNER JOIN users u ON u.id = wm.user_id
        WHERE wm.workspace_id = $1
        ORDER BY wm.joined_at ASC
        "#,
    )
    .bind(workspace_id)
    .fetch_all(pool)
    .await
}

enum MemberInsertError {
    AlreadyMember,
    Database(sqlx::Error),
}

async fn insert_workspace_member(
    pool: &PgPool,
    workspace_id: Uuid,
    email: &str,
    role: &str,
) -> Result<WorkspaceMemberResponse, MemberInsertError> {
    let mut tx = pool.begin().await.map_err(MemberInsertError::Database)?;

    let user_id: String = match sqlx::query_scalar::<_, String>(
        "SELECT id FROM users WHERE lower(email) = lower($1)",
    )
    .bind(email)
    .fetch_optional(&mut *tx)
    .await
    .map_err(MemberInsertError::Database)?
    {
        Some(existing_id) => existing_id,
        None => {
            let invite_id = invite_placeholder_user_id(email);
            sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
                .bind(&invite_id)
                .bind(email)
                .execute(&mut *tx)
                .await
                .map_err(MemberInsertError::Database)?;
            invite_id
        }
    };

    let already_member: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM workspace_members wm
            INNER JOIN users u ON u.id = wm.user_id
            WHERE wm.workspace_id = $1
              AND lower(u.email) = lower($2)
        )
        "#,
    )
    .bind(workspace_id)
    .bind(email)
    .fetch_one(&mut *tx)
    .await
    .map_err(MemberInsertError::Database)?;

    if already_member {
        return Err(MemberInsertError::AlreadyMember);
    }

    let member: WorkspaceMemberResponse = sqlx::query_as(
        r#"
        INSERT INTO workspace_members (workspace_id, user_id, role)
        VALUES ($1, $2, $3)
        RETURNING user_id AS id, $4::varchar AS email, role, joined_at
        "#,
    )
    .bind(workspace_id)
    .bind(&user_id)
    .bind(role)
    .bind(email)
    .fetch_one(&mut *tx)
    .await
    .map_err(MemberInsertError::Database)?;

    tx.commit().await.map_err(MemberInsertError::Database)?;

    Ok(member)
}

fn normalize_member_role(role: &str) -> Option<&'static str> {
    match role.trim().to_ascii_lowercase().as_str() {
        "owner" | "admin" => Some("ADMIN"),
        "member" | "user" => Some("USER"),
        _ => None,
    }
}
