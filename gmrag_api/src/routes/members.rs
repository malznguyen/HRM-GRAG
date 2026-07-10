use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use tracing::error;
use uuid::Uuid;

use crate::audit::{AuditEventRecord, AuditEventType, insert_audit_event};
use crate::auth::authz::{ApiError, Authz, AuthzError, Object, Relation, TupleKey};
use crate::auth::outbox::enqueue_tuple_write;
use crate::invite::normalize_email;
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

#[derive(Deserialize)]
pub struct UpdateWorkspaceMemberRoleRequest {
    pub role: String,
}

/// GET /workspaces/{workspace_id}/members
/// Xem danh sách thành viên workspace
pub async fn list_workspace_members(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::Member, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    match fetch_workspace_members(&state.pool, workspace_id).await {
        Ok(members) => Json(members).into_response(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                "Failed to list workspace members"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /workspaces/{workspace_id}/members
/// Thêm thành viên đã có tài khoản Keycloak (verified) vào workspace.
///
/// Không tạo placeholder `invite_{email}` — Keycloak là source of truth;
/// chỉ ghi SQL + OpenFGA với `sub` thật để tránh desync authz.
pub async fn add_workspace_member(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
    Json(body): Json<AddWorkspaceMemberRequest>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::CanManageMember, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
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

    // Chỉ chấp nhận user đã đăng ký + verify email trong Keycloak
    let keycloak_user = match state
        .keycloak_client
        .get_verified_user_by_email(&email)
        .await
    {
        Ok(Some(user)) => user,
        Ok(None) => {
            return ApiError {
                status: StatusCode::NOT_FOUND,
                code: "USER_NOT_FOUND_IN_IDENTITY",
                message: "User has not signed up yet. Please ask them to register first."
                    .to_string(),
            }
            .into_response();
        }
        Err(err) => {
            error!(error = %err, email = %email, "Failed to lookup user in Keycloak");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let member_user_id = keycloak_user.id;

    match insert_workspace_member(&state.pool, workspace_id, &member_user_id, &email, role).await {
        Ok(member) => {
            let member_id = member.id.clone();

            let fga_relation = if role == "ADMIN" {
                Relation::Admin
            } else {
                Relation::Member
            };

            // Ghi tuple OpenFGA ngay với user id thật (Keycloak sub)
            let tuple = TupleKey {
                user: format!("user:{member_id}"),
                relation: fga_relation.as_str().to_string(),
                object: Object::Workspace(workspace_id).to_string(),
            };

            if let Err(err) = state
                .authz_client
                .write_tuples(vec![tuple.clone()], Vec::new())
                .await
            {
                error!(error = %err, workspace_id = %workspace_id, user_id = %member_id, "Failed to write workspace member to OpenFGA");

                if let Err(outbox_err) = enqueue_tuple_write(&state.pool, &tuple).await {
                    error!(
                        error = %outbox_err,
                        workspace_id = %workspace_id,
                        user_id = %member_id,
                        "Failed to enqueue authz outbox recovery event for member add"
                    );
                }
            }

            if let Err(err) = insert_audit_event(
                &state.pool,
                AuditEventRecord::new(AuditEventType::MemberAdded)
                    .with_actor_user_id(authz.user_id.clone())
                    .with_workspace_id(workspace_id)
                    .with_target("workspace_member", member_id.clone())
                    .with_metadata(json!({
                        "role": role,
                        "member_user_id": member_id,
                        "member_email": email
                    })),
            )
            .await
            {
                error!(
                    error = %err,
                    actor_user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    "Failed to write audit event for member add"
                );
            }

            (StatusCode::CREATED, Json(member)).into_response()
        }
        Err(MemberInsertError::AlreadyMember) => {
            (StatusCode::CONFLICT, "User is already a workspace member").into_response()
        }
        Err(MemberInsertError::Database(err)) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                member_user_id = %member_user_id,
                email = %email,
                "Failed to add workspace member"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// PATCH /workspaces/{workspace_id}/members/{member_id}/role
/// Thay đổi role thành viên workspace (chỉ Tenant Owner có quyền)
pub async fn update_workspace_member_role(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, member_id)): Path<(Uuid, String)>,
    Json(body): Json<UpdateWorkspaceMemberRoleRequest>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::CanAssignRole, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    let new_role = match normalize_member_role(&body.role) {
        Some(role) => role,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                "Role must be owner or member (admin/user also accepted)",
            )
                .into_response();
        }
    };

    let current_role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(workspace_id)
    .bind(&member_id)
    .fetch_optional(&state.pool)
    .await
    .unwrap_or(None);

    let Some(current_role_str) = current_role else {
        return StatusCode::NOT_FOUND.into_response();
    };

    if current_role_str == new_role {
        return StatusCode::NO_CONTENT.into_response();
    }

    let mut tx = match state.pool.begin().await {
        Ok(t) => t,
        Err(err) => {
            error!(error = %err, "Failed to start transaction");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if let Err(err) = sqlx::query(
        "UPDATE workspace_members SET role = $1 WHERE workspace_id = $2 AND user_id = $3",
    )
    .bind(new_role)
    .bind(workspace_id)
    .bind(&member_id)
    .execute(&mut *tx)
    .await
    {
        error!(error = %err, workspace_id = %workspace_id, user_id = %member_id, "Failed to update member role in DB");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let old_relation = if current_role_str == "ADMIN" {
        Relation::Admin
    } else {
        Relation::Member
    };
    let new_relation = if new_role == "ADMIN" {
        Relation::Admin
    } else {
        Relation::Member
    };

    if let Err(err) = state
        .authz_client
        .write_tuples(
            vec![TupleKey {
                user: format!("user:{}", member_id),
                relation: new_relation.as_str().to_string(),
                object: Object::Workspace(workspace_id).to_string(),
            }],
            vec![TupleKey {
                user: format!("user:{}", member_id),
                relation: old_relation.as_str().to_string(),
                object: Object::Workspace(workspace_id).to_string(),
            }],
        )
        .await
    {
        error!(error = %err, workspace_id = %workspace_id, user_id = %member_id, "Failed to sync role update to OpenFGA");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = tx.commit().await {
        error!(error = %err, "Failed to commit transaction");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = insert_audit_event(
        &state.pool,
        AuditEventRecord::new(AuditEventType::MemberRoleChanged)
            .with_actor_user_id(authz.user_id.clone())
            .with_workspace_id(workspace_id)
            .with_target("workspace_member", member_id.clone())
            .with_metadata(json!({
                "member_user_id": member_id,
                "old_role": current_role_str,
                "new_role": new_role
            })),
    )
    .await
    {
        error!(
            error = %err,
            actor_user_id = %authz.user_id,
            workspace_id = %workspace_id,
            "Failed to write audit event for member role change"
        );
    }

    StatusCode::NO_CONTENT.into_response()
}

/// DELETE /workspaces/{workspace_id}/members/{member_id}
/// Xoá thành viên khỏi workspace.
///
/// OpenFGA revoke trước SQL (fail-closed): thà từ chối tạm thời còn hơn
/// báo xoá thành công trong khi user vẫn còn quyền truy cập.
pub async fn remove_workspace_member(
    State(state): State<AppState>,
    authz: Authz,
    Path((workspace_id, member_id)): Path<(Uuid, String)>,
) -> impl IntoResponse {
    if let Err(err) = authz
        .require_relation(Relation::CanManageMember, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    if member_id == authz.user_id {
        return (
            StatusCode::BAD_REQUEST,
            "You cannot remove yourself from the workspace",
        )
            .into_response();
    }

    let role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(workspace_id)
    .bind(&member_id)
    .fetch_optional(&state.pool)
    .await
    .unwrap_or(None);

    let Some(role_str) = role else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let fga_relation = if role_str == "ADMIN" {
        Relation::Admin
    } else {
        Relation::Member
    };

    let tuple = TupleKey {
        user: format!("user:{member_id}"),
        relation: fga_relation.as_str().to_string(),
        object: Object::Workspace(workspace_id).to_string(),
    };

    // OpenFGA trước SQL — outbox không được coi là bằng chứng đã revoke
    match state
        .authz_client
        .write_tuples(Vec::new(), vec![tuple.clone()])
        .await
    {
        Ok(()) => {}
        // Tuple đã không còn = revoke idempotent (cùng semantics outbox)
        Err(err) if is_missing_tuple_delete_error(&err) => {}
        Err(err) => {
            error!(
                error = %err,
                workspace_id = %workspace_id,
                user_id = %member_id,
                "Failed to delete workspace member from OpenFGA"
            );
            return ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "AUTHZ_REVOKE_FAILED",
                message: "Failed to revoke workspace membership in authorization store".to_string(),
            }
            .into_response();
        }
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
        Ok(_) => {
            if let Err(err) = insert_audit_event(
                &state.pool,
                AuditEventRecord::new(AuditEventType::MemberRemoved)
                    .with_actor_user_id(authz.user_id.clone())
                    .with_workspace_id(workspace_id)
                    .with_target("workspace_member", member_id.clone())
                    .with_metadata(json!({
                        "member_user_id": member_id,
                        "removed_role": role_str
                    })),
            )
            .await
            {
                error!(
                    error = %err,
                    actor_user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    "Failed to write audit event for member remove"
                );
            }

            StatusCode::NO_CONTENT.into_response()
        }
        Err(err) => {
            // FGA đã revoke nhưng SQL vẫn còn membership — ghi tuple_write để khôi phục
            // đồng bộ với read model; admin nhận lỗi và có thể retry remove.
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                member_id = %member_id,
                "Failed to remove workspace member from SQL after OpenFGA revoke"
            );

            if let Err(outbox_err) = enqueue_tuple_write(&state.pool, &tuple).await {
                error!(
                    error = %outbox_err,
                    workspace_id = %workspace_id,
                    user_id = %member_id,
                    "Failed to enqueue authz outbox recovery write after member remove SQL failure"
                );
            }

            ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "MEMBER_REMOVE_FAILED",
                message: "Membership authorization was revoked but membership record could not be removed"
                    .to_string(),
            }
            .into_response()
        }
    }
}

/// OpenFGA trả lỗi khi xoá tuple không tồn tại — coi là success cho revoke idempotent.
fn is_missing_tuple_delete_error(err: &AuthzError) -> bool {
    match err {
        AuthzError::OpenFga { body, .. } => {
            let body_lower = body.to_ascii_lowercase();
            body_lower.contains("does not exist") || body_lower.contains("not found")
        }
        AuthzError::Http(_) => false,
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

/// Chèn membership với `user_id` thật từ Keycloak (không tạo placeholder).
async fn insert_workspace_member(
    pool: &PgPool,
    workspace_id: Uuid,
    user_id: &str,
    email: &str,
    role: &str,
) -> Result<WorkspaceMemberResponse, MemberInsertError> {
    let mut tx = pool.begin().await.map_err(MemberInsertError::Database)?;

    // Đồng bộ users row với Keycloak sub trước khi ghi membership
    sqlx::query(
        r#"
        INSERT INTO users (id, email)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email
        "#,
    )
    .bind(user_id)
    .bind(email)
    .execute(&mut *tx)
    .await
    .map_err(MemberInsertError::Database)?;

    let already_member: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM workspace_members
            WHERE workspace_id = $1
              AND user_id = $2
        )
        "#,
    )
    .bind(workspace_id)
    .bind(user_id)
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
    .bind(user_id)
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
        "member" | "user" => Some("MEMBER"),
        _ => None,
    }
}
