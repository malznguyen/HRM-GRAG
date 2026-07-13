use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};
use tracing::error;
use uuid::Uuid;

use crate::audit::{AuditEventRecord, AuditEventType, insert_audit_event};
use crate::auth::authz::{ApiError, Authz, AuthzClient, AuthzError, Object, Relation, TupleKey};
use crate::auth::outbox::enqueue_tuple_write;
use crate::auth::workspace_role::WorkspaceMemberRole;
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
/// Role `admin` yêu cầu `can_assign_role` (Tenant Owner). `can_manage_member`
/// chỉ đủ để thêm `member`. Không tạo placeholder `invite_{email}`.
pub async fn add_workspace_member(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
    Json(body): Json<AddWorkspaceMemberRequest>,
) -> impl IntoResponse {
    // 1) Validate role trước mọi side effect
    let role = match WorkspaceMemberRole::parse_api(&body.role) {
        Ok(role) => role,
        Err(err) => return err.into_response(),
    };

    // 2) Baseline: can_manage_member
    if let Err(err) = authz
        .require_relation(Relation::CanManageMember, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    // 3) Admin assignment: can_assign_role (Tenant Owner only)
    if role.is_admin() {
        match authz
            .check(Relation::CanAssignRole, &Object::Workspace(workspace_id))
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return ApiError {
                    status: StatusCode::FORBIDDEN,
                    code: "ROLE_ASSIGNMENT_DENIED",
                    message: "Only tenant owners can assign workspace admin roles".to_string(),
                }
                .into_response();
            }
            Err(_) => {
                return ApiError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: "AUTHZ_ERROR",
                    message: "Authorization service unavailable".to_string(),
                }
                .into_response();
            }
        }
    }

    let email = normalize_email(&body.email);
    if email.is_empty() || !email.contains('@') {
        return ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "INVALID_EMAIL",
            message: "Valid email is required".to_string(),
        }
        .into_response();
    }

    // Chỉ sau khi authz PASS mới lookup Keycloak / ghi SQL / FGA / audit
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
    let member_email = keycloak_user
        .email
        .as_deref()
        .map(normalize_email)
        .unwrap_or_else(|| email.clone());

    match insert_workspace_member(
        &state.pool,
        workspace_id,
        &member_user_id,
        &member_email,
        role.as_sql(),
    )
    .await
    {
        Ok(member) => {
            let member_id = member.id.clone();
            let fga_relation = role.as_fga_relation();

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
                        "role": role.as_sql(),
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
        Err(MemberInsertError::AlreadyMember) => ApiError {
            status: StatusCode::CONFLICT,
            code: "ALREADY_WORKSPACE_MEMBER",
            message: "User is already a workspace member".to_string(),
        }
        .into_response(),
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

/// PATCH /workspaces/{workspace_id}/members/{member_id}
/// Thay đổi role thành viên workspace (chỉ Tenant Owner có `can_assign_role`).
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

    let new_role = match WorkspaceMemberRole::parse_api(&body.role) {
        Ok(role) => role,
        Err(err) => return err.into_response(),
    };

    let mut tx = match state.pool.begin().await {
        Ok(t) => t,
        Err(err) => {
            error!(error = %err, "Failed to start transaction for member role update");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Serialize admin mutations theo workspace
    let tenant_id = match lock_workspace_tenant(&mut tx, workspace_id).await {
        Ok(Some(tenant_id)) => tenant_id,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(error = %err, workspace_id = %workspace_id, "Failed to lock workspace for role update");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let current_role_sql: Option<String> = match sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(workspace_id)
    .bind(&member_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(role) => role,
        Err(err) => {
            error!(error = %err, workspace_id = %workspace_id, "Failed to reload member role");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let Some(current_role_str) = current_role_sql else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let Some(current_role) = WorkspaceMemberRole::from_sql(&current_role_str) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };

    if current_role == new_role {
        return StatusCode::NO_CONTENT.into_response();
    }

    // Demote admin → member: last-admin guard
    if current_role.is_admin() && !new_role.is_admin() {
        match ensure_management_path_after_losing_admin(
            &mut tx,
            &state.authz_client,
            workspace_id,
            tenant_id,
            &member_id,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => return last_workspace_admin_error().into_response(),
            Err(err) => {
                error!(
                    error = %err,
                    workspace_id = %workspace_id,
                    member_id = %member_id,
                    "Failed last-admin check on role demote"
                );
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    }

    if let Err(err) = sqlx::query(
        "UPDATE workspace_members SET role = $1 WHERE workspace_id = $2 AND user_id = $3",
    )
    .bind(new_role.as_sql())
    .bind(workspace_id)
    .bind(&member_id)
    .execute(&mut *tx)
    .await
    {
        error!(error = %err, workspace_id = %workspace_id, user_id = %member_id, "Failed to update member role in DB");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let old_relation = current_role.as_fga_relation();
    let new_relation = new_role.as_fga_relation();

    // OpenFGA trước commit SQL — fail-closed nếu write lỗi (tx rollback)
    if let Err(err) = state
        .authz_client
        .write_tuples(
            vec![TupleKey {
                user: format!("user:{member_id}"),
                relation: new_relation.as_str().to_string(),
                object: Object::Workspace(workspace_id).to_string(),
            }],
            vec![TupleKey {
                user: format!("user:{member_id}"),
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
        error!(error = %err, "Failed to commit member role update");
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
                "old_role": current_role.as_sql(),
                "new_role": new_role.as_sql()
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
/// OpenFGA revoke trước SQL (fail-closed). Admin removal có last-admin guard
/// với workspace row lock để chống concurrent demote/remove.
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
        return ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "CANNOT_REMOVE_SELF",
            message: "You cannot remove yourself from the workspace".to_string(),
        }
        .into_response();
    }

    let mut tx = match state.pool.begin().await {
        Ok(t) => t,
        Err(err) => {
            error!(error = %err, "Failed to start transaction for member remove");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let tenant_id = match lock_workspace_tenant(&mut tx, workspace_id).await {
        Ok(Some(tenant_id)) => tenant_id,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(error = %err, workspace_id = %workspace_id, "Failed to lock workspace for member remove");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let role_sql: Option<String> = match sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(workspace_id)
    .bind(&member_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(role) => role,
        Err(err) => {
            error!(error = %err, workspace_id = %workspace_id, "Failed to reload member role for remove");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let Some(role_str) = role_sql else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let Some(role) = WorkspaceMemberRole::from_sql(&role_str) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };

    if role.is_admin() {
        match ensure_management_path_after_losing_admin(
            &mut tx,
            &state.authz_client,
            workspace_id,
            tenant_id,
            &member_id,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => return last_workspace_admin_error().into_response(),
            Err(err) => {
                error!(
                    error = %err,
                    workspace_id = %workspace_id,
                    member_id = %member_id,
                    "Failed last-admin check on member remove"
                );
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    }

    let fga_relation = role.as_fga_relation();
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
    .execute(&mut *tx)
    .await;

    match result {
        Ok(outcome) if outcome.rows_affected() == 0 => StatusCode::NOT_FOUND.into_response(),
        Ok(_) => {
            if let Err(err) = tx.commit().await {
                error!(error = %err, "Failed to commit member remove");
                // FGA đã revoke; cố gắng recovery write nếu SQL commit fail
                if let Err(outbox_err) = enqueue_tuple_write(&state.pool, &tuple).await {
                    error!(
                        error = %outbox_err,
                        workspace_id = %workspace_id,
                        user_id = %member_id,
                        "Failed to enqueue authz outbox recovery after member remove commit failure"
                    );
                }
                return ApiError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: "MEMBER_REMOVE_FAILED",
                    message:
                        "Membership authorization was revoked but membership record could not be removed"
                            .to_string(),
                }
                .into_response();
            }

            if let Err(err) = insert_audit_event(
                &state.pool,
                AuditEventRecord::new(AuditEventType::MemberRemoved)
                    .with_actor_user_id(authz.user_id.clone())
                    .with_workspace_id(workspace_id)
                    .with_target("workspace_member", member_id.clone())
                    .with_metadata(json!({
                        "member_user_id": member_id,
                        "removed_role": role.as_sql()
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
            error!(
                error = %err,
                user_id = %authz.user_id,
                workspace_id = %workspace_id,
                member_id = %member_id,
                "Failed to remove workspace member from SQL after OpenFGA revoke"
            );

            let _ = tx.rollback().await;

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
                message:
                    "Membership authorization was revoked but membership record could not be removed"
                        .to_string(),
            }
            .into_response()
        }
    }
}

fn last_workspace_admin_error() -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "LAST_WORKSPACE_ADMIN",
        message: "Cannot remove or demote the last workspace administrator without a valid tenant owner management path"
            .to_string(),
    }
}

/// Khoá hàng workspace để serialize mutation admin (FOR UPDATE).
async fn lock_workspace_tenant(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_id FROM workspaces WHERE id = $1 FOR UPDATE")
        .bind(workspace_id)
        .fetch_optional(&mut **tx)
        .await
}

/// True nếu sau khi mất admin của `exclude_user_id` vẫn còn management path hợp lệ.
///
/// Path hợp lệ:
/// 1. Explicit workspace ADMIN khác + OpenFGA admin relation còn,
/// 2. hoặc Tenant Owner SQL OWNER + OpenFGA owner trên tenant.
async fn ensure_management_path_after_losing_admin(
    tx: &mut Transaction<'_, Postgres>,
    authz: &AuthzClient,
    workspace_id: Uuid,
    tenant_id: Uuid,
    exclude_user_id: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let other_admins: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT user_id
        FROM workspace_members
        WHERE workspace_id = $1
          AND role = 'ADMIN'
          AND user_id <> $2
        "#,
    )
    .bind(workspace_id)
    .bind(exclude_user_id)
    .fetch_all(&mut **tx)
    .await?;

    for admin_id in &other_admins {
        if authz.check_workspace_admin(admin_id, workspace_id).await? {
            return Ok(true);
        }
    }

    let tenant_owners: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT user_id
        FROM tenant_members
        WHERE tenant_id = $1
          AND role = 'OWNER'
        "#,
    )
    .bind(tenant_id)
    .fetch_all(&mut **tx)
    .await?;

    for owner_id in &tenant_owners {
        if authz.check_tenant_owner(owner_id, tenant_id).await? {
            return Ok(true);
        }
    }

    Ok(false)
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
