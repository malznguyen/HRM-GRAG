use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use tracing::error;

use crate::auth::authz::{Authz, Relation, Object};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateTenantRequest {
    pub name: String,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct TenantResponse {
    pub id: Uuid,
    pub name: String,
    pub created_at: NaiveDateTime,
}

#[derive(Deserialize)]
pub struct AddTenantOwnerRequest {
    pub email: String,
}

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

/// POST /tenants
/// Platform Admin tạo Tenant mới
pub async fn create_tenant(
    State(state): State<AppState>,
    authz: Authz,
    Json(body): Json<CreateTenantRequest>,
) -> impl IntoResponse {
    let name = body.name.trim();
    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, "Tenant name is required").into_response();
    }

    // Check Platform Admin
    if let Err(err) = authz.require_relation(Relation::Admin, &Object::Platform).await {
        return err.into_response();
    }

    let tenant: TenantResponse = match sqlx::query_as(
        "INSERT INTO tenants (name) VALUES ($1) RETURNING id, name, created_at"
    )
    .bind(name)
    .fetch_one(&state.pool)
    .await {
        Ok(t) => t,
        Err(err) => {
            error!(error = %err, "Failed to insert tenant");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Ghi quan hệ Platform vào OpenFGA: platform:system - platform - tenant:{tenant_id}
    if let Err(err) = state.authz_client.write_tuple(
        "platform:system",
        Relation::Platform,
        &Object::Tenant(tenant.id)
    ).await {
        error!(error = %err, tenant_id = %tenant.id, "Failed to write tenant-platform relation to OpenFGA");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    (StatusCode::CREATED, Json(tenant)).into_response()
}

/// POST /tenants/{id}/owners
/// Platform Admin thêm Tenant Owner
pub async fn add_tenant_owner(
    State(state): State<AppState>,
    authz: Authz,
    Path(tenant_id): Path<Uuid>,
    Json(body): Json<AddTenantOwnerRequest>,
) -> impl IntoResponse {
    let email = body.email.trim();
    if email.is_empty() || !email.contains('@') {
        return (StatusCode::BAD_REQUEST, "Valid email is required").into_response();
    }

    // Check Platform Admin
    if let Err(err) = authz.require_relation(Relation::Admin, &Object::Platform).await {
        return err.into_response();
    }

    // Tìm kiếm user từ Keycloak Admin API
    let keycloak_user = match state.keycloak_client.get_verified_user_by_email(email).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            return (StatusCode::BAD_REQUEST, "User with verified email not found in Keycloak").into_response();
        }
        Err(err) => {
            error!(error = %err, email = %email, "Failed to lookup user in Keycloak");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Sync user vào SQL nếu chưa tồn tại
    let user_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)")
        .bind(&keycloak_user.id)
        .fetch_one(&state.pool)
        .await
        .unwrap_or(false);

    if !user_exists {
        if let Err(err) = sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING")
            .bind(&keycloak_user.id)
            .bind(email)
            .execute(&state.pool)
            .await {
            error!(error = %err, user_id = %keycloak_user.id, "Failed to insert synced user");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    let mut tx = match state.pool.begin().await {
        Ok(t) => t,
        Err(err) => {
            error!(error = %err, "Failed to start transaction");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Thêm owner vào SQL
    if let Err(err) = sqlx::query(
        "INSERT INTO tenant_members (tenant_id, user_id, role) VALUES ($1, $2, 'OWNER') ON CONFLICT DO NOTHING"
    )
    .bind(tenant_id)
    .bind(&keycloak_user.id)
    .execute(&mut *tx)
    .await {
        error!(error = %err, tenant_id = %tenant_id, user_id = %keycloak_user.id, "Failed to insert tenant member");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // Ghi quan hệ Owner vào OpenFGA: user:{id} - owner - tenant:{tenant_id}
    if let Err(err) = state.authz_client.write_tuple(
        &format!("user:{}", keycloak_user.id),
        Relation::Owner,
        &Object::Tenant(tenant_id)
    ).await {
        error!(error = %err, tenant_id = %tenant_id, user_id = %keycloak_user.id, "Failed to write tenant owner to OpenFGA");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = tx.commit().await {
        error!(error = %err, "Failed to commit transaction");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    StatusCode::NO_CONTENT.into_response()
}

/// POST /tenants/{id}/workspaces
/// Tenant Owner tạo Workspace mới
pub async fn create_workspace(
    State(state): State<AppState>,
    authz: Authz,
    Path(tenant_id): Path<Uuid>,
    Json(body): Json<CreateWorkspaceRequest>,
) -> impl IntoResponse {
    let name = body.name.trim();
    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, "Workspace name is required").into_response();
    }

    // Check Tenant Owner (owner trên tenant)
    if let Err(err) = authz.require_relation(Relation::Owner, &Object::Tenant(tenant_id)).await {
        return err.into_response();
    }

    let mut tx = match state.pool.begin().await {
        Ok(t) => t,
        Err(err) => {
            error!(error = %err, "Failed to start transaction");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Thêm workspace vào SQL
    let workspace: WorkspaceResponse = match sqlx::query_as(
        "INSERT INTO workspaces (tenant_id, name) VALUES ($1, $2) RETURNING id, name, created_at"
    )
    .bind(tenant_id)
    .bind(name)
    .fetch_one(&mut *tx)
    .await {
        Ok(w) => w,
        Err(err) => {
            error!(error = %err, "Failed to insert workspace");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Thêm Workspace member là ADMIN cho Tenant Owner để hiển thị UI
    if let Err(err) = sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')"
    )
    .bind(workspace.id)
    .bind(&authz.user_id)
    .execute(&mut *tx)
    .await {
        error!(error = %err, workspace_id = %workspace.id, "Failed to insert workspace member");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // Ghi quan hệ workspace-tenant vào OpenFGA: tenant:{tenant_id} - tenant - workspace:{workspace_id}
    if let Err(err) = state.authz_client.write_tuple(
        &format!("tenant:{}", tenant_id),
        Relation::Tenant,
        &Object::Workspace(workspace.id)
    ).await {
        error!(error = %err, workspace_id = %workspace.id, "Failed to write workspace tenant to OpenFGA");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // Ghi quan hệ workspace admin vào OpenFGA cho Tenant Owner: user:{id} - admin - workspace:{workspace_id}
    if let Err(err) = state.authz_client.write_tuple(
        &format!("user:{}", authz.user_id),
        Relation::Admin,
        &Object::Workspace(workspace.id)
    ).await {
        error!(error = %err, workspace_id = %workspace.id, "Failed to write workspace admin to OpenFGA");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = tx.commit().await {
        error!(error = %err, "Failed to commit transaction");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    (StatusCode::CREATED, Json(workspace)).into_response()
}

/// GET /workspaces
/// Liệt kê các Workspace mà user thuộc về (hoặc sở hữu Tenant chứa Workspace đó)
pub async fn list_workspaces(
    State(state): State<AppState>,
    authz: Authz,
) -> impl IntoResponse {
    let rows: Result<Vec<WorkspaceResponse>, _> = sqlx::query_as(
        r#"
        SELECT w.id, w.name, w.created_at
        FROM workspaces w
        LEFT JOIN workspace_members wm ON wm.workspace_id = w.id AND wm.user_id = $1
        LEFT JOIN tenant_members tm ON tm.tenant_id = w.tenant_id AND tm.user_id = $1 AND tm.role = 'OWNER'
        WHERE wm.user_id IS NOT NULL OR tm.user_id IS NOT NULL
        GROUP BY w.id, w.name, w.created_at
        ORDER BY w.created_at DESC
        "#,
    )
    .bind(&authz.user_id)
    .fetch_all(&state.pool)
    .await;

    match rows {
        Ok(workspaces) => Json(workspaces).into_response(),
        Err(err) => {
            error!(error = %err, user_id = %authz.user_id, "Failed to list workspaces");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// DELETE /workspaces/{id}
/// Tenant Owner xoá Workspace
pub async fn delete_workspace(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    // Check Tenant Owner (CanAssignRole đại diện cho tenant_owner trên workspace)
    if let Err(err) = authz.require_relation(Relation::CanAssignRole, &Object::Workspace(workspace_id)).await {
        return err.into_response();
    }

    let tenant_id: Option<Uuid> = sqlx::query_scalar("SELECT tenant_id FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .fetch_optional(&state.pool)
        .await
        .unwrap_or(None);

    let result = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(&state.pool)
        .await;

    match result {
        Ok(outcome) if outcome.rows_affected() == 0 => StatusCode::NOT_FOUND.into_response(),
        Ok(_) => {
            if let Some(tid) = tenant_id {
                // Xoá tuple quan hệ workspace-tenant trong OpenFGA
                if let Err(err) = state.authz_client.delete_tuple(
                    &format!("tenant:{}", tid),
                    Relation::Tenant,
                    &Object::Workspace(workspace_id)
                ).await {
                    error!(error = %err, workspace_id = %workspace_id, "Failed to delete workspace tenant from OpenFGA");
                }
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Err(err) => {
            error!(error = %err, user_id = %authz.user_id, workspace_id = %workspace_id, "Failed to delete workspace");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
