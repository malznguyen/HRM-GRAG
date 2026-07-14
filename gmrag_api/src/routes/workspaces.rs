use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::error;
use uuid::Uuid;

use crate::audit::{AuditEventRecord, AuditEventType, insert_audit_event, insert_audit_event_tx};
use crate::auth::authz::{ApiError, Authz, Object, Relation, TupleKey, delete_tuples_fga_first};
use crate::auth::outbox::enqueue_tuple_write;
use crate::auth::resource_cleanup::capture_workspace_delete_plan;
use crate::invite::{normalize_email, upsert_user};
use crate::retrieval::outbox::enqueue_delete_by_workspace_tx;
use crate::state::AppState;
use crate::storage::cleanup::build_workspace_prefix;
use crate::storage::outbox::enqueue_delete_prefix_tx;
use crate::tenant_directory::{
    OwnedTenant, TenantDirectoryPage, list_owned_tenant_candidates, list_tenants as read_tenants,
};

const DEFAULT_TENANT_PAGE_LIMIT: i64 = 20;
const MAX_TENANT_PAGE_LIMIT: i64 = 100;

#[derive(Deserialize)]
pub struct ListTenantsQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    q: Option<String>,
}

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

/// Liệt kê tenant cho Platform Admin từ SQL read model.
pub async fn list_tenants(
    State(state): State<AppState>,
    authz: Authz,
    Query(query): Query<ListTenantsQuery>,
) -> Result<Json<TenantDirectoryPage>, crate::api_error::ApiError> {
    let limit = query.limit.unwrap_or(DEFAULT_TENANT_PAGE_LIMIT);
    let offset = query.offset.unwrap_or(0);
    if !(0..=MAX_TENANT_PAGE_LIMIT).contains(&limit) || offset < 0 {
        return Err(crate::api_error::ApiError::new(
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
            "limit must be between 0 and 100 and offset must be non-negative",
        ));
    }

    authz
        .require_relation(Relation::Admin, &Object::Platform)
        .await?;

    let normalized_query = query.q.as_deref().map(str::trim).filter(|q| !q.is_empty());
    let page = read_tenants(&state.pool, limit, offset, normalized_query)
        .await
        .map_err(|err| {
            error!(error = %err, "Failed to list tenants");
            crate::api_error::ApiError::from_status(StatusCode::INTERNAL_SERVER_ERROR)
        })?;

    Ok(Json(page))
}

/// Liệt kê tenant mà caller là owner theo giao của SQL read model và OpenFGA.
pub async fn list_my_tenants(
    State(state): State<AppState>,
    authz: Authz,
) -> Result<Json<Vec<OwnedTenant>>, crate::api_error::ApiError> {
    let candidates = list_owned_tenant_candidates(&state.pool, &authz.user_id)
        .await
        .map_err(|err| {
            error!(error = %err, user_id = %authz.user_id, "Failed to list owned tenant candidates");
            crate::api_error::ApiError::from_status(StatusCode::INTERNAL_SERVER_ERROR)
        })?;

    let mut tenants = Vec::with_capacity(candidates.len());
    for tenant in candidates {
        let allowed = authz
            .check(Relation::Owner, &Object::Tenant(tenant.id))
            .await
            .map_err(|err| {
                error!(
                    error = %err,
                    user_id = %authz.user_id,
                    tenant_id = %tenant.id,
                    "OpenFGA tenant owner check failed; refusing SQL-only fallback"
                );
                crate::api_error::ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "AUTHZ_ERROR",
                    "Authorization service unavailable",
                )
            })?;

        if allowed {
            tenants.push(tenant);
        }
    }

    Ok(Json(tenants))
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
    if let Err(err) = authz
        .require_relation(Relation::Admin, &Object::Platform)
        .await
    {
        return err.into_response();
    }

    let tenant: TenantResponse = match sqlx::query_as(
        "INSERT INTO tenants (name) VALUES ($1) RETURNING id, name, created_at",
    )
    .bind(name)
    .fetch_one(&state.pool)
    .await
    {
        Ok(t) => t,
        Err(err) => {
            error!(error = %err, "Failed to insert tenant");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let platform_tenant_tuple = TupleKey {
        user: "platform:system".to_string(),
        relation: Relation::Platform.as_str().to_string(),
        object: Object::Tenant(tenant.id).to_string(),
    };

    if let Err(err) = state
        .authz_client
        .write_tuples(vec![platform_tenant_tuple.clone()], Vec::new())
        .await
    {
        error!(error = %err, tenant_id = %tenant.id, "Failed to write tenant-platform relation to OpenFGA");

        if let Err(outbox_err) = enqueue_tuple_write(&state.pool, &platform_tenant_tuple).await {
            error!(
                error = %outbox_err,
                tenant_id = %tenant.id,
                "Failed to enqueue authz outbox recovery event for tenant-platform relation"
            );
        }

        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = insert_audit_event(
        &state.pool,
        AuditEventRecord::new(AuditEventType::TenantCreated)
            .with_actor_user_id(authz.user_id.clone())
            .with_tenant_id(tenant.id)
            .with_target("tenant", tenant.id.to_string())
            .with_metadata(json!({ "tenant_name": tenant.name.clone() })),
    )
    .await
    {
        error!(
            error = %err,
            actor_user_id = %authz.user_id,
            tenant_id = %tenant.id,
            "Failed to write audit event for tenant creation"
        );
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
    let email = normalize_email(&body.email);
    if email.is_empty() || !email.contains('@') {
        return (StatusCode::BAD_REQUEST, "Valid email is required").into_response();
    }

    // Check Platform Admin
    if let Err(err) = authz
        .require_relation(Relation::Admin, &Object::Platform)
        .await
    {
        return err.into_response();
    }

    // Tìm kiếm user từ Keycloak Admin API
    let keycloak_user = match state
        .keycloak_client
        .get_verified_user_by_email(&email)
        .await
    {
        Ok(Some(user)) => user,
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                "User with verified email not found in Keycloak",
            )
                .into_response();
        }
        Err(err) => {
            error!(error = %err, email = %email, "Failed to lookup user in Keycloak");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let Some(keycloak_email) = keycloak_user.email.as_deref().map(normalize_email) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if let Err(err) = upsert_user(&state.pool, &keycloak_user.id, &keycloak_email).await {
        error!(error = %err, user_id = %keycloak_user.id, "Failed to sync Keycloak owner into SQL");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
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
    if let Err(err) = state
        .authz_client
        .write_tuple(
            &format!("user:{}", keycloak_user.id),
            Relation::Owner,
            &Object::Tenant(tenant_id),
        )
        .await
    {
        error!(error = %err, tenant_id = %tenant_id, user_id = %keycloak_user.id, "Failed to write tenant owner to OpenFGA");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = tx.commit().await {
        error!(error = %err, "Failed to commit transaction");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = insert_audit_event(
        &state.pool,
        AuditEventRecord::new(AuditEventType::TenantOwnerAdded)
            .with_actor_user_id(authz.user_id.clone())
            .with_tenant_id(tenant_id)
            .with_target("tenant_owner", keycloak_user.id.clone())
            .with_metadata(json!({
                "owner_user_id": keycloak_user.id,
                "owner_email": email
            })),
    )
    .await
    {
        error!(
            error = %err,
            actor_user_id = %authz.user_id,
            tenant_id = %tenant_id,
            "Failed to write audit event for tenant owner add"
        );
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
    if let Err(err) = authz
        .require_relation(Relation::Owner, &Object::Tenant(tenant_id))
        .await
    {
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
        "INSERT INTO workspaces (tenant_id, name) VALUES ($1, $2) RETURNING id, name, created_at",
    )
    .bind(tenant_id)
    .bind(name)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(w) => w,
        Err(err) => {
            error!(error = %err, "Failed to insert workspace");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Thêm Workspace member là ADMIN cho Tenant Owner để hiển thị UI
    if let Err(err) = sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
    )
    .bind(workspace.id)
    .bind(&authz.user_id)
    .execute(&mut *tx)
    .await
    {
        error!(error = %err, workspace_id = %workspace.id, "Failed to insert workspace member");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // Ghi quan hệ workspace-tenant vào OpenFGA: tenant:{tenant_id} - tenant - workspace:{workspace_id}
    if let Err(err) = state
        .authz_client
        .write_tuple(
            &format!("tenant:{}", tenant_id),
            Relation::Tenant,
            &Object::Workspace(workspace.id),
        )
        .await
    {
        error!(error = %err, workspace_id = %workspace.id, "Failed to write workspace tenant to OpenFGA");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // Ghi quan hệ workspace admin vào OpenFGA cho Tenant Owner: user:{id} - admin - workspace:{workspace_id}
    if let Err(err) = state
        .authz_client
        .write_tuple(
            &format!("user:{}", authz.user_id),
            Relation::Admin,
            &Object::Workspace(workspace.id),
        )
        .await
    {
        error!(error = %err, workspace_id = %workspace.id, "Failed to write workspace admin to OpenFGA");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = tx.commit().await {
        error!(error = %err, "Failed to commit transaction");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    if let Err(err) = insert_audit_event(
        &state.pool,
        AuditEventRecord::new(AuditEventType::WorkspaceCreated)
            .with_actor_user_id(authz.user_id.clone())
            .with_tenant_id(tenant_id)
            .with_workspace_id(workspace.id)
            .with_target("workspace", workspace.id.to_string())
            .with_metadata(json!({ "workspace_name": workspace.name.clone() })),
    )
    .await
    {
        error!(
            error = %err,
            actor_user_id = %authz.user_id,
            tenant_id = %tenant_id,
            workspace_id = %workspace.id,
            "Failed to write audit event for workspace creation"
        );
    }

    (StatusCode::CREATED, Json(workspace)).into_response()
}

/// GET /workspaces
/// SQL candidates (membership / tenant owner read model) ∩ OpenFGA `member`.
///
/// SQL chỉ là candidate list — không phải authorization source of truth.
/// Lỗi OpenFGA fail-closed (không trả SQL-only).
pub async fn list_workspaces(State(state): State<AppState>, authz: Authz) -> impl IntoResponse {
    let candidates: Result<Vec<WorkspaceResponse>, _> = sqlx::query_as(
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

    let candidates = match candidates {
        Ok(rows) => rows,
        Err(err) => {
            error!(error = %err, user_id = %authz.user_id, "Failed to list workspace candidates");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if candidates.is_empty() {
        return Json(Vec::<WorkspaceResponse>::new()).into_response();
    }

    // Deduplicate theo id (GROUP BY đã đảm bảo, vẫn giữ deterministic order)
    let mut seen = std::collections::HashSet::new();
    let mut unique: Vec<WorkspaceResponse> = Vec::with_capacity(candidates.len());
    for row in candidates {
        if seen.insert(row.id) {
            unique.push(row);
        }
    }

    let workspace_ids: Vec<Uuid> = unique.iter().map(|w| w.id).collect();
    let allowed_ids = match state
        .authz_client
        .filter_workspaces_by_member(&authz.user_id, &workspace_ids)
        .await
    {
        Ok(ids) => ids.into_iter().collect::<std::collections::HashSet<_>>(),
        Err(err) => {
            error!(
                error = %err,
                user_id = %authz.user_id,
                "OpenFGA workspace list filter failed; refusing SQL-only fallback"
            );
            return crate::auth::authz::ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "AUTHZ_ERROR",
                message: "Authorization service unavailable".to_string(),
            }
            .into_response();
        }
    };

    let filtered: Vec<WorkspaceResponse> = unique
        .into_iter()
        .filter(|w| allowed_ids.contains(&w.id))
        .collect();

    Json(filtered).into_response()
}

/// DELETE /workspaces/{id}
/// Tenant Owner xoá Workspace
pub async fn delete_workspace(
    State(state): State<AppState>,
    authz: Authz,
    Path(workspace_id): Path<Uuid>,
) -> impl IntoResponse {
    // Check Tenant Owner (CanAssignRole đại diện cho tenant_owner trên workspace)
    if let Err(err) = authz
        .require_relation(Relation::CanAssignRole, &Object::Workspace(workspace_id))
        .await
    {
        return err.into_response();
    }

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => {
            error!(error = %err, workspace_id = %workspace_id, "Failed to start workspace delete transaction");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let plan = match capture_workspace_delete_plan(&mut tx, workspace_id).await {
        Ok(Some(plan)) => plan,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(error = %err, workspace_id = %workspace_id, "Failed to capture workspace authorization cleanup plan");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if let Err(err) = delete_tuples_fga_first(&state.authz_client, &plan.tuples).await {
        error!(error = %err, workspace_id = %workspace_id, "Failed to revoke workspace subtree tuples in OpenFGA");
        return ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "AUTHZ_REVOKE_FAILED",
            message: "Failed to revoke workspace authorization before deletion".to_string(),
        }
        .into_response();
    }

    let tenant_id = plan.tenant_id;
    let result: Result<(), sqlx::Error> = async {
        enqueue_delete_by_workspace_tx(&mut tx, workspace_id).await?;
        let prefix = build_workspace_prefix(tenant_id, workspace_id);
        enqueue_delete_prefix_tx(
            &mut tx,
            &prefix,
            state.storage.bucket(),
            Some(tenant_id),
            Some(workspace_id),
        )
        .await?;
        insert_audit_event_tx(
            &mut tx,
            AuditEventRecord::new(AuditEventType::WorkspaceDeleted)
                .with_actor_user_id(authz.user_id.clone())
                .with_workspace_id(workspace_id)
                .with_tenant_id(tenant_id)
                .with_target("workspace", workspace_id.to_string())
                .with_metadata(json!({
                    "openfga_tuples_revoked": plan.tuples.len(),
                    "qdrant_cleanup": "outbox_enqueued",
                    "storage_cleanup": "outbox_enqueued",
                })),
        )
        .await?;
        let outcome = sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind(workspace_id)
            .execute(&mut *tx)
            .await?;
        if outcome.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(
                "workspace disappeared before delete could commit".to_string(),
            ));
        }
        tx.commit().await
    }
    .await;

    match result {
        Ok(()) => {
            // wait=true + timeout ngắn (request path). Outbox đã durable sau commit.
            // Không dùng worker timeout trên HTTP — tránh treo DELETE khi Qdrant chậm.
            if let Err(err) = state
                .retrieval
                .delete_points_by_workspace_for_request(workspace_id)
                .await
            {
                error!(
                    error = %err,
                    user_id = %authz.user_id,
                    workspace_id = %workspace_id,
                    request_timeout_secs = state.retrieval.delete_request_timeout_secs(),
                    "Failed to delete workspace points from Qdrant after database cleanup"
                );
            }

            // storage_outbox delete_prefix đã durable sau commit; S3 prefix cleanup qua
            // process-storage-outbox (OPS-003 unattended). Không gọi S3 trên HTTP path.
            StatusCode::NO_CONTENT.into_response()
        }
        Err(err) => {
            error!(error = %err, user_id = %authz.user_id, workspace_id = %workspace_id, "Failed to delete workspace");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
