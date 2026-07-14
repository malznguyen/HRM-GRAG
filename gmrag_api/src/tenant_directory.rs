use std::collections::HashMap;

use chrono::NaiveDateTime;
use serde::Serialize;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Serialize)]
pub struct TenantOwner {
    pub id: String,
    pub email: String,
}

#[derive(Serialize)]
pub struct TenantDirectoryItem {
    pub id: Uuid,
    pub name: String,
    pub created_at: NaiveDateTime,
    pub owners: Vec<TenantOwner>,
}

#[derive(Serialize)]
pub struct TenantDirectoryPage {
    pub tenants: Vec<TenantDirectoryItem>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Serialize, FromRow)]
/// Tenant tối giản dùng cho danh sách tenant mà caller sở hữu.
pub struct OwnedTenant {
    pub id: Uuid,
    pub name: String,
    pub created_at: NaiveDateTime,
}

#[derive(FromRow)]
struct TenantRow {
    id: Uuid,
    name: String,
    created_at: NaiveDateTime,
}

#[derive(FromRow)]
struct TenantOwnerRow {
    tenant_id: Uuid,
    id: String,
    email: String,
}

/// Đọc một trang tenant và owner từ SQL read model.
pub async fn list_tenants(
    pool: &PgPool,
    limit: i64,
    offset: i64,
    query: Option<&str>,
) -> Result<TenantDirectoryPage, sqlx::Error> {
    let total = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM tenants t
        WHERE $1::text IS NULL
           OR t.name ILIKE '%' || $1 || '%'
           OR t.id::text ILIKE '%' || $1 || '%'
           OR EXISTS (
                SELECT 1
                FROM tenant_members tm
                JOIN users u ON u.id = tm.user_id
                WHERE tm.tenant_id = t.id
                  AND tm.role = 'OWNER'
                  AND u.email ILIKE '%' || $1 || '%'
           )
        "#,
    )
    .bind(query)
    .fetch_one(pool)
    .await?;

    let rows = sqlx::query_as::<_, TenantRow>(
        r#"
        SELECT id, name, created_at
        FROM tenants t
        WHERE $1::text IS NULL
           OR t.name ILIKE '%' || $1 || '%'
           OR t.id::text ILIKE '%' || $1 || '%'
           OR EXISTS (
                SELECT 1
                FROM tenant_members tm
                JOIN users u ON u.id = tm.user_id
                WHERE tm.tenant_id = t.id
                  AND tm.role = 'OWNER'
                  AND u.email ILIKE '%' || $1 || '%'
           )
        ORDER BY created_at DESC, id DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(query)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    let tenant_ids: Vec<Uuid> = rows.iter().map(|tenant| tenant.id).collect();
    let mut owners_by_tenant: HashMap<Uuid, Vec<TenantOwner>> = HashMap::new();

    if !tenant_ids.is_empty() {
        let owner_rows = sqlx::query_as::<_, TenantOwnerRow>(
            r#"
            SELECT tm.tenant_id, u.id, u.email
            FROM tenant_members tm
            JOIN users u ON u.id = tm.user_id
            WHERE tm.role = 'OWNER' AND tm.tenant_id = ANY($1::uuid[])
            ORDER BY u.email ASC, u.id ASC
            "#,
        )
        .bind(&tenant_ids)
        .fetch_all(pool)
        .await?;

        for owner in owner_rows {
            owners_by_tenant
                .entry(owner.tenant_id)
                .or_default()
                .push(TenantOwner {
                    id: owner.id,
                    email: owner.email,
                });
        }
    }

    let tenants = rows
        .into_iter()
        .map(|tenant| TenantDirectoryItem {
            id: tenant.id,
            name: tenant.name,
            created_at: tenant.created_at,
            owners: owners_by_tenant.remove(&tenant.id).unwrap_or_default(),
        })
        .collect();

    Ok(TenantDirectoryPage {
        tenants,
        total,
        limit,
        offset,
    })
}

/// Đọc các tenant mà SQL read model ghi nhận người dùng là owner.
pub async fn list_owned_tenant_candidates(
    pool: &PgPool,
    user_id: &str,
) -> Result<Vec<OwnedTenant>, sqlx::Error> {
    sqlx::query_as::<_, OwnedTenant>(
        r#"
        SELECT t.id, t.name, t.created_at
        FROM tenants t
        JOIN tenant_members tm ON tm.tenant_id = t.id
        WHERE tm.user_id = $1 AND tm.role = 'OWNER'
        GROUP BY t.id, t.name, t.created_at
        ORDER BY t.created_at DESC, t.id DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
}
