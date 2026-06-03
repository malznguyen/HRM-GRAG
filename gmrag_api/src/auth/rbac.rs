use axum::http::StatusCode;
use sqlx::PgPool;
use uuid::Uuid;

pub async fn require_workspace_member(
    pool: &PgPool,
    workspace_id: Uuid,
    user_id: &str,
) -> Result<(), StatusCode> {
    let member: Result<Option<String>, _> = sqlx::query_scalar(
        r#"
        SELECT role
        FROM workspace_members
        WHERE workspace_id = $1 AND user_id = $2
        "#,
    )
    .bind(workspace_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await;

    match member {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(StatusCode::FORBIDDEN),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn require_workspace_admin(
    pool: &PgPool,
    workspace_id: Uuid,
    user_id: &str,
) -> Result<(), StatusCode> {
    let role: Result<Option<String>, _> = sqlx::query_scalar(
        r#"
        SELECT role
        FROM workspace_members
        WHERE workspace_id = $1 AND user_id = $2
        "#,
    )
    .bind(workspace_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await;

    match role {
        Ok(Some(ref r)) if r.as_str() == "ADMIN" => Ok(()),
        Ok(_) => Err(StatusCode::FORBIDDEN),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
