use sqlx::PgPool;

/// Chuẩn hoá email để lookup nhất quán (trim + lowercase).
pub fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

/// Upsert user SQL theo identity id (JWT `sub` / Keycloak `sub`).
/// Dùng cho sync profile — không liên quan placeholder invite.
pub async fn upsert_user(pool: &PgPool, user_id: &str, email: &str) -> Result<(), sqlx::Error> {
    let email = normalize_email(email);
    sqlx::query(
        r#"
        INSERT INTO users (id, email)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email
        "#,
    )
    .bind(user_id)
    .bind(&email)
    .execute(pool)
    .await?;
    Ok(())
}
