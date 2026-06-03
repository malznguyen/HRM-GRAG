use sqlx::PgPool;
use std::hash::{Hash, Hasher};

/// Normalizes emails for consistent lookup, invite ids, and reconciliation.
pub fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

/// Stable placeholder `users.id` for pending invites (must match webhook reconciliation).
pub fn invite_placeholder_user_id(email: &str) -> String {
    let email = normalize_email(email);
    let candidate = format!("invite_{email}");
    if candidate.len() <= 255 {
        return candidate;
    }

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    email.hash(&mut hasher);
    format!("invite_{:016x}", hasher.finish())
}

async fn upsert_clerk_user(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    clerk_user_id: &str,
    email: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO users (id, email)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email
        "#,
    )
    .bind(clerk_user_id)
    .bind(email)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn migrate_user_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    old_user_id: &str,
    clerk_user_id: &str,
    email: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        DELETE FROM workspace_members wm_old
        WHERE wm_old.user_id = $2
          AND EXISTS (
              SELECT 1
              FROM workspace_members wm_new
              WHERE wm_new.user_id = $1
                AND wm_new.workspace_id = wm_old.workspace_id
          )
        "#,
    )
    .bind(clerk_user_id)
    .bind(old_user_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE workspace_members
        SET user_id = $1
        WHERE user_id = $2
        "#,
    )
    .bind(clerk_user_id)
    .bind(old_user_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE chat_sessions
        SET user_id = $1
        WHERE user_id = $2
        "#,
    )
    .bind(clerk_user_id)
    .bind(old_user_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(old_user_id)
        .execute(&mut **tx)
        .await?;

    upsert_clerk_user(tx, clerk_user_id, email).await
}

async fn reconcile_invite_placeholder(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    clerk_user_id: &str,
    email: &str,
    placeholder_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE users
        SET email = LEFT(CONCAT('reconciled_', id), 255)
        WHERE id = $1
        "#,
    )
    .bind(placeholder_id)
    .execute(&mut **tx)
    .await?;

    upsert_clerk_user(tx, clerk_user_id, email).await?;

    sqlx::query(
        r#"
        DELETE FROM workspace_members wm_invite
        WHERE wm_invite.user_id = $2
          AND EXISTS (
              SELECT 1
              FROM workspace_members wm_real
              WHERE wm_real.user_id = $1
                AND wm_real.workspace_id = wm_invite.workspace_id
          )
        "#,
    )
    .bind(clerk_user_id)
    .bind(placeholder_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE workspace_members
        SET user_id = $1
        WHERE user_id = $2
        "#,
    )
    .bind(clerk_user_id)
    .bind(placeholder_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(placeholder_id)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

/// After Clerk signup, attach pending `workspace_members` rows to the real user id.
pub async fn reconcile_pending_invites(
    pool: &PgPool,
    clerk_user_id: &str,
    email: &str,
) -> Result<(), sqlx::Error> {
    let email = normalize_email(email);
    let expected_placeholder_id = invite_placeholder_user_id(&email);
    let mut tx = pool.begin().await?;

    let placeholder_id: Option<String> = sqlx::query_scalar(
        r#"
        SELECT id
        FROM users
        WHERE id LIKE 'invite_%'
          AND (lower(email) = lower($1) OR id = $2)
        LIMIT 1
        "#,
    )
    .bind(&email)
    .bind(&expected_placeholder_id)
    .fetch_optional(&mut *tx)
    .await?;

    if let Some(placeholder_id) = placeholder_id {
        reconcile_invite_placeholder(&mut tx, clerk_user_id, &email, &placeholder_id).await?;
    } else {
        let conflicting_user_id: Option<String> = sqlx::query_scalar(
            r#"
            SELECT id
            FROM users
            WHERE lower(email) = lower($1)
              AND id <> $2
            LIMIT 1
            "#,
        )
        .bind(&email)
        .bind(clerk_user_id)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(old_user_id) = conflicting_user_id {
            migrate_user_id(&mut tx, &old_user_id, clerk_user_id, &email).await?;
        } else {
            upsert_clerk_user(&mut tx, clerk_user_id, &email).await?;
        }
    }

    tx.commit().await?;
    Ok(())
}
