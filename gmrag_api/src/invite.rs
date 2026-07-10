use sqlx::PgPool;
use std::hash::{Hash, Hasher};

/// Chuẩn hoá email để lookup nhất quán (trim + lowercase).
pub fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

/// LEGACY — chỉ dùng cho migration dữ liệu cũ còn `invite_*`.
///
/// Flow placeholder invite đã bị gỡ: không được tạo user `invite_{email}` mới.
/// Hàm này còn lại để nhận diện id placeholder trong migration một lần.
#[deprecated(note = "Placeholder invites removed; only for one-time legacy migration")]
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

async fn upsert_identity_user(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    user_id: &str,
    email: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO users (id, email)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email
        "#,
    )
    .bind(user_id)
    .bind(email)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn migrate_user_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    old_user_id: &str,
    real_user_id: &str,
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
    .bind(real_user_id)
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
    .bind(real_user_id)
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
    .bind(real_user_id)
    .bind(old_user_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(old_user_id)
        .execute(&mut **tx)
        .await?;

    upsert_identity_user(tx, real_user_id, email).await
}

async fn reconcile_invite_placeholder(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    real_user_id: &str,
    email: &str,
    placeholder_id: &str,
) -> Result<(), sqlx::Error> {
    // Giải phóng unique email trước khi insert user thật
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

    upsert_identity_user(tx, real_user_id, email).await?;

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
    .bind(real_user_id)
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
    .bind(real_user_id)
    .bind(placeholder_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(placeholder_id)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

/// LEGACY MIGRATION ONLY — không gọi từ request path thông thường.
///
/// Lý do gỡ placeholder invite: `reconcile_pending_invites` chỉ cập nhật SQL,
/// không rewrite tuple OpenFGA → user thật bị 403 dù vẫn hiện trong member list.
///
/// Dùng một lần cho dữ liệu cũ còn `users.id LIKE 'invite_%'`. Sau migration,
/// member addition chỉ chấp nhận user đã tồn tại (verified) trong Keycloak.
///
/// **Cảnh báo:** hàm này không ghi OpenFGA. Sau khi chạy, operator phải
/// kiểm tra/sửa tuple workspace membership cho user thật nếu còn desync.
#[deprecated(note = "One-time legacy migration only; not called from normal request flows")]
pub async fn reconcile_pending_invites(
    pool: &PgPool,
    real_user_id: &str,
    email: &str,
) -> Result<(), sqlx::Error> {
    #[allow(deprecated)]
    let expected_placeholder_id = invite_placeholder_user_id(email);
    let email = normalize_email(email);
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
        reconcile_invite_placeholder(&mut tx, real_user_id, &email, &placeholder_id).await?;
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
        .bind(real_user_id)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(old_user_id) = conflicting_user_id {
            migrate_user_id(&mut tx, &old_user_id, real_user_id, &email).await?;
        } else {
            upsert_identity_user(&mut tx, real_user_id, &email).await?;
        }
    }

    tx.commit().await?;
    Ok(())
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
