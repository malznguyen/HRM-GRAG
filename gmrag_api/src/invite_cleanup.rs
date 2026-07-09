//! Dọn dẹp placeholder invite legacy (`users.id LIKE 'invite_%'`).
//!
//! Chỉ dùng qua operator binary `cleanup-invite-placeholders`.
//! Mặc định dry-run; chỉ xoá khi `allow_delete = true`.

use std::collections::HashSet;
use std::fmt;

use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::authz::{AuthzClient, AuthzError, Object, Relation, TupleKey};

/// Cờ vận hành: mặc định không xoá gì cả.
#[derive(Debug, Clone, Copy)]
pub struct InvitePlaceholderCleanupOptions {
    pub allow_delete: bool,
}

impl Default for InvitePlaceholderCleanupOptions {
    fn default() -> Self {
        Self {
            allow_delete: false,
        }
    }
}

#[derive(Debug)]
pub enum InvitePlaceholderCleanupError {
    Database(sqlx::Error),
    Authz(AuthzError),
}

impl fmt::Display for InvitePlaceholderCleanupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvitePlaceholderCleanupError::Database(err) => write!(f, "database error: {err}"),
            InvitePlaceholderCleanupError::Authz(err) => write!(f, "openfga error: {err}"),
        }
    }
}

impl std::error::Error for InvitePlaceholderCleanupError {}

impl From<sqlx::Error> for InvitePlaceholderCleanupError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

impl From<AuthzError> for InvitePlaceholderCleanupError {
    fn from(value: AuthzError) -> Self {
        Self::Authz(value)
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PlaceholderUser {
    pub id: String,
    pub email: String,
}

#[derive(Debug, Clone)]
pub struct PlaceholderMembershipSnapshot {
    pub user_id: String,
    pub email: String,
    pub workspace_member_count: i64,
    pub tenant_member_count: i64,
    pub document_share_count: i64,
    pub chat_session_count: i64,
    /// Tuple OpenFGA suy ra từ SQL + list-objects (nếu có).
    pub openfga_tuples: Vec<TupleKey>,
}

#[derive(Debug, Clone, Default)]
pub struct InvitePlaceholderCleanupReport {
    pub placeholders_found: usize,
    pub placeholders: Vec<PlaceholderMembershipSnapshot>,
    pub openfga_tuples_found: usize,
    pub openfga_tuples_deleted: usize,
    pub document_shares_deleted: usize,
    pub workspace_members_deleted: usize,
    pub tenant_members_deleted: usize,
    pub chat_sessions_deleted: usize,
    pub users_deleted: usize,
    pub errors: Vec<String>,
    pub deleted: bool,
}

/// Tìm user placeholder `invite_%` còn sót trong DB legacy.
pub async fn find_invite_placeholders(
    pool: &PgPool,
) -> Result<Vec<PlaceholderUser>, InvitePlaceholderCleanupError> {
    let rows = sqlx::query_as::<_, PlaceholderUser>(
        r#"
        SELECT id, email
        FROM users
        WHERE id LIKE 'invite_%'
        ORDER BY id
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Thu thập snapshot membership + tuple OpenFGA cho từng placeholder (không xoá).
pub async fn collect_placeholder_snapshots(
    pool: &PgPool,
    authz_client: &AuthzClient,
    placeholders: &[PlaceholderUser],
) -> Result<Vec<PlaceholderMembershipSnapshot>, InvitePlaceholderCleanupError> {
    let mut snapshots = Vec::with_capacity(placeholders.len());

    for user in placeholders {
        let workspace_member_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM workspace_members WHERE user_id = $1",
        )
        .bind(&user.id)
        .fetch_one(pool)
        .await?;

        let tenant_member_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM tenant_members WHERE user_id = $1")
                .bind(&user.id)
                .fetch_one(pool)
                .await?;

        let document_share_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM document_shares WHERE user_id = $1")
                .bind(&user.id)
                .fetch_one(pool)
                .await?;

        let chat_session_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM chat_sessions WHERE user_id = $1")
                .bind(&user.id)
                .fetch_one(pool)
                .await?;

        let openfga_tuples = collect_openfga_tuples_for_user(pool, authz_client, &user.id).await?;

        snapshots.push(PlaceholderMembershipSnapshot {
            user_id: user.id.clone(),
            email: user.email.clone(),
            workspace_member_count,
            tenant_member_count,
            document_share_count,
            chat_session_count,
            openfga_tuples,
        });
    }

    Ok(snapshots)
}

/// Chạy cleanup placeholder invite.
///
/// Thứ tự fail-safe khi `allow_delete`:
/// 1. Xoá tuple OpenFGA trước (tránh orphan authz nếu SQL xoá xong mà FGA còn)
/// 2. `document_shares`
/// 3. `workspace_members` + `tenant_members`
/// 4. `chat_sessions` (FK → users; xoá tường minh trước khi xoá user)
/// 5. `users`
pub async fn cleanup_invite_placeholders(
    pool: &PgPool,
    authz_client: &AuthzClient,
    options: InvitePlaceholderCleanupOptions,
) -> Result<InvitePlaceholderCleanupReport, InvitePlaceholderCleanupError> {
    let placeholders = find_invite_placeholders(pool).await?;
    let snapshots = collect_placeholder_snapshots(pool, authz_client, &placeholders).await?;

    let openfga_tuples_found = snapshots
        .iter()
        .map(|s| s.openfga_tuples.len())
        .sum::<usize>();

    let mut report = InvitePlaceholderCleanupReport {
        placeholders_found: snapshots.len(),
        openfga_tuples_found,
        placeholders: snapshots,
        deleted: options.allow_delete,
        ..InvitePlaceholderCleanupReport::default()
    };

    // Dry-run: chỉ báo cáo, không đụng dữ liệu.
    if !options.allow_delete {
        return Ok(report);
    }

    // Clone user ids để xoá tuần tự; giữ snapshot đã thu thập cho summary.
    let user_ids: Vec<String> = report
        .placeholders
        .iter()
        .map(|s| s.user_id.clone())
        .collect();

    for (index, user_id) in user_ids.iter().enumerate() {
        let tuples = report.placeholders[index].openfga_tuples.clone();

        // 1) Xoá OpenFGA trước SQL — thà chặn nhầm còn hơn cho qua nhầm nếu SQL xoá
        //    xong mà tuple còn sót (xem ADR-12 / CLAUDE.md safety order).
        match delete_openfga_tuples(authz_client, &tuples).await {
            Ok(deleted) => report.openfga_tuples_deleted += deleted,
            Err(err) => {
                report
                    .errors
                    .push(format!("openfga delete failed for user {user_id}: {err}"));
                // Không tiếp tục SQL cho user này nếu FGA thất bại không-idempotent
                // — tránh orphan state ngược (SQL mất, FGA còn).
                continue;
            }
        }

        match delete_sql_for_placeholder(pool, user_id).await {
            Ok(counts) => {
                report.document_shares_deleted += counts.document_shares;
                report.workspace_members_deleted += counts.workspace_members;
                report.tenant_members_deleted += counts.tenant_members;
                report.chat_sessions_deleted += counts.chat_sessions;
                report.users_deleted += counts.users;
            }
            Err(err) => {
                report
                    .errors
                    .push(format!("sql delete failed for user {user_id}: {err}"));
            }
        }
    }

    Ok(report)
}

#[derive(Debug, Clone, Copy, Default)]
struct SqlDeleteCounts {
    document_shares: usize,
    workspace_members: usize,
    tenant_members: usize,
    chat_sessions: usize,
    users: usize,
}

async fn delete_sql_for_placeholder(
    pool: &PgPool,
    user_id: &str,
) -> Result<SqlDeleteCounts, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let mut counts = SqlDeleteCounts::default();

    // 2) document_shares
    let result = sqlx::query("DELETE FROM document_shares WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    counts.document_shares = result.rows_affected() as usize;

    // 3) workspace_members + tenant_members
    let result = sqlx::query("DELETE FROM workspace_members WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    counts.workspace_members = result.rows_affected() as usize;

    let result = sqlx::query("DELETE FROM tenant_members WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    counts.tenant_members = result.rows_affected() as usize;

    // chat_sessions cũng tham chiếu users; xoá tường minh trước bước users
    // (FK CASCADE cũng sẽ xử lý, nhưng explicit giúp đếm chính xác).
    let result = sqlx::query("DELETE FROM chat_sessions WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    counts.chat_sessions = result.rows_affected() as usize;

    // 4) users — chỉ xoá đúng id invite_* (phòng hờ caller truyền nhầm)
    let result = sqlx::query("DELETE FROM users WHERE id = $1 AND id LIKE 'invite_%'")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    counts.users = result.rows_affected() as usize;

    tx.commit().await?;
    Ok(counts)
}

async fn delete_openfga_tuples(
    authz_client: &AuthzClient,
    tuples: &[TupleKey],
) -> Result<usize, AuthzError> {
    let mut deleted = 0usize;

    for tuple in tuples {
        match authz_client
            .write_tuples(Vec::new(), vec![tuple.clone()])
            .await
        {
            Ok(()) => deleted += 1,
            // Xoá tuple không tồn tại = idempotent success (an toàn re-run).
            Err(err) if is_missing_tuple_delete_error(&err) => deleted += 1,
            Err(err) => return Err(err),
        }
    }

    Ok(deleted)
}

/// Thu thập tuple OpenFGA liên quan placeholder từ SQL membership + list-objects.
async fn collect_openfga_tuples_for_user(
    pool: &PgPool,
    authz_client: &AuthzClient,
    user_id: &str,
) -> Result<Vec<TupleKey>, InvitePlaceholderCleanupError> {
    let fga_user = format!("user:{user_id}");
    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    let mut tuples = Vec::new();

    let mut push_tuple = |user: String, relation: String, object: String| {
        let key = (user.clone(), relation.clone(), object.clone());
        if seen.insert(key) {
            tuples.push(TupleKey {
                user,
                relation,
                object,
            });
        }
    };

    // --- Từ SQL (nguồn tin cậy cho membership UI) ---
    let workspace_rows: Vec<(Uuid, String)> = sqlx::query_as(
        r#"
        SELECT workspace_id, role
        FROM workspace_members
        WHERE user_id = $1
        "#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    for (workspace_id, role) in workspace_rows {
        let relation = if role.eq_ignore_ascii_case("ADMIN") {
            Relation::Admin.as_str()
        } else {
            Relation::Member.as_str()
        };
        push_tuple(
            fga_user.clone(),
            relation.to_string(),
            Object::Workspace(workspace_id).to_string(),
        );
        // Có thể đã ghi cả member lẫn admin khi đổi role — thu cả hai để xoá sạch.
        push_tuple(
            fga_user.clone(),
            Relation::Member.as_str().to_string(),
            Object::Workspace(workspace_id).to_string(),
        );
        push_tuple(
            fga_user.clone(),
            Relation::Admin.as_str().to_string(),
            Object::Workspace(workspace_id).to_string(),
        );
    }

    let tenant_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT tenant_id FROM tenant_members WHERE user_id = $1")
            .bind(user_id)
            .fetch_all(pool)
            .await?;

    for tenant_id in tenant_ids {
        push_tuple(
            fga_user.clone(),
            Relation::Owner.as_str().to_string(),
            Object::Tenant(tenant_id).to_string(),
        );
    }

    let document_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT document_id FROM document_shares WHERE user_id = $1")
            .bind(user_id)
            .fetch_all(pool)
            .await?;

    for document_id in document_ids {
        push_tuple(
            fga_user.clone(),
            Relation::ExplicitViewer.as_str().to_string(),
            Object::Document(document_id).to_string(),
        );
    }

    // --- Bổ sung từ OpenFGA list-objects (tuple SQL đã mất / desync) ---
    let list_queries: &[(&str, Relation)] = &[
        ("workspace", Relation::Member),
        ("workspace", Relation::Admin),
        ("tenant", Relation::Owner),
        ("document", Relation::ExplicitViewer),
        ("document", Relation::Owner),
    ];

    for (object_type, relation) in list_queries {
        match authz_client
            .list_objects(&fga_user, *relation, object_type)
            .await
        {
            Ok(objects) => {
                for object in objects {
                    push_tuple(
                        fga_user.clone(),
                        relation.as_str().to_string(),
                        object,
                    );
                }
            }
            // list-objects lỗi không chặn cleanup SQL — ghi nhận qua Err chỉ khi hard fail
            // toàn bộ; ở đây bỏ qua list lỗi và dựa vào SQL.
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    user_id = %user_id,
                    object_type = %object_type,
                    relation = %relation.as_str(),
                    "OpenFGA list-objects failed while collecting invite placeholder tuples; continuing with SQL-derived tuples"
                );
            }
        }
    }

    Ok(tuples)
}

fn is_missing_tuple_delete_error(err: &AuthzError) -> bool {
    let AuthzError::OpenFga { body, .. } = err else {
        return false;
    };
    let body_lower = body.to_ascii_lowercase();
    body_lower.contains("does not exist") || body_lower.contains("not found")
}

/// Kiểm tra id có phải placeholder invite legacy không.
pub fn is_invite_placeholder_user_id(user_id: &str) -> bool {
    user_id.starts_with("invite_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_invite_placeholder_ids() {
        assert!(is_invite_placeholder_user_id("invite_user@example.com"));
        assert!(is_invite_placeholder_user_id("invite_abcdef0123456789"));
        assert!(!is_invite_placeholder_user_id("real-keycloak-sub"));
        assert!(!is_invite_placeholder_user_id("user_invite_x"));
    }

    #[test]
    fn default_options_are_dry_run() {
        let options = InvitePlaceholderCleanupOptions::default();
        assert!(!options.allow_delete);
    }

    #[test]
    fn missing_tuple_delete_is_idempotent() {
        let err = AuthzError::OpenFga {
            status: reqwest::StatusCode::BAD_REQUEST,
            body: "cannot delete a tuple which does not exist".to_string(),
        };
        assert!(is_missing_tuple_delete_error(&err));

        let hard = AuthzError::OpenFga {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            body: "internal error".to_string(),
        };
        assert!(!is_missing_tuple_delete_error(&hard));
    }
}
