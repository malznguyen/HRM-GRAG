use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashSet;
use tracing::{error, info};
use uuid::Uuid;

use crate::auth::authz::{AuthzClient, AuthzError, Object, Relation, TupleKey};
use crate::auth::workspace_role::WorkspaceMemberRole;

const DEFAULT_AUTHZ_OUTBOX_BATCH_SIZE: i64 = 50;
const DEFAULT_AUTHZ_OUTBOX_MAX_RETRIES: i32 = 5;
const DEFAULT_AUTHZ_OUTBOX_POLL_INTERVAL_SECS: u64 = 30;
const MAX_ERROR_MESSAGE_LEN: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthzOutboxEventType {
    TupleWrite,
    TupleDelete,
    WorkspaceRoleSync,
}

impl AuthzOutboxEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthzOutboxEventType::TupleWrite => "tuple_write",
            AuthzOutboxEventType::TupleDelete => "tuple_delete",
            AuthzOutboxEventType::WorkspaceRoleSync => "workspace_role_sync",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");

        match normalized.as_str() {
            "tuple_write"
            | "write_tuple"
            | "create_tuple"
            | "grant_tuple"
            | "document_workspace_write"
            | "document_explicit_viewer_grant"
            | "workspace_tenant_write"
            | "workspace_member_write"
            | "tenant_owner_write"
            | "tenant_platform_write" => Some(Self::TupleWrite),
            "tuple_delete"
            | "delete_tuple"
            | "remove_tuple"
            | "revoke_tuple"
            | "document_workspace_delete"
            | "document_explicit_viewer_revoke"
            | "workspace_tenant_delete"
            | "workspace_member_delete"
            | "tenant_owner_delete"
            | "tenant_platform_delete" => Some(Self::TupleDelete),
            "workspace_role_sync" => Some(Self::WorkspaceRoleSync),
            _ => {
                if normalized.contains("delete")
                    || normalized.contains("remove")
                    || normalized.contains("revoke")
                {
                    return Some(Self::TupleDelete);
                }

                if normalized.contains("write")
                    || normalized.contains("create")
                    || normalized.contains("grant")
                    || normalized.contains("backfill")
                {
                    return Some(Self::TupleWrite);
                }

                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzOutboxTuplePayload {
    pub user: String,
    pub relation: String,
    pub object: String,
}

/// Payload role-agnostic; processor luôn nạp role hiện tại từ SQL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRoleSyncPayload {
    pub workspace_id: Uuid,
    pub user_id: String,
}

#[derive(Debug, Clone, Copy)]
pub struct AuthzOutboxProcessorConfig {
    pub batch_size: i64,
    pub max_retries: i32,
}

impl Default for AuthzOutboxProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_AUTHZ_OUTBOX_BATCH_SIZE,
            max_retries: DEFAULT_AUTHZ_OUTBOX_MAX_RETRIES,
        }
    }
}

impl AuthzOutboxProcessorConfig {
    pub fn from_env() -> Self {
        Self {
            batch_size: parse_env_i64(
                "AUTHZ_OUTBOX_BATCH_SIZE",
                DEFAULT_AUTHZ_OUTBOX_BATCH_SIZE,
                1,
                500,
            ),
            max_retries: parse_env_i32(
                "AUTHZ_OUTBOX_MAX_RETRIES",
                DEFAULT_AUTHZ_OUTBOX_MAX_RETRIES,
                1,
                100,
            ),
        }
    }
}

/// Chế độ chạy binary `process-authz-outbox` (one-shot thủ công vs loop Compose).
///
/// Không chứa logic xử lý row — chỉ cấu hình vòng lặp; processor vẫn là
/// [`process_authz_outbox`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthzOutboxRunMode {
    /// `true` = lặp drain + sleep; `false` = một lần rồi thoát (mặc định, manual/debug).
    pub loop_mode: bool,
    /// Khoảng nghỉ giữa các lần drain khi `loop_mode` (giây).
    pub interval_secs: u64,
}

impl Default for AuthzOutboxRunMode {
    fn default() -> Self {
        Self {
            loop_mode: false,
            interval_secs: DEFAULT_AUTHZ_OUTBOX_POLL_INTERVAL_SECS,
        }
    }
}

impl AuthzOutboxRunMode {
    /// Parse CLI + env. CLI thắng env. Mặc định one-shot.
    pub fn from_args_and_env(args: &[String]) -> Self {
        let mut mode = Self {
            loop_mode: false,
            interval_secs: parse_env_u64(
                "AUTHZ_OUTBOX_POLL_INTERVAL_SECS",
                DEFAULT_AUTHZ_OUTBOX_POLL_INTERVAL_SECS,
                1,
                86_400,
            ),
        };

        let mut index = 0;
        while index < args.len() {
            let arg = args[index].as_str();
            match arg {
                "--loop" => mode.loop_mode = true,
                "--once" => mode.loop_mode = false,
                "--interval-secs" => {
                    index += 1;
                    if let Some(raw) = args.get(index) {
                        if let Ok(parsed) = raw.parse::<u64>() {
                            mode.interval_secs = parsed.clamp(1, 86_400);
                        }
                    }
                }
                _ if arg.starts_with("--interval-secs=") => {
                    if let Some(raw) = arg.strip_prefix("--interval-secs=") {
                        if let Ok(parsed) = raw.parse::<u64>() {
                            mode.interval_secs = parsed.clamp(1, 86_400);
                        }
                    }
                }
                "--help" | "-h" => {
                    // Binary in ra usage rồi exit; giữ parse pure cho unit test.
                }
                _ => {}
            }
            index += 1;
        }

        mode
    }
}

/// Exit code binary: `0` khi drain xong (one-shot) hoặc shutdown sạch (loop);
/// `1` khi lỗi cứng (DB/SQL) — Compose restart (local demo).
pub const AUTHZ_OUTBOX_EXIT_SUCCESS: i32 = 0;
pub const AUTHZ_OUTBOX_EXIT_FAILURE: i32 = 1;

/// Map kết quả processor → exit code quan sát được cho failure drill / orchestrator.
pub fn authz_outbox_exit_code<T, E>(result: &Result<T, E>) -> i32 {
    match result {
        Ok(_) => AUTHZ_OUTBOX_EXIT_SUCCESS,
        Err(_) => AUTHZ_OUTBOX_EXIT_FAILURE,
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AuthzOutboxRunResult {
    pub batches: usize,
    pub fetched_rows: usize,
    pub processed_rows: usize,
    pub failed_rows: usize,
    pub skipped_max_retry_rows: i64,
}

#[derive(sqlx::FromRow)]
struct AuthzOutboxRow {
    id: Uuid,
    event_type: String,
    payload: Value,
    retry_count: i32,
}

pub async fn enqueue_tuple_event(
    pool: &PgPool,
    event_type: AuthzOutboxEventType,
    tuple: &TupleKey,
) -> Result<Uuid, sqlx::Error> {
    let payload = json!({
        "user": tuple.user,
        "relation": tuple.relation,
        "object": tuple.object,
    });

    sqlx::query_scalar(
        r#"
        INSERT INTO authz_outbox (event_type, payload, status, retry_count)
        VALUES ($1, $2, 'PENDING', 0)
        RETURNING id
        "#,
    )
    .bind(event_type.as_str())
    .bind(payload)
    .fetch_one(pool)
    .await
}

pub async fn enqueue_tuple_write(pool: &PgPool, tuple: &TupleKey) -> Result<Uuid, sqlx::Error> {
    enqueue_tuple_event(pool, AuthzOutboxEventType::TupleWrite, tuple).await
}

pub async fn enqueue_tuple_delete(pool: &PgPool, tuple: &TupleKey) -> Result<Uuid, sqlx::Error> {
    enqueue_tuple_event(pool, AuthzOutboxEventType::TupleDelete, tuple).await
}

/// Enqueue recovery role-sync ngoài transaction đang xử lý request.
pub async fn enqueue_workspace_role_sync(
    pool: &PgPool,
    workspace_id: Uuid,
    user_id: &str,
) -> Result<Uuid, sqlx::Error> {
    let payload = json!({
        "workspace_id": workspace_id,
        "user_id": user_id,
    });

    sqlx::query_scalar(
        r#"
        INSERT INTO authz_outbox (event_type, payload, status, retry_count)
        VALUES ($1, $2, 'PENDING', 0)
        RETURNING id
        "#,
    )
    .bind(AuthzOutboxEventType::WorkspaceRoleSync.as_str())
    .bind(payload)
    .fetch_one(pool)
    .await
}

/// Enqueue role-sync trong cùng transaction với thay đổi membership.
pub async fn enqueue_workspace_role_sync_tx(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    user_id: &str,
) -> Result<Uuid, sqlx::Error> {
    let payload = json!({
        "workspace_id": workspace_id,
        "user_id": user_id,
    });

    sqlx::query_scalar(
        r#"
        INSERT INTO authz_outbox (event_type, payload, status, retry_count)
        VALUES ($1, $2, 'PENDING', 0)
        RETURNING id
        "#,
    )
    .bind(AuthzOutboxEventType::WorkspaceRoleSync.as_str())
    .bind(payload)
    .fetch_one(&mut **tx)
    .await
}

pub async fn process_authz_outbox(
    pool: &PgPool,
    authz_client: &AuthzClient,
    config: AuthzOutboxProcessorConfig,
) -> Result<AuthzOutboxRunResult, sqlx::Error> {
    let mut result = AuthzOutboxRunResult::default();
    let mut processed_ids: HashSet<Uuid> = HashSet::new();

    loop {
        let mut rows: Vec<AuthzOutboxRow> = sqlx::query_as(
            r#"
            SELECT id, event_type, payload, retry_count
            FROM authz_outbox
            WHERE status = 'PENDING'
               OR (status = 'FAILED' AND retry_count < $1)
            ORDER BY created_at ASC
            LIMIT $2
            "#,
        )
        .bind(config.max_retries)
        .bind(config.batch_size)
        .fetch_all(pool)
        .await?;

        rows.retain(|row| !processed_ids.contains(&row.id));

        if rows.is_empty() {
            break;
        }

        result.batches += 1;
        result.fetched_rows += rows.len();

        for row in rows {
            processed_ids.insert(row.id);
            match process_single_row(pool, authz_client, row).await {
                Ok(true) => {
                    result.processed_rows += 1;
                }
                Ok(false) => {
                    result.failed_rows += 1;
                }
                Err(err) => {
                    error!(error = %err, "Failed to update authz_outbox row state");
                    return Err(err);
                }
            }
        }
    }

    result.skipped_max_retry_rows = count_exhausted_failures(pool, config.max_retries).await?;

    info!(
        batches = result.batches,
        fetched_rows = result.fetched_rows,
        processed_rows = result.processed_rows,
        failed_rows = result.failed_rows,
        skipped_max_retry_rows = result.skipped_max_retry_rows,
        "Authz outbox processing completed"
    );

    Ok(result)
}

async fn process_single_row(
    pool: &PgPool,
    authz_client: &AuthzClient,
    row: AuthzOutboxRow,
) -> Result<bool, sqlx::Error> {
    let event_type = match AuthzOutboxEventType::parse(&row.event_type) {
        Some(event_type) => event_type,
        None => {
            mark_outbox_row_failed(
                pool,
                row.id,
                row.retry_count,
                "unsupported_event_type".to_string(),
            )
            .await?;
            return Ok(false);
        }
    };

    if event_type == AuthzOutboxEventType::WorkspaceRoleSync {
        return process_workspace_role_sync_row(pool, authz_client, row).await;
    }

    let tuple = match parse_tuple_payload(row.payload) {
        Ok(tuple) => tuple,
        Err(error_code) => {
            mark_outbox_row_failed(pool, row.id, row.retry_count, error_code).await?;
            return Ok(false);
        }
    };

    let tuple_key = TupleKey {
        user: tuple.user,
        relation: tuple.relation,
        object: tuple.object,
    };

    let write_result = match event_type {
        AuthzOutboxEventType::TupleWrite => {
            authz_client.write_tuples(vec![tuple_key], Vec::new()).await
        }
        AuthzOutboxEventType::TupleDelete => {
            authz_client.write_tuples(Vec::new(), vec![tuple_key]).await
        }
        AuthzOutboxEventType::WorkspaceRoleSync => unreachable!(),
    };

    match write_result {
        Ok(()) => {
            mark_authz_outbox_processed(pool, row.id).await?;
            Ok(true)
        }
        Err(err) if is_idempotent_outcome(event_type, &err) => {
            mark_authz_outbox_processed(pool, row.id).await?;
            Ok(true)
        }
        Err(err) => {
            mark_outbox_row_failed(pool, row.id, row.retry_count, sanitize_error_message(&err))
                .await?;
            Ok(false)
        }
    }
}

async fn process_workspace_role_sync_row(
    pool: &PgPool,
    authz_client: &AuthzClient,
    row: AuthzOutboxRow,
) -> Result<bool, sqlx::Error> {
    let payload = match serde_json::from_value::<WorkspaceRoleSyncPayload>(row.payload) {
        Ok(payload) => payload,
        Err(_) => {
            mark_outbox_row_failed(
                pool,
                row.id,
                row.retry_count,
                "invalid_workspace_role_sync_payload".to_string(),
            )
            .await?;
            return Ok(false);
        }
    };

    let current_role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(payload.workspace_id)
    .bind(&payload.user_id)
    .fetch_optional(pool)
    .await?;

    let sync_result = match current_role {
        Some(role) => {
            let Some(role) = WorkspaceMemberRole::from_sql(&role) else {
                mark_outbox_row_failed(
                    pool,
                    row.id,
                    row.retry_count,
                    "invalid_workspace_member_role".to_string(),
                )
                .await?;
                return Ok(false);
            };

            sync_existing_workspace_role(authz_client, &payload, role).await
        }
        None => delete_workspace_role_tuples(authz_client, &payload).await,
    };

    match sync_result {
        Ok(()) => {
            mark_authz_outbox_processed(pool, row.id).await?;
            Ok(true)
        }
        Err(err) => {
            mark_outbox_row_failed(pool, row.id, row.retry_count, sanitize_error_message(&err))
                .await?;
            Ok(false)
        }
    }
}

async fn sync_existing_workspace_role(
    authz_client: &AuthzClient,
    payload: &WorkspaceRoleSyncPayload,
    current_role: WorkspaceMemberRole,
) -> Result<(), AuthzError> {
    let matching_relation = current_role.as_fga_relation();
    let opposite_relation = match current_role {
        WorkspaceMemberRole::Admin => Relation::Member,
        WorkspaceMemberRole::Member => Relation::Admin,
    };

    let matching_tuple = workspace_role_tuple(payload, matching_relation);
    let opposite_tuple = workspace_role_tuple(payload, opposite_relation);

    delete_tuple_idempotently(authz_client, opposite_tuple).await?;
    write_tuple_idempotently(authz_client, matching_tuple).await
}

async fn delete_workspace_role_tuples(
    authz_client: &AuthzClient,
    payload: &WorkspaceRoleSyncPayload,
) -> Result<(), AuthzError> {
    delete_tuple_idempotently(authz_client, workspace_role_tuple(payload, Relation::Admin)).await?;
    delete_tuple_idempotently(
        authz_client,
        workspace_role_tuple(payload, Relation::Member),
    )
    .await
}

fn workspace_role_tuple(payload: &WorkspaceRoleSyncPayload, relation: Relation) -> TupleKey {
    TupleKey {
        user: format!("user:{}", payload.user_id),
        relation: relation.as_str().to_string(),
        object: Object::Workspace(payload.workspace_id).to_string(),
    }
}

async fn write_tuple_idempotently(
    authz_client: &AuthzClient,
    tuple: TupleKey,
) -> Result<(), AuthzError> {
    match authz_client.write_tuples(vec![tuple], Vec::new()).await {
        Ok(()) => Ok(()),
        Err(err) if is_idempotent_outcome(AuthzOutboxEventType::TupleWrite, &err) => Ok(()),
        Err(err) => Err(err),
    }
}

async fn delete_tuple_idempotently(
    authz_client: &AuthzClient,
    tuple: TupleKey,
) -> Result<(), AuthzError> {
    match authz_client.write_tuples(Vec::new(), vec![tuple]).await {
        Ok(()) => Ok(()),
        Err(err) if is_idempotent_outcome(AuthzOutboxEventType::TupleDelete, &err) => Ok(()),
        Err(err) => Err(err),
    }
}

/// Đánh dấu event đã xử lý sau khi direct OpenFGA grant thành công.
pub async fn mark_authz_outbox_processed(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE authz_outbox
        SET status = 'PROCESSED',
            error_message = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        "#,
    )
    .bind(id)
    .execute(pool)
    .await?;

    Ok(())
}

async fn mark_outbox_row_failed(
    pool: &PgPool,
    id: Uuid,
    retry_count: i32,
    error_message: String,
) -> Result<(), sqlx::Error> {
    let next_retry_count = next_retry_count(retry_count);
    let sanitized_message = truncate_error_message(error_message);

    sqlx::query(
        r#"
        UPDATE authz_outbox
        SET status = 'FAILED',
            retry_count = $2,
            error_message = $3,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(next_retry_count)
    .bind(sanitized_message)
    .execute(pool)
    .await?;

    Ok(())
}

async fn count_exhausted_failures(pool: &PgPool, max_retries: i32) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM authz_outbox
        WHERE status = 'FAILED'
          AND retry_count >= $1
        "#,
    )
    .bind(max_retries)
    .fetch_one(pool)
    .await
}

fn parse_tuple_payload(payload: Value) -> Result<AuthzOutboxTuplePayload, String> {
    if let Ok(tuple) = serde_json::from_value::<AuthzOutboxTuplePayload>(payload.clone()) {
        return Ok(tuple);
    }

    if let Some(tuple) = payload.get("tuple") {
        if let Ok(tuple) = serde_json::from_value::<AuthzOutboxTuplePayload>(tuple.clone()) {
            return Ok(tuple);
        }
    }

    Err("invalid_payload".to_string())
}

fn sanitize_error_message(err: &AuthzError) -> String {
    match err {
        AuthzError::Http(_) => "openfga_http_error".to_string(),
        AuthzError::OpenFga { status, .. } => {
            format!("openfga_status_{}", status.as_u16())
        }
    }
}

fn is_idempotent_outcome(event_type: AuthzOutboxEventType, err: &AuthzError) -> bool {
    let AuthzError::OpenFga { body, .. } = err else {
        return false;
    };

    let body_lower = body.to_ascii_lowercase();

    match event_type {
        AuthzOutboxEventType::TupleWrite => {
            body_lower.contains("already exists") || body_lower.contains("already existed")
        }
        AuthzOutboxEventType::TupleDelete => {
            body_lower.contains("does not exist") || body_lower.contains("not found")
        }
        AuthzOutboxEventType::WorkspaceRoleSync => false,
    }
}

fn next_retry_count(current_retry_count: i32) -> i32 {
    current_retry_count.saturating_add(1)
}

fn truncate_error_message(message: String) -> String {
    if message.len() <= MAX_ERROR_MESSAGE_LEN {
        return message;
    }

    let mut truncated = message
        .chars()
        .take(MAX_ERROR_MESSAGE_LEN)
        .collect::<String>();
    truncated.push_str("...");
    truncated
}

fn parse_env_i64(var_name: &str, default: i64, min: i64, max: i64) -> i64 {
    std::env::var(var_name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

fn parse_env_i32(var_name: &str, default: i32, min: i32, max: i32) -> i32 {
    std::env::var(var_name)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

fn parse_env_u64(var_name: &str, default: u64, min: u64, max: u64) -> u64 {
    std::env::var(var_name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn outbox_event_type_roundtrip() {
        let encoded = serde_json::to_string(&AuthzOutboxEventType::TupleWrite).unwrap();
        assert_eq!(encoded, "\"tuple_write\"");

        let decoded: AuthzOutboxEventType = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, AuthzOutboxEventType::TupleWrite);
    }

    #[test]
    fn run_mode_defaults_to_once() {
        let mode = AuthzOutboxRunMode::from_args_and_env(&[]);
        assert!(!mode.loop_mode);
        assert!(mode.interval_secs >= 1);
    }

    #[test]
    fn run_mode_parses_loop_and_interval_cli() {
        let args = vec![
            "--loop".to_string(),
            "--interval-secs".to_string(),
            "15".to_string(),
        ];
        let mode = AuthzOutboxRunMode::from_args_and_env(&args);
        assert!(mode.loop_mode);
        assert_eq!(mode.interval_secs, 15);

        let once = AuthzOutboxRunMode::from_args_and_env(&["--once".to_string()]);
        assert!(!once.loop_mode);

        let eq = AuthzOutboxRunMode::from_args_and_env(&["--interval-secs=45".to_string()]);
        assert_eq!(eq.interval_secs, 45);
    }

    #[test]
    fn exit_code_maps_ok_and_err_for_failure_drill() {
        let ok: Result<(), &str> = Ok(());
        let err: Result<(), &str> = Err("db_unavailable");
        assert_eq!(authz_outbox_exit_code(&ok), AUTHZ_OUTBOX_EXIT_SUCCESS);
        assert_eq!(authz_outbox_exit_code(&err), AUTHZ_OUTBOX_EXIT_FAILURE);
    }

    #[test]
    fn retry_count_increments_with_saturation() {
        assert_eq!(next_retry_count(0), 1);
        assert_eq!(next_retry_count(i32::MAX), i32::MAX);
    }

    #[test]
    fn idempotent_duplicate_create_is_treated_as_success() {
        let err = AuthzError::OpenFga {
            status: StatusCode::BAD_REQUEST,
            body: "cannot write tuple because it already exists".to_string(),
        };

        assert!(is_idempotent_outcome(
            AuthzOutboxEventType::TupleWrite,
            &err
        ));
    }

    #[test]
    fn idempotent_missing_delete_is_treated_as_success() {
        let err = AuthzError::OpenFga {
            status: StatusCode::BAD_REQUEST,
            body: "cannot delete tuple because it does not exist".to_string(),
        };

        assert!(is_idempotent_outcome(
            AuthzOutboxEventType::TupleDelete,
            &err
        ));
    }

    #[test]
    fn parse_tuple_payload_accepts_nested_tuple() {
        let payload = json!({
            "tuple": {
                "user": "user:test",
                "relation": "member",
                "object": "workspace:abc"
            }
        });

        let parsed = parse_tuple_payload(payload).unwrap();
        assert_eq!(parsed.user, "user:test");
        assert_eq!(parsed.relation, "member");
        assert_eq!(parsed.object, "workspace:abc");
    }
}
