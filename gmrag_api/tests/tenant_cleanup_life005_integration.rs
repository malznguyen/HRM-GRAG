//! LIFE-005: tenant cleanup strategy — operator/library lifecycle, no public route.
//!
//! Chứng minh:
//! 1. Populated tenant: capture workspace IDs trước cascade; cùng TX enqueue
//!    `qdrant_outbox` (`delete_by_workspaces`) + `storage_outbox` (`delete_prefix`
//!    `tenants/{tenant_id}/` + configured bucket).
//! 2. Empty tenant: workspace list `[]` tường minh (không silent omit / silent success
//!    nhầm với missing tenant).
//! 3. Rollback TX → tenant còn, không có cleanup outbox rows.
//! 4. Missing tenant → hard error, không enqueue.
//! 5. Payload correctness: full captured workspace IDs + tenant prefix + bucket.
//!
//! Không gọi public API. Workers outbox vẫn manual (OPS-003).

mod support;

use gmrag_api::storage::cleanup::build_tenant_prefix;
use gmrag_api::tenant_cleanup::{
    TenantCleanupError, capture_tenant_delete_plan, commit_tenant_delete_lifecycle,
    delete_tenant_with_cleanup_tx,
};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::collections::HashSet;
use std::sync::OnceLock;
use uuid::Uuid;

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();

fn init_test_env() {
    TEST_ENV_INIT.get_or_init(|| unsafe {
        dotenvy::dotenv().ok();
        if std::env::var_os("S3_BUCKET").is_none() {
            std::env::set_var("S3_BUCKET", "gmrag-documents");
        }
    });
}

async fn pool_or_skip() -> Option<PgPool> {
    init_test_env();
    let database_url = support::database_url().ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .ok()?;
    sqlx::migrate!("./migrations").run(&pool).await.ok()?;
    Some(pool)
}

fn configured_bucket() -> String {
    std::env::var("S3_BUCKET").unwrap_or_else(|_| "gmrag-documents".to_string())
}

struct SeededTenant {
    tenant_id: Uuid,
    tenant_name: String,
    workspace_ids: Vec<Uuid>,
    user_id: String,
}

async fn seed_tenant(pool: &PgPool, workspace_count: usize) -> SeededTenant {
    let tenant_id = Uuid::new_v4();
    let tenant_name = format!("life005-tenant-{tenant_id}");
    let user_id = format!("life005-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&user_id)
        .bind(format!("{user_id}@test.local"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(&tenant_name)
        .execute(pool)
        .await
        .unwrap();

    let mut workspace_ids = Vec::with_capacity(workspace_count);
    for i in 0..workspace_count {
        let workspace_id = Uuid::new_v4();
        sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
            .bind(workspace_id)
            .bind(tenant_id)
            .bind(format!("life005-ws-{i}-{workspace_id}"))
            .execute(pool)
            .await
            .unwrap();
        workspace_ids.push(workspace_id);
    }
    workspace_ids.sort();

    SeededTenant {
        tenant_id,
        tenant_name,
        workspace_ids,
        user_id,
    }
}

async fn cleanup_outbox_for_tenant(pool: &PgPool, tenant_id: Uuid, workspace_ids: &[Uuid]) {
    let _ = sqlx::query(
        r#"
        DELETE FROM storage_outbox
        WHERE event_type = 'delete_prefix'
          AND payload->>'tenant_id' = $1
        "#,
    )
    .bind(tenant_id.to_string())
    .execute(pool)
    .await;

    // qdrant payload chỉ có workspace_ids — dọn theo id list nếu còn.
    for workspace_id in workspace_ids {
        let _ = sqlx::query(
            r#"
            DELETE FROM qdrant_outbox
            WHERE event_type = 'delete_by_workspaces'
              AND payload->'workspace_ids' ? $1
            "#,
        )
        .bind(workspace_id.to_string())
        .execute(pool)
        .await;
    }

    // Empty list events: match payload with empty array for this run if we can find by time is hard;
    // tests that create empty list also delete by outbox id when available.
}

async fn cleanup_seed(pool: &PgPool, seed: &SeededTenant) {
    cleanup_outbox_for_tenant(pool, seed.tenant_id, &seed.workspace_ids).await;
    let _ = sqlx::query("DELETE FROM workspaces WHERE tenant_id = $1")
        .bind(seed.tenant_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(seed.tenant_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&seed.user_id)
        .execute(pool)
        .await;
}

async fn count_storage_tenant_prefix(pool: &PgPool, tenant_id: Uuid) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM storage_outbox
        WHERE event_type = 'delete_prefix'
          AND status = 'PENDING'
          AND payload->>'tenant_id' = $1
          AND payload->>'workspace_id' IS NULL
        "#,
    )
    .bind(tenant_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fetch_qdrant_workspaces_payload(pool: &PgPool, outbox_id: Uuid) -> (String, Vec<Uuid>) {
    let row: (String, Value) = sqlx::query_as(
        r#"
        SELECT event_type, payload
        FROM qdrant_outbox
        WHERE id = $1
        "#,
    )
    .bind(outbox_id)
    .fetch_one(pool)
    .await
    .expect("qdrant outbox row must exist after commit");

    let workspace_ids: Vec<Uuid> = row
        .1
        .get("workspace_ids")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .expect("workspace_ids must be present in payload (explicit, even if empty)");

    (row.0, workspace_ids)
}

async fn fetch_storage_prefix_payload(
    pool: &PgPool,
    outbox_id: Uuid,
) -> (String, String, String, Option<String>, Option<String>) {
    let row: (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        r#"
        SELECT
            event_type,
            payload->>'prefix',
            payload->>'bucket',
            payload->>'tenant_id',
            payload->>'workspace_id'
        FROM storage_outbox
        WHERE id = $1
        "#,
    )
    .bind(outbox_id)
    .fetch_one(pool)
    .await
    .expect("storage outbox row must exist after commit");

    (
        row.0,
        row.1.unwrap_or_default(),
        row.2.unwrap_or_default(),
        row.3,
        row.4,
    )
}

#[tokio::test]
async fn populated_tenant_delete_enqueues_qdrant_and_storage_with_captured_ids() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let seed = seed_tenant(&pool, 3).await;
    let bucket = configured_bucket();
    let expected_prefix = build_tenant_prefix(seed.tenant_id);

    let result = commit_tenant_delete_lifecycle(&pool, seed.tenant_id, &bucket)
        .await
        .expect("populated tenant lifecycle must commit");

    assert_eq!(result.plan.tenant_id, seed.tenant_id);
    assert_eq!(result.plan.tenant_name, seed.tenant_name);
    assert_eq!(result.plan.workspace_ids, seed.workspace_ids);
    assert!(!result.plan.has_empty_workspace_list());
    assert_eq!(result.plan.storage_prefix, expected_prefix);
    assert_eq!(result.plan.storage_bucket, bucket);

    // Crash-after-commit: recovery rows durable trước mọi S3/Qdrant worker call.
    assert_eq!(count_storage_tenant_prefix(&pool, seed.tenant_id).await, 1);

    let (qdrant_event, captured_ids) =
        fetch_qdrant_workspaces_payload(&pool, result.qdrant_outbox_id).await;
    assert_eq!(qdrant_event, "delete_by_workspaces");
    assert_eq!(
        captured_ids.iter().copied().collect::<HashSet<_>>(),
        seed.workspace_ids.iter().copied().collect::<HashSet<_>>(),
        "all captured workspace IDs must survive in durable outbox payload"
    );
    assert_eq!(captured_ids.len(), 3);

    let (storage_event, prefix, payload_bucket, payload_tenant, payload_workspace) =
        fetch_storage_prefix_payload(&pool, result.storage_outbox_id).await;
    assert_eq!(storage_event, "delete_prefix");
    assert_eq!(prefix, expected_prefix);
    assert_eq!(payload_bucket, bucket, "bucket from trusted config only");
    assert_eq!(
        payload_tenant.as_deref(),
        Some(seed.tenant_id.to_string().as_str())
    );
    assert!(
        payload_workspace.is_none(),
        "tenant-level delete_prefix must not set workspace_id"
    );

    let tenant_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE id = $1)")
            .bind(seed.tenant_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!tenant_exists, "tenant row must be gone after commit");

    let remaining_workspaces: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM workspaces WHERE id = ANY($1::uuid[])")
            .bind(&seed.workspace_ids)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        remaining_workspaces, 0,
        "SQL cascade must remove workspaces with tenant"
    );

    // Cleanup outbox rows created by this test.
    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE id = $1")
        .bind(result.qdrant_outbox_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM storage_outbox WHERE id = $1")
        .bind(result.storage_outbox_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&seed.user_id)
        .execute(&pool)
        .await;
}

#[tokio::test]
async fn empty_tenant_delete_records_explicit_empty_workspace_list() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let seed = seed_tenant(&pool, 0).await;
    let bucket = configured_bucket();

    let plan = capture_tenant_delete_plan(&pool, seed.tenant_id, &bucket)
        .await
        .expect("empty but existing tenant must capture plan");
    assert!(
        plan.has_empty_workspace_list(),
        "empty list must be explicit on plan"
    );
    assert_eq!(plan.workspace_ids, Vec::<Uuid>::new());

    let result = commit_tenant_delete_lifecycle(&pool, seed.tenant_id, &bucket)
        .await
        .expect("empty tenant delete must still commit (not silent-fail as missing)");

    assert!(result.plan.has_empty_workspace_list());
    assert_eq!(result.plan.workspace_ids, Vec::<Uuid>::new());

    let (qdrant_event, captured_ids) =
        fetch_qdrant_workspaces_payload(&pool, result.qdrant_outbox_id).await;
    assert_eq!(qdrant_event, "delete_by_workspaces");
    assert!(
        captured_ids.is_empty(),
        "empty workspace list must be present as [] in durable payload, not omitted"
    );

    let (storage_event, prefix, payload_bucket, payload_tenant, payload_workspace) =
        fetch_storage_prefix_payload(&pool, result.storage_outbox_id).await;
    assert_eq!(storage_event, "delete_prefix");
    assert_eq!(prefix, build_tenant_prefix(seed.tenant_id));
    assert_eq!(payload_bucket, bucket);
    assert_eq!(
        payload_tenant.as_deref(),
        Some(seed.tenant_id.to_string().as_str())
    );
    assert!(payload_workspace.is_none());

    let tenant_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE id = $1)")
            .bind(seed.tenant_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!tenant_exists);

    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE id = $1")
        .bind(result.qdrant_outbox_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM storage_outbox WHERE id = $1")
        .bind(result.storage_outbox_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&seed.user_id)
        .execute(&pool)
        .await;
}

#[tokio::test]
async fn tenant_delete_rollback_leaves_tenant_and_no_cleanup_events() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let seed = seed_tenant(&pool, 2).await;
    let bucket = configured_bucket();

    {
        let mut tx = pool.begin().await.unwrap();
        let result = delete_tenant_with_cleanup_tx(&mut tx, seed.tenant_id, &bucket)
            .await
            .expect("in-TX delete must succeed before rollback");
        assert_eq!(result.plan.workspace_ids.len(), 2);
        // Không commit — chứng minh atomicity.
        tx.rollback().await.unwrap();
    }

    let tenant_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE id = $1)")
            .bind(seed.tenant_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        tenant_exists,
        "rolled-back transaction must leave tenant row"
    );

    let workspace_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM workspaces WHERE tenant_id = $1")
            .bind(seed.tenant_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(workspace_count, 2, "workspaces must remain after rollback");

    assert_eq!(
        count_storage_tenant_prefix(&pool, seed.tenant_id).await,
        0,
        "rolled-back transaction must not leave storage_outbox"
    );

    // Không có qdrant outbox cho các workspace này ở trạng thái PENDING delete_by_workspaces
    // với đúng bộ ids (khó query empty set) — kiểm tra không có row storage là đủ;
    // kiểm tra thêm qdrant count theo workspace membership string.
    let qdrant_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM qdrant_outbox
        WHERE event_type = 'delete_by_workspaces'
          AND status = 'PENDING'
          AND payload->'workspace_ids' ? $1
        "#,
    )
    .bind(seed.workspace_ids[0].to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        qdrant_count, 0,
        "rolled-back transaction must not leave qdrant_outbox"
    );

    cleanup_seed(&pool, &seed).await;
}

#[tokio::test]
async fn missing_tenant_is_hard_error_and_enqueues_nothing() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let missing = Uuid::new_v4();
    let bucket = configured_bucket();

    let err = capture_tenant_delete_plan(&pool, missing, &bucket)
        .await
        .expect_err("missing tenant must not silent-succeed as empty list");
    match err {
        TenantCleanupError::TenantNotFound { tenant_id } => {
            assert_eq!(tenant_id, missing);
        }
        other => panic!("expected TenantNotFound, got {other}"),
    }

    let err = commit_tenant_delete_lifecycle(&pool, missing, &bucket)
        .await
        .expect_err("commit must refuse missing tenant");
    match err {
        TenantCleanupError::TenantNotFound { tenant_id } => {
            assert_eq!(tenant_id, missing);
        }
        other => panic!("expected TenantNotFound, got {other}"),
    }

    assert_eq!(count_storage_tenant_prefix(&pool, missing).await, 0);
}

#[tokio::test]
async fn empty_bucket_is_rejected_before_delete() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let seed = seed_tenant(&pool, 1).await;

    let err = commit_tenant_delete_lifecycle(&pool, seed.tenant_id, "   ")
        .await
        .expect_err("empty/whitespace bucket must be rejected");
    match err {
        TenantCleanupError::EmptyBucket => {}
        other => panic!("expected EmptyBucket, got {other}"),
    }

    let tenant_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE id = $1)")
            .bind(seed.tenant_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        tenant_exists,
        "rejected empty bucket must not delete tenant"
    );
    assert_eq!(count_storage_tenant_prefix(&pool, seed.tenant_id).await, 0);

    cleanup_seed(&pool, &seed).await;
}
