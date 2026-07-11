//! LIFE-004: workspace delete enqueue `storage_outbox` `delete_prefix` trong cùng TX lifecycle.
//!
//! Chứng minh:
//! 1. Workspace SQL delete + qdrant_outbox + storage_outbox (`delete_prefix`) commit cùng lúc;
//!    recovery row durable trước mọi S3 call (crash-after-commit).
//! 2. Rollback lifecycle TX / workspace không tồn tại → không có storage outbox row.
//! 3. Process event xóa đúng prefix workspace target; sibling workspace object còn lại.
//!
//! Scheduling unattended (`process-storage-outbox` định kỳ) = OPS-003.
//! Tenant cascade strategy = LIFE-005 (không trong scope file này).

use gmrag_api::retrieval::outbox::enqueue_delete_by_workspace_tx;
use gmrag_api::storage::cleanup::build_workspace_prefix;
use gmrag_api::storage::outbox::{
    StorageOutboxProcessorConfig, enqueue_delete_prefix_tx, process_storage_outbox,
};
use gmrag_api::storage::{StorageClient, StorageConfig, build_original_document_object_key};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::OnceLock;
use uuid::Uuid;

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();

fn init_test_env() {
    TEST_ENV_INIT.get_or_init(|| unsafe {
        dotenvy::dotenv().ok();
        if std::env::var_os("S3_ENDPOINT_URL").is_none() {
            std::env::set_var("S3_ENDPOINT_URL", "http://localhost:9000");
        }
        if std::env::var_os("S3_REGION").is_none() {
            std::env::set_var("S3_REGION", "us-east-1");
        }
        if std::env::var_os("S3_BUCKET").is_none() {
            std::env::set_var("S3_BUCKET", "gmrag-documents");
        }
        if std::env::var_os("S3_ACCESS_KEY_ID").is_none() {
            std::env::set_var("S3_ACCESS_KEY_ID", "minioadmin");
        }
        if std::env::var_os("S3_SECRET_ACCESS_KEY").is_none() {
            std::env::set_var("S3_SECRET_ACCESS_KEY", "minioadmin");
        }
        if std::env::var_os("S3_FORCE_PATH_STYLE").is_none() {
            std::env::set_var("S3_FORCE_PATH_STYLE", "true");
        }
        if std::env::var_os("S3_PRESIGN_EXPIRY_SECS").is_none() {
            std::env::set_var("S3_PRESIGN_EXPIRY_SECS", "900");
        }
    });
}

async fn pool_or_skip() -> Option<PgPool> {
    init_test_env();
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .ok()?;
    sqlx::migrate!("./migrations").run(&pool).await.ok()?;
    Some(pool)
}

async fn storage_or_skip() -> Option<StorageClient> {
    init_test_env();
    let config = StorageConfig::from_env().ok()?;
    let storage = StorageClient::from_config(config).await;
    match storage.list_objects(Some("life004-probe/")).await {
        Ok(_) => Some(storage),
        Err(_) => None,
    }
}

fn configured_bucket() -> String {
    std::env::var("S3_BUCKET").unwrap_or_else(|_| "gmrag-documents".to_string())
}

struct SeededWorkspace {
    tenant_id: Uuid,
    workspace_id: Uuid,
    user_id: String,
}

async fn seed_workspace(pool: &PgPool) -> SeededWorkspace {
    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let user_id = format!("life004-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&user_id)
        .bind(format!("{user_id}@test.local"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("life004-tenant-{tenant_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("life004-ws-{workspace_id}"))
        .execute(pool)
        .await
        .unwrap();

    SeededWorkspace {
        tenant_id,
        workspace_id,
        user_id,
    }
}

async fn cleanup_seed(pool: &PgPool, workspace: &SeededWorkspace) {
    let _ = sqlx::query("DELETE FROM storage_outbox WHERE payload->>'workspace_id' = $1")
        .bind(workspace.workspace_id.to_string())
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE payload->>'workspace_id' = $1")
        .bind(workspace.workspace_id.to_string())
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace.workspace_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(workspace.tenant_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&workspace.user_id)
        .execute(pool)
        .await;
}

async fn count_storage_delete_prefix(pool: &PgPool, workspace_id: Uuid) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM storage_outbox
        WHERE event_type = 'delete_prefix'
          AND status = 'PENDING'
          AND payload->>'workspace_id' = $1
        "#,
    )
    .bind(workspace_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn count_qdrant_workspace_outbox(pool: &PgPool, workspace_id: Uuid) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM qdrant_outbox
        WHERE event_type = 'delete_by_workspace'
          AND status = 'PENDING'
          AND payload->>'workspace_id' = $1
        "#,
    )
    .bind(workspace_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Mirrors workspace-delete SQL lifecycle (không gọi S3/Qdrant) — crash-after-commit fixture.
async fn commit_workspace_delete_lifecycle(
    pool: &PgPool,
    tenant_id: Uuid,
    workspace_id: Uuid,
    bucket: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let found_tenant: Option<Uuid> =
        sqlx::query_scalar("SELECT tenant_id FROM workspaces WHERE id = $1")
            .bind(workspace_id)
            .fetch_optional(&mut *tx)
            .await?;

    let Some(found_tenant) = found_tenant else {
        return Ok(false);
    };
    assert_eq!(
        found_tenant, tenant_id,
        "fixture tenant_id must match SQL row"
    );

    let outcome = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(&mut *tx)
        .await?;

    if outcome.rows_affected() == 0 {
        return Ok(false);
    }

    enqueue_delete_by_workspace_tx(&mut tx, workspace_id).await?;

    let prefix = build_workspace_prefix(tenant_id, workspace_id);
    enqueue_delete_prefix_tx(
        &mut tx,
        &prefix,
        bucket,
        Some(tenant_id),
        Some(workspace_id),
    )
    .await?;

    tx.commit().await?;
    Ok(true)
}

#[tokio::test]
async fn workspace_delete_storage_prefix_outbox_exists_after_sql_commit_without_s3_call() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let workspace = seed_workspace(&pool).await;
    let bucket = configured_bucket();
    let expected_prefix = build_workspace_prefix(workspace.tenant_id, workspace.workspace_id);

    let deleted = commit_workspace_delete_lifecycle(
        &pool,
        workspace.tenant_id,
        workspace.workspace_id,
        &bucket,
    )
    .await
    .expect("workspace lifecycle TX must commit");
    assert!(deleted);

    // Crash-after-commit: recovery row durable trước mọi S3 call.
    assert_eq!(
        count_storage_delete_prefix(&pool, workspace.workspace_id).await,
        1,
        "delete_prefix storage_outbox must exist immediately after SQL commit"
    );
    assert_eq!(
        count_qdrant_workspace_outbox(&pool, workspace.workspace_id).await,
        1,
        "qdrant_outbox delete_by_workspace must commit with storage_outbox (LIFE-001)"
    );

    let payload: (String, String, String, String) = sqlx::query_as(
        r#"
        SELECT
            payload->>'prefix',
            payload->>'bucket',
            payload->>'tenant_id',
            payload->>'workspace_id'
        FROM storage_outbox
        WHERE event_type = 'delete_prefix'
          AND payload->>'workspace_id' = $1
        "#,
    )
    .bind(workspace.workspace_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("delete_prefix payload must be queryable");

    assert_eq!(payload.0, expected_prefix, "canonical workspace prefix");
    assert_eq!(payload.1, bucket, "configured storage bucket only");
    assert_eq!(payload.2, workspace.tenant_id.to_string());
    assert_eq!(payload.3, workspace.workspace_id.to_string());

    let workspace_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspaces WHERE id = $1)")
            .bind(workspace.workspace_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!workspace_exists, "workspace row must be gone after commit");

    cleanup_seed(&pool, &workspace).await;
}

#[tokio::test]
async fn workspace_delete_storage_prefix_outbox_rolls_back_with_sql_delete() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let workspace = seed_workspace(&pool).await;
    let bucket = configured_bucket();

    {
        let mut tx = pool.begin().await.unwrap();
        let outcome = sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind(workspace.workspace_id)
            .execute(&mut *tx)
            .await
            .unwrap();
        assert_eq!(outcome.rows_affected(), 1);
        enqueue_delete_by_workspace_tx(&mut tx, workspace.workspace_id)
            .await
            .unwrap();
        let prefix = build_workspace_prefix(workspace.tenant_id, workspace.workspace_id);
        enqueue_delete_prefix_tx(
            &mut tx,
            &prefix,
            &bucket,
            Some(workspace.tenant_id),
            Some(workspace.workspace_id),
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
    }

    assert_eq!(
        count_storage_delete_prefix(&pool, workspace.workspace_id).await,
        0,
        "rolled-back transaction must not leave delete_prefix storage_outbox"
    );
    assert_eq!(
        count_qdrant_workspace_outbox(&pool, workspace.workspace_id).await,
        0,
        "rolled-back transaction must not leave qdrant_outbox either"
    );

    let workspace_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspaces WHERE id = $1)")
            .bind(workspace.workspace_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        workspace_exists,
        "workspace must remain when lifecycle TX rolls back"
    );

    cleanup_seed(&pool, &workspace).await;
}

#[tokio::test]
async fn workspace_delete_missing_workspace_leaves_no_storage_outbox() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let missing_workspace_id = Uuid::new_v4();
    let bucket = configured_bucket();

    let mut tx = pool.begin().await.unwrap();
    let tenant_id: Option<Uuid> =
        sqlx::query_scalar("SELECT tenant_id FROM workspaces WHERE id = $1")
            .bind(missing_workspace_id)
            .fetch_optional(&mut *tx)
            .await
            .unwrap();
    assert!(tenant_id.is_none());

    // Handler returns Ok(None) without enqueue when workspace row is absent.
    tx.rollback().await.unwrap();

    assert_eq!(
        count_storage_delete_prefix(&pool, missing_workspace_id).await,
        0,
        "missing workspace must not enqueue delete_prefix"
    );
    let _ = bucket;
}

#[tokio::test]
async fn processor_workspace_prefix_from_lifecycle_removes_only_target_prefix() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let Some(storage) = storage_or_skip().await else {
        eprintln!("skip: S3/MinIO unavailable");
        return;
    };

    let workspace = seed_workspace(&pool).await;
    let sibling_workspace_id = Uuid::new_v4();
    let bucket = storage.bucket().to_string();

    // Sibling workspace row (same tenant) — prefix object must survive target cleanup.
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(sibling_workspace_id)
        .bind(workspace.tenant_id)
        .bind(format!("life004-sibling-{sibling_workspace_id}"))
        .execute(&pool)
        .await
        .unwrap();

    let target_key = build_original_document_object_key(
        workspace.tenant_id,
        workspace.workspace_id,
        Uuid::new_v4(),
    );
    let sibling_key = build_original_document_object_key(
        workspace.tenant_id,
        sibling_workspace_id,
        Uuid::new_v4(),
    );

    storage
        .put_original_document(
            &target_key,
            b"%PDF-1.4 life004 target",
            Some("application/pdf"),
        )
        .await
        .expect("put target object");
    storage
        .put_original_document(
            &sibling_key,
            b"%PDF-1.4 life004 sibling",
            Some("application/pdf"),
        )
        .await
        .expect("put sibling object");
    assert!(storage.object_exists(&target_key).await.unwrap());
    assert!(storage.object_exists(&sibling_key).await.unwrap());

    // Lifecycle commit only — no request-path S3 (proves outbox-before-S3).
    let deleted = commit_workspace_delete_lifecycle(
        &pool,
        workspace.tenant_id,
        workspace.workspace_id,
        &bucket,
    )
    .await
    .expect("lifecycle commit");
    assert!(deleted);

    assert_eq!(
        count_storage_delete_prefix(&pool, workspace.workspace_id).await,
        1,
        "prefix event durable before processor S3 call"
    );
    // Object still present until processor runs.
    assert!(
        storage.object_exists(&target_key).await.unwrap(),
        "target object must remain until process-storage-outbox"
    );

    let outbox_id: Uuid = sqlx::query_scalar(
        r#"
        SELECT id
        FROM storage_outbox
        WHERE event_type = 'delete_prefix'
          AND payload->>'workspace_id' = $1
        "#,
    )
    .bind(workspace.workspace_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("storage outbox row after commit");

    // Claim immediately if a prior run left lease in the future.
    sqlx::query(
        r#"
        UPDATE storage_outbox
        SET next_attempt_at = CURRENT_TIMESTAMP - interval '1 second',
            status = 'PENDING',
            retry_count = 0,
            error_message = NULL
        WHERE id = $1
        "#,
    )
    .bind(outbox_id)
    .execute(&pool)
    .await
    .unwrap();

    let result = process_storage_outbox(&pool, &storage, StorageOutboxProcessorConfig::default())
        .await
        .expect("processor must complete");
    assert!(
        result.processed_rows >= 1,
        "processor must process at least the workspace prefix row"
    );

    let status: String = sqlx::query_scalar("SELECT status FROM storage_outbox WHERE id = $1")
        .bind(outbox_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "PROCESSED");

    assert!(
        !storage.object_exists(&target_key).await.unwrap(),
        "target workspace prefix object must be removed"
    );
    assert!(
        storage.object_exists(&sibling_key).await.unwrap(),
        "sibling workspace prefix object must remain"
    );

    // Cleanup sibling object + seed rows.
    let _ = storage.delete_object(&sibling_key).await;
    let _ = sqlx::query("DELETE FROM storage_outbox WHERE id = $1")
        .bind(outbox_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE payload->>'workspace_id' = $1")
        .bind(workspace.workspace_id.to_string())
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(sibling_workspace_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(workspace.tenant_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&workspace.user_id)
        .execute(&pool)
        .await;
}
