//! LIFE-003: storage_outbox durable recovery cho S3/MinIO object cleanup.
//!
//! Chứng minh:
//! 1. Document lifecycle commit tạo `delete_object` row trước mọi storage call.
//! 2. Rollback lifecycle TX không để lại outbox row.
//! 3. Crash-after-commit (bỏ request-path) giữ row PENDING retryable.
//! 4. Processor gặp S3 thật sự unavailable → FAILED + retry/backoff; SQL lifecycle không restore.
//! 5. Processor dùng `payload.bucket` (không silent fallback runtime bucket).
//! 6. Missing object = success idempotent; prefix chỉ xóa đúng target + event bucket.

mod support;

use gmrag_api::retrieval::outbox::enqueue_delete_by_document_tx;
use gmrag_api::storage::cleanup::build_workspace_prefix;
use gmrag_api::storage::outbox::{
    StorageOutboxProcessorConfig, enqueue_delete_object, enqueue_delete_object_tx,
    enqueue_delete_prefix, process_storage_outbox,
};
use gmrag_api::storage::{
    StorageClient, StorageClientOptions, StorageConfig, build_original_document_object_key,
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::OnceLock;
use std::time::Duration;
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
    let database_url = support::database_url().ok()?;
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
    // Probe connectivity — skip nếu MinIO không sẵn.
    match storage.list_objects(Some("life003-probe/")).await {
        Ok(_) => Some(storage),
        Err(_) => None,
    }
}

/// Client trỏ endpoint không reachable — chứng minh processor fail thật, không mock skip.
async fn unavailable_storage_client() -> StorageClient {
    init_test_env();
    let mut config = StorageConfig::from_env().expect("storage env for unavailable client");
    // Cổng discard / không lắng nghe — connection fail nhanh, không đụng MinIO thật.
    config.endpoint_url = Some("http://127.0.0.1:9".to_string());
    StorageClient::from_config_with_options(
        config,
        StorageClientOptions {
            connect_timeout: Some(Duration::from_millis(300)),
            operation_timeout: Some(Duration::from_secs(2)),
            max_attempts: Some(1),
        },
    )
    .await
}

struct SeededWorkspace {
    tenant_id: Uuid,
    workspace_id: Uuid,
    user_id: String,
}

struct SeededDocument {
    document_id: Uuid,
    object_key: String,
    bucket: String,
}

async fn seed_workspace(pool: &PgPool) -> SeededWorkspace {
    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let user_id = format!("life003-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&user_id)
        .bind(format!("{user_id}@test.local"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("life003-tenant-{tenant_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("life003-ws-{workspace_id}"))
        .execute(pool)
        .await
        .unwrap();

    SeededWorkspace {
        tenant_id,
        workspace_id,
        user_id,
    }
}

async fn seed_document(pool: &PgPool, workspace: &SeededWorkspace) -> SeededDocument {
    let document_id = Uuid::new_v4();
    let object_key = build_original_document_object_key(
        workspace.tenant_id,
        workspace.workspace_id,
        document_id,
    );
    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "gmrag-documents".to_string());

    sqlx::query(
        r#"
        INSERT INTO documents (
            id, workspace_id, owner_id, filename, status, processing_stage,
            object_key, bucket, uploaded_by
        )
        VALUES ($1, $2, $3, 'life003.pdf', 'FAILED', 'FAILED', $4, $5, $3)
        "#,
    )
    .bind(document_id)
    .bind(workspace.workspace_id)
    .bind(&workspace.user_id)
    .bind(&object_key)
    .bind(&bucket)
    .execute(pool)
    .await
    .unwrap();

    SeededDocument {
        document_id,
        object_key,
        bucket,
    }
}

async fn cleanup_workspace(pool: &PgPool, workspace: &SeededWorkspace) {
    let _ = sqlx::query("DELETE FROM storage_outbox WHERE payload->>'workspace_id' = $1")
        .bind(workspace.workspace_id.to_string())
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE payload->>'workspace_id' = $1")
        .bind(workspace.workspace_id.to_string())
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM documents WHERE workspace_id = $1")
        .bind(workspace.workspace_id)
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

async fn count_storage_delete_object(pool: &PgPool, document_id: Uuid) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM storage_outbox
        WHERE event_type = 'delete_object'
          AND status = 'PENDING'
          AND payload->>'document_id' = $1
        "#,
    )
    .bind(document_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn outbox_status(pool: &PgPool, outbox_id: Uuid) -> String {
    sqlx::query_scalar(
        r#"
        SELECT status
        FROM storage_outbox
        WHERE id = $1
        "#,
    )
    .bind(outbox_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Mirrors document-delete SQL lifecycle (không gọi S3/Qdrant) — crash-after-commit fixture.
async fn commit_document_delete_lifecycle(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
    object_key: &str,
    bucket: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    sqlx::query(
        r#"
        DELETE FROM graph_edge_sources
        WHERE workspace_id = $1 AND document_id = $2
        "#,
    )
    .bind(workspace_id)
    .bind(document_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        DELETE FROM graph_node_sources
        WHERE workspace_id = $1 AND document_id = $2
        "#,
    )
    .bind(workspace_id)
    .bind(document_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        DELETE FROM graph_edges edge
        WHERE edge.workspace_id = $1
          AND NOT EXISTS (
            SELECT 1
            FROM graph_edge_sources source
            WHERE source.graph_edge_id = edge.id
          )
        "#,
    )
    .bind(workspace_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        DELETE FROM graph_nodes node
        WHERE node.workspace_id = $1
          AND NOT EXISTS (
            SELECT 1
            FROM graph_node_sources source
            WHERE source.graph_node_id = node.id
          )
          AND NOT EXISTS (
            SELECT 1
            FROM graph_edges edge
            WHERE edge.source_node_id = node.id
               OR edge.target_node_id = node.id
          )
        "#,
    )
    .bind(workspace_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        DELETE FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .execute(&mut *tx)
    .await?;

    enqueue_delete_by_document_tx(&mut tx, workspace_id, document_id).await?;
    enqueue_delete_object_tx(&mut tx, object_key, bucket, workspace_id, document_id).await?;
    tx.commit().await
}

#[tokio::test]
async fn document_delete_storage_outbox_exists_after_sql_commit_without_storage_call() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let workspace = seed_workspace(&pool).await;
    let document = seed_document(&pool, &workspace).await;

    commit_document_delete_lifecycle(
        &pool,
        workspace.workspace_id,
        document.document_id,
        &document.object_key,
        &document.bucket,
    )
    .await
    .expect("document lifecycle TX must commit");

    // Crash-after-commit: recovery row durable trước mọi S3 call.
    assert_eq!(
        count_storage_delete_object(&pool, document.document_id).await,
        1,
        "delete_object storage_outbox must exist immediately after SQL commit"
    );

    let document_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM documents WHERE id = $1)")
            .bind(document.document_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!document_exists, "document row must be gone after commit");

    let _ = sqlx::query("DELETE FROM storage_outbox WHERE payload->>'document_id' = $1")
        .bind(document.document_id.to_string())
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE payload->>'document_id' = $1")
        .bind(document.document_id.to_string())
        .execute(&pool)
        .await;
    cleanup_workspace(&pool, &workspace).await;
}

#[tokio::test]
async fn document_delete_storage_outbox_rolls_back_with_sql_delete() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let workspace = seed_workspace(&pool).await;
    let document = seed_document(&pool, &workspace).await;

    {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("DELETE FROM documents WHERE id = $1 AND workspace_id = $2")
            .bind(document.document_id)
            .bind(workspace.workspace_id)
            .execute(&mut *tx)
            .await
            .unwrap();
        enqueue_delete_object_tx(
            &mut tx,
            &document.object_key,
            &document.bucket,
            workspace.workspace_id,
            document.document_id,
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
    }

    assert_eq!(
        count_storage_delete_object(&pool, document.document_id).await,
        0,
        "rolled-back transaction must not leave delete_object storage_outbox"
    );

    let document_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM documents WHERE id = $1)")
            .bind(document.document_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        document_exists,
        "document must remain when lifecycle TX rolls back"
    );

    cleanup_workspace(&pool, &workspace).await;
}

/// Crash-after-commit: SQL + outbox durable khi bỏ qua request-path S3 (không gọi processor).
#[tokio::test]
async fn document_delete_with_s3_skipped_leaves_retryable_storage_outbox() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let workspace = seed_workspace(&pool).await;
    let document = seed_document(&pool, &workspace).await;

    // Lifecycle commit giống handler sau SQL; không gọi storage.delete_object.
    commit_document_delete_lifecycle(
        &pool,
        workspace.workspace_id,
        document.document_id,
        &document.object_key,
        &document.bucket,
    )
    .await
    .expect("document delete must succeed without S3");

    let status: (String, i32) = sqlx::query_as(
        r#"
        SELECT status, retry_count
        FROM storage_outbox
        WHERE event_type = 'delete_object'
          AND payload->>'document_id' = $1
        "#,
    )
    .bind(document.document_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("storage_outbox row must remain for recovery");

    assert_eq!(status.0, "PENDING");
    assert_eq!(status.1, 0);

    // Payload giữ object_key + bucket tin cậy từ SQL (không client-supplied).
    let payload: (String, String) = sqlx::query_as(
        r#"
        SELECT payload->>'object_key', payload->>'bucket'
        FROM storage_outbox
        WHERE event_type = 'delete_object'
          AND payload->>'document_id' = $1
        "#,
    )
    .bind(document.document_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(payload.0, document.object_key);
    assert_eq!(payload.1, document.bucket);

    let _ = sqlx::query("DELETE FROM storage_outbox WHERE payload->>'document_id' = $1")
        .bind(document.document_id.to_string())
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE payload->>'document_id' = $1")
        .bind(document.document_id.to_string())
        .execute(&pool)
        .await;
    cleanup_workspace(&pool, &workspace).await;
}

/// Processor thật sự gọi S3 client unavailable → FAILED + backoff; SQL lifecycle không restore.
#[tokio::test]
async fn processor_s3_unavailable_marks_failed_with_backoff_without_restoring_sql() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let workspace = seed_workspace(&pool).await;
    let document = seed_document(&pool, &workspace).await;

    commit_document_delete_lifecycle(
        &pool,
        workspace.workspace_id,
        document.document_id,
        &document.object_key,
        &document.bucket,
    )
    .await
    .expect("lifecycle commit before processor outage");

    let outbox_id: Uuid = sqlx::query_scalar(
        r#"
        SELECT id
        FROM storage_outbox
        WHERE event_type = 'delete_object'
          AND payload->>'document_id' = $1
        "#,
    )
    .bind(document.document_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("recovery row after commit");

    // Cho phép claim ngay (claim lease có thể đã set next_attempt_at tương lai nếu re-run).
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

    let unavailable = unavailable_storage_client().await;
    let result = process_storage_outbox(
        &pool,
        &unavailable,
        StorageOutboxProcessorConfig {
            // Backoff ngắn để assert next_attempt_at được đẩy ra, không kẹt claim lease.
            batch_size: 50,
            max_retries: 5,
            backoff: gmrag_api::outbox::OutboxBackoffConfig {
                base_backoff_secs: 2,
                max_backoff_secs: 300,
                claim_lease_secs: 120,
            },
        },
    )
    .await
    .expect("processor must complete even when S3 is down");

    assert!(
        result.failed_rows >= 1,
        "processor must record at least one FAILED row on S3 outage"
    );

    let row: (String, i32, Option<String>, bool) = sqlx::query_as(
        r#"
        SELECT status, retry_count, error_message,
               (next_attempt_at > CURRENT_TIMESTAMP) AS backoff_scheduled
        FROM storage_outbox
        WHERE id = $1
        "#,
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(row.0, "FAILED", "transient S3 outage must mark FAILED");
    assert!(
        row.1 >= 1,
        "retry_count must increment after failed processor attempt, got {}",
        row.1
    );
    assert!(
        row.2.as_deref().is_some_and(|msg| !msg.is_empty()),
        "error_message must capture storage failure class"
    );
    assert!(
        row.3,
        "next_attempt_at must be scheduled in the future for backoff"
    );

    // SQL lifecycle đã commit — document không được restore khi storage fail.
    let document_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM documents WHERE id = $1)")
            .bind(document.document_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        !document_exists,
        "document lifecycle SQL must remain deleted after storage processor failure"
    );

    let _ = sqlx::query("DELETE FROM storage_outbox WHERE id = $1")
        .bind(outbox_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE payload->>'document_id' = $1")
        .bind(document.document_id.to_string())
        .execute(&pool)
        .await;
    cleanup_workspace(&pool, &workspace).await;
}

/// Bucket trong payload được dùng thật — bucket không tồn tại không silent-success qua runtime bucket.
#[tokio::test]
async fn processor_honors_payload_bucket_and_fails_on_unknown_bucket() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let Some(storage) = storage_or_skip().await else {
        eprintln!("skip: S3/MinIO unavailable");
        return;
    };

    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let tenant_id = Uuid::new_v4();
    let object_key = build_original_document_object_key(tenant_id, workspace_id, document_id);
    // Bucket giả — nếu processor bỏ qua payload và dùng runtime bucket + missing key → PROCESSED.
    // Kỳ vọng: gọi đúng bucket lạ → lỗi transient FAILED, không PROCESSED.
    let fake_bucket = format!("life003-missing-bucket-{}", Uuid::new_v4());

    let outbox_id =
        enqueue_delete_object(&pool, &object_key, &fake_bucket, workspace_id, document_id)
            .await
            .unwrap();

    sqlx::query(
        r#"
        UPDATE storage_outbox
        SET next_attempt_at = CURRENT_TIMESTAMP - interval '1 second'
        WHERE id = $1
        "#,
    )
    .bind(outbox_id)
    .execute(&pool)
    .await
    .unwrap();

    let result = process_storage_outbox(&pool, &storage, StorageOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert!(
        result.failed_rows >= 1 || result.dead_rows >= 1,
        "unknown payload bucket must not be treated as idempotent success"
    );

    let status = outbox_status(&pool, outbox_id).await;
    assert_ne!(
        status, "PROCESSED",
        "must not mark PROCESSED when operating on payload bucket that is not the runtime default"
    );
    assert!(
        status == "FAILED" || status == "DEAD",
        "expected FAILED/DEAD for unknown bucket, got {status}"
    );

    let _ = sqlx::query("DELETE FROM storage_outbox WHERE id = $1")
        .bind(outbox_id)
        .execute(&pool)
        .await;
}

#[tokio::test]
async fn processor_marks_missing_object_as_idempotent_success() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let Some(storage) = storage_or_skip().await else {
        eprintln!("skip: S3/MinIO unavailable");
        return;
    };

    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let tenant_id = Uuid::new_v4();
    let object_key = build_original_document_object_key(tenant_id, workspace_id, document_id);
    let bucket = storage.bucket().to_string();

    // Không put object — missing key phải PROCESSED.
    let outbox_id = enqueue_delete_object(&pool, &object_key, &bucket, workspace_id, document_id)
        .await
        .unwrap();

    let result = process_storage_outbox(&pool, &storage, StorageOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert!(result.processed_rows >= 1);
    assert_eq!(outbox_status(&pool, outbox_id).await, "PROCESSED");

    let _ = sqlx::query("DELETE FROM storage_outbox WHERE id = $1")
        .bind(outbox_id)
        .execute(&pool)
        .await;
}

#[tokio::test]
async fn processor_deletes_existing_object_and_marks_processed() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let Some(storage) = storage_or_skip().await else {
        eprintln!("skip: S3/MinIO unavailable");
        return;
    };

    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let tenant_id = Uuid::new_v4();
    let object_key = build_original_document_object_key(tenant_id, workspace_id, document_id);
    let bucket = storage.bucket().to_string();
    let body = b"%PDF-1.4 life003 delete object";

    storage
        .put_original_document(&object_key, body, Some("application/pdf"))
        .await
        .expect("put test object");
    assert!(storage.object_exists(&object_key).await.unwrap());

    let outbox_id = enqueue_delete_object(&pool, &object_key, &bucket, workspace_id, document_id)
        .await
        .unwrap();

    let result = process_storage_outbox(&pool, &storage, StorageOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert!(result.processed_rows >= 1);
    assert_eq!(outbox_status(&pool, outbox_id).await, "PROCESSED");
    assert!(
        !storage.object_exists(&object_key).await.unwrap(),
        "object must be deleted"
    );

    let _ = sqlx::query("DELETE FROM storage_outbox WHERE id = $1")
        .bind(outbox_id)
        .execute(&pool)
        .await;
}

#[tokio::test]
async fn processor_prefix_delete_removes_only_target_prefix() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let Some(storage) = storage_or_skip().await else {
        eprintln!("skip: S3/MinIO unavailable");
        return;
    };

    let tenant_id = Uuid::new_v4();
    let workspace_target = Uuid::new_v4();
    let workspace_other = Uuid::new_v4();
    let bucket = storage.bucket().to_string();

    let target_key =
        build_original_document_object_key(tenant_id, workspace_target, Uuid::new_v4());
    let other_key = build_original_document_object_key(tenant_id, workspace_other, Uuid::new_v4());
    let target_prefix = build_workspace_prefix(tenant_id, workspace_target);

    storage
        .put_original_document(&target_key, b"%PDF-1.4 target", Some("application/pdf"))
        .await
        .unwrap();
    storage
        .put_original_document(&other_key, b"%PDF-1.4 other", Some("application/pdf"))
        .await
        .unwrap();

    let outbox_id = enqueue_delete_prefix(
        &pool,
        &target_prefix,
        &bucket,
        Some(tenant_id),
        Some(workspace_target),
    )
    .await
    .unwrap();

    let result = process_storage_outbox(&pool, &storage, StorageOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert!(result.processed_rows >= 1);
    assert_eq!(outbox_status(&pool, outbox_id).await, "PROCESSED");

    assert!(
        !storage.object_exists(&target_key).await.unwrap(),
        "target prefix object must be removed"
    );
    assert!(
        storage.object_exists(&other_key).await.unwrap(),
        "sibling workspace prefix must remain"
    );

    // Cleanup sibling.
    let _ = storage.delete_object(&other_key).await;
    let _ = sqlx::query("DELETE FROM storage_outbox WHERE id = $1")
        .bind(outbox_id)
        .execute(&pool)
        .await;
}
