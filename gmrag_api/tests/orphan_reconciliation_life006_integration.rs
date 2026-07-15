//! LIFE-006: orphan reconciliation (operator full-scan + dry-run/report + bounded apply).
//!
//! Chứng minh drill bắt buộc:
//! 1. Inject orphan storage object (không có row `documents`) và orphan Qdrant
//!    point (không có workspace/document SQL).
//! 2. Dry-run report candidates, không mutate.
//! 3. Apply xoá an toàn (storage: `--delete-orphans`; Qdrant: scoped workspace
//!    orphan đã discovered qua full-scan — không `--force` vì SQL không còn live).
//! 4. Re-run apply idempotent.
//!
//! Không claim scheduled/unattended execution (OPS-002 / OPS-003). Live-resource
//! guards và tenant empty-list hard-fail đã cover ở phase3a; không lặp lại ở đây.

mod support;

use gmrag_api::ingestion::embedding::DEFAULT_EMBEDDING_DIM;
use gmrag_api::retrieval::cleanup::{QdrantCleanupOptions, cleanup_qdrant_orphans};
use gmrag_api::retrieval::{ChunkPoint, RetrievalClient};
use gmrag_api::storage::cleanup::{StorageCleanupOptions, scan_documents_and_orphans};
use gmrag_api::storage::{StorageClient, StorageConfig, build_original_document_object_key};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::OnceLock;
use tokio::sync::Mutex;
use uuid::Uuid;

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();
static LIFE006_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn life006_test_lock() -> &'static Mutex<()> {
    LIFE006_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

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
        if std::env::var_os("QDRANT_URL").is_none() {
            std::env::set_var("QDRANT_URL", "http://localhost:6333");
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
    match storage.list_objects(Some("life006-probe/")).await {
        Ok(_) => Some(storage),
        Err(_) => None,
    }
}

async fn retrieval_or_skip() -> Option<RetrievalClient> {
    init_test_env();
    let client = RetrievalClient::from_env().ok()?;
    // Probe bằng upsert/delete nhỏ — skip nếu Qdrant không sẵn.
    let probe_ws = Uuid::new_v4();
    let probe_doc = Uuid::new_v4();
    let dim = DEFAULT_EMBEDDING_DIM;
    let embedding = vec![0.001_f32; dim];
    match client
        .upsert_chunk_points(&[ChunkPoint {
            chunk_id: Uuid::new_v4(),
            workspace_id: probe_ws,
            document_id: probe_doc,
            chunk_index: 0,
            embedding: embedding.clone(),
        }])
        .await
    {
        Ok(()) => {
            let _ = client.delete_points_by_document(probe_ws, probe_doc).await;
            Some(client)
        }
        Err(_) => None,
    }
}

fn sample_pdf_bytes() -> Vec<u8> {
    b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\ntrailer\n<< /Root 1 0 R >>\n%%EOF".to_vec()
}

/// Drill storage: inject orphan object → dry-run report (no delete) → apply → re-run idempotent.
#[tokio::test]
async fn life006_storage_orphan_dry_run_apply_idempotent() {
    let _guard = life006_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip life006 storage drill: DATABASE_URL unavailable");
        return;
    };
    let Some(storage) = storage_or_skip().await else {
        eprintln!("skip life006 storage drill: MinIO/S3 unavailable");
        return;
    };

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let object_key = build_original_document_object_key(tenant_id, workspace_id, document_id);
    let bytes = sample_pdf_bytes();

    // Không insert SQL document — object là orphan thuần storage.
    storage
        .put_original_document(&object_key, &bytes, Some("application/pdf"))
        .await
        .expect("put orphan object");

    // Dry-run / report: liệt kê orphan, không xoá.
    let dry = scan_documents_and_orphans(&pool, &storage, StorageCleanupOptions::default())
        .await
        .expect("storage dry-run scan");
    assert!(
        dry.orphan_object_keys.contains(&object_key),
        "dry-run must report injected orphan object_key"
    );
    assert_eq!(dry.deleted_orphan_objects, 0);
    assert!(
        storage.object_exists(&object_key).await.unwrap(),
        "dry-run must not delete orphan object"
    );

    // Apply remediation (bounded by explicit operator flags, not auto-schedule).
    let apply = scan_documents_and_orphans(
        &pool,
        &storage,
        StorageCleanupOptions {
            allow_delete: true,
            delete_orphans: true,
            mark_missing_documents_failed: false,
        },
    )
    .await
    .expect("storage apply");
    assert!(apply.orphan_object_keys.contains(&object_key));
    assert!(apply.deleted_orphan_objects >= 1);
    assert!(
        !storage.object_exists(&object_key).await.unwrap(),
        "apply must delete orphan object"
    );

    // Re-run: idempotent — object vẫn absent, không fail.
    let rerun = scan_documents_and_orphans(
        &pool,
        &storage,
        StorageCleanupOptions {
            allow_delete: true,
            delete_orphans: true,
            mark_missing_documents_failed: false,
        },
    )
    .await
    .expect("storage re-run");
    assert!(
        !rerun.orphan_object_keys.contains(&object_key),
        "re-run must not still list deleted orphan as present object"
    );
    assert!(
        !storage.object_exists(&object_key).await.unwrap(),
        "re-run must leave object absent (idempotent)"
    );
}

/// Drill Qdrant: inject orphan point (no SQL) → full-scan dry-run → apply → re-run idempotent.
#[tokio::test]
async fn life006_qdrant_full_scan_orphan_dry_run_apply_idempotent() {
    let _guard = life006_test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip life006 qdrant drill: DATABASE_URL unavailable");
        return;
    };
    let Some(retrieval) = retrieval_or_skip().await else {
        eprintln!("skip life006 qdrant drill: Qdrant unavailable");
        return;
    };

    // Point orphan: workspace/document IDs không tồn tại trong SQL.
    let orphan_workspace_id = Uuid::new_v4();
    let orphan_document_id = Uuid::new_v4();
    let chunk_id = Uuid::new_v4();
    let embedding = vec![0.003_f32; DEFAULT_EMBEDDING_DIM];

    retrieval
        .upsert_chunk_points(&[ChunkPoint {
            chunk_id,
            workspace_id: orphan_workspace_id,
            document_id: orphan_document_id,
            chunk_index: 0,
            embedding: embedding.clone(),
        }])
        .await
        .expect("upsert orphan qdrant point");

    let live_before = retrieval
        .search_chunk_ids(orphan_workspace_id, &[orphan_document_id], &embedding, 5)
        .await
        .expect("search orphan before cleanup");
    assert!(
        !live_before.is_empty(),
        "injected orphan point must be searchable before cleanup"
    );

    // Full-scan dry-run: phát hiện orphan, không mutate.
    let dry = cleanup_qdrant_orphans(
        &pool,
        &retrieval,
        &QdrantCleanupOptions {
            dry_run: true,
            delete: false,
            workspace_id: None,
            tenant_id: None,
            full_scan: true,
            scroll_page_size: 128,
            force: false,
        },
    )
    .await
    .expect("qdrant full-scan dry-run");

    assert!(dry.dry_run);
    assert!(
        dry.mode.contains("full_scan"),
        "mode must indicate full_scan, got {}",
        dry.mode
    );
    assert!(
        dry.candidates_from_full_scan >= 1,
        "full-scan must report at least the injected orphan"
    );
    assert_eq!(dry.deletes_attempted, 0);
    assert_eq!(dry.deletes_succeeded, 0);

    let still_live = retrieval
        .search_chunk_ids(orphan_workspace_id, &[orphan_document_id], &embedding, 5)
        .await
        .expect("search after dry-run");
    assert!(
        !still_live.is_empty(),
        "dry-run must not delete orphan qdrant point"
    );

    // Apply: scoped workspace delete — SQL không còn workspace nên không cần --force
    // (live-resource guard cho phép xoá orphan đã cascade / chưa từng tồn tại).
    let apply = cleanup_qdrant_orphans(
        &pool,
        &retrieval,
        &QdrantCleanupOptions {
            dry_run: false,
            delete: true,
            workspace_id: Some(orphan_workspace_id),
            tenant_id: None,
            full_scan: false,
            scroll_page_size: 128,
            force: false,
        },
    )
    .await
    .expect("qdrant apply orphan workspace delete");

    assert_eq!(apply.deletes_attempted, 1);
    assert_eq!(apply.deletes_succeeded, 1);
    assert_eq!(apply.deletes_failed, 0);

    let gone = retrieval
        .search_chunk_ids(orphan_workspace_id, &[orphan_document_id], &embedding, 5)
        .await
        .expect("search after apply");
    assert!(gone.is_empty(), "apply must remove orphan qdrant point");

    // Re-run apply: filter-delete idempotent (không fail khi không còn point).
    let rerun = cleanup_qdrant_orphans(
        &pool,
        &retrieval,
        &QdrantCleanupOptions {
            dry_run: false,
            delete: true,
            workspace_id: Some(orphan_workspace_id),
            tenant_id: None,
            full_scan: false,
            scroll_page_size: 128,
            force: false,
        },
    )
    .await
    .expect("qdrant re-run must be idempotent");

    assert_eq!(rerun.deletes_attempted, 1);
    assert_eq!(rerun.deletes_succeeded, 1);
    assert_eq!(rerun.deletes_failed, 0);

    let still_gone = retrieval
        .search_chunk_ids(orphan_workspace_id, &[orphan_document_id], &embedding, 5)
        .await
        .expect("search after re-run");
    assert!(still_gone.is_empty());
}
