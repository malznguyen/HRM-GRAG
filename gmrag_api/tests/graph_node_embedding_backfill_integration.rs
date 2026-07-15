//! Integration tests for operator backfill of legacy `graph_nodes.embedding`.
//!
//! Embedder is injected (no live Ollama required). Database requires DATABASE_URL.

mod support;

use std::sync::OnceLock;

use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Mutex;
use uuid::Uuid;

use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event};
use gmrag_api::ingestion::backfill_node_embeddings::{
    BackfillGraphNodeEmbeddingsOptions, backfill_graph_node_embeddings_with_embedder,
};
use gmrag_api::ingestion::embedding::DEFAULT_EMBEDDING_DIM;
use gmrag_api::ingestion::graph::node_text_for_embedding;

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();
static BACKFILL_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn backfill_test_lock() -> &'static Mutex<()> {
    BACKFILL_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

fn init_test_env() {
    TEST_ENV_INIT.get_or_init(|| unsafe {
        std::env::set_var("APP_ENV", "test");
    });
}

async fn setup_pool() -> sqlx::PgPool {
    dotenvy::dotenv().ok();
    let database_url = support::database_url().expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();

    match sqlx::migrate!("./migrations").run(&pool).await {
        Ok(_) | Err(sqlx::migrate::MigrateError::VersionMismatch(_)) => {}
        Err(err) => panic!("Failed to run migrations: {err}"),
    }

    pool
}

struct SeededWorkspace {
    tenant_id: Uuid,
    workspace_id: Uuid,
    null_node_ids: Vec<Uuid>,
    embedded_node_id: Uuid,
    /// Vector seed written for the pre-embedded node (must not change).
    existing_seed: f32,
}

async fn seed_workspace(pool: &sqlx::PgPool, label: &str) -> SeededWorkspace {
    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let existing_seed = 0.42_f32;

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("backfill-tenant-{label}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("backfill-ws-{label}"))
        .execute(pool)
        .await
        .unwrap();

    let null_a = insert_graph_node(
        pool,
        workspace_id,
        &format!("NullEntityA-{label}"),
        Some("description A"),
        None,
    )
    .await;
    let null_b = insert_graph_node(
        pool,
        workspace_id,
        &format!("NullEntityB-{label}"),
        None,
        None,
    )
    .await;
    let embedded = insert_graph_node(
        pool,
        workspace_id,
        &format!("AlreadyEmbedded-{label}"),
        Some("keep me"),
        Some(existing_seed),
    )
    .await;

    SeededWorkspace {
        tenant_id,
        workspace_id,
        null_node_ids: vec![null_a, null_b],
        embedded_node_id: embedded,
        existing_seed,
    }
}

async fn insert_graph_node(
    pool: &sqlx::PgPool,
    workspace_id: Uuid,
    entity_name: &str,
    description: Option<&str>,
    embedding_seed: Option<f32>,
) -> Uuid {
    if let Some(seed) = embedding_seed {
        let literal = format_pgvector_literal(seed);
        sqlx::query_scalar(
            r#"
            INSERT INTO graph_nodes (workspace_id, entity_name, description, embedding)
            VALUES ($1, $2, $3, $4::vector)
            RETURNING id
            "#,
        )
        .bind(workspace_id)
        .bind(entity_name)
        .bind(description)
        .bind(literal)
        .fetch_one(pool)
        .await
        .unwrap()
    } else {
        sqlx::query_scalar(
            r#"
            INSERT INTO graph_nodes (workspace_id, entity_name, description, embedding)
            VALUES ($1, $2, $3, NULL)
            RETURNING id
            "#,
        )
        .bind(workspace_id)
        .bind(entity_name)
        .bind(description)
        .fetch_one(pool)
        .await
        .unwrap()
    }
}

async fn cleanup_workspace(pool: &sqlx::PgPool, seed: &SeededWorkspace) {
    let _ = sqlx::query("DELETE FROM graph_nodes WHERE workspace_id = $1")
        .bind(seed.workspace_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(seed.workspace_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(seed.tenant_id)
        .execute(pool)
        .await;
}

fn embedding_vec(seed: f32) -> Vec<f32> {
    let mut vector = vec![0.0_f32; DEFAULT_EMBEDDING_DIM];
    vector[0] = seed;
    vector
}

fn format_pgvector_literal(seed: f32) -> String {
    let values = embedding_vec(seed)
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("[{values}]")
}

/// Mock embedder: deterministic 1024-d vectors; fails if text contains "FAIL_EMBED".
fn success_embedder() -> impl FnMut(
    Vec<String>,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<Result<Vec<f32>, String>>, String>> + Send>,
> {
    move |texts: Vec<String>| {
        Box::pin(async move {
            let mut results = Vec::with_capacity(texts.len());
            for (idx, text) in texts.iter().enumerate() {
                if text.contains("FAIL_EMBED") {
                    results.push(Err(format!("mock ollama failure for text index {idx}")));
                } else {
                    // Seed from first char code so re-run produces same shape; not content leak.
                    let seed = 1.0 + (idx as f32) * 0.1;
                    results.push(Ok(embedding_vec(seed)));
                }
            }
            Ok(results)
        })
    }
}

async fn node_embedding_is_null(pool: &sqlx::PgPool, node_id: Uuid) -> bool {
    let is_null: bool =
        sqlx::query_scalar("SELECT embedding IS NULL FROM graph_nodes WHERE id = $1")
            .bind(node_id)
            .fetch_one(pool)
            .await
            .unwrap();
    is_null
}

async fn node_embedding_first_dim(pool: &sqlx::PgPool, node_id: Uuid) -> Option<f32> {
    // pgvector → text "[v0,v1,...]" rồi parse phần tử đầu (đủ để assert không bị overwrite).
    let literal: Option<String> = sqlx::query_scalar(
        "SELECT embedding::text FROM graph_nodes WHERE id = $1 AND embedding IS NOT NULL",
    )
    .bind(node_id)
    .fetch_optional(pool)
    .await
    .unwrap();

    literal.and_then(|text| {
        let trimmed = text.trim().trim_start_matches('[').trim_end_matches(']');
        trimmed
            .split(',')
            .next()
            .and_then(|part| part.trim().parse::<f32>().ok())
    })
}

#[tokio::test]
async fn dry_run_reports_null_count_without_writing() {
    let _guard = backfill_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let seed = seed_workspace(&pool, "dry").await;

    let report = backfill_graph_node_embeddings_with_embedder(
        &pool,
        BackfillGraphNodeEmbeddingsOptions {
            allow_apply: false,
            workspace_id: Some(seed.workspace_id),
            batch_size: 10,
        },
        success_embedder(),
    )
    .await
    .unwrap();

    assert!(!report.applied);
    assert_eq!(report.nodes_found, 2);
    assert_eq!(report.nodes_updated, 0);
    assert_eq!(report.error_count, 0);
    assert_eq!(report.counts_by_workspace.len(), 1);
    assert_eq!(
        report.counts_by_workspace[0].workspace_id,
        seed.workspace_id
    );
    assert_eq!(report.counts_by_workspace[0].null_count, 2);

    for node_id in &seed.null_node_ids {
        assert!(
            node_embedding_is_null(&pool, *node_id).await,
            "dry-run must not write embedding"
        );
    }
    assert!(!node_embedding_is_null(&pool, seed.embedded_node_id).await);
    assert_eq!(
        node_embedding_first_dim(&pool, seed.embedded_node_id).await,
        Some(seed.existing_seed)
    );

    cleanup_workspace(&pool, &seed).await;
}

#[tokio::test]
async fn apply_backfills_null_nodes_and_skips_existing() {
    let _guard = backfill_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let seed = seed_workspace(&pool, "apply").await;

    let report = backfill_graph_node_embeddings_with_embedder(
        &pool,
        BackfillGraphNodeEmbeddingsOptions {
            allow_apply: true,
            workspace_id: Some(seed.workspace_id),
            batch_size: 10,
        },
        success_embedder(),
    )
    .await
    .unwrap();

    assert!(report.applied);
    assert_eq!(report.nodes_found, 2);
    assert_eq!(report.nodes_updated, 2);
    assert_eq!(report.error_count, 0);

    for node_id in &seed.null_node_ids {
        assert!(
            !node_embedding_is_null(&pool, *node_id).await,
            "NULL node should be backfilled"
        );
        let dims: i32 =
            sqlx::query_scalar("SELECT vector_dims(embedding) FROM graph_nodes WHERE id = $1")
                .bind(node_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(dims as usize, DEFAULT_EMBEDDING_DIM);
    }

    // Node đã có embedding không bị ghi đè.
    assert_eq!(
        node_embedding_first_dim(&pool, seed.embedded_node_id).await,
        Some(seed.existing_seed)
    );

    cleanup_workspace(&pool, &seed).await;
}

#[tokio::test]
async fn apply_is_idempotent_on_rerun() {
    let _guard = backfill_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let seed = seed_workspace(&pool, "idem").await;

    let first = backfill_graph_node_embeddings_with_embedder(
        &pool,
        BackfillGraphNodeEmbeddingsOptions {
            allow_apply: true,
            workspace_id: Some(seed.workspace_id),
            batch_size: 10,
        },
        success_embedder(),
    )
    .await
    .unwrap();
    assert_eq!(first.nodes_updated, 2);

    let second = backfill_graph_node_embeddings_with_embedder(
        &pool,
        BackfillGraphNodeEmbeddingsOptions {
            allow_apply: true,
            workspace_id: Some(seed.workspace_id),
            batch_size: 10,
        },
        success_embedder(),
    )
    .await
    .unwrap();

    assert_eq!(second.nodes_found, 0);
    assert_eq!(second.nodes_updated, 0);
    assert_eq!(second.error_count, 0);
    assert!(second.counts_by_workspace.is_empty());

    assert_eq!(
        node_embedding_first_dim(&pool, seed.embedded_node_id).await,
        Some(seed.existing_seed)
    );

    cleanup_workspace(&pool, &seed).await;
}

#[tokio::test]
async fn one_node_embed_failure_does_not_stop_batch() {
    let _guard = backfill_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind("backfill-tenant-partial")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind("backfill-ws-partial")
        .execute(&pool)
        .await
        .unwrap();

    let ok_id = insert_graph_node(
        &pool,
        workspace_id,
        "PartialOkEntity",
        Some("ok description"),
        None,
    )
    .await;
    // entity_name chứa marker để mock embedder fail — production không làm vậy;
    // chỉ dùng để simulate Ollama lỗi cho 1 text.
    let fail_id = insert_graph_node(
        &pool,
        workspace_id,
        "PartialFAIL_EMBEDEntity",
        Some("will fail"),
        None,
    )
    .await;

    // Guard: text format forward-path có chứa marker fail.
    let fail_text = node_text_for_embedding("PartialFAIL_EMBEDEntity", Some("will fail"));
    assert!(fail_text.contains("FAIL_EMBED"));

    let report = backfill_graph_node_embeddings_with_embedder(
        &pool,
        BackfillGraphNodeEmbeddingsOptions {
            allow_apply: true,
            workspace_id: Some(workspace_id),
            batch_size: 10,
        },
        success_embedder(),
    )
    .await
    .unwrap();

    assert_eq!(report.nodes_found, 2);
    assert_eq!(report.nodes_updated, 1);
    assert_eq!(report.error_count, 1);
    assert_eq!(report.error_samples.len(), 1);
    assert!(
        report.error_samples[0].contains(&fail_id.to_string()),
        "error sample should reference failed node id only: {:?}",
        report.error_samples
    );

    assert!(!node_embedding_is_null(&pool, ok_id).await);
    assert!(node_embedding_is_null(&pool, fail_id).await);

    let _ = sqlx::query("DELETE FROM graph_nodes WHERE workspace_id = $1")
        .bind(workspace_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(&pool)
        .await;
}

#[tokio::test]
async fn apply_audit_metadata_is_counts_only() {
    // Unit-style: metadata builder shape (không nhét entity content).
    let _guard = backfill_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let seed = seed_workspace(&pool, "audit").await;

    let report = backfill_graph_node_embeddings_with_embedder(
        &pool,
        BackfillGraphNodeEmbeddingsOptions {
            allow_apply: true,
            workspace_id: Some(seed.workspace_id),
            batch_size: 10,
        },
        success_embedder(),
    )
    .await
    .unwrap();

    let metadata = json!({
        "apply": report.applied,
        "workspace_id": report.workspace_filter,
        "batch_size": report.batch_size,
        "nodes_found": report.nodes_found,
        "nodes_updated": report.nodes_updated,
        "nodes_skipped_already_embedded": report.nodes_skipped_already_embedded,
        "error_count": report.error_count,
        "workspace_count": report.counts_by_workspace.len(),
    });

    assert!(metadata.get("entity_name").is_none());
    assert!(metadata.get("description").is_none());
    assert_eq!(metadata["nodes_updated"], json!(2));

    let _ = insert_audit_event(
        &pool,
        AuditEventRecord::new(AuditEventType::GraphNodeEmbeddingBackfillCompleted)
            .with_workspace_id(seed.workspace_id)
            .with_metadata(metadata),
    )
    .await
    .unwrap();

    let event_type: String = sqlx::query_scalar(
        r#"
        SELECT event_type
        FROM audit_events
        WHERE workspace_id = $1
          AND event_type = 'graph_node_embedding_backfill_completed'
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(seed.workspace_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_type, "graph_node_embedding_backfill_completed");

    let _ = sqlx::query("DELETE FROM audit_events WHERE workspace_id = $1 AND event_type = $2")
        .bind(seed.workspace_id)
        .bind(AuditEventType::GraphNodeEmbeddingBackfillCompleted.as_str())
        .execute(&pool)
        .await;

    cleanup_workspace(&pool, &seed).await;
}
