//! LIFE-001: qdrant_outbox phải nằm trong cùng SQL transaction với document/workspace delete.
//!
//! Các test này chỉ cần PostgreSQL — không gọi Qdrant — để chứng minh:
//! 1. Sau commit lifecycle TX, recovery row đã tồn tại (crash-after-commit an toàn).
//! 2. Rollback lifecycle TX không để lại outbox row mồ côi.

use gmrag_api::retrieval::outbox::{enqueue_delete_by_document_tx, enqueue_delete_by_workspace_tx};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

async fn pool_or_skip() -> Option<PgPool> {
    dotenvy::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .ok()?;
    sqlx::migrate!("./migrations").run(&pool).await.ok()?;
    Some(pool)
}

struct SeededWorkspace {
    tenant_id: Uuid,
    workspace_id: Uuid,
    user_id: String,
}

struct SeededDocument {
    document_id: Uuid,
    object_key: String,
}

async fn seed_workspace(pool: &PgPool) -> SeededWorkspace {
    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let user_id = format!("life001-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&user_id)
        .bind(format!("{user_id}@test.local"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("life001-tenant-{tenant_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("life001-ws-{workspace_id}"))
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
    let object_key = format!(
        "tenants/{}/workspaces/{}/documents/{}/original.pdf",
        workspace.tenant_id, workspace.workspace_id, document_id
    );

    sqlx::query(
        r#"
        INSERT INTO documents (
            id, workspace_id, owner_id, filename, status, processing_stage,
            object_key, bucket, uploaded_by
        )
        VALUES ($1, $2, $3, 'life001.pdf', 'FAILED', 'FAILED', $4, 'test', $3)
        "#,
    )
    .bind(document_id)
    .bind(workspace.workspace_id)
    .bind(&workspace.user_id)
    .bind(&object_key)
    .execute(pool)
    .await
    .unwrap();

    SeededDocument {
        document_id,
        object_key,
    }
}

async fn cleanup_workspace(pool: &PgPool, workspace: &SeededWorkspace) {
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

async fn count_document_outbox(pool: &PgPool, document_id: Uuid) -> i64 {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM qdrant_outbox
        WHERE event_type = 'delete_by_document'
          AND status = 'PENDING'
          AND payload->>'document_id' = $1
        "#,
    )
    .bind(document_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn count_workspace_outbox(pool: &PgPool, workspace_id: Uuid) -> i64 {
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

/// Mirrors document-delete SQL lifecycle trong handler (không gọi Qdrant/storage).
async fn commit_document_delete_lifecycle(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
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
    tx.commit().await
}

/// Mirrors workspace-delete SQL lifecycle trong handler (không gọi Qdrant).
async fn commit_workspace_delete_lifecycle(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let outcome = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(&mut *tx)
        .await?;

    if outcome.rows_affected() == 0 {
        return Ok(false);
    }

    enqueue_delete_by_workspace_tx(&mut tx, workspace_id).await?;
    tx.commit().await?;
    Ok(true)
}

#[tokio::test]
async fn document_delete_outbox_exists_after_sql_commit_without_qdrant() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let workspace = seed_workspace(&pool).await;
    let document = seed_document(&pool, &workspace).await;

    commit_document_delete_lifecycle(&pool, workspace.workspace_id, document.document_id)
        .await
        .expect("document lifecycle TX must commit");

    // Điểm crash-after-commit: recovery row đã durable trước mọi HTTP Qdrant call.
    assert_eq!(
        count_document_outbox(&pool, document.document_id).await,
        1,
        "delete_by_document outbox must exist immediately after SQL commit"
    );

    let document_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM documents WHERE id = $1)")
            .bind(document.document_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!document_exists, "document row must be gone after commit");

    // Cleanup outbox + tenant seed (workspace có thể còn)
    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE payload->>'document_id' = $1")
        .bind(document.document_id.to_string())
        .execute(&pool)
        .await;
    cleanup_workspace(&pool, &workspace).await;
    let _ = document.object_key;
}

#[tokio::test]
async fn document_delete_outbox_rolls_back_with_sql_delete() {
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
        enqueue_delete_by_document_tx(&mut tx, workspace.workspace_id, document.document_id)
            .await
            .unwrap();
        // Cố ý không commit — mô phỏng lỗi giữa chừng / drop transaction.
        tx.rollback().await.unwrap();
    }

    assert_eq!(
        count_document_outbox(&pool, document.document_id).await,
        0,
        "rolled-back transaction must not leave delete_by_document outbox"
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

#[tokio::test]
async fn workspace_delete_outbox_exists_after_sql_commit_without_qdrant() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let workspace = seed_workspace(&pool).await;
    let workspace_id = workspace.workspace_id;

    let deleted = commit_workspace_delete_lifecycle(&pool, workspace_id)
        .await
        .expect("workspace lifecycle TX must commit");
    assert!(deleted);

    assert_eq!(
        count_workspace_outbox(&pool, workspace_id).await,
        1,
        "delete_by_workspace outbox must exist immediately after SQL commit"
    );

    let workspace_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspaces WHERE id = $1)")
            .bind(workspace_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!workspace_exists, "workspace row must be gone after commit");

    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE payload->>'workspace_id' = $1")
        .bind(workspace_id.to_string())
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

#[tokio::test]
async fn workspace_delete_outbox_rolls_back_with_sql_delete() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };

    let workspace = seed_workspace(&pool).await;

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
        tx.rollback().await.unwrap();
    }

    assert_eq!(
        count_workspace_outbox(&pool, workspace.workspace_id).await,
        0,
        "rolled-back transaction must not leave delete_by_workspace outbox"
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

    cleanup_workspace(&pool, &workspace).await;
}
