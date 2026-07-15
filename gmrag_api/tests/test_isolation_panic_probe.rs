mod support;

use gmrag_api::ingestion::embedding::DEFAULT_EMBEDDING_DIM;
use gmrag_api::{
    auth::authz::{AuthzClient, Object, Relation},
    retrieval::{ChunkPoint, RetrievalClient},
    storage::{StorageClient, StorageConfig},
};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn forced_panic_after_writing_all_external_stores() {
    let environment = support::require_external_test_environment();
    if std::env::var("GMRAG_TEST_FORCE_PANIC").as_deref() != Ok("1") {
        return;
    }

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&environment.database_url)
        .await
        .expect("connect isolated PostgreSQL");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("migrate isolated PostgreSQL");

    let tenant_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("panic-probe-{tenant_id}"))
        .execute(&pool)
        .await
        .expect("write isolated PostgreSQL");

    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let chunk_id = Uuid::new_v4();
    let embedding = vec![0.25_f32; DEFAULT_EMBEDDING_DIM];
    let retrieval = RetrievalClient::from_env().expect("isolated Qdrant config");
    retrieval
        .upsert_chunk_points(&[ChunkPoint {
            chunk_id,
            workspace_id,
            document_id,
            chunk_index: 0,
            embedding: embedding.clone(),
        }])
        .await
        .expect("write isolated Qdrant");

    let storage =
        StorageClient::from_config(StorageConfig::from_env().expect("isolated MinIO config")).await;
    let object_key = format!(
        "tenants/{tenant_id}/workspaces/{workspace_id}/documents/{document_id}/original.pdf"
    );
    storage
        .put_original_document(&object_key, b"panic-probe", Some("application/pdf"))
        .await
        .expect("write isolated MinIO");

    let authz = AuthzClient::from_env().expect("isolated OpenFGA config");
    let probe_user = format!("user:panic-probe-{tenant_id}");
    authz
        .write_tuple(&probe_user, Relation::Admin, &Object::Platform)
        .await
        .expect("write isolated OpenFGA");

    let tenant_count: i64 = sqlx::query_scalar("SELECT count(*) FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .fetch_one(&pool)
        .await
        .expect("read isolated PostgreSQL");
    let qdrant_points = retrieval
        .search_chunk_ids(workspace_id, &[document_id], &embedding, 5)
        .await
        .expect("read isolated Qdrant");
    let stored_bytes = storage
        .get_original_document(&object_key)
        .await
        .expect("read isolated MinIO");
    let tuple_exists = authz
        .check_fga(&probe_user, Relation::Admin, &Object::Platform)
        .await
        .expect("read isolated OpenFGA");

    assert_eq!(tenant_count, 1);
    assert_eq!(qdrant_points, vec![chunk_id]);
    assert_eq!(stored_bytes, b"panic-probe");
    assert!(tuple_exists);
    eprintln!("forced-panic probe wrote and read PostgreSQL, Qdrant, MinIO, and OpenFGA");
    panic!("forced panic after all four external-store writes");
}
