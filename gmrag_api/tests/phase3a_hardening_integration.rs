use std::sync::{Arc, OnceLock};

use reqwest::Client;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{Mutex, Semaphore};
use uuid::Uuid;

use gmrag_api::auth::authz::{AuthzClient, Object, Relation, TupleKey};
use gmrag_api::auth::outbox::{
    AuthzOutboxProcessorConfig, enqueue_tuple_delete, enqueue_tuple_write, process_authz_outbox,
};
use gmrag_api::state::AppState;
use gmrag_api::storage::cleanup::{
    StorageCleanupOptions, build_tenant_prefix, build_workspace_prefix, cleanup_prefix,
    scan_documents_and_orphans,
};
use gmrag_api::storage::{StorageClient, StorageConfig, build_original_document_object_key};

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();
static PHASE3A_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn phase3a_test_lock() -> &'static Mutex<()> {
    PHASE3A_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
async fn authz_outbox_processor_processes_pending_tuple_write() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let authz_client = AuthzClient::from_env().unwrap();

    let user_id = format!("phase3a-outbox-write-{}", Uuid::new_v4());
    let workspace_id = Uuid::new_v4();
    let tuple = TupleKey {
        user: format!("user:{user_id}"),
        relation: Relation::Member.as_str().to_string(),
        object: format!("workspace:{workspace_id}"),
    };

    let event_id = enqueue_tuple_write(&pool, &tuple).await.unwrap();

    let _ = process_authz_outbox(&pool, &authz_client, AuthzOutboxProcessorConfig::default())
        .await
        .unwrap();

    let row: (String, i32) =
        sqlx::query_as("SELECT status, retry_count FROM authz_outbox WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, "PROCESSED");
    assert_eq!(row.1, 0);

    let allowed = authz_client
        .check_fga(
            &format!("user:{user_id}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();
    assert!(allowed);

    let _ = authz_client
        .delete_tuple(
            &format!("user:{user_id}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await;
}

#[tokio::test]
async fn authz_outbox_processor_processes_pending_tuple_delete() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let authz_client = AuthzClient::from_env().unwrap();

    let user_id = format!("phase3a-outbox-delete-{}", Uuid::new_v4());
    let workspace_id = Uuid::new_v4();

    authz_client
        .write_tuple(
            &format!("user:{user_id}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();

    let tuple = TupleKey {
        user: format!("user:{user_id}"),
        relation: Relation::Member.as_str().to_string(),
        object: format!("workspace:{workspace_id}"),
    };
    let event_id = enqueue_tuple_delete(&pool, &tuple).await.unwrap();

    let _ = process_authz_outbox(&pool, &authz_client, AuthzOutboxProcessorConfig::default())
        .await
        .unwrap();

    let row: (String, i32) =
        sqlx::query_as("SELECT status, retry_count FROM authz_outbox WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, "PROCESSED");
    assert_eq!(row.1, 0);

    let allowed = authz_client
        .check_fga(
            &format!("user:{user_id}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();
    assert!(!allowed);
}

#[tokio::test]
async fn authz_outbox_failed_event_increments_retry_count_and_stores_sanitized_error() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let authz_client = AuthzClient::from_env().unwrap();

    let event_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO authz_outbox (event_type, payload, status, retry_count)
        VALUES ('tuple_write', '{}'::jsonb, 'PENDING', 0)
        RETURNING id
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let _ = process_authz_outbox(&pool, &authz_client, AuthzOutboxProcessorConfig::default())
        .await
        .unwrap();

    let row: (String, i32, Option<String>) =
        sqlx::query_as("SELECT status, retry_count, error_message FROM authz_outbox WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(row.0, "FAILED");
    assert_eq!(row.1, 1);
    assert_eq!(row.2.as_deref(), Some("invalid_payload"));
}

#[tokio::test]
async fn storage_cleanup_dry_run_lists_orphan_without_deleting_it() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let storage = setup_storage().await;

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let object_key = build_original_document_object_key(tenant_id, workspace_id, document_id);
    let bytes = sample_pdf_bytes();

    storage
        .put_original_document(&object_key, &bytes, Some("application/pdf"))
        .await
        .unwrap();

    let report = scan_documents_and_orphans(&pool, &storage, StorageCleanupOptions::default())
        .await
        .unwrap();

    assert!(report.orphan_object_keys.contains(&object_key));
    assert!(storage.object_exists(&object_key).await.unwrap());

    let _ = storage.delete_object(&object_key).await;
}

#[tokio::test]
async fn storage_cleanup_with_delete_removes_orphan_object() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let storage = setup_storage().await;

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let object_key = build_original_document_object_key(tenant_id, workspace_id, document_id);
    let bytes = sample_pdf_bytes();

    storage
        .put_original_document(&object_key, &bytes, Some("application/pdf"))
        .await
        .unwrap();

    let report = scan_documents_and_orphans(
        &pool,
        &storage,
        StorageCleanupOptions {
            allow_delete: true,
            delete_orphans: true,
            mark_missing_documents_failed: false,
        },
    )
    .await
    .unwrap();

    assert!(report.orphan_object_keys.contains(&object_key));
    assert!(report.deleted_orphan_objects >= 1);
    assert!(!storage.object_exists(&object_key).await.unwrap());
}

#[tokio::test]
async fn workspace_prefix_cleanup_deletes_only_workspace_prefix_objects() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let storage = setup_storage().await;

    let tenant_id = Uuid::new_v4();
    let workspace_a = Uuid::new_v4();
    let workspace_b = Uuid::new_v4();

    let ws_a_key = build_original_document_object_key(tenant_id, workspace_a, Uuid::new_v4());
    let ws_b_key = build_original_document_object_key(tenant_id, workspace_b, Uuid::new_v4());
    let tenant_other_key = format!("{}loose-object.pdf", build_tenant_prefix(tenant_id));

    let bytes = sample_pdf_bytes();
    storage
        .put_original_document(&ws_a_key, &bytes, Some("application/pdf"))
        .await
        .unwrap();
    storage
        .put_original_document(&ws_b_key, &bytes, Some("application/pdf"))
        .await
        .unwrap();
    storage
        .put_original_document(&tenant_other_key, &bytes, Some("application/pdf"))
        .await
        .unwrap();

    let prefix = build_workspace_prefix(tenant_id, workspace_a);
    let report = cleanup_prefix(&storage, prefix, true).await.unwrap();

    assert!(report.object_keys.contains(&ws_a_key));
    assert!(!storage.object_exists(&ws_a_key).await.unwrap());
    assert!(storage.object_exists(&ws_b_key).await.unwrap());
    assert!(storage.object_exists(&tenant_other_key).await.unwrap());

    let _ = storage.delete_object(&ws_b_key).await;
    let _ = storage.delete_object(&tenant_other_key).await;
}

#[tokio::test]
async fn member_role_change_writes_audit_event() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let authz_client = AuthzClient::from_env().unwrap();

    let jwt = gmrag_api::auth::jwt::JwtValidator::from_env().unwrap();
    let keycloak_client = gmrag_api::auth::keycloak::KeycloakClient::from_env().unwrap();
    let storage = setup_storage().await;
    let retrieval = gmrag_api::retrieval::RetrievalClient::from_env().unwrap();

    let state = AppState {
        pool: pool.clone(),
        jwt,
        storage,
        retrieval,
        ingestion_limiter: Arc::new(Semaphore::new(0)),
        authz_client: authz_client.clone(),
        keycloak_client,
    };

    let app = gmrag_api::app_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let owner_user_id = format!("phase3a-owner-{}", Uuid::new_v4());
    let member_user_id = format!("phase3a-member-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Phase3A Tenant {tenant_id}"))
        .execute(&pool)
        .await
        .unwrap();

    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("Phase3A Workspace {workspace_id}"))
        .execute(&pool)
        .await
        .unwrap();

    for user_id in [&owner_user_id, &member_user_id] {
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@test.local"))
            .execute(&pool)
            .await
            .unwrap();
    }

    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
    )
    .bind(workspace_id)
    .bind(&owner_user_id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'MEMBER')",
    )
    .bind(workspace_id)
    .bind(&member_user_id)
    .execute(&pool)
    .await
    .unwrap();

    authz_client
        .write_tuple(
            &format!("user:{owner_user_id}"),
            Relation::Owner,
            &Object::Tenant(tenant_id),
        )
        .await
        .unwrap();
    authz_client
        .write_tuple(
            &format!("tenant:{tenant_id}"),
            Relation::Tenant,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();
    authz_client
        .write_tuple(
            &format!("user:{member_user_id}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();

    let response = Client::new()
        .patch(format!(
            "{addr}/workspaces/{workspace_id}/members/{member_user_id}"
        ))
        .bearer_auth(&owner_user_id)
        .json(&json!({ "role": "ADMIN" }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let row: Option<(String, serde_json::Value)> = sqlx::query_as(
        r#"
        SELECT event_type, metadata
        FROM audit_events
        WHERE event_type = 'member_role_changed'
          AND workspace_id = $1
          AND target_id = $2
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(workspace_id)
    .bind(&member_user_id)
    .fetch_optional(&pool)
    .await
    .unwrap();

    let Some((event_type, metadata)) = row else {
        panic!("expected member_role_changed audit event");
    };

    assert_eq!(event_type, "member_role_changed");
    assert_eq!(metadata["member_user_id"], json!(member_user_id));
    assert_eq!(metadata["new_role"], json!("ADMIN"));

    let _ = authz_client
        .delete_tuple(
            &format!("user:{owner_user_id}"),
            Relation::Owner,
            &Object::Tenant(tenant_id),
        )
        .await;
    let _ = authz_client
        .delete_tuple(
            &format!("tenant:{tenant_id}"),
            Relation::Tenant,
            &Object::Workspace(workspace_id),
        )
        .await;
    let _ = authz_client
        .delete_tuple(
            &format!("user:{member_user_id}"),
            Relation::Admin,
            &Object::Workspace(workspace_id),
        )
        .await;
}

fn init_test_env() {
    TEST_ENV_INIT.get_or_init(|| unsafe {
        std::env::set_var("APP_ENV", "test");
        std::env::set_var("TEST_BYPASS_JWT", "1");
        std::env::set_var("TEST_BYPASS_KEYCLOAK", "1");
        std::env::set_var("S3_ENDPOINT_URL", "http://localhost:9000");
        std::env::set_var("S3_REGION", "us-east-1");
        std::env::set_var("S3_BUCKET", "gmrag-documents");
        std::env::set_var("S3_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("S3_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("S3_FORCE_PATH_STYLE", "true");
        std::env::set_var("S3_PRESIGN_EXPIRY_SECS", "900");
        std::env::set_var("GMRAG_GRAPH_EXTRACTION_ENABLED", "false");
    });
}

async fn setup_pool() -> sqlx::PgPool {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
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

async fn setup_storage() -> StorageClient {
    let config = StorageConfig::from_env().unwrap();
    StorageClient::from_config(config).await
}

fn sample_pdf_bytes() -> Vec<u8> {
    b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\ntrailer\n<< /Root 1 0 R >>\n%%EOF".to_vec()
}
