use std::process::Stdio;
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
use gmrag_api::invite_cleanup::{
    InvitePlaceholderCleanupOptions, cleanup_invite_placeholders, find_invite_placeholders,
};
use gmrag_api::retrieval::RetrievalClient;
use gmrag_api::retrieval::cleanup::{QdrantCleanupOptions, cleanup_qdrant_orphans};
use gmrag_api::retrieval::outbox::{
    QdrantOutboxProcessorConfig, enqueue_delete_by_document, enqueue_delete_by_workspace,
    process_qdrant_outbox,
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

/// OPS-001: hai lần drain song song không để status ngoài enum CHECK;
/// OpenFGA write idempotent → terminal PROCESSED; invalid payload → FAILED hợp lệ.
#[tokio::test]
async fn authz_outbox_concurrent_invocations_leave_valid_terminal_state() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let authz_client = AuthzClient::from_env().unwrap();

    let user_id = format!("phase3a-outbox-concurrent-{}", Uuid::new_v4());
    let workspace_id = Uuid::new_v4();
    let good_tuple = TupleKey {
        user: format!("user:{user_id}"),
        relation: Relation::Member.as_str().to_string(),
        object: format!("workspace:{workspace_id}"),
    };
    let good_id = enqueue_tuple_write(&pool, &good_tuple).await.unwrap();

    let bad_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO authz_outbox (event_type, payload, status, retry_count)
        VALUES ('tuple_write', '{}'::jsonb, 'PENDING', 0)
        RETURNING id
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let pool_a = pool.clone();
    let pool_b = pool.clone();
    let client_a = authz_client.clone();
    let client_b = authz_client.clone();
    let config = AuthzOutboxProcessorConfig::default();

    let (r1, r2) = tokio::join!(
        process_authz_outbox(&pool_a, &client_a, config),
        process_authz_outbox(&pool_b, &client_b, config),
    );
    r1.expect("first concurrent drain must not hard-fail");
    r2.expect("second concurrent drain must not hard-fail");

    let good_row: (String, i32) =
        sqlx::query_as("SELECT status, retry_count FROM authz_outbox WHERE id = $1")
            .bind(good_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(good_row.0, "PROCESSED");
    assert_eq!(good_row.1, 0);

    let bad_row: (String, i32, Option<String>) =
        sqlx::query_as("SELECT status, retry_count, error_message FROM authz_outbox WHERE id = $1")
            .bind(bad_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(bad_row.0, "FAILED");
    assert!(
        bad_row.1 >= 1 && bad_row.1 <= 2,
        "retry_count must stay in a valid range under dual process: got {}",
        bad_row.1
    );
    assert_eq!(bad_row.2.as_deref(), Some("invalid_payload"));

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
    let _ = sqlx::query("DELETE FROM authz_outbox WHERE id = $1 OR id = $2")
        .bind(good_id)
        .bind(bad_id)
        .execute(&pool)
        .await;
}

/// OPS-001 failure drill: lỗi SQL cứng propagate (binary map → exit 1 / restart).
#[tokio::test]
async fn authz_outbox_hard_sql_failure_is_observable() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let authz_client = AuthzClient::from_env().unwrap();

    // Đóng pool → lần fetch tiếp theo fail cứng (không chỉ FAILED per-row).
    pool.close().await;

    let result =
        process_authz_outbox(&pool, &authz_client, AuthzOutboxProcessorConfig::default()).await;
    assert!(
        result.is_err(),
        "closed pool must surface Err for supervisor restart path"
    );
    assert_eq!(
        gmrag_api::auth::outbox::authz_outbox_exit_code(&result),
        gmrag_api::auth::outbox::AUTHZ_OUTBOX_EXIT_FAILURE
    );
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
async fn storage_orphan_scan_loop_reports_each_round_without_deleting_objects() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let storage = setup_storage().await;

    let object_key = format!("ops003/{}.pdf", Uuid::new_v4());
    storage
        .put_original_document(
            &object_key,
            sample_pdf_bytes().as_slice(),
            Some("application/pdf"),
        )
        .await
        .unwrap();

    let before_reports: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit_events WHERE event_type = 'storage_orphan_scan_report'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let binary_path = std::env::var("CARGO_BIN_EXE_cleanup-storage-objects")
        .expect("Cargo must expose the cleanup-storage-objects binary for integration tests");
    let mut child = tokio::process::Command::new(binary_path)
        .args(["--loop", "--dry-run", "--interval-secs", "2"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let report_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM audit_events WHERE event_type = 'storage_orphan_scan_report'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        if report_count - before_reports >= 2 {
            break;
        }

        if tokio::time::Instant::now() >= deadline {
            let _ = child.kill().await;
            let output = child.wait_with_output().await.unwrap();
            panic!(
                "storage orphan scan loop did not report twice: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let _ = child.kill().await;
    let _ = child.wait_with_output().await.unwrap();

    let after_reports: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit_events WHERE event_type = 'storage_orphan_scan_report'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after_reports - before_reports, 2);

    let report_metadata: Vec<(serde_json::Value,)> = sqlx::query_as(
        r#"
        SELECT metadata
        FROM audit_events
        WHERE event_type = 'storage_orphan_scan_report'
        ORDER BY created_at DESC
        LIMIT 2
        "#,
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        report_metadata
            .iter()
            .all(|(metadata,)| !metadata.to_string().contains("object_key"))
    );
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

#[tokio::test]
async fn invite_placeholder_cleanup_dry_run_does_not_delete() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let authz_client = AuthzClient::from_env().unwrap();

    let suffix = Uuid::new_v4();
    let placeholder_id = format!("invite_cleanup-test-{suffix}@example.com");
    let email = format!("cleanup-test-{suffix}@example.com");
    let workspace_id = Uuid::new_v4();
    let tenant_id = Uuid::new_v4();

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind("invite-cleanup-tenant")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind("invite-cleanup-ws")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&placeholder_id)
        .bind(&email)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'MEMBER')",
    )
    .bind(workspace_id)
    .bind(&placeholder_id)
    .execute(&pool)
    .await
    .unwrap();

    authz_client
        .write_tuple(
            &format!("user:{placeholder_id}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();

    // Dry-run: báo cáo có placeholder nhưng không xoá.
    let report = cleanup_invite_placeholders(
        &pool,
        &authz_client,
        InvitePlaceholderCleanupOptions {
            allow_delete: false,
        },
    )
    .await
    .unwrap();

    assert!(
        report
            .placeholders
            .iter()
            .any(|p| p.user_id == placeholder_id),
        "dry-run should report the seeded placeholder"
    );
    assert!(!report.deleted);
    assert_eq!(report.users_deleted, 0);
    assert_eq!(report.openfga_tuples_deleted, 0);

    let still_exists: Option<String> = sqlx::query_scalar("SELECT id FROM users WHERE id = $1")
        .bind(&placeholder_id)
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert_eq!(still_exists.as_deref(), Some(placeholder_id.as_str()));

    let still_member: Option<String> = sqlx::query_scalar(
        "SELECT user_id FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(workspace_id)
    .bind(&placeholder_id)
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(still_member.is_some());

    let still_allowed = authz_client
        .check_fga(
            &format!("user:{placeholder_id}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();
    assert!(still_allowed);

    // Cleanup test fixtures (delete path — also validates --delete is non-destructive to dry-run).
    let delete_report = cleanup_invite_placeholders(
        &pool,
        &authz_client,
        InvitePlaceholderCleanupOptions { allow_delete: true },
    )
    .await
    .unwrap();
    assert!(delete_report.deleted);
    assert!(delete_report.users_deleted >= 1);
    assert!(
        delete_report.errors.is_empty(),
        "{:?}",
        delete_report.errors
    );

    let after: Option<String> = sqlx::query_scalar("SELECT id FROM users WHERE id = $1")
        .bind(&placeholder_id)
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(after.is_none());

    // Idempotent re-run: không lỗi khi không còn placeholder.
    let second = cleanup_invite_placeholders(
        &pool,
        &authz_client,
        InvitePlaceholderCleanupOptions { allow_delete: true },
    )
    .await
    .unwrap();
    assert_eq!(second.users_deleted, 0);
    assert!(second.errors.is_empty());

    // Đảm bảo find helper không còn trả id test này
    let remaining = find_invite_placeholders(&pool).await.unwrap();
    assert!(!remaining.iter().any(|u| u.id == placeholder_id));

    let _ = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(&pool)
        .await;
}

/// Migration `20260709000000_graph_nodes_hnsw_index` phải tạo HNSW L2 (khớp `ORDER BY embedding <->`).
#[tokio::test]
async fn graph_nodes_embedding_hnsw_index_exists_with_l2_ops() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;

    let indexdef: Option<String> = sqlx::query_scalar(
        r#"
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'graph_nodes'
          AND indexname = 'graph_nodes_embedding_hnsw_idx'
        "#,
    )
    .fetch_optional(&pool)
    .await
    .expect("query pg_indexes");

    let indexdef = indexdef.expect("graph_nodes_embedding_hnsw_idx must exist after migrations");
    let lower = indexdef.to_lowercase();
    assert!(
        lower.contains("hnsw"),
        "index must be HNSW, got: {indexdef}"
    );
    assert!(
        lower.contains("vector_l2_ops"),
        "index must use vector_l2_ops (retrieval uses <->), got: {indexdef}"
    );
    assert!(
        !lower.contains("vector_cosine_ops"),
        "must not use cosine ops class on graph_nodes.embedding, got: {indexdef}"
    );
}

/// Xoá row outbox còn claimable — tránh test chậm / assert lẫn state leftover.
async fn clear_claimable_qdrant_outbox(pool: &sqlx::PgPool) {
    sqlx::query(
        r#"
        DELETE FROM qdrant_outbox
        WHERE status IN ('PENDING', 'FAILED')
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
}

/// Probe filter-delete rỗng: Qdrant reachable + collection tồn tại (LIFE-002 hard PROCESSED).
async fn qdrant_empty_filter_delete_ready(retrieval: &RetrievalClient) -> bool {
    retrieval
        .delete_points_by_document(Uuid::new_v4(), Uuid::new_v4())
        .await
        .is_ok()
}

#[tokio::test]
async fn qdrant_outbox_processor_processes_pending_document_delete() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    clear_claimable_qdrant_outbox(&pool).await;

    // Qdrant local — delete filter không match point vẫn Ok (idempotent).
    let retrieval = match RetrievalClient::from_env() {
        Ok(client) => client,
        Err(_) => {
            eprintln!("skip: Qdrant retrieval config unavailable");
            return;
        }
    };

    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let event_id = enqueue_delete_by_document(&pool, workspace_id, document_id)
        .await
        .unwrap();

    let result = process_qdrant_outbox(&pool, &retrieval, QdrantOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert!(result.processed_rows >= 1 || result.failed_rows >= 1);

    let status: String = sqlx::query_scalar("SELECT status FROM qdrant_outbox WHERE id = $1")
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    // Local Qdrant reachable → PROCESSED; unreachable → FAILED (retryable).
    assert!(
        status == "PROCESSED" || status == "FAILED",
        "unexpected status={status}"
    );
}

/// LIFE-002: delete filter không match point → cùng durable row PROCESSED và xoá stale error.
#[tokio::test]
async fn qdrant_outbox_empty_delete_marks_same_row_processed_and_clears_error() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    clear_claimable_qdrant_outbox(&pool).await;

    let retrieval = match RetrievalClient::from_env() {
        Ok(client) => client,
        Err(_) => {
            eprintln!("skip: Qdrant retrieval config unavailable");
            return;
        }
    };
    if !qdrant_empty_filter_delete_ready(&retrieval).await {
        eprintln!("skip: Qdrant unreachable or collection missing (LIFE-002 PROCESSED path)");
        return;
    }

    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    // Seed FAILED + error: worker success phải clear error, không tạo row mới.
    let event_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO qdrant_outbox (
            event_type, payload, status, retry_count, error_message, next_attempt_at
        )
        VALUES (
            'delete_by_document',
            jsonb_build_object(
                'workspace_id', $1::text,
                'document_id', $2::text
            ),
            'FAILED',
            1,
            'qdrant_http_error',
            CURRENT_TIMESTAMP
        )
        RETURNING id
        "#,
    )
    .bind(workspace_id.to_string())
    .bind(document_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();

    let result = process_qdrant_outbox(&pool, &retrieval, QdrantOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert_eq!(result.processed_rows, 1);
    assert_eq!(result.failed_rows, 0);
    assert_eq!(result.dead_rows, 0);
    assert_eq!(result.fetched_rows, 1);

    let row: (String, i32, Option<String>) = sqlx::query_as(
        r#"
        SELECT status, retry_count, error_message
        FROM qdrant_outbox
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        row.0, "PROCESSED",
        "empty filter-delete must mark PROCESSED"
    );
    assert_eq!(row.1, 1, "retry_count must stay on the same durable row");
    assert_eq!(
        row.2, None,
        "success must clear stale error_message on the same row"
    );
}

/// LIFE-002: replay worker sau PROCESSED không claim lại / không đổi state durable.
#[tokio::test]
async fn qdrant_outbox_replay_after_processed_does_not_reclaim_row() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    clear_claimable_qdrant_outbox(&pool).await;

    let retrieval = match RetrievalClient::from_env() {
        Ok(client) => client,
        Err(_) => {
            eprintln!("skip: Qdrant retrieval config unavailable");
            return;
        }
    };
    if !qdrant_empty_filter_delete_ready(&retrieval).await {
        eprintln!("skip: Qdrant unreachable or collection missing (LIFE-002 replay path)");
        return;
    }

    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let event_id = enqueue_delete_by_document(&pool, workspace_id, document_id)
        .await
        .unwrap();

    let first = process_qdrant_outbox(&pool, &retrieval, QdrantOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert_eq!(first.fetched_rows, 1);
    assert_eq!(first.processed_rows, 1);
    assert_eq!(first.failed_rows, 0);

    let after_first: (String, i32, Option<String>, String) = sqlx::query_as(
        r#"
        SELECT status, retry_count, error_message, updated_at::text
        FROM qdrant_outbox
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after_first.0, "PROCESSED");
    assert_eq!(after_first.1, 0);
    assert_eq!(after_first.2, None);
    let updated_at_after_first = after_first.3;

    // Replay ngay: claim chỉ lấy PENDING/FAILED → row PROCESSED không được đụng.
    let second = process_qdrant_outbox(&pool, &retrieval, QdrantOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert_eq!(
        second.fetched_rows, 0,
        "PROCESSED row must not be claimed again on replay"
    );
    assert_eq!(second.processed_rows, 0);
    assert_eq!(second.failed_rows, 0);
    assert_eq!(second.dead_rows, 0);

    let after_second: (String, i32, Option<String>, String) = sqlx::query_as(
        r#"
        SELECT status, retry_count, error_message, updated_at::text
        FROM qdrant_outbox
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after_second.0, "PROCESSED");
    assert_eq!(after_second.1, 0);
    assert_eq!(after_second.2, None);
    assert_eq!(
        after_second.3, updated_at_after_first,
        "replay must not mutate durable PROCESSED row (including updated_at)"
    );
}

#[tokio::test]
async fn qdrant_outbox_processor_marks_unreachable_as_failed() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    clear_claimable_qdrant_outbox(&pool).await;

    use gmrag_api::retrieval::RetrievalConfig;
    let broken = RetrievalClient::from_config(RetrievalConfig {
        qdrant_url: "http://127.0.0.1:1".to_string(),
        collection_name: "gmrag_document_chunks".to_string(),
        vector_size: 768,
        top_k: 5,
        api_key: None,
        delete_request_timeout_secs: 2,
        delete_worker_timeout_secs: 2,
    });

    let workspace_id = Uuid::new_v4();
    let event_id = enqueue_delete_by_workspace(&pool, workspace_id)
        .await
        .unwrap();

    let result = process_qdrant_outbox(&pool, &broken, QdrantOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert_eq!(result.failed_rows, 1);

    let row: (String, i32) =
        sqlx::query_as("SELECT status, retry_count FROM qdrant_outbox WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, "FAILED");
    assert_eq!(row.1, 1);

    // Backoff: next_attempt_at phải ở tương lai → lần process ngay sau không claim lại event này.
    let due_now: bool = sqlx::query_scalar(
        r#"
        SELECT next_attempt_at <= CURRENT_TIMESTAMP
        FROM qdrant_outbox
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !due_now,
        "FAILED row must schedule next_attempt_at in the future (backoff)"
    );

    let second = process_qdrant_outbox(&pool, &broken, QdrantOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert_eq!(
        second.fetched_rows, 0,
        "backoff must prevent immediate re-claim of the same failed row"
    );
    let retry_after_second: i32 =
        sqlx::query_scalar("SELECT retry_count FROM qdrant_outbox WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(retry_after_second, 1);
}

#[tokio::test]
async fn qdrant_outbox_poison_invalid_payload_marked_dead() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    clear_claimable_qdrant_outbox(&pool).await;

    use gmrag_api::retrieval::RetrievalConfig;
    let broken = RetrievalClient::from_config(RetrievalConfig {
        qdrant_url: "http://127.0.0.1:1".to_string(),
        collection_name: "gmrag_document_chunks".to_string(),
        vector_size: 768,
        top_k: 5,
        api_key: None,
        delete_request_timeout_secs: 2,
        delete_worker_timeout_secs: 2,
    });

    let event_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO qdrant_outbox (event_type, payload, status, retry_count, next_attempt_at)
        VALUES ('delete_by_document', '{"bad":true}'::jsonb, 'PENDING', 0, CURRENT_TIMESTAMP)
        RETURNING id
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let result = process_qdrant_outbox(&pool, &broken, QdrantOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert_eq!(result.dead_rows, 1);
    assert_eq!(result.failed_rows, 0);

    let row: (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status, retry_count, error_message FROM qdrant_outbox WHERE id = $1",
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "DEAD");
    assert_eq!(row.1, 1);
    assert_eq!(row.2.as_deref(), Some("invalid_payload"));

    // DEAD không được claim lại.
    let again = process_qdrant_outbox(&pool, &broken, QdrantOutboxProcessorConfig::default())
        .await
        .unwrap();
    assert_eq!(again.fetched_rows, 0);
}

#[tokio::test]
async fn qdrant_outbox_exhausted_retries_marked_dead() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    clear_claimable_qdrant_outbox(&pool).await;

    use gmrag_api::outbox::OutboxBackoffConfig;
    use gmrag_api::retrieval::RetrievalConfig;
    let broken = RetrievalClient::from_config(RetrievalConfig {
        qdrant_url: "http://127.0.0.1:1".to_string(),
        collection_name: "gmrag_document_chunks".to_string(),
        vector_size: 768,
        top_k: 5,
        api_key: None,
        delete_request_timeout_secs: 2,
        delete_worker_timeout_secs: 2,
    });

    let workspace_id = Uuid::new_v4();
    // retry_count = max-1: lần fail tiếp theo → DEAD
    let event_id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO qdrant_outbox (event_type, payload, status, retry_count, next_attempt_at)
        VALUES (
            'delete_by_workspace',
            jsonb_build_object('workspace_id', $1::text),
            'FAILED',
            4,
            CURRENT_TIMESTAMP
        )
        RETURNING id
        "#,
    )
    .bind(workspace_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();

    let config = QdrantOutboxProcessorConfig {
        batch_size: 50,
        max_retries: 5,
        backoff: OutboxBackoffConfig {
            base_backoff_secs: 0,
            max_backoff_secs: 0,
            claim_lease_secs: 30,
        },
    };

    let result = process_qdrant_outbox(&pool, &broken, config).await.unwrap();
    assert_eq!(result.dead_rows, 1);

    let status: String = sqlx::query_scalar("SELECT status FROM qdrant_outbox WHERE id = $1")
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "DEAD");
}

#[tokio::test]
async fn qdrant_outbox_claim_skip_locked_is_exclusive() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    clear_claimable_qdrant_outbox(&pool).await;

    let workspace_id = Uuid::new_v4();
    let event_id = enqueue_delete_by_workspace(&pool, workspace_id)
        .await
        .unwrap();

    // Mô phỏng 2 worker: transaction A claim + giữ lock; transaction B không thấy row.
    let mut tx_a = pool.begin().await.unwrap();
    let claimed_a: Vec<(Uuid,)> = sqlx::query_as(
        r#"
        SELECT id
        FROM qdrant_outbox
        WHERE id = $1
          AND status IN ('PENDING', 'FAILED')
          AND next_attempt_at <= CURRENT_TIMESTAMP
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(event_id)
    .fetch_all(&mut *tx_a)
    .await
    .unwrap();
    assert_eq!(claimed_a.len(), 1);

    let mut tx_b = pool.begin().await.unwrap();
    let claimed_b: Vec<(Uuid,)> = sqlx::query_as(
        r#"
        SELECT id
        FROM qdrant_outbox
        WHERE id = $1
          AND status IN ('PENDING', 'FAILED')
          AND next_attempt_at <= CURRENT_TIMESTAMP
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(event_id)
    .fetch_all(&mut *tx_b)
    .await
    .unwrap();
    assert!(
        claimed_b.is_empty(),
        "second worker must skip locked row (no double claim)"
    );

    tx_b.rollback().await.unwrap();
    tx_a.rollback().await.unwrap();
}

#[tokio::test]
async fn qdrant_outbox_backoff_zero_allows_immediate_retry() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    clear_claimable_qdrant_outbox(&pool).await;

    use gmrag_api::outbox::OutboxBackoffConfig;
    use gmrag_api::retrieval::RetrievalConfig;
    let broken = RetrievalClient::from_config(RetrievalConfig {
        qdrant_url: "http://127.0.0.1:1".to_string(),
        collection_name: "gmrag_document_chunks".to_string(),
        vector_size: 768,
        top_k: 5,
        api_key: None,
        delete_request_timeout_secs: 2,
        delete_worker_timeout_secs: 2,
    });

    let workspace_id = Uuid::new_v4();
    let event_id = enqueue_delete_by_workspace(&pool, workspace_id)
        .await
        .unwrap();

    // base/max = 0 → delay 0; claim_lease = 0 để test claim lại ngay ở run sau.
    let config = QdrantOutboxProcessorConfig {
        batch_size: 50,
        max_retries: 5,
        backoff: OutboxBackoffConfig {
            base_backoff_secs: 0,
            max_backoff_secs: 0,
            claim_lease_secs: 0,
        },
    };

    let first = process_qdrant_outbox(&pool, &broken, config).await.unwrap();
    assert_eq!(first.failed_rows, 1);

    let retry_after_first: i32 =
        sqlx::query_scalar("SELECT retry_count FROM qdrant_outbox WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(retry_after_first, 1);

    // claim_lease_secs = 0 + backoff 0: next_attempt_at ≈ now → claim lại được ở run sau.
    let second = process_qdrant_outbox(&pool, &broken, config).await.unwrap();
    assert_eq!(second.failed_rows, 1);

    let retry_count: i32 =
        sqlx::query_scalar("SELECT retry_count FROM qdrant_outbox WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(retry_count, 2);
}

#[tokio::test]
async fn qdrant_cleanup_dry_run_reports_outbox_candidates_without_delete() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;

    use gmrag_api::retrieval::RetrievalConfig;
    let broken = RetrievalClient::from_config(RetrievalConfig {
        qdrant_url: "http://127.0.0.1:1".to_string(),
        collection_name: "gmrag_document_chunks".to_string(),
        vector_size: 768,
        top_k: 5,
        api_key: None,
        delete_request_timeout_secs: 2,
        delete_worker_timeout_secs: 2,
    });

    let workspace_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    enqueue_delete_by_document(&pool, workspace_id, document_id)
        .await
        .unwrap();

    let report = cleanup_qdrant_orphans(
        &pool,
        &broken,
        &QdrantCleanupOptions {
            dry_run: true,
            delete: false,
            workspace_id: None,
            tenant_id: None,
            full_scan: false,
            scroll_page_size: 64,
            force: false,
        },
    )
    .await
    .unwrap();

    assert!(report.dry_run);
    assert!(report.candidates_from_outbox >= 1);
    assert_eq!(report.deletes_attempted, 0);
}

fn broken_retrieval_client() -> RetrievalClient {
    use gmrag_api::retrieval::RetrievalConfig;
    RetrievalClient::from_config(RetrievalConfig {
        qdrant_url: "http://127.0.0.1:1".to_string(),
        collection_name: "gmrag_document_chunks".to_string(),
        vector_size: 768,
        top_k: 5,
        api_key: None,
        delete_request_timeout_secs: 2,
        delete_worker_timeout_secs: 2,
    })
}

#[tokio::test]
async fn qdrant_cleanup_refuses_delete_on_live_workspace_without_force() {
    // Fix High #2: scoped --delete trên workspace còn SQL phải refuse (tránh wipe live vectors).
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let retrieval = broken_retrieval_client();

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("cleanup-live-tenant-{tenant_id}"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("cleanup-live-ws-{workspace_id}"))
        .execute(&pool)
        .await
        .unwrap();

    let err = cleanup_qdrant_orphans(
        &pool,
        &retrieval,
        &QdrantCleanupOptions {
            dry_run: false,
            delete: true,
            workspace_id: Some(workspace_id),
            tenant_id: None,
            full_scan: false,
            scroll_page_size: 64,
            force: false,
        },
    )
    .await
    .expect_err("live workspace --delete without --force must refuse");

    let msg = err.to_string();
    assert!(
        msg.contains("still exists") || msg.contains("refusing"),
        "unexpected error: {msg}"
    );

    // Dry-run trên live workspace vẫn OK (chỉ report).
    let dry = cleanup_qdrant_orphans(
        &pool,
        &retrieval,
        &QdrantCleanupOptions {
            dry_run: true,
            delete: false,
            workspace_id: Some(workspace_id),
            tenant_id: None,
            full_scan: false,
            scroll_page_size: 64,
            force: false,
        },
    )
    .await
    .expect("dry-run on live workspace must be allowed");
    assert!(dry.dry_run);
    assert_eq!(dry.deletes_attempted, 0);

    sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(&pool)
        .await
        .ok();
}

#[tokio::test]
async fn qdrant_cleanup_refuses_delete_on_live_tenant_without_force() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let retrieval = broken_retrieval_client();

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("cleanup-live-tenant2-{tenant_id}"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("cleanup-live-ws2-{workspace_id}"))
        .execute(&pool)
        .await
        .unwrap();

    let err = cleanup_qdrant_orphans(
        &pool,
        &retrieval,
        &QdrantCleanupOptions {
            dry_run: false,
            delete: true,
            workspace_id: None,
            tenant_id: Some(tenant_id),
            full_scan: false,
            scroll_page_size: 64,
            force: false,
        },
    )
    .await
    .expect_err("live tenant --delete without --force must refuse");

    let msg = err.to_string();
    assert!(
        msg.contains("still has") || msg.contains("refusing"),
        "unexpected error: {msg}"
    );

    sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(&pool)
        .await
        .ok();
}

#[tokio::test]
async fn qdrant_cleanup_empty_tenant_is_hard_error() {
    // Fix High #3: tenant cascade / unknown id → không silent no-op success.
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let retrieval = broken_retrieval_client();

    let missing_tenant = Uuid::new_v4();
    let err = cleanup_qdrant_orphans(
        &pool,
        &retrieval,
        &QdrantCleanupOptions {
            dry_run: true,
            delete: false,
            workspace_id: None,
            tenant_id: Some(missing_tenant),
            full_scan: false,
            scroll_page_size: 64,
            force: false,
        },
    )
    .await
    .expect_err("empty tenant workspace list must hard-fail");

    let msg = err.to_string();
    assert!(
        msg.contains("no workspaces found") || msg.contains("already cascaded"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn delete_points_by_tenant_empty_list_hard_fails() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let retrieval = broken_retrieval_client();

    let missing_tenant = Uuid::new_v4();
    let err = retrieval
        .delete_points_by_tenant(&pool, missing_tenant)
        .await
        .expect_err("delete_points_by_tenant must not no-op on empty list");

    assert!(
        matches!(
            err,
            gmrag_api::retrieval::RetrievalError::EmptyTenantWorkspaceList { tenant_id }
            if tenant_id == missing_tenant
        ),
        "got {err:?}"
    );
}

#[tokio::test]
async fn qdrant_cleanup_force_allows_delete_attempt_on_live_workspace() {
    // --force bỏ qua live guard; delete có thể fail Qdrant (broken client) nhưng không bị refuse.
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let retrieval = broken_retrieval_client();

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("cleanup-force-tenant-{tenant_id}"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("cleanup-force-ws-{workspace_id}"))
        .execute(&pool)
        .await
        .unwrap();

    let report = cleanup_qdrant_orphans(
        &pool,
        &retrieval,
        &QdrantCleanupOptions {
            dry_run: false,
            delete: true,
            workspace_id: Some(workspace_id),
            tenant_id: None,
            full_scan: false,
            scroll_page_size: 64,
            force: true,
        },
    )
    .await
    .expect("--force must bypass live workspace refuse");

    assert_eq!(report.deletes_attempted, 1);
    // Broken Qdrant → fail delete + requeue outbox.
    assert_eq!(report.deletes_failed, 1);
    assert_eq!(report.outbox_requeued, 1);

    sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(&pool)
        .await
        .ok();
}

/// OpenFGA revoke lỗi → SQL membership giữ nguyên, không trả 204.
#[tokio::test]
async fn remove_member_openfga_failure_leaves_sql_and_returns_error() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;

    let mock_fga_url = spawn_authz_mock(AuthzMockMode::CheckAllowWriteFail).await;
    let store_id = std::env::var("OPENFGA_STORE_ID").unwrap_or_else(|_| "test-store".into());
    let authz_client = AuthzClient::new(mock_fga_url, store_id, None);

    let (addr, tenant_id, workspace_id, owner_user_id, member_user_id) =
        bootstrap_member_remove_fixture(&pool, authz_client, false).await;

    let response = Client::new()
        .delete(format!(
            "{addr}/workspaces/{workspace_id}/members/{member_user_id}"
        ))
        .bearer_auth(&owner_user_id)
        .send()
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        reqwest::StatusCode::NO_CONTENT,
        "must not report success when OpenFGA revoke fails"
    );
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], "AUTHZ_REVOKE_FAILED");

    let still_member: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM workspace_members WHERE workspace_id = $1 AND user_id = $2)",
    )
    .bind(workspace_id)
    .bind(&member_user_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        still_member,
        "SQL row must remain when OpenFGA revoke fails"
    );

    cleanup_member_remove_fixture(
        &pool,
        tenant_id,
        workspace_id,
        &owner_user_id,
        &member_user_id,
    )
    .await;
}

/// SQL DELETE lỗi sau khi OpenFGA revoke thành công → fail-closed + outbox tuple_write.
#[tokio::test]
async fn remove_member_sql_failure_after_fga_enqueues_tuple_write() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let authz_client = AuthzClient::from_env().unwrap();

    let (addr, tenant_id, workspace_id, owner_user_id, member_user_id) =
        bootstrap_member_remove_fixture(&pool, authz_client.clone(), true).await;

    install_reject_member_delete_trigger(&pool).await;

    let response = Client::new()
        .delete(format!(
            "{addr}/workspaces/{workspace_id}/members/{member_user_id}"
        ))
        .bearer_auth(&owner_user_id)
        .send()
        .await
        .unwrap();

    assert_ne!(response.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], "MEMBER_REMOVE_FAILED");

    let still_member: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM workspace_members WHERE workspace_id = $1 AND user_id = $2)",
    )
    .bind(workspace_id)
    .bind(&member_user_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(still_member);

    let outbox: Option<(String, serde_json::Value)> = sqlx::query_as(
        r#"
        SELECT event_type, payload
        FROM authz_outbox
        WHERE status = 'PENDING'
          AND event_type = 'tuple_write'
          AND payload->>'user' = $1
          AND payload->>'object' = $2
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(format!("user:{member_user_id}"))
    .bind(format!("workspace:{workspace_id}"))
    .fetch_optional(&pool)
    .await
    .unwrap();

    drop_reject_member_delete_trigger(&pool).await;

    let Some((event_type, payload)) = outbox else {
        panic!("expected tuple_write recovery outbox event after SQL failure");
    };
    assert_eq!(event_type, "tuple_write");
    assert_eq!(payload["relation"], json!("member"));

    // FGA đã revoke; member không còn quyền dù SQL vẫn còn row
    let still_allowed = authz_client
        .check_fga(
            &format!("user:{member_user_id}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();
    assert!(
        !still_allowed,
        "OpenFGA access must stay revoked (fail-closed) until recovery write runs"
    );
    let _ = sqlx::query(
        "DELETE FROM authz_outbox WHERE payload->>'user' = $1 AND payload->>'object' = $2",
    )
    .bind(format!("user:{member_user_id}"))
    .bind(format!("workspace:{workspace_id}"))
    .execute(&pool)
    .await;

    // Khôi phục FGA admin/owner tuples cleanup
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
    cleanup_member_remove_fixture(
        &pool,
        tenant_id,
        workspace_id,
        &owner_user_id,
        &member_user_id,
    )
    .await;
}

/// SQL + outbox enqueue đều lỗi → vẫn trả failure, không bao giờ 204.
#[tokio::test]
async fn remove_member_sql_and_outbox_failure_still_returns_error() {
    let _guard = phase3a_test_lock().lock().await;
    init_test_env();
    let pool = setup_pool().await;
    let authz_client = AuthzClient::from_env().unwrap();

    let (addr, tenant_id, workspace_id, owner_user_id, member_user_id) =
        bootstrap_member_remove_fixture(&pool, authz_client.clone(), true).await;

    install_reject_member_delete_trigger(&pool).await;
    install_reject_outbox_insert_trigger(&pool).await;

    let response = Client::new()
        .delete(format!(
            "{addr}/workspaces/{workspace_id}/members/{member_user_id}"
        ))
        .bearer_auth(&owner_user_id)
        .send()
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        reqwest::StatusCode::NO_CONTENT,
        "must never claim removal succeeded when recovery enqueue fails"
    );
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], "MEMBER_REMOVE_FAILED");

    drop_reject_member_delete_trigger(&pool).await;
    drop_reject_outbox_insert_trigger(&pool).await;

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
    // Member FGA may already be revoked
    let _ = authz_client
        .delete_tuple(
            &format!("user:{member_user_id}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await;
    cleanup_member_remove_fixture(
        &pool,
        tenant_id,
        workspace_id,
        &owner_user_id,
        &member_user_id,
    )
    .await;
}

#[derive(Clone, Copy)]
enum AuthzMockMode {
    /// Check luôn allow; Write luôn fail (mô phỏng outage lúc revoke).
    CheckAllowWriteFail,
}

async fn spawn_authz_mock(mode: AuthzMockMode) -> String {
    use axum::http::StatusCode as AxumStatus;
    use axum::response::IntoResponse;
    use axum::{Router, routing::post};

    let router = Router::new()
        .route(
            "/stores/{store_id}/check",
            post(move || async move {
                match mode {
                    AuthzMockMode::CheckAllowWriteFail => {
                        axum::Json(json!({ "allowed": true })).into_response()
                    }
                }
            }),
        )
        .route(
            "/stores/{store_id}/write",
            post(move || async move {
                match mode {
                    AuthzMockMode::CheckAllowWriteFail => {
                        (AxumStatus::SERVICE_UNAVAILABLE, "openfga unavailable").into_response()
                    }
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{}", addr)
}

async fn bootstrap_member_remove_fixture(
    pool: &sqlx::PgPool,
    authz_client: AuthzClient,
    seed_real_fga: bool,
) -> (String, Uuid, Uuid, String, String) {
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
    let owner_user_id = format!("phase3a-rm-owner-{}", Uuid::new_v4());
    let member_user_id = format!("phase3a-rm-member-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Phase3A RM Tenant {tenant_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("Phase3A RM Workspace {workspace_id}"))
        .execute(pool)
        .await
        .unwrap();

    for user_id in [&owner_user_id, &member_user_id] {
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@test.local"))
            .execute(pool)
            .await
            .unwrap();
    }

    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
    )
    .bind(workspace_id)
    .bind(&owner_user_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'MEMBER')",
    )
    .bind(workspace_id)
    .bind(&member_user_id)
    .execute(pool)
    .await
    .unwrap();

    if seed_real_fga {
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
    }

    (addr, tenant_id, workspace_id, owner_user_id, member_user_id)
}

async fn cleanup_member_remove_fixture(
    pool: &sqlx::PgPool,
    tenant_id: Uuid,
    workspace_id: Uuid,
    owner_user_id: &str,
    member_user_id: &str,
) {
    let _ = sqlx::query("DELETE FROM workspace_members WHERE workspace_id = $1")
        .bind(workspace_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(workspace_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1 OR id = $2")
        .bind(owner_user_id)
        .bind(member_user_id)
        .execute(pool)
        .await;
}

/// Trigger tĩnh (sqlx 0.9 cấm dynamic SQL) — phase3a_test_lock đảm bảo không chạy song song.
async fn install_reject_member_delete_trigger(pool: &sqlx::PgPool) {
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION test_reject_member_delete() RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'injected member delete failure for tests';
        END;
        $$ LANGUAGE plpgsql;
        "#,
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query("DROP TRIGGER IF EXISTS test_reject_member_delete ON workspace_members")
        .execute(pool)
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE TRIGGER test_reject_member_delete
        BEFORE DELETE ON workspace_members
        FOR EACH ROW EXECUTE FUNCTION test_reject_member_delete();
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn drop_reject_member_delete_trigger(pool: &sqlx::PgPool) {
    let _ = sqlx::query("DROP TRIGGER IF EXISTS test_reject_member_delete ON workspace_members")
        .execute(pool)
        .await;
    let _ = sqlx::query("DROP FUNCTION IF EXISTS test_reject_member_delete()")
        .execute(pool)
        .await;
}

async fn install_reject_outbox_insert_trigger(pool: &sqlx::PgPool) {
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION test_reject_outbox_insert() RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'injected authz_outbox insert failure for tests';
        END;
        $$ LANGUAGE plpgsql;
        "#,
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query("DROP TRIGGER IF EXISTS test_reject_outbox_insert ON authz_outbox")
        .execute(pool)
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE TRIGGER test_reject_outbox_insert
        BEFORE INSERT ON authz_outbox
        FOR EACH ROW EXECUTE FUNCTION test_reject_outbox_insert();
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn drop_reject_outbox_insert_trigger(pool: &sqlx::PgPool) {
    let _ = sqlx::query("DROP TRIGGER IF EXISTS test_reject_outbox_insert ON authz_outbox")
        .execute(pool)
        .await;
    let _ = sqlx::query("DROP FUNCTION IF EXISTS test_reject_outbox_insert()")
        .execute(pool)
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
