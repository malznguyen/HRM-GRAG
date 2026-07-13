use std::{fs, process::Command, sync::OnceLock};

use gmrag_api::{
    auth::authz::{AuthzClient, TupleKey},
    tenant_cleanup::{
        OperatorTenantDeleteResult, capture_operator_tenant_delete_impact,
        execute_operator_tenant_delete, find_tenants_by_exact_name,
    },
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();

struct Seed {
    tenant_id: Uuid,
    owner_id: String,
    viewer_id: String,
    workspace_ids: Vec<Uuid>,
    tuples: Vec<TupleKey>,
}

fn init_test_env() {
    TEST_ENV_INIT.get_or_init(|| {
        dotenvy::dotenv().ok();
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

fn authz_or_skip() -> Option<AuthzClient> {
    AuthzClient::from_env().ok()
}

async fn seed_tenant(pool: &PgPool, authz: &AuthzClient) -> Seed {
    let tenant_id = Uuid::new_v4();
    let owner_id = format!("delete-tenant-owner-{tenant_id}");
    let viewer_id = format!("delete-tenant-viewer-{tenant_id}");
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2), ($3, $4)")
        .bind(&owner_id)
        .bind(format!("{owner_id}@test.local"))
        .bind(&viewer_id)
        .bind(format!("{viewer_id}@test.local"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("delete-tenant-{tenant_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenant_members (tenant_id, user_id, role) VALUES ($1, $2, 'OWNER')")
        .bind(tenant_id)
        .bind(&owner_id)
        .execute(pool)
        .await
        .unwrap();

    let mut workspace_ids = Vec::new();
    let mut tuples = vec![
        TupleKey {
            user: "platform:system".to_string(),
            relation: "platform".to_string(),
            object: format!("tenant:{tenant_id}"),
        },
        TupleKey {
            user: format!("user:{owner_id}"),
            relation: "owner".to_string(),
            object: format!("tenant:{tenant_id}"),
        },
    ];

    for index in 0..2 {
        let workspace_id = Uuid::new_v4();
        let document_id = Uuid::new_v4();
        workspace_ids.push(workspace_id);
        sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
            .bind(workspace_id)
            .bind(tenant_id)
            .bind(format!("delete-tenant-workspace-{index}"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
        )
        .bind(workspace_id)
        .bind(&owner_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO documents (
                id, workspace_id, owner_id, filename, status, processing_stage,
                object_key, bucket, uploaded_by
            ) VALUES ($1, $2, $3, 'delete-tenant.pdf', 'COMPLETED', 'DONE', $4, 'gmrag-documents', $3)
            "#,
        )
        .bind(document_id)
        .bind(workspace_id)
        .bind(&owner_id)
        .bind(format!(
            "tenants/{tenant_id}/workspaces/{workspace_id}/documents/{document_id}/original.pdf"
        ))
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO document_chunks (document_id, workspace_id, chunk_index, original_text) VALUES ($1, $2, 0, 'test')",
        )
        .bind(document_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO graph_nodes (workspace_id, entity_name) VALUES ($1, $2)")
            .bind(workspace_id)
            .bind(format!("delete-tenant-node-{index}"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO chat_sessions (workspace_id, user_id, title) VALUES ($1, $2, 'test')",
        )
        .bind(workspace_id)
        .bind(&owner_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO document_shares (document_id, user_id) VALUES ($1, $2)")
            .bind(document_id)
            .bind(&viewer_id)
            .execute(pool)
            .await
            .unwrap();
        tuples.extend([
            TupleKey {
                user: format!("tenant:{tenant_id}"),
                relation: "tenant".to_string(),
                object: format!("workspace:{workspace_id}"),
            },
            TupleKey {
                user: format!("user:{owner_id}"),
                relation: "admin".to_string(),
                object: format!("workspace:{workspace_id}"),
            },
            TupleKey {
                user: format!("workspace:{workspace_id}"),
                relation: "workspace".to_string(),
                object: format!("document:{document_id}"),
            },
            TupleKey {
                user: format!("user:{viewer_id}"),
                relation: "explicit_viewer".to_string(),
                object: format!("document:{document_id}"),
            },
        ]);
    }
    authz
        .write_tuples(tuples.clone(), Vec::new())
        .await
        .unwrap();

    Seed {
        tenant_id,
        owner_id,
        viewer_id,
        workspace_ids,
        tuples,
    }
}

async fn cleanup_seed(pool: &PgPool, authz: &AuthzClient, seed: &Seed) {
    let _ = authz.write_tuples(Vec::new(), seed.tuples.clone()).await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(seed.tenant_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = ANY($1::text[])")
        .bind(vec![seed.owner_id.clone(), seed.viewer_id.clone()])
        .execute(pool)
        .await;
}

#[tokio::test]
async fn dry_run_reports_full_impact_without_mutating_sql_or_openfga() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let Some(authz) = authz_or_skip() else {
        eprintln!("skip: OPENFGA_STORE_ID unavailable");
        return;
    };
    let seed = seed_tenant(&pool, &authz).await;

    let impact =
        capture_operator_tenant_delete_impact(&pool, &authz, seed.tenant_id, "gmrag-documents")
            .await
            .unwrap()
            .expect("seeded tenant must be found");

    assert_eq!(impact.owner_emails.len(), 1);
    assert_eq!(impact.workspaces.len(), 2);
    assert_eq!(impact.document_count, 2);
    assert_eq!(impact.chunk_count, 2);
    assert_eq!(impact.graph_node_count, 2);
    assert_eq!(impact.chat_session_count, 2);
    assert_eq!(impact.openfga_tuples.len(), seed.tuples.len());
    let tenant_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE id = $1)")
            .bind(seed.tenant_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(tenant_exists);
    let remaining_tuples = authz.list_all_tuples().await.unwrap();
    assert!(
        seed.tuples
            .iter()
            .all(|tuple| remaining_tuples.contains(tuple))
    );

    cleanup_seed(&pool, &authz, &seed).await;
}

#[tokio::test]
async fn delete_commits_outboxes_audit_and_removes_tenant_subtree_tuples() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let Some(authz) = authz_or_skip() else {
        eprintln!("skip: OPENFGA_STORE_ID unavailable");
        return;
    };
    let seed = seed_tenant(&pool, &authz).await;
    let recovery_dir = std::env::temp_dir().join(format!("delete-tenant-test-{}", Uuid::new_v4()));
    fs::create_dir_all(&recovery_dir).unwrap();

    let result = execute_operator_tenant_delete(
        &pool,
        &authz,
        seed.tenant_id,
        "gmrag-documents",
        "integration-test",
        &recovery_dir,
    )
    .await
    .unwrap();
    let OperatorTenantDeleteResult::Deleted {
        recovery_file,
        qdrant_outbox_id,
        storage_outbox_id,
        ..
    } = result
    else {
        panic!("seeded tenant must be deleted");
    };

    assert!(recovery_file.exists());
    let recovery: serde_json::Value =
        serde_json::from_slice(&fs::read(&recovery_file).unwrap()).unwrap();
    assert_eq!(
        recovery["openfga_tuples"].as_array().map(Vec::len),
        Some(seed.tuples.len())
    );
    let tenant_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE id = $1)")
            .bind(seed.tenant_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!tenant_exists);
    let qdrant_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT jsonb_array_elements_text(payload->'workspace_ids')::uuid FROM qdrant_outbox WHERE id = $1",
    )
    .bind(qdrant_outbox_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(qdrant_ids, seed.workspace_ids);
    let storage_prefix: String =
        sqlx::query_scalar("SELECT payload->>'prefix' FROM storage_outbox WHERE id = $1")
            .bind(storage_outbox_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(storage_prefix, format!("tenants/{}/", seed.tenant_id));
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit_events WHERE event_type = 'tenant_deleted' AND tenant_id = $1",
    )
    .bind(seed.tenant_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audit_count, 1);
    let remaining_tuples = authz.list_all_tuples().await.unwrap();
    assert!(
        seed.tuples
            .iter()
            .all(|tuple| !remaining_tuples.contains(tuple))
    );

    let rerun = execute_operator_tenant_delete(
        &pool,
        &authz,
        seed.tenant_id,
        "gmrag-documents",
        "integration-test",
        &recovery_dir,
    )
    .await
    .unwrap();
    assert!(matches!(rerun, OperatorTenantDeleteResult::NotFound));

    let _ = sqlx::query("DELETE FROM qdrant_outbox WHERE id = $1")
        .bind(qdrant_outbox_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM storage_outbox WHERE id = $1")
        .bind(storage_outbox_id)
        .execute(&pool)
        .await;
    let _ = fs::remove_dir_all(&recovery_dir);
    cleanup_seed(&pool, &authz, &seed).await;
}

#[tokio::test]
async fn duplicate_exact_name_is_ambiguous_without_mutation() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let name = format!("delete-tenant-duplicate-{}", Uuid::new_v4());
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2), ($3, $2)")
        .bind(first)
        .bind(&name)
        .bind(second)
        .execute(&pool)
        .await
        .unwrap();

    let matches = find_tenants_by_exact_name(&pool, &name).await.unwrap();
    assert_eq!(matches.len(), 2);
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM tenants WHERE name = $1")
        .bind(&name)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 2);

    let _ = sqlx::query("DELETE FROM tenants WHERE id = ANY($1::uuid[])")
        .bind(vec![first, second])
        .execute(&pool)
        .await;
}

#[tokio::test]
async fn cli_sql_failure_after_openfga_removal_writes_recovery_file_without_authz_outbox() {
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip: DATABASE_URL unavailable");
        return;
    };
    let Some(authz) = authz_or_skip() else {
        eprintln!("skip: OPENFGA_STORE_ID unavailable");
        return;
    };
    let seed = seed_tenant(&pool, &authz).await;
    let authz_outbox_before: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM authz_outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    let recovery_dir =
        std::env::temp_dir().join(format!("delete-tenant-failure-{}", Uuid::new_v4()));
    fs::create_dir_all(&recovery_dir).unwrap();

    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION test_delete_tenant_failure() RETURNS trigger AS $$
        BEGIN
            IF OLD.id = current_setting('gmrag.test_delete_tenant_id', true)::uuid THEN
                RAISE EXCEPTION 'test tenant delete failure';
            END IF;
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER test_delete_tenant_failure BEFORE DELETE ON tenants FOR EACH ROW EXECUTE FUNCTION test_delete_tenant_failure()")
        .execute(&pool)
        .await
        .unwrap();

    let binary = env!("CARGO_BIN_EXE_delete-tenant");
    let output = Command::new(binary)
        .current_dir(&recovery_dir)
        .env(
            "PGOPTIONS",
            format!("-c gmrag.test_delete_tenant_id={}", seed.tenant_id),
        )
        .args([
            "--tenant-id",
            &seed.tenant_id.to_string(),
            "--delete",
            "--yes",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("DANGER"));
    assert!(stderr.contains("Recovery file"));

    let recovery_file = fs::read_dir(&recovery_dir)
        .unwrap()
        .find_map(|entry| {
            let path = entry.ok()?.path();
            path.file_name()?
                .to_string_lossy()
                .starts_with("tenant-delete-recovery-")
                .then_some(path)
        })
        .expect("failure path must write a recovery file");
    let recovery: serde_json::Value =
        serde_json::from_slice(&fs::read(&recovery_file).unwrap()).unwrap();
    assert_eq!(
        recovery["openfga_tuples"].as_array().map(Vec::len),
        Some(seed.tuples.len())
    );
    let authz_outbox_after: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM authz_outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(authz_outbox_after, authz_outbox_before);
    let remaining_tuples = authz.list_all_tuples().await.unwrap();
    assert!(
        seed.tuples
            .iter()
            .all(|tuple| !remaining_tuples.contains(tuple))
    );

    let _ = sqlx::query("DROP TRIGGER IF EXISTS test_delete_tenant_failure ON tenants")
        .execute(&pool)
        .await;
    let _ = sqlx::query("DROP FUNCTION IF EXISTS test_delete_tenant_failure()")
        .execute(&pool)
        .await;
    let _ = fs::remove_dir_all(&recovery_dir);
    cleanup_seed(&pool, &authz, &seed).await;
}
