use reqwest::Client;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;
use uuid::Uuid;

use gmrag_api::auth::authz::{Object, Relation};
use gmrag_api::retrieval::{ChunkPoint, RetrievalClient, RetrievalConfig};
use gmrag_api::state::AppState;
use gmrag_api::storage::build_original_document_object_key;

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();

struct TestServer {
    addr: String,
    pool: sqlx::PgPool,
    state: AppState,
}

struct WorkspaceSeed {
    tenant_id: Uuid,
    workspace_id: Uuid,
    admin_user_id: String,
}

struct SeededDocument {
    document_id: Uuid,
    object_key: String,
}

impl TestServer {
    async fn bootstrap() -> Self {
        Self::bootstrap_with_retrieval(None).await
    }

    async fn bootstrap_with_retrieval(retrieval: Option<RetrievalClient>) -> Self {
        dotenvy::dotenv().ok();
        init_test_env();

        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to connect to database");

        match sqlx::migrate!("./migrations").run(&pool).await {
            Ok(_) | Err(sqlx::migrate::MigrateError::VersionMismatch(_)) => {}
            Err(err) => panic!("Failed to run migrations: {err}"),
        }

        let jwt = gmrag_api::auth::jwt::JwtValidator::from_env().unwrap();
        let authz_client = gmrag_api::auth::authz::AuthzClient::from_env().unwrap();
        let keycloak_client = gmrag_api::auth::keycloak::KeycloakClient::from_env().unwrap();
        let storage_config = gmrag_api::storage::StorageConfig::from_env().unwrap();
        let storage = gmrag_api::storage::StorageClient::from_config(storage_config).await;
        let retrieval =
            retrieval.unwrap_or_else(|| RetrievalClient::from_env().expect("retrieval config"));

        let state = AppState {
            pool: pool.clone(),
            jwt,
            storage,
            retrieval,
            ingestion_limiter: Arc::new(Semaphore::new(0)),
            authz_client,
            keycloak_client,
        };

        let app = gmrag_api::app_router(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let addr = format!("http://{}", local_addr);

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self { addr, pool, state }
    }

    async fn seed_workspace_admin(&self) -> WorkspaceSeed {
        let tenant_id = Uuid::new_v4();
        let workspace_id = Uuid::new_v4();
        let admin_user_id = format!("storage-test-{}", Uuid::new_v4());

        sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
            .bind(tenant_id)
            .bind(format!("Storage Test Tenant {tenant_id}"))
            .execute(&self.pool)
            .await
            .unwrap();

        sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
            .bind(workspace_id)
            .bind(tenant_id)
            .bind(format!("Storage Test Workspace {workspace_id}"))
            .execute(&self.pool)
            .await
            .unwrap();

        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(&admin_user_id)
            .bind(format!("{admin_user_id}@test.local"))
            .execute(&self.pool)
            .await
            .unwrap();

        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
        )
        .bind(workspace_id)
        .bind(&admin_user_id)
        .execute(&self.pool)
        .await
        .unwrap();

        self.state
            .authz_client
            .write_tuple(
                &format!("user:{admin_user_id}"),
                Relation::Admin,
                &Object::Workspace(workspace_id),
            )
            .await
            .unwrap();

        WorkspaceSeed {
            tenant_id,
            workspace_id,
            admin_user_id,
        }
    }

    /// Tenant Owner có `can_assign_role` trên workspace (cần cho DELETE /workspaces/{id}).
    async fn seed_tenant_owner_workspace(&self) -> WorkspaceSeed {
        let seed = self.seed_workspace_admin().await;

        sqlx::query(
            "INSERT INTO tenant_members (tenant_id, user_id, role) VALUES ($1, $2, 'OWNER') ON CONFLICT DO NOTHING",
        )
        .bind(seed.tenant_id)
        .bind(&seed.admin_user_id)
        .execute(&self.pool)
        .await
        .unwrap();

        // can_assign_role = owner from tenant; cần cả tuple tenant -> workspace.
        self.state
            .authz_client
            .write_tuple(
                &format!("user:{}", seed.admin_user_id),
                Relation::Owner,
                &Object::Tenant(seed.tenant_id),
            )
            .await
            .unwrap();

        self.state
            .authz_client
            .write_tuple(
                &format!("tenant:{}", seed.tenant_id),
                Relation::Tenant,
                &Object::Workspace(seed.workspace_id),
            )
            .await
            .unwrap();

        seed
    }

    async fn insert_failed_document(
        &self,
        seed: &WorkspaceSeed,
        create_object: bool,
    ) -> SeededDocument {
        let document_id = Uuid::new_v4();
        let object_key =
            build_original_document_object_key(seed.tenant_id, seed.workspace_id, document_id);
        let bytes = sample_pdf_bytes();
        let checksum_sha256 = hex_sha256(&bytes);

        if create_object {
            self.state
                .storage
                .put_original_document(&object_key, &bytes, Some("application/pdf"))
                .await
                .unwrap();
        }

        sqlx::query(
            r#"
            INSERT INTO documents (
                id,
                workspace_id,
                owner_id,
                filename,
                status,
                processing_stage,
                object_key,
                bucket,
                content_type,
                size_bytes,
                checksum_sha256,
                storage_etag,
                uploaded_by
            )
            VALUES ($1, $2, $3, $4, 'FAILED', 'DONE', $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(document_id)
        .bind(seed.workspace_id)
        .bind(&seed.admin_user_id)
        .bind("seeded.pdf")
        .bind(&object_key)
        .bind(self.state.storage.bucket())
        .bind("application/pdf")
        .bind(i64::try_from(bytes.len()).unwrap_or(i64::MAX))
        .bind(&checksum_sha256)
        .bind(Option::<&str>::None)
        .bind(&seed.admin_user_id)
        .execute(&self.pool)
        .await
        .unwrap();

        SeededDocument {
            document_id,
            object_key,
        }
    }
}

#[tokio::test]
async fn upload_pdf_persists_object_and_metadata() {
    let server = TestServer::bootstrap().await;
    let seed = server.seed_workspace_admin().await;

    let client = Client::new();
    let file_bytes = sample_pdf_bytes();
    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(file_bytes.clone())
            .file_name("contract.pdf")
            .mime_str("application/pdf")
            .unwrap(),
    );

    let response = client
        .post(format!(
            "{}/workspaces/{}/documents/upload",
            server.addr, seed.workspace_id
        ))
        .bearer_auth(&seed.admin_user_id)
        .multipart(form)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let payload: serde_json::Value = response.json().await.unwrap();
    let document_id =
        Uuid::parse_str(payload["documents"][0]["document_id"].as_str().unwrap()).unwrap();

    let row: (String, String, Option<i64>, Option<String>, String) = sqlx::query_as(
        r#"
        SELECT object_key, bucket, size_bytes, checksum_sha256, access_mode
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(seed.workspace_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();

    assert!(row.0.starts_with("tenants/"));
    assert_eq!(row.1, std::env::var("S3_BUCKET").unwrap());
    assert_eq!(row.2, Some(i64::try_from(file_bytes.len()).unwrap()));
    let expected_checksum = hex_sha256(&file_bytes);
    assert_eq!(row.3.as_deref(), Some(expected_checksum.as_str()));
    assert_eq!(row.4, "workspace_default");

    let exists = server.state.storage.object_exists(&row.0).await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn upload_accepts_access_mode_restricted() {
    let server = TestServer::bootstrap().await;
    let seed = server.seed_workspace_admin().await;

    let form = reqwest::multipart::Form::new()
        .text("access_mode", "restricted")
        .part(
            "file",
            reqwest::multipart::Part::bytes(sample_pdf_bytes())
                .file_name("secret.pdf")
                .mime_str("application/pdf")
                .unwrap(),
        );

    let response = Client::new()
        .post(format!(
            "{}/workspaces/{}/documents/upload",
            server.addr, seed.workspace_id
        ))
        .bearer_auth(&seed.admin_user_id)
        .multipart(form)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let payload: serde_json::Value = response.json().await.unwrap();
    let document_id =
        Uuid::parse_str(payload["documents"][0]["document_id"].as_str().unwrap()).unwrap();

    let access_mode: String = sqlx::query_scalar(
        "SELECT access_mode FROM documents WHERE id = $1 AND workspace_id = $2",
    )
    .bind(document_id)
    .bind(seed.workspace_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(access_mode, "restricted");
}

#[tokio::test]
async fn upload_rejects_invalid_access_mode() {
    let server = TestServer::bootstrap().await;
    let seed = server.seed_workspace_admin().await;

    let form = reqwest::multipart::Form::new()
        .text("access_mode", "private")
        .part(
            "file",
            reqwest::multipart::Part::bytes(sample_pdf_bytes())
                .file_name("bad-mode.pdf")
                .mime_str("application/pdf")
                .unwrap(),
        );

    let response = Client::new()
        .post(format!(
            "{}/workspaces/{}/documents/upload",
            server.addr, seed.workspace_id
        ))
        .bearer_auth(&seed.admin_user_id)
        .multipart(form)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], "INVALID_ACCESS_MODE");
}

#[tokio::test]
async fn retry_document_reads_from_object_storage_without_local_file() {
    let server = TestServer::bootstrap().await;
    let seed = server.seed_workspace_admin().await;
    let seeded_document = server.insert_failed_document(&seed, true).await;

    let response = Client::new()
        .post(format!(
            "{}/workspaces/{}/documents/{}/retry",
            server.addr, seed.workspace_id, seeded_document.document_id
        ))
        .bearer_auth(&seed.admin_user_id)
        .json(&json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);

    let row: (String, String) = sqlx::query_as(
        r#"
        SELECT status, processing_stage
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(seeded_document.document_id)
    .bind(seed.workspace_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();

    assert_eq!(row.0, "PROCESSING");
    assert_eq!(row.1, "QUEUED");
}

#[tokio::test]
async fn retry_document_returns_document_object_missing_when_object_absent() {
    let server = TestServer::bootstrap().await;
    let seed = server.seed_workspace_admin().await;
    let seeded_document = server.insert_failed_document(&seed, false).await;

    let response = Client::new()
        .post(format!(
            "{}/workspaces/{}/documents/{}/retry",
            server.addr, seed.workspace_id, seeded_document.document_id
        ))
        .bearer_auth(&seed.admin_user_id)
        .json(&json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::GONE);
    let payload: serde_json::Value = response.json().await.unwrap();
    assert_eq!(payload["error"]["code"], "DOCUMENT_OBJECT_MISSING");
}

#[tokio::test]
async fn delete_document_removes_db_row_and_storage_object() {
    let server = TestServer::bootstrap().await;
    let seed = server.seed_workspace_admin().await;
    let seeded_document = server.insert_failed_document(&seed, true).await;

    let exists_before = server
        .state
        .storage
        .object_exists(&seeded_document.object_key)
        .await
        .unwrap();
    assert!(exists_before);

    let response = Client::new()
        .delete(format!(
            "{}/workspaces/{}/documents/{}",
            server.addr, seed.workspace_id, seeded_document.document_id
        ))
        .bearer_auth(&seed.admin_user_id)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let exists_after_db: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM documents WHERE id = $1 AND workspace_id = $2)",
    )
    .bind(seeded_document.document_id)
    .bind(seed.workspace_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert!(!exists_after_db);

    let exists_after_object = server
        .state
        .storage
        .object_exists(&seeded_document.object_key)
        .await
        .unwrap();
    assert!(!exists_after_object);
}

#[tokio::test]
async fn delete_document_also_removes_qdrant_points() {
    let server = TestServer::bootstrap().await;
    let seed = server.seed_workspace_admin().await;
    let seeded_document = server.insert_failed_document(&seed, true).await;

    let chunk_id = Uuid::new_v4();
    let vector_size = std::env::var("QDRANT_VECTOR_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(768);
    let embedding = vec![0.02_f32; vector_size];

    server
        .state
        .retrieval
        .upsert_chunk_points(&[ChunkPoint {
            chunk_id,
            workspace_id: seed.workspace_id,
            document_id: seeded_document.document_id,
            chunk_index: 0,
            embedding: embedding.clone(),
        }])
        .await
        .expect("seed Qdrant points before document delete");

    let before = server
        .state
        .retrieval
        .search_chunk_ids(
            seed.workspace_id,
            &[seeded_document.document_id],
            &embedding,
            5,
        )
        .await
        .expect("search before delete");
    assert!(
        before.contains(&chunk_id),
        "seeded point must be searchable before delete"
    );

    let response = Client::new()
        .delete(format!(
            "{}/workspaces/{}/documents/{}",
            server.addr, seed.workspace_id, seeded_document.document_id
        ))
        .bearer_auth(&seed.admin_user_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let after = server
        .state
        .retrieval
        .search_chunk_ids(
            seed.workspace_id,
            &[seeded_document.document_id],
            &embedding,
            5,
        )
        .await
        .expect("search after delete");
    assert!(
        after.is_empty(),
        "Qdrant points for deleted document must be removed"
    );
}

#[tokio::test]
async fn delete_document_succeeds_when_qdrant_unavailable() {
    // Retrieval client trỏ tới port chết — SQL/storage delete vẫn phải thành công.
    let broken_retrieval = RetrievalClient::from_config(RetrievalConfig {
        qdrant_url: "http://127.0.0.1:1".to_string(),
        collection_name: "gmrag_document_chunks".to_string(),
        vector_size: 768,
        top_k: 5,
        api_key: None,
        delete_request_timeout_secs: 5,
        delete_worker_timeout_secs: 5,
    });
    let server = TestServer::bootstrap_with_retrieval(Some(broken_retrieval)).await;
    let seed = server.seed_workspace_admin().await;
    let seeded_document = server.insert_failed_document(&seed, true).await;

    let response = Client::new()
        .delete(format!(
            "{}/workspaces/{}/documents/{}",
            server.addr, seed.workspace_id, seeded_document.document_id
        ))
        .bearer_auth(&seed.admin_user_id)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let exists_after_db: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM documents WHERE id = $1 AND workspace_id = $2)",
    )
    .bind(seeded_document.document_id)
    .bind(seed.workspace_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert!(!exists_after_db);

    let exists_after_object = server
        .state
        .storage
        .object_exists(&seeded_document.object_key)
        .await
        .unwrap();
    assert!(!exists_after_object);

    let audit_metadata: Option<serde_json::Value> = sqlx::query_scalar(
        r#"
        SELECT metadata
        FROM audit_events
        WHERE event_type = 'document_deleted'
          AND document_id = $1
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(seeded_document.document_id)
    .fetch_optional(&server.pool)
    .await
    .unwrap();

    if let Some(metadata) = audit_metadata {
        assert_eq!(metadata["qdrant_delete_succeeded"], false);
        assert_eq!(metadata["storage_delete_succeeded"], true);
    }

    // Fail path phải enqueue recovery — không để orphan vĩnh viễn.
    let outbox_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM qdrant_outbox
        WHERE event_type = 'delete_by_document'
          AND status = 'PENDING'
          AND payload->>'document_id' = $1
        "#,
    )
    .bind(seeded_document.document_id.to_string())
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(outbox_count, 1, "document delete failure must enqueue qdrant_outbox");
}

#[tokio::test]
async fn delete_document_short_circuits_when_qdrant_hangs_and_enqueues_outbox() {
    // Fix High #1 (integration): request timeout ngắn → HTTP 204 nhanh, enqueue recovery.
    use std::time::Instant;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hang_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                drop(stream);
            });
        }
    });

    let hanging_retrieval = RetrievalClient::from_config(RetrievalConfig {
        qdrant_url: format!("http://{hang_addr}"),
        collection_name: "gmrag_document_chunks".to_string(),
        vector_size: 768,
        top_k: 5,
        api_key: None,
        delete_request_timeout_secs: 1,
        delete_worker_timeout_secs: 30,
    });
    let server = TestServer::bootstrap_with_retrieval(Some(hanging_retrieval)).await;
    let seed = server.seed_workspace_admin().await;
    let seeded_document = server.insert_failed_document(&seed, true).await;

    let started = Instant::now();
    let response = Client::new()
        .delete(format!(
            "{}/workspaces/{}/documents/{}",
            server.addr, seed.workspace_id, seeded_document.document_id
        ))
        .bearer_auth(&seed.admin_user_id)
        .send()
        .await
        .unwrap();
    let elapsed = started.elapsed();

    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    assert!(
        elapsed.as_secs_f64() < 8.0,
        "HTTP delete must not wait worker timeout when Qdrant hangs; elapsed={elapsed:?}"
    );

    let outbox_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM qdrant_outbox
        WHERE event_type = 'delete_by_document'
          AND status = 'PENDING'
          AND payload->>'document_id' = $1
        "#,
    )
    .bind(seeded_document.document_id.to_string())
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(
        outbox_count, 1,
        "timeout on request path must enqueue qdrant_outbox"
    );
}

#[tokio::test]
async fn delete_workspace_also_removes_qdrant_points() {
    let server = TestServer::bootstrap().await;
    let seed = server.seed_tenant_owner_workspace().await;

    let document_id = Uuid::new_v4();
    let chunk_id = Uuid::new_v4();
    let other_workspace_id = Uuid::new_v4();
    let other_document_id = Uuid::new_v4();
    let other_chunk_id = Uuid::new_v4();
    let vector_size = std::env::var("QDRANT_VECTOR_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(768);
    let embedding = vec![0.03_f32; vector_size];

    server
        .state
        .retrieval
        .upsert_chunk_points(&[
            ChunkPoint {
                chunk_id,
                workspace_id: seed.workspace_id,
                document_id,
                chunk_index: 0,
                embedding: embedding.clone(),
            },
            // Point workspace khác — không được bị xoá khi xoá seed.workspace_id.
            ChunkPoint {
                chunk_id: other_chunk_id,
                workspace_id: other_workspace_id,
                document_id: other_document_id,
                chunk_index: 0,
                embedding: embedding.clone(),
            },
        ])
        .await
        .expect("seed Qdrant points before workspace delete");

    let before = server
        .state
        .retrieval
        .search_chunk_ids(seed.workspace_id, &[document_id], &embedding, 5)
        .await
        .expect("search before workspace delete");
    assert!(
        before.contains(&chunk_id),
        "seeded point must be searchable before workspace delete"
    );

    let response = Client::new()
        .delete(format!("{}/workspaces/{}", server.addr, seed.workspace_id))
        .bearer_auth(&seed.admin_user_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let exists_after_db: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspaces WHERE id = $1)")
            .bind(seed.workspace_id)
            .fetch_one(&server.pool)
            .await
            .unwrap();
    assert!(!exists_after_db);

    let after = server
        .state
        .retrieval
        .search_chunk_ids(seed.workspace_id, &[document_id], &embedding, 5)
        .await
        .expect("search after workspace delete");
    assert!(
        after.is_empty(),
        "Qdrant points for deleted workspace must be removed"
    );

    let other_after = server
        .state
        .retrieval
        .search_chunk_ids(other_workspace_id, &[other_document_id], &embedding, 5)
        .await
        .expect("search other workspace after delete");
    assert!(
        other_after.contains(&other_chunk_id),
        "points outside deleted workspace must remain"
    );

    let audit_metadata: Option<serde_json::Value> = sqlx::query_scalar(
        r#"
        SELECT metadata
        FROM audit_events
        WHERE event_type = 'workspace_deleted'
          AND workspace_id = $1
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(seed.workspace_id)
    .fetch_optional(&server.pool)
    .await
    .unwrap();

    if let Some(metadata) = audit_metadata {
        assert_eq!(metadata["qdrant_workspace_delete_succeeded"], true);
    }
}

#[tokio::test]
async fn delete_workspace_succeeds_when_qdrant_unavailable() {
    let broken_retrieval = RetrievalClient::from_config(RetrievalConfig {
        qdrant_url: "http://127.0.0.1:1".to_string(),
        collection_name: "gmrag_document_chunks".to_string(),
        vector_size: 768,
        top_k: 5,
        api_key: None,
        delete_request_timeout_secs: 5,
        delete_worker_timeout_secs: 5,
    });
    let server = TestServer::bootstrap_with_retrieval(Some(broken_retrieval)).await;
    let seed = server.seed_tenant_owner_workspace().await;

    let response = Client::new()
        .delete(format!("{}/workspaces/{}", server.addr, seed.workspace_id))
        .bearer_auth(&seed.admin_user_id)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let exists_after_db: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspaces WHERE id = $1)")
            .bind(seed.workspace_id)
            .fetch_one(&server.pool)
            .await
            .unwrap();
    assert!(!exists_after_db);

    let audit_metadata: Option<serde_json::Value> = sqlx::query_scalar(
        r#"
        SELECT metadata
        FROM audit_events
        WHERE event_type = 'workspace_deleted'
          AND workspace_id = $1
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(seed.workspace_id)
    .fetch_optional(&server.pool)
    .await
    .unwrap();

    if let Some(metadata) = audit_metadata {
        assert_eq!(metadata["qdrant_workspace_delete_succeeded"], false);
    }

    let outbox_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM qdrant_outbox
        WHERE event_type = 'delete_by_workspace'
          AND status = 'PENDING'
          AND payload->>'workspace_id' = $1
        "#,
    )
    .bind(seed.workspace_id.to_string())
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(outbox_count, 1, "workspace delete failure must enqueue qdrant_outbox");
}

#[tokio::test]
async fn delete_workspace_is_idempotent_when_no_qdrant_points() {
    let server = TestServer::bootstrap().await;
    let seed = server.seed_tenant_owner_workspace().await;

    // Không seed point — delete filter vẫn best-effort Ok và HTTP 204.
    let response = Client::new()
        .delete(format!("{}/workspaces/{}", server.addr, seed.workspace_id))
        .bearer_auth(&seed.admin_user_id)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let exists_after_db: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspaces WHERE id = $1)")
            .bind(seed.workspace_id)
            .fetch_one(&server.pool)
            .await
            .unwrap();
    assert!(!exists_after_db);

    let audit_metadata: Option<serde_json::Value> = sqlx::query_scalar(
        r#"
        SELECT metadata
        FROM audit_events
        WHERE event_type = 'workspace_deleted'
          AND workspace_id = $1
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(seed.workspace_id)
    .fetch_optional(&server.pool)
    .await
    .unwrap();

    if let Some(metadata) = audit_metadata {
        // Collection có thể chưa tồn tại: Qdrant thường vẫn 2xx cho filter delete empty,
        // hoặc trả lỗi API — dù vậy HTTP delete workspace đã thành công.
        assert!(metadata.get("qdrant_workspace_delete_succeeded").is_some());
    }
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

fn sample_pdf_bytes() -> Vec<u8> {
    b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\ntrailer\n<< /Root 1 0 R >>\n%%EOF".to_vec()
}

fn hex_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
