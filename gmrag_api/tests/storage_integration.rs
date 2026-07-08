use reqwest::Client;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;
use uuid::Uuid;

use gmrag_api::auth::authz::{Object, Relation};
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
        let retrieval = gmrag_api::retrieval::RetrievalClient::from_env().unwrap();

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

    let row: (String, String, Option<i64>, Option<String>) = sqlx::query_as(
        r#"
        SELECT object_key, bucket, size_bytes, checksum_sha256
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

    let exists = server.state.storage.object_exists(&row.0).await.unwrap();
    assert!(exists);
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
