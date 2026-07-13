use std::{
    process::Command,
    sync::{Arc, OnceLock},
};

use gmrag_api::{
    auth::authz::AuthzClient,
    authz_orphan_report::run_authz_orphan_report,
    retrieval::RetrievalClient,
    state::AppState,
    storage::{StorageClient, StorageConfig},
};
use lopdf::{Document, Object, dictionary};
use reqwest::Client;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Semaphore;
use uuid::Uuid;

static TEST_ENV_INIT: OnceLock<()> = OnceLock::new();
const PLATFORM_ADMIN_ID: &str = "authz-orphan-invariant-platform-admin";
const OWNER_ID: &str = "verified-keycloak-owner-uuid";
const ADMIN_ID: &str = "test-workspace-admin-id";
const MEMBER_ID: &str = "test-workspace-member-id";

struct Fixture {
    addr: String,
    pool: sqlx::PgPool,
    authz: AuthzClient,
    tenant_id: Uuid,
    workspace_id: Uuid,
    document_delete_id: Uuid,
    workspace_delete_id: Uuid,
}

#[tokio::test]
async fn delete_mutations_leave_no_authz_orphans() {
    let fixture = Fixture::bootstrap().await;
    let client = Client::new();

    assert_status(
        client
            .delete(format!(
                "{}/workspaces/{}/documents/{}/shares/{}",
                fixture.addr, fixture.workspace_id, fixture.document_delete_id, MEMBER_ID
            ))
            .bearer_auth(OWNER_ID)
            .send()
            .await
            .unwrap(),
        reqwest::StatusCode::NO_CONTENT,
    );
    assert_status(
        client
            .post(format!(
                "{}/workspaces/{}/documents/{}/shares/{}",
                fixture.addr, fixture.workspace_id, fixture.document_delete_id, ADMIN_ID
            ))
            .bearer_auth(OWNER_ID)
            .send()
            .await
            .unwrap(),
        reqwest::StatusCode::CREATED,
    );
    assert_status(
        client
            .patch(format!(
                "{}/workspaces/{}/documents/{}/access-mode",
                fixture.addr, fixture.workspace_id, fixture.document_delete_id
            ))
            .bearer_auth(OWNER_ID)
            .json(&serde_json::json!({ "access_mode": "workspace_default" }))
            .send()
            .await
            .unwrap(),
        reqwest::StatusCode::NO_CONTENT,
    );
    assert_status(
        client
            .delete(format!(
                "{}/workspaces/{}/members/{}",
                fixture.addr, fixture.workspace_id, MEMBER_ID
            ))
            .bearer_auth(OWNER_ID)
            .send()
            .await
            .unwrap(),
        reqwest::StatusCode::NO_CONTENT,
    );
    assert_status(
        client
            .delete(format!(
                "{}/workspaces/{}/documents/{}",
                fixture.addr, fixture.workspace_id, fixture.document_delete_id
            ))
            .bearer_auth(OWNER_ID)
            .send()
            .await
            .unwrap(),
        reqwest::StatusCode::NO_CONTENT,
    );
    assert_no_fixture_orphans(&fixture).await;

    assert_status(
        client
            .delete(format!(
                "{}/workspaces/{}",
                fixture.addr, fixture.workspace_id
            ))
            .bearer_auth(OWNER_ID)
            .send()
            .await
            .unwrap(),
        reqwest::StatusCode::NO_CONTENT,
    );
    assert_no_fixture_orphans(&fixture).await;
}

impl Fixture {
    async fn bootstrap() -> Self {
        dotenvy::dotenv().ok();
        init_test_env();
        ensure_platform_admin();

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&std::env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();
        match sqlx::migrate!("./migrations").run(&pool).await {
            Ok(()) | Err(sqlx::migrate::MigrateError::VersionMismatch(_)) => {}
            Err(error) => panic!("migration failed: {error}"),
        }

        let authz = AuthzClient::from_env().unwrap();
        let state = AppState {
            pool: pool.clone(),
            jwt: gmrag_api::auth::jwt::JwtValidator::from_env().unwrap(),
            storage: StorageClient::from_config(StorageConfig::from_env().unwrap()).await,
            retrieval: RetrievalClient::from_env().unwrap(),
            ingestion_limiter: Arc::new(Semaphore::new(0)),
            authz_client: authz.clone(),
            keycloak_client: gmrag_api::auth::keycloak::KeycloakClient::from_env().unwrap(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, gmrag_api::app_router(state))
                .await
                .unwrap();
        });

        let client = Client::new();
        let tenant_id = create_tenant(&client, &addr).await;
        add_tenant_owner(&client, &addr, tenant_id).await;
        let workspace_id = create_workspace(&client, &addr, tenant_id).await;
        add_workspace_member(&client, &addr, workspace_id, "admin@test.com", "admin").await;
        add_workspace_member(&client, &addr, workspace_id, "member@test.com", "member").await;

        let document_delete_id = upload_restricted_document(&client, &addr, workspace_id).await;
        let workspace_delete_id = upload_restricted_document(&client, &addr, workspace_id).await;

        assert_status(
            client
                .post(format!(
                    "{addr}/workspaces/{workspace_id}/documents/{document_delete_id}/shares/{MEMBER_ID}"
                ))
                .bearer_auth(OWNER_ID)
                .send()
                .await
                .unwrap(),
            reqwest::StatusCode::CREATED,
        );
        assert_status(
            client
                .post(format!(
                    "{addr}/workspaces/{workspace_id}/documents/{workspace_delete_id}/shares/{ADMIN_ID}"
                ))
                .bearer_auth(OWNER_ID)
                .send()
                .await
                .unwrap(),
            reqwest::StatusCode::CREATED,
        );

        Self {
            addr,
            pool,
            authz,
            tenant_id,
            workspace_id,
            document_delete_id,
            workspace_delete_id,
        }
    }
}

async fn create_tenant(client: &Client, addr: &str) -> Uuid {
    let response = client
        .post(format!("{addr}/tenants"))
        .bearer_auth(PLATFORM_ADMIN_ID)
        .json(&serde_json::json!({ "name": format!("authz invariant {}", Uuid::new_v4()) }))
        .send()
        .await
        .unwrap();
    assert_status(response, reqwest::StatusCode::CREATED)
        .json::<serde_json::Value>()
        .await
        .unwrap()["id"]
        .as_str()
        .and_then(|id| Uuid::parse_str(id).ok())
        .expect("tenant create response must include an id")
}

async fn add_tenant_owner(client: &Client, addr: &str, tenant_id: Uuid) {
    assert_status(
        client
            .post(format!("{addr}/tenants/{tenant_id}/owners"))
            .bearer_auth(PLATFORM_ADMIN_ID)
            .json(&serde_json::json!({ "email": "verified-owner@test.com" }))
            .send()
            .await
            .unwrap(),
        reqwest::StatusCode::NO_CONTENT,
    );
}

async fn create_workspace(client: &Client, addr: &str, tenant_id: Uuid) -> Uuid {
    let response = client
        .post(format!("{addr}/tenants/{tenant_id}/workspaces"))
        .bearer_auth(OWNER_ID)
        .json(&serde_json::json!({ "name": format!("authz invariant {}", Uuid::new_v4()) }))
        .send()
        .await
        .unwrap();
    assert_status(response, reqwest::StatusCode::CREATED)
        .json::<serde_json::Value>()
        .await
        .unwrap()["id"]
        .as_str()
        .and_then(|id| Uuid::parse_str(id).ok())
        .expect("workspace create response must include an id")
}

async fn add_workspace_member(
    client: &Client,
    addr: &str,
    workspace_id: Uuid,
    email: &str,
    role: &str,
) {
    assert_status(
        client
            .post(format!("{addr}/workspaces/{workspace_id}/members"))
            .bearer_auth(OWNER_ID)
            .json(&serde_json::json!({ "email": email, "role": role }))
            .send()
            .await
            .unwrap(),
        reqwest::StatusCode::CREATED,
    );
}

async fn upload_restricted_document(client: &Client, addr: &str, workspace_id: Uuid) -> Uuid {
    let form = reqwest::multipart::Form::new()
        .text("access_mode", "restricted")
        .part(
            "file",
            reqwest::multipart::Part::bytes(minimal_valid_pdf_bytes())
                .file_name("fixture.pdf")
                .mime_str("application/pdf")
                .unwrap(),
        );
    let response = client
        .post(format!("{addr}/workspaces/{workspace_id}/documents/upload"))
        .bearer_auth(OWNER_ID)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_status(response, reqwest::StatusCode::ACCEPTED)
        .json::<serde_json::Value>()
        .await
        .unwrap()["documents"][0]["document_id"]
        .as_str()
        .and_then(|id| Uuid::parse_str(id).ok())
        .expect("upload response must include one document id")
}

async fn assert_no_fixture_orphans(fixture: &Fixture) {
    let report = run_authz_orphan_report(&fixture.pool, &fixture.authz)
        .await
        .unwrap();
    let fixture_resource_ids = [
        fixture.tenant_id,
        fixture.workspace_id,
        fixture.document_delete_id,
        fixture.workspace_delete_id,
    ];
    let fixture_orphans: Vec<_> = report
        .findings
        .iter()
        .filter(|finding| fixture_resource_ids.contains(&finding.resource_id))
        .collect();
    assert!(
        fixture_orphans.is_empty(),
        "production delete paths left OpenFGA orphan tuples: {fixture_orphans:?}"
    );
}

fn ensure_platform_admin() {
    let output = Command::new(env!("CARGO_BIN_EXE_seed-platform-admin"))
        .arg(format!("--user-id={PLATFORM_ADMIN_ID}"))
        .output()
        .expect("seed-platform-admin must start");
    if output.status.success() {
        return;
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.to_ascii_lowercase().contains("already exists"),
        "seed-platform-admin failed: {stderr}"
    );
}

fn minimal_valid_pdf_bytes() -> Vec<u8> {
    let mut doc = Document::with_version("1.4");
    let pages_id = doc.new_object_id();
    let page_id = doc.add_object(dictionary! {
        "Type" => "Page",
        "Parent" => pages_id,
        "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
    });
    doc.objects.insert(
        pages_id,
        Object::Dictionary(dictionary! {
            "Type" => "Pages",
            "Kids" => vec![Object::Reference(page_id)],
            "Count" => 1,
        }),
    );
    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id,
    });
    doc.trailer.set("Root", catalog_id);

    let mut bytes = Vec::new();
    doc.save_to(&mut bytes).unwrap();
    bytes
}

fn assert_status(response: reqwest::Response, expected: reqwest::StatusCode) -> reqwest::Response {
    assert_eq!(response.status(), expected);
    response
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
        std::env::set_var("GMRAG_GRAPH_EXTRACTION_ENABLED", "false");
    });
}
