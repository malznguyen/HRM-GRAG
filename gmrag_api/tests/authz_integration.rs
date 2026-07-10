use reqwest::Client;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::sync::Semaphore;
use uuid::Uuid;

use gmrag_api::auth::authz::{Object, Relation};
use gmrag_api::state::AppState;

// Setup a helper structure to run tests cleanly
struct TestServer {
    addr: String,
    pool: sqlx::PgPool,
    state: AppState,
}

impl TestServer {
    async fn bootstrap() -> Self {
        dotenvy::dotenv().ok();

        // Force bypass settings for test execution
        unsafe {
            std::env::set_var("APP_ENV", "test");
            std::env::set_var("TEST_BYPASS_JWT", "1");
            std::env::set_var("TEST_BYPASS_KEYCLOAK", "1");
            std::env::set_var("S3_REGION", "us-east-1");
            std::env::set_var("S3_BUCKET", "gmrag-documents");
            std::env::set_var("S3_ACCESS_KEY_ID", "minioadmin");
            std::env::set_var("S3_SECRET_ACCESS_KEY", "minioadmin");
            std::env::set_var("S3_FORCE_PATH_STYLE", "true");
            std::env::set_var("S3_PRESIGN_EXPIRY_SECS", "900");
            if std::env::var_os("S3_ENDPOINT_URL").is_none() {
                std::env::set_var("S3_ENDPOINT_URL", "http://localhost:9000");
            }
        }

        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to connect to database");

        // Run migrations
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
            ingestion_limiter: Arc::new(Semaphore::new(1)),
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

        TestServer { addr, pool, state }
    }

    async fn clean_db(&self) {
        // Clean up test data
        let _ = sqlx::query(
            "DELETE FROM workspace_members WHERE user_id LIKE 'test-%' OR user_id LIKE '%-test-%'",
        )
        .execute(&self.pool)
        .await;
        let _ = sqlx::query("DELETE FROM workspaces WHERE name LIKE 'Test %'")
            .execute(&self.pool)
            .await;
        let _ = sqlx::query(
            "DELETE FROM tenant_members WHERE user_id LIKE 'test-%' OR user_id LIKE '%-test-%'",
        )
        .execute(&self.pool)
        .await;
        let _ = sqlx::query("DELETE FROM tenants WHERE name LIKE 'Test %'")
            .execute(&self.pool)
            .await;
    }
}

#[tokio::test]
async fn test_authz_enforcement_suite() {
    let test_server = TestServer::bootstrap().await;
    test_server.clean_db().await;

    let client = Client::new();

    // -------------------------------------------------------------
    // 1. BOOTSTRAP PLATFORM ADMIN
    // -------------------------------------------------------------
    let platform_admin_id = "test-platform-admin-id";

    // Seed Platform Admin in OpenFGA (delete first to avoid duplicate errors)
    let _ = test_server
        .state
        .authz_client
        .delete_tuple(
            &format!("user:{}", platform_admin_id),
            Relation::Admin,
            &Object::Platform,
        )
        .await;
    test_server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{}", platform_admin_id),
            Relation::Admin,
            &Object::Platform,
        )
        .await
        .unwrap();

    // Verification 1a: Platform Admin can POST /tenants
    let tenant_resp = client
        .post(&format!("{}/tenants", test_server.addr))
        .bearer_auth(platform_admin_id)
        .json(&json!({
            "name": "Test Tenant 1"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(tenant_resp.status(), reqwest::StatusCode::CREATED);
    let tenant_json: serde_json::Value = tenant_resp.json().await.unwrap();
    let tenant_id = tenant_json["id"].as_str().unwrap();
    let tenant_uuid = Uuid::parse_str(tenant_id).unwrap();

    // Verification 1b: Platform Admin can POST /tenants/{tenant_id}/owners
    // email verified-owner@test.com will be resolved to verified-keycloak-owner-uuid by Keycloak Client mock
    let owner_resp = client
        .post(&format!(
            "{}/tenants/{}/owners",
            test_server.addr, tenant_id
        ))
        .bearer_auth(platform_admin_id)
        .json(&json!({
            "email": "verified-owner@test.com"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(owner_resp.status(), reqwest::StatusCode::NO_CONTENT);

    // Let's verify that Keycloak verified-keycloak-owner-uuid is registered in DB as tenant owner
    let db_owner_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tenant_members WHERE tenant_id = $1 AND user_id = 'verified-keycloak-owner-uuid')"
    )
    .bind(tenant_uuid)
    .fetch_one(&test_server.pool)
    .await
    .unwrap();
    assert!(db_owner_exists);

    // Verification 6: Keycloak Owner Lookup rejection cases
    // Case A: Unverified email
    let reject_unverified = client
        .post(&format!(
            "{}/tenants/{}/owners",
            test_server.addr, tenant_id
        ))
        .bearer_auth(platform_admin_id)
        .json(&json!({
            "email": "unverified-owner@test.com"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(reject_unverified.status(), reqwest::StatusCode::BAD_REQUEST);

    // Case B: Nonexistent user email
    let reject_nonexistent = client
        .post(&format!(
            "{}/tenants/{}/owners",
            test_server.addr, tenant_id
        ))
        .bearer_auth(platform_admin_id)
        .json(&json!({
            "email": "nonexistent-owner@test.com"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        reject_nonexistent.status(),
        reqwest::StatusCode::BAD_REQUEST
    );

    // -------------------------------------------------------------
    // 2. TENANT OWNER CHECKS
    // -------------------------------------------------------------
    let tenant_owner_id = "verified-keycloak-owner-uuid";

    // Setup SQL record for tenant owner (delete first to avoid duplicate errors)
    let _ = test_server
        .state
        .authz_client
        .delete_tuple(
            &format!("user:{}", tenant_owner_id),
            Relation::Owner,
            &Object::Tenant(tenant_uuid),
        )
        .await;
    test_server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{}", tenant_owner_id),
            Relation::Owner,
            &Object::Tenant(tenant_uuid),
        )
        .await
        .unwrap();

    // Verification 2a: Tenant Owner can POST /tenants/{tenant_id}/workspaces
    let workspace_resp = client
        .post(&format!(
            "{}/tenants/{}/workspaces",
            test_server.addr, tenant_id
        ))
        .bearer_auth(tenant_owner_id)
        .json(&json!({
            "name": "Test Workspace 1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(workspace_resp.status(), reqwest::StatusCode::CREATED);
    let workspace_json: serde_json::Value = workspace_resp.json().await.unwrap();
    let workspace_id = workspace_json["id"].as_str().unwrap();
    let workspace_uuid = Uuid::parse_str(workspace_id).unwrap();

    // Verification 1c: Platform Admin cannot access workspace business data
    let list_docs_by_admin = client
        .get(&format!(
            "{}/workspaces/{}/documents",
            test_server.addr, workspace_id
        ))
        .bearer_auth(platform_admin_id)
        .send()
        .await
        .unwrap();
    assert_eq!(list_docs_by_admin.status(), reqwest::StatusCode::FORBIDDEN);

    // Verification 2b: Tenant Owner can add workspace member
    // Email được Keycloak test-bypass resolve → test-workspace-member-id
    let workspace_member_id = "test-workspace-member-id";

    let add_member_resp = client
        .post(&format!(
            "{}/workspaces/{}/members",
            test_server.addr, workspace_id
        ))
        .bearer_auth(tenant_owner_id)
        .json(&json!({
            "email": "member@test.com",
            "role": "MEMBER"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(add_member_resp.status(), reqwest::StatusCode::CREATED);

    // Verification 2b-not-found: email chưa có trong Keycloak → USER_NOT_FOUND_IN_IDENTITY
    let missing_member_resp = client
        .post(&format!(
            "{}/workspaces/{}/members",
            test_server.addr, workspace_id
        ))
        .bearer_auth(tenant_owner_id)
        .json(&json!({
            "email": "never-signed-up@test.com",
            "role": "MEMBER"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(missing_member_resp.status(), reqwest::StatusCode::NOT_FOUND);
    let missing_body: serde_json::Value = missing_member_resp.json().await.unwrap();
    assert_eq!(missing_body["error"]["code"], "USER_NOT_FOUND_IN_IDENTITY");

    // Verification 2c: Tenant Owner can patch member role
    let patch_role_resp = client
        .patch(&format!(
            "{}/workspaces/{}/members/{}",
            test_server.addr, workspace_id, workspace_member_id
        ))
        .bearer_auth(tenant_owner_id)
        .json(&json!({
            "role": "ADMIN"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(patch_role_resp.status(), reqwest::StatusCode::NO_CONTENT);

    // Set role back to MEMBER for next tests
    let _ = client
        .patch(&format!(
            "{}/workspaces/{}/members/{}",
            test_server.addr, workspace_id, workspace_member_id
        ))
        .bearer_auth(tenant_owner_id)
        .json(&json!({
            "role": "MEMBER"
        }))
        .send()
        .await
        .unwrap();

    // -------------------------------------------------------------
    // 3. WORKSPACE ADMIN CHECKS
    // -------------------------------------------------------------
    let workspace_admin_id = "test-workspace-admin-id";
    let _ = sqlx::query(
        "INSERT INTO users (id, email) VALUES ($1, 'admin@test.com') ON CONFLICT DO NOTHING",
    )
    .bind(workspace_admin_id)
    .execute(&test_server.pool)
    .await
    .unwrap();

    // Add workspace_admin as ADMIN in OpenFGA (delete first to avoid duplicate errors)
    let _ = test_server
        .state
        .authz_client
        .delete_tuple(
            &format!("user:{}", workspace_admin_id),
            Relation::Admin,
            &Object::Workspace(workspace_uuid),
        )
        .await;
    test_server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{}", workspace_admin_id),
            Relation::Admin,
            &Object::Workspace(workspace_uuid),
        )
        .await
        .unwrap();
    let _ = sqlx::query("INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN') ON CONFLICT DO NOTHING")
        .bind(workspace_uuid)
        .bind(workspace_admin_id)
        .execute(&test_server.pool)
        .await
        .unwrap();

    // Verification 3a: Workspace Admin can add/remove workspace members
    // Keycloak test-bypass map new_member@test.com → test-new-member-id
    let new_member_id = "test-new-member-id";

    let admin_add_resp = client
        .post(&format!(
            "{}/workspaces/{}/members",
            test_server.addr, workspace_id
        ))
        .bearer_auth(workspace_admin_id)
        .json(&json!({
            "email": "new_member@test.com",
            "role": "MEMBER"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(admin_add_resp.status(), reqwest::StatusCode::CREATED);

    // Verification 3b: Workspace Admin CANNOT patch member role
    let admin_patch_resp = client
        .patch(&format!(
            "{}/workspaces/{}/members/{}",
            test_server.addr, workspace_id, new_member_id
        ))
        .bearer_auth(workspace_admin_id)
        .json(&json!({
            "role": "ADMIN"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(admin_patch_resp.status(), reqwest::StatusCode::FORBIDDEN);

    // -------------------------------------------------------------
    // 4. WORKSPACE MEMBER CHECKS
    // -------------------------------------------------------------
    // Verification 4a: Workspace Member can list/read workspace documents
    let member_list_resp = client
        .get(&format!(
            "{}/workspaces/{}/documents",
            test_server.addr, workspace_id
        ))
        .bearer_auth(workspace_member_id)
        .send()
        .await
        .unwrap();
    assert_eq!(member_list_resp.status(), reqwest::StatusCode::OK);

    // Verification 4b: Workspace Member can chat
    // Session is created automatically or can be verified by checking endpoint validation status
    let chat_resp = client
        .post(&format!(
            "{}/workspaces/{}/chat",
            test_server.addr, workspace_id
        ))
        .bearer_auth(workspace_member_id)
        .json(&json!({
            "session_id": Uuid::new_v4().to_string(),
            "message": "Hello Test"
        }))
        .send()
        .await
        .unwrap();
    // Since deepseek key is dummy/invalid, it might return 500 but shouldn't be 403 Forbidden!
    assert_ne!(chat_resp.status(), reqwest::StatusCode::FORBIDDEN);

    let boundary = "------------------------testboundary";
    let body = format!("--{boundary}--\r\n");
    let upload_resp = client
        .post(&format!(
            "{}/workspaces/{}/documents/upload",
            test_server.addr, workspace_id
        ))
        .bearer_auth(workspace_member_id)
        .header(
            "Content-Type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(upload_resp.status(), reqwest::StatusCode::FORBIDDEN);

    // Verification 4d: Workspace Member cannot add members
    let member_add_resp = client
        .post(&format!(
            "{}/workspaces/{}/members",
            test_server.addr, workspace_id
        ))
        .bearer_auth(workspace_member_id)
        .json(&json!({
            "email": "some-email@test.com",
            "role": "MEMBER"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(member_add_resp.status(), reqwest::StatusCode::FORBIDDEN);

    // Verification 4e: Workspace Member cannot patch roles
    let member_patch_resp = client
        .patch(&format!(
            "{}/workspaces/{}/members/{}",
            test_server.addr, workspace_id, new_member_id
        ))
        .bearer_auth(workspace_member_id)
        .json(&json!({
            "role": "ADMIN"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(member_patch_resp.status(), reqwest::StatusCode::FORBIDDEN);

    // Verification 3c: successful remove returns 204, clears SQL, revokes OpenFGA access
    let member_access_before = client
        .get(&format!(
            "{}/workspaces/{}/documents",
            test_server.addr, workspace_id
        ))
        .bearer_auth(new_member_id)
        .send()
        .await
        .unwrap();
    assert_eq!(member_access_before.status(), reqwest::StatusCode::OK);

    let admin_remove_resp = client
        .delete(&format!(
            "{}/workspaces/{}/members/{}",
            test_server.addr, workspace_id, new_member_id
        ))
        .bearer_auth(workspace_admin_id)
        .send()
        .await
        .unwrap();
    assert_eq!(admin_remove_resp.status(), reqwest::StatusCode::NO_CONTENT);

    let removed_still_member: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM workspace_members WHERE workspace_id = $1 AND user_id = $2)",
    )
    .bind(workspace_uuid)
    .bind(new_member_id)
    .fetch_one(&test_server.pool)
    .await
    .unwrap();
    assert!(
        !removed_still_member,
        "SQL membership row must be deleted after successful remove"
    );

    let member_access_after = client
        .get(&format!(
            "{}/workspaces/{}/documents",
            test_server.addr, workspace_id
        ))
        .bearer_auth(new_member_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        member_access_after.status(),
        reqwest::StatusCode::FORBIDDEN,
        "removed member must be denied workspace-protected routes"
    );

    // -------------------------------------------------------------
    // 5. NON-MEMBER CHECKS
    // -------------------------------------------------------------
    let outsider_id = "test-outsider-id";

    // Try to access workspace endpoints as outsider
    let outsider_list = client
        .get(&format!(
            "{}/workspaces/{}/documents",
            test_server.addr, workspace_id
        ))
        .bearer_auth(outsider_id)
        .send()
        .await
        .unwrap();
    assert_eq!(outsider_list.status(), reqwest::StatusCode::FORBIDDEN);

    let outsider_chat = client
        .post(&format!(
            "{}/workspaces/{}/chat",
            test_server.addr, workspace_id
        ))
        .bearer_auth(outsider_id)
        .json(&json!({
            "session_id": Uuid::new_v4().to_string(),
            "message": "Hello"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(outsider_chat.status(), reqwest::StatusCode::FORBIDDEN);

    let outsider_graph = client
        .get(&format!(
            "{}/workspaces/{}/graph",
            test_server.addr, workspace_id
        ))
        .bearer_auth(outsider_id)
        .send()
        .await
        .unwrap();
    assert_eq!(outsider_graph.status(), reqwest::StatusCode::FORBIDDEN);

    // Cleanup OpenFGA tuples written during tests
    let _ = test_server
        .state
        .authz_client
        .delete_tuple(
            &format!("user:{}", platform_admin_id),
            Relation::Admin,
            &Object::Platform,
        )
        .await;

    let _ = test_server
        .state
        .authz_client
        .delete_tuple(
            &format!("user:{}", tenant_owner_id),
            Relation::Owner,
            &Object::Tenant(tenant_uuid),
        )
        .await;

    let _ = test_server
        .state
        .authz_client
        .delete_tuple(
            &format!("user:{}", workspace_admin_id),
            Relation::Admin,
            &Object::Workspace(workspace_uuid),
        )
        .await;

    test_server.clean_db().await;
}
