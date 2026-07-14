use std::sync::Arc;

use gmrag_api::{
    auth::authz::{AuthzClient, Object, Relation},
    state::AppState,
};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{Mutex, OnceCell, Semaphore};
use uuid::Uuid;

static TEST_LOCK: OnceCell<Mutex<()>> = OnceCell::const_new();

async fn test_lock() -> &'static Mutex<()> {
    TEST_LOCK.get_or_init(|| async { Mutex::new(()) }).await
}

#[tokio::test]
async fn tenant_directory_enforces_authz_and_returns_filtered_pages_with_owners() {
    let _guard = test_lock().await.lock().await;
    init_test_env();

    let pool = setup_pool().await;
    let authz = AuthzClient::from_env().expect("OpenFGA config");
    let admin_id = format!("tenant-directory-admin-{}", Uuid::new_v4());
    let member_id = format!("tenant-directory-member-{}", Uuid::new_v4());
    let owner_id = format!("tenant-directory-owner-{}", Uuid::new_v4());
    let prefix = format!("Directory {}", Uuid::new_v4());
    let tenant_ids = [
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    ];

    seed_fixture(&pool, &owner_id, &prefix, &tenant_ids).await;
    authz
        .write_tuple(
            &format!("user:{admin_id}"),
            Relation::Admin,
            &Object::Platform,
        )
        .await
        .expect("grant platform admin");

    let address = spawn_api(pool.clone(), authz.clone()).await;
    let client = Client::new();

    let page = client
        .get(format!("{address}/tenants"))
        .bearer_auth(&admin_id)
        .query(&[("limit", "2"), ("offset", "0"), ("q", prefix.as_str())])
        .send()
        .await
        .expect("tenant page response");
    assert_eq!(page.status(), StatusCode::OK);
    let page: Value = page.json().await.expect("tenant page JSON");
    assert_eq!(page["total"], 4);
    assert_eq!(page["limit"], 2);
    assert_eq!(page["offset"], 0);
    assert_eq!(page["tenants"].as_array().map(Vec::len), Some(2));
    assert_eq!(page["tenants"][0]["id"], tenant_ids[3].to_string());
    assert!(page["tenants"][0]["owners"].is_array());

    let owner_result = client
        .get(format!("{address}/tenants"))
        .bearer_auth(&admin_id)
        .query(&[("q", "DIRECTORY-OWNER@TEST.LOCAL")])
        .send()
        .await
        .expect("owner search response");
    assert_eq!(owner_result.status(), StatusCode::OK);
    let owner_result: Value = owner_result.json().await.expect("owner search JSON");
    assert_eq!(owner_result["total"], 1);
    assert_eq!(owner_result["tenants"][0]["owners"][0]["id"], owner_id);
    assert_eq!(
        owner_result["tenants"][0]["owners"][0]["email"],
        "directory-owner@test.local"
    );

    let id_query = tenant_ids[2].to_string()[..8].to_uppercase();
    let id_result = client
        .get(format!("{address}/tenants"))
        .bearer_auth(&admin_id)
        .query(&[("q", id_query.as_str())])
        .send()
        .await
        .expect("tenant id search response");
    assert_eq!(id_result.status(), StatusCode::OK);
    let id_result: Value = id_result.json().await.expect("tenant id search JSON");
    assert_eq!(id_result["total"], 1);
    assert_eq!(id_result["tenants"][0]["id"], tenant_ids[2].to_string());

    let zero_owner = client
        .get(format!("{address}/tenants"))
        .bearer_auth(&admin_id)
        .query(&[("q", format!("{prefix} beta"))])
        .send()
        .await
        .expect("zero-owner response");
    assert_eq!(zero_owner.status(), StatusCode::OK);
    let zero_owner: Value = zero_owner.json().await.expect("zero-owner JSON");
    assert_eq!(zero_owner["tenants"][0]["owners"], serde_json::json!([]));

    let denied = client
        .get(format!("{address}/tenants"))
        .bearer_auth(&member_id)
        .send()
        .await
        .expect("denied response");
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert_json_error(denied, "WORKSPACE_ADMIN_REQUIRED").await;

    let invalid = client
        .get(format!("{address}/tenants?limit=101"))
        .bearer_auth(&admin_id)
        .send()
        .await
        .expect("invalid pagination response");
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_json_error(invalid, "INVALID_REQUEST").await;

    cleanup_fixture(&pool, &authz, &admin_id, &owner_id, &tenant_ids).await;
}

fn init_test_env() {
    dotenvy::dotenv().ok();
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
}

async fn setup_pool() -> sqlx::PgPool {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("connect database");
    match sqlx::migrate!("./migrations").run(&pool).await {
        Ok(_) | Err(sqlx::migrate::MigrateError::VersionMismatch(_)) => {}
        Err(error) => panic!("migrations: {error}"),
    }
    pool
}

async fn seed_fixture(pool: &sqlx::PgPool, owner_id: &str, prefix: &str, tenant_ids: &[Uuid; 4]) {
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(owner_id)
        .bind("directory-owner@test.local")
        .execute(pool)
        .await
        .expect("insert owner");

    for (index, suffix) in ["Alpha", "Beta", "Gamma", "Delta"].into_iter().enumerate() {
        sqlx::query(
            "INSERT INTO tenants (id, name, created_at) VALUES ($1, $2, TIMESTAMP '2026-07-01 00:00:00' + ($3 * INTERVAL '1 day'))",
        )
        .bind(tenant_ids[index])
        .bind(format!("{prefix} {suffix}"))
        .bind(index as i32)
        .execute(pool)
        .await
        .expect("insert tenant");
    }

    sqlx::query("INSERT INTO tenant_members (tenant_id, user_id, role) VALUES ($1, $2, 'OWNER')")
        .bind(tenant_ids[0])
        .bind(owner_id)
        .execute(pool)
        .await
        .expect("insert owner membership");
}

async fn spawn_api(pool: sqlx::PgPool, authz_client: AuthzClient) -> String {
    let state = AppState {
        pool,
        jwt: gmrag_api::auth::jwt::JwtValidator::from_env().expect("bypass JWT"),
        storage: gmrag_api::storage::StorageClient::from_config(
            gmrag_api::storage::StorageConfig::from_env().expect("storage config"),
        )
        .await,
        retrieval: gmrag_api::retrieval::RetrievalClient::from_env().expect("retrieval config"),
        ingestion_limiter: Arc::new(Semaphore::new(0)),
        authz_client,
        keycloak_client: gmrag_api::auth::keycloak::KeycloakClient::from_env()
            .expect("bypass Keycloak"),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(listener, gmrag_api::app_router(state))
            .await
            .expect("tenant directory test server");
    });
    format!("http://{address}")
}

async fn assert_json_error(response: reqwest::Response, code: &str) {
    let body: Value = response.json().await.expect("error body JSON");
    assert_eq!(body["error"]["code"], code);
    assert!(body["error"]["message"].is_string());
}

async fn cleanup_fixture(
    pool: &sqlx::PgPool,
    authz: &AuthzClient,
    admin_id: &str,
    owner_id: &str,
    tenant_ids: &[Uuid; 4],
) {
    let _ = authz
        .delete_tuple(
            &format!("user:{admin_id}"),
            Relation::Admin,
            &Object::Platform,
        )
        .await;
    let _ = sqlx::query("DELETE FROM tenant_members WHERE tenant_id = ANY($1::uuid[])")
        .bind(tenant_ids.as_slice())
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = ANY($1::uuid[])")
        .bind(tenant_ids.as_slice())
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(owner_id)
        .execute(pool)
        .await;
}
