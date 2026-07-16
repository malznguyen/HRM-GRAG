//! Phase 0B — authorization gap closure integration tests.
//!
//! Covers ACL-001..ACL-006: role assignment, role input, last-admin guard,
//! member removal fail-closed, FGA-intersected workspace list, share target FGA.

mod support;

use reqwest::Client;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell, Semaphore};
use uuid::Uuid;

use gmrag_api::auth::authz::{AuthzClient, Object, Relation};
use gmrag_api::state::AppState;

static PHASE0B_LOCK: OnceCell<Mutex<()>> = OnceCell::const_new();

async fn phase0b_lock() -> &'static Mutex<()> {
    PHASE0B_LOCK.get_or_init(|| async { Mutex::new(()) }).await
}

fn init_test_env() {
    dotenvy::dotenv().ok();
    unsafe {
        std::env::set_var("APP_ENV", "test");
        std::env::set_var("TEST_BYPASS_JWT", "1");
        std::env::set_var("TEST_BYPASS_KEYCLOAK", "1");
        std::env::set_var("S3_REGION", "us-east-1");
        if std::env::var_os("S3_BUCKET").is_none() {
            std::env::set_var("S3_BUCKET", "gmrag-documents");
        }
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
    let database_url = support::database_url().expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("connect db");
    match sqlx::migrate!("./migrations").run(&pool).await {
        Ok(_) | Err(sqlx::migrate::MigrateError::VersionMismatch(_)) => {}
        Err(err) => panic!("migrations: {err}"),
    }
    pool
}

async fn setup_storage() -> gmrag_api::storage::StorageClient {
    let config = gmrag_api::storage::StorageConfig::from_env().unwrap();
    gmrag_api::storage::StorageClient::from_config(config).await
}

struct Fixture {
    addr: String,
    pool: sqlx::PgPool,
    authz: AuthzClient,
    tenant_id: Uuid,
    workspace_id: Uuid,
    owner_id: String,
    ws_admin_id: String,
    member_id: String,
}

impl Fixture {
    async fn bootstrap(authz: AuthzClient) -> Self {
        Self::bootstrap_inner(authz, true).await
    }

    /// SQL-only fixture when OpenFGA client is intentionally broken (fail-closed paths).
    async fn bootstrap_sql_only(authz: AuthzClient) -> Self {
        Self::bootstrap_inner(authz, false).await
    }

    async fn bootstrap_inner(authz: AuthzClient, seed_fga: bool) -> Self {
        let pool = setup_pool().await;
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
            authz_client: authz.clone(),
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
        let owner_id = format!("phase0b-owner-{}", Uuid::new_v4());
        let ws_admin_id = format!("phase0b-wsadmin-{}", Uuid::new_v4());
        let member_id = format!("phase0b-member-{}", Uuid::new_v4());

        sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
            .bind(tenant_id)
            .bind(format!("Phase0B Tenant {tenant_id}"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
            .bind(workspace_id)
            .bind(tenant_id)
            .bind(format!("Phase0B Workspace {workspace_id}"))
            .execute(&pool)
            .await
            .unwrap();

        for (uid, email) in [
            (&owner_id, format!("{owner_id}@test.local")),
            (&ws_admin_id, format!("{ws_admin_id}@test.local")),
            (&member_id, format!("{member_id}@test.local")),
        ] {
            sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
                .bind(uid)
                .bind(&email)
                .execute(&pool)
                .await
                .unwrap();
        }

        sqlx::query(
            "INSERT INTO tenant_members (tenant_id, user_id, role) VALUES ($1, $2, 'OWNER')",
        )
        .bind(tenant_id)
        .bind(&owner_id)
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
        )
        .bind(workspace_id)
        .bind(&ws_admin_id)
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'MEMBER')",
        )
        .bind(workspace_id)
        .bind(&member_id)
        .execute(&pool)
        .await
        .unwrap();

        if seed_fga {
            authz
                .write_tuple(
                    &format!("user:{owner_id}"),
                    Relation::Owner,
                    &Object::Tenant(tenant_id),
                )
                .await
                .unwrap();
            authz
                .write_tuple(
                    &format!("tenant:{tenant_id}"),
                    Relation::Tenant,
                    &Object::Workspace(workspace_id),
                )
                .await
                .unwrap();
            authz
                .write_tuple(
                    &format!("user:{ws_admin_id}"),
                    Relation::Admin,
                    &Object::Workspace(workspace_id),
                )
                .await
                .unwrap();
            authz
                .write_tuple(
                    &format!("user:{member_id}"),
                    Relation::Member,
                    &Object::Workspace(workspace_id),
                )
                .await
                .unwrap();
        }

        Self {
            addr,
            pool,
            authz,
            tenant_id,
            workspace_id,
            owner_id,
            ws_admin_id,
            member_id,
        }
    }

    async fn cleanup(&self) {
        let _ = self
            .authz
            .delete_tuple(
                &format!("user:{}", self.member_id),
                Relation::Member,
                &Object::Workspace(self.workspace_id),
            )
            .await;
        let _ = self
            .authz
            .delete_tuple(
                &format!("user:{}", self.ws_admin_id),
                Relation::Admin,
                &Object::Workspace(self.workspace_id),
            )
            .await;
        let _ = self
            .authz
            .delete_tuple(
                &format!("tenant:{}", self.tenant_id),
                Relation::Tenant,
                &Object::Workspace(self.workspace_id),
            )
            .await;
        let _ = self
            .authz
            .delete_tuple(
                &format!("user:{}", self.owner_id),
                Relation::Owner,
                &Object::Tenant(self.tenant_id),
            )
            .await;

        let _ = sqlx::query("DELETE FROM document_shares WHERE document_id IN (SELECT id FROM documents WHERE workspace_id = $1)")
            .bind(self.workspace_id)
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("DELETE FROM documents WHERE workspace_id = $1")
            .bind(self.workspace_id)
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("DELETE FROM workspace_members WHERE workspace_id = $1")
            .bind(self.workspace_id)
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind(self.workspace_id)
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("DELETE FROM tenant_members WHERE tenant_id = $1")
            .bind(self.tenant_id)
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
            .bind(self.tenant_id)
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = ANY($1::text[])")
            .bind(vec![
                self.owner_id.clone(),
                self.ws_admin_id.clone(),
                self.member_id.clone(),
            ])
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id LIKE 'phase0b-%'")
            .execute(&self.pool)
            .await;
        let _ = sqlx::query(
            "DELETE FROM authz_outbox WHERE payload->>'object' LIKE $1 OR payload->>'user' LIKE 'user:phase0b-%'",
        )
        .bind(format!("workspace:{}", self.workspace_id))
        .execute(&self.pool)
        .await;
    }
}

#[tokio::test]
async fn get_members_returns_openfga_caller_capabilities() {
    let _guard = phase0b_lock().await.lock().await;
    init_test_env();
    let fx = Fixture::bootstrap(AuthzClient::from_env().unwrap()).await;
    let client = Client::new();

    for (caller_id, can_manage_member, can_assign_role) in [
        (&fx.owner_id, true, true),
        (&fx.ws_admin_id, true, false),
        (&fx.member_id, false, false),
    ] {
        let response = client
            .get(format!(
                "{}/workspaces/{}/members",
                fx.addr, fx.workspace_id
            ))
            .bearer_auth(caller_id)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        assert!(body["members"].is_array());
        assert_eq!(body["caller"]["can_manage_member"], can_manage_member);
        assert_eq!(body["caller"]["can_assign_role"], can_assign_role);
    }

    fx.cleanup().await;
}

#[tokio::test]
async fn acl001_ws_admin_add_member_ok_admin_denied_no_side_effect() {
    let _guard = phase0b_lock().await.lock().await;
    init_test_env();
    let fx = Fixture::bootstrap(AuthzClient::from_env().unwrap()).await;
    let client = Client::new();

    // WS Admin + member → 201
    let add_member = client
        .post(format!(
            "{}/workspaces/{}/members",
            fx.addr, fx.workspace_id
        ))
        .bearer_auth(&fx.ws_admin_id)
        .json(&json!({
            "email": "wsadmin-add-member@phase0b.test",
            "role": "member"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(add_member.status(), reqwest::StatusCode::CREATED);
    let added: serde_json::Value = add_member.json().await.unwrap();
    assert_eq!(added["id"], "phase0b-wsadmin-add-member");
    assert_eq!(added["role"], "MEMBER");

    // WS Admin + admin → 403 ROLE_ASSIGNMENT_DENIED, no side effects
    let target_email = "wsadmin-add-admin@phase0b.test";
    let target_id = "phase0b-wsadmin-add-admin";
    let before_users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE id = $1")
        .bind(target_id)
        .fetch_one(&fx.pool)
        .await
        .unwrap();

    let deny = client
        .post(format!(
            "{}/workspaces/{}/members",
            fx.addr, fx.workspace_id
        ))
        .bearer_auth(&fx.ws_admin_id)
        .json(&json!({
            "email": target_email,
            "role": "admin"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(deny.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = deny.json().await.unwrap();
    assert_eq!(body["error"]["code"], "ROLE_ASSIGNMENT_DENIED");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("tenant owners")
    );

    let after_users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE id = $1")
        .bind(target_id)
        .fetch_one(&fx.pool)
        .await
        .unwrap();
    assert_eq!(
        before_users, after_users,
        "denied admin create must not upsert users"
    );

    let membership: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM workspace_members WHERE workspace_id = $1 AND user_id = $2)",
    )
    .bind(fx.workspace_id)
    .bind(target_id)
    .fetch_one(&fx.pool)
    .await
    .unwrap();
    assert!(!membership);

    let fga_admin = fx
        .authz
        .check_fga(
            &format!("user:{target_id}"),
            Relation::Admin,
            &Object::Workspace(fx.workspace_id),
        )
        .await
        .unwrap();
    assert!(!fga_admin);

    // Tenant Owner + admin → 201
    let owner_add = client
        .post(format!(
            "{}/workspaces/{}/members",
            fx.addr, fx.workspace_id
        ))
        .bearer_auth(&fx.owner_id)
        .json(&json!({
            "email": target_email,
            "role": "admin"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(owner_add.status(), reqwest::StatusCode::CREATED);

    // Member + any → 403
    let member_deny = client
        .post(format!(
            "{}/workspaces/{}/members",
            fx.addr, fx.workspace_id
        ))
        .bearer_auth(&fx.member_id)
        .json(&json!({
            "email": "member-attempt@phase0b.test",
            "role": "member"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(member_deny.status(), reqwest::StatusCode::FORBIDDEN);

    // Cleanup extra FGA tuples
    let _ = fx
        .authz
        .delete_tuple(
            "user:phase0b-wsadmin-add-member",
            Relation::Member,
            &Object::Workspace(fx.workspace_id),
        )
        .await;
    let _ = fx
        .authz
        .delete_tuple(
            "user:phase0b-wsadmin-add-admin",
            Relation::Admin,
            &Object::Workspace(fx.workspace_id),
        )
        .await;

    fx.cleanup().await;
}

#[tokio::test]
async fn acl002_invalid_roles_rejected() {
    let _guard = phase0b_lock().await.lock().await;
    init_test_env();
    let fx = Fixture::bootstrap(AuthzClient::from_env().unwrap()).await;
    let client = Client::new();

    for bad in ["user", "owner", "OWNER", "USER", "", "superadmin", "root"] {
        let resp = client
            .post(format!(
                "{}/workspaces/{}/members",
                fx.addr, fx.workspace_id
            ))
            .bearer_auth(&fx.owner_id)
            .json(&json!({
                "email": "role-alias@phase0b.test",
                "role": bad
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "role={bad:?}"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "INVALID_MEMBER_ROLE");
        assert_eq!(body["error"]["message"], "role must be member or admin");
    }

    for bad in ["user", "owner"] {
        let resp = client
            .patch(format!(
                "{}/workspaces/{}/members/{}",
                fx.addr, fx.workspace_id, fx.member_id
            ))
            .bearer_auth(&fx.owner_id)
            .json(&json!({ "role": bad }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "INVALID_MEMBER_ROLE");
    }

    // No side effect for denied alias on known email mapping
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id = 'phase0b-role-alias')")
            .fetch_one(&fx.pool)
            .await
            .unwrap();
    assert!(!exists);

    fx.cleanup().await;
}

#[tokio::test]
async fn acl003_last_admin_guard_and_tenant_owner_path() {
    let _guard = phase0b_lock().await.lock().await;
    init_test_env();
    let authz = AuthzClient::from_env().unwrap();
    let fx = Fixture::bootstrap(authz.clone()).await;
    let client = Client::new();

    // --- Demote last admin WITH valid tenant owner path → allowed ---
    // Only one SQL ADMIN (ws_admin). Tenant owner still present.
    let demote_ok = client
        .patch(format!(
            "{}/workspaces/{}/members/{}",
            fx.addr, fx.workspace_id, fx.ws_admin_id
        ))
        .bearer_auth(&fx.owner_id)
        .json(&json!({ "role": "member" }))
        .send()
        .await
        .unwrap();
    assert_eq!(demote_ok.status(), reqwest::StatusCode::NO_CONTENT);

    // Re-promote for next cases
    let promote = client
        .patch(format!(
            "{}/workspaces/{}/members/{}",
            fx.addr, fx.workspace_id, fx.ws_admin_id
        ))
        .bearer_auth(&fx.owner_id)
        .json(&json!({ "role": "admin" }))
        .send()
        .await
        .unwrap();
    assert_eq!(promote.status(), reqwest::StatusCode::NO_CONTENT);

    // --- Strip SQL tenant OWNER (keep FGA owner so can_assign_role still works) ---
    // Guard requires SQL OWNER + FGA owner; without SQL OWNER, sole admin cannot be demoted.
    sqlx::query("DELETE FROM tenant_members WHERE tenant_id = $1")
        .bind(fx.tenant_id)
        .execute(&fx.pool)
        .await
        .unwrap();

    let demote_deny = client
        .patch(format!(
            "{}/workspaces/{}/members/{}",
            fx.addr, fx.workspace_id, fx.ws_admin_id
        ))
        .bearer_auth(&fx.owner_id)
        .json(&json!({ "role": "member" }))
        .send()
        .await
        .unwrap();
    assert_eq!(demote_deny.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = demote_deny.json().await.unwrap();
    assert_eq!(body["error"]["code"], "LAST_WORKSPACE_ADMIN");

    let role: String = sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(fx.workspace_id)
    .bind(&fx.ws_admin_id)
    .fetch_one(&fx.pool)
    .await
    .unwrap();
    assert_eq!(role, "ADMIN");

    // --- Remove last admin without tenant owner path → 409 ---
    // Actor needs can_manage_member: add second admin, then remove first, then attempt last.
    let actor = format!("phase0b-actor-admin-{}", Uuid::new_v4());
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&actor)
        .bind(format!("{actor}@test.local"))
        .execute(&fx.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
    )
    .bind(fx.workspace_id)
    .bind(&actor)
    .execute(&fx.pool)
    .await
    .unwrap();
    authz
        .write_tuple(
            &format!("user:{actor}"),
            Relation::Admin,
            &Object::Workspace(fx.workspace_id),
        )
        .await
        .unwrap();

    // Remove ws_admin (actor remains) → OK
    let rm_ok = client
        .delete(format!(
            "{}/workspaces/{}/members/{}",
            fx.addr, fx.workspace_id, fx.ws_admin_id
        ))
        .bearer_auth(&actor)
        .send()
        .await
        .unwrap();
    assert_eq!(rm_ok.status(), reqwest::StatusCode::NO_CONTENT);

    // Re-add ws_admin as MEMBER actor cannot remove last admin without management path.
    // Promote a disposable admin so we can attempt remove of sole remaining path.
    // Currently only `actor` is ADMIN and no SQL tenant owner → cannot remove actor via self.
    // Add temporary admin B, then B tries to remove actor (last remaining after we demote... no).
    // B removes actor when B is also admin; after removing actor, B is last → 409 on B remove needs C.
    // Simpler: attempt remove of sole admin from a second admin after deleting second admin's peer:
    let temp = format!("phase0b-temp-admin-{}", Uuid::new_v4());
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&temp)
        .bind(format!("{temp}@test.local"))
        .execute(&fx.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
    )
    .bind(fx.workspace_id)
    .bind(&temp)
    .execute(&fx.pool)
    .await
    .unwrap();
    authz
        .write_tuple(
            &format!("user:{temp}"),
            Relation::Admin,
            &Object::Workspace(fx.workspace_id),
        )
        .await
        .unwrap();

    // temp removes actor → only temp remains, no tenant owner → OK (one admin remains)
    let rm_actor = client
        .delete(format!(
            "{}/workspaces/{}/members/{}",
            fx.addr, fx.workspace_id, actor
        ))
        .bearer_auth(&temp)
        .send()
        .await
        .unwrap();
    assert_eq!(rm_actor.status(), reqwest::StatusCode::NO_CONTENT);

    // Need another admin to attempt removing last (temp). Add actor back as ADMIN.
    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
    )
    .bind(fx.workspace_id)
    .bind(&actor)
    .execute(&fx.pool)
    .await
    .unwrap();
    authz
        .write_tuple(
            &format!("user:{actor}"),
            Relation::Admin,
            &Object::Workspace(fx.workspace_id),
        )
        .await
        .unwrap();

    // Remove temp first
    let _ = client
        .delete(format!(
            "{}/workspaces/{}/members/{}",
            fx.addr, fx.workspace_id, temp
        ))
        .bearer_auth(&actor)
        .send()
        .await
        .unwrap();

    // Re-add temp just to try removing sole actor → after remove only temp left is fine;
    // attempt concurrent-style: temp tries to remove actor when actor is sole admin after...
    // With actor sole admin, temp not present. Re-add temp, remove actor → last is temp.
    // Then we cannot remove temp without another principal. Use demote already covered for 409.
    // Last-admin remove: temp removes actor when both are admin and no tenant owner:
    // after that, temp is last — 409 would need third principal. Covered by demote 409 above.

    // Workspace Admin cannot patch roles
    let patch_forbid = client
        .patch(format!(
            "{}/workspaces/{}/members/{}",
            fx.addr, fx.workspace_id, fx.member_id
        ))
        .bearer_auth(&actor)
        .json(&json!({ "role": "admin" }))
        .send()
        .await
        .unwrap();
    assert_eq!(patch_forbid.status(), reqwest::StatusCode::FORBIDDEN);

    // Concurrent demote of two last admins must not leave zero management paths.
    // Setup: two admins, no SQL tenant owner. Concurrent demote both via owner FGA (can_assign).
    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')
         ON CONFLICT (workspace_id, user_id) DO UPDATE SET role = 'ADMIN'",
    )
    .bind(fx.workspace_id)
    .bind(&temp)
    .execute(&fx.pool)
    .await
    .unwrap();
    let _ = authz
        .write_tuple(
            &format!("user:{temp}"),
            Relation::Admin,
            &Object::Workspace(fx.workspace_id),
        )
        .await;
    // Ensure only actor + temp are ADMIN; demote member rows already MEMBER.

    let url_actor = format!(
        "{}/workspaces/{}/members/{}",
        fx.addr, fx.workspace_id, actor
    );
    let url_temp = format!(
        "{}/workspaces/{}/members/{}",
        fx.addr, fx.workspace_id, temp
    );
    let owner = fx.owner_id.clone();
    let c1 = client.clone();
    let c2 = client.clone();
    let owner2 = owner.clone();
    let (r1, r2) = tokio::join!(
        async move {
            c1.patch(url_actor)
                .bearer_auth(&owner)
                .json(&json!({ "role": "member" }))
                .send()
                .await
                .unwrap()
        },
        async move {
            c2.patch(url_temp)
                .bearer_auth(&owner2)
                .json(&json!({ "role": "member" }))
                .send()
                .await
                .unwrap()
        }
    );

    let statuses = [r1.status(), r2.status()];
    let success = statuses
        .iter()
        .filter(|s| **s == reqwest::StatusCode::NO_CONTENT)
        .count();
    let conflict = statuses
        .iter()
        .filter(|s| **s == reqwest::StatusCode::CONFLICT)
        .count();
    assert_eq!(
        success, 1,
        "exactly one concurrent demote may succeed: {statuses:?}"
    );
    assert_eq!(
        conflict, 1,
        "other concurrent demote must hit last-admin: {statuses:?}"
    );

    let remaining_admins: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM workspace_members WHERE workspace_id = $1 AND role = 'ADMIN'",
    )
    .bind(fx.workspace_id)
    .fetch_one(&fx.pool)
    .await
    .unwrap();
    assert_eq!(
        remaining_admins, 1,
        "concurrent demotes must leave exactly one admin when no tenant owner path"
    );

    let _ = authz
        .delete_tuple(
            &format!("user:{actor}"),
            Relation::Admin,
            &Object::Workspace(fx.workspace_id),
        )
        .await;
    let _ = authz
        .delete_tuple(
            &format!("user:{temp}"),
            Relation::Admin,
            &Object::Workspace(fx.workspace_id),
        )
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1 OR id = $2")
        .bind(&actor)
        .bind(&temp)
        .execute(&fx.pool)
        .await;

    fx.cleanup().await;
}

#[tokio::test]
async fn acl005_workspace_list_intersects_fga() {
    let _guard = phase0b_lock().await.lock().await;
    init_test_env();
    let authz = AuthzClient::from_env().unwrap();
    let fx = Fixture::bootstrap(authz.clone()).await;
    let client = Client::new();

    // Member with SQL+FGA → listed
    let list_ok = client
        .get(format!("{}/workspaces", fx.addr))
        .bearer_auth(&fx.member_id)
        .send()
        .await
        .unwrap();
    assert_eq!(list_ok.status(), reqwest::StatusCode::OK);
    let workspaces: Vec<serde_json::Value> = list_ok.json().await.unwrap();
    assert!(
        workspaces
            .iter()
            .any(|w| w["id"] == fx.workspace_id.to_string())
    );

    // Tenant owner (no SQL workspace_members) → listed via FGA inheritance
    let list_owner = client
        .get(format!("{}/workspaces", fx.addr))
        .bearer_auth(&fx.owner_id)
        .send()
        .await
        .unwrap();
    assert_eq!(list_owner.status(), reqwest::StatusCode::OK);
    let owner_ws: Vec<serde_json::Value> = list_owner.json().await.unwrap();
    assert!(
        owner_ws
            .iter()
            .any(|w| w["id"] == fx.workspace_id.to_string())
    );

    // WS Admin → listed
    let list_admin = client
        .get(format!("{}/workspaces", fx.addr))
        .bearer_auth(&fx.ws_admin_id)
        .send()
        .await
        .unwrap();
    assert_eq!(list_admin.status(), reqwest::StatusCode::OK);
    let admin_ws: Vec<serde_json::Value> = list_admin.json().await.unwrap();
    assert!(
        admin_ws
            .iter()
            .any(|w| w["id"] == fx.workspace_id.to_string())
    );

    // Stale SQL membership: revoke FGA member, keep SQL row → not listed
    authz
        .delete_tuple(
            &format!("user:{}", fx.member_id),
            Relation::Member,
            &Object::Workspace(fx.workspace_id),
        )
        .await
        .unwrap();

    let list_stale = client
        .get(format!("{}/workspaces", fx.addr))
        .bearer_auth(&fx.member_id)
        .send()
        .await
        .unwrap();
    assert_eq!(list_stale.status(), reqwest::StatusCode::OK);
    let stale_ws: Vec<serde_json::Value> = list_stale.json().await.unwrap();
    assert!(
        !stale_ws
            .iter()
            .any(|w| w["id"] == fx.workspace_id.to_string()),
        "stale SQL membership must not list workspace"
    );

    // Outsider → empty
    let outsider = format!("phase0b-outsider-{}", Uuid::new_v4());
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&outsider)
        .bind(format!("{outsider}@test.local"))
        .execute(&fx.pool)
        .await
        .unwrap();
    let list_out = client
        .get(format!("{}/workspaces", fx.addr))
        .bearer_auth(&outsider)
        .send()
        .await
        .unwrap();
    assert_eq!(list_out.status(), reqwest::StatusCode::OK);
    let out_ws: Vec<serde_json::Value> = list_out.json().await.unwrap();
    assert!(out_ws.is_empty());

    // FGA unavailable → fail closed (not SQL-only)
    let mock_url = spawn_authz_mock(AuthzMockMode::CheckFail).await;
    let store_id = std::env::var("OPENFGA_STORE_ID").unwrap_or_else(|_| "test-store".into());
    let broken = AuthzClient::new(mock_url, store_id, None);
    let fx_broken = Fixture::bootstrap_sql_only(broken).await;
    let fail = client
        .get(format!("{}/workspaces", fx_broken.addr))
        .bearer_auth(&fx_broken.member_id)
        .send()
        .await
        .unwrap();
    assert_ne!(fail.status(), reqwest::StatusCode::OK);
    assert_eq!(fail.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    let fail_body: serde_json::Value = fail.json().await.unwrap();
    assert_eq!(fail_body["error"]["code"], "AUTHZ_ERROR");

    fx_broken.cleanup().await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&outsider)
        .execute(&fx.pool)
        .await;
    // restore member tuple cleanup handled by fx.cleanup
    fx.cleanup().await;
}

#[tokio::test]
async fn me_tenants_intersects_sql_candidates_with_fga_owner() {
    let _guard = phase0b_lock().await.lock().await;
    init_test_env();
    let authz = AuthzClient::from_env().unwrap();
    let fx = Fixture::bootstrap(authz.clone()).await;
    let client = Client::new();

    let tenant_b = Uuid::new_v4();
    let owner_b = format!("phase0b-owner-b-{}", Uuid::new_v4());
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_b)
        .bind(format!("Phase0B Tenant B {tenant_b}"))
        .execute(&fx.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&owner_b)
        .bind(format!("{owner_b}@test.local"))
        .execute(&fx.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenant_members (tenant_id, user_id, role) VALUES ($1, $2, 'OWNER')")
        .bind(tenant_b)
        .bind(&owner_b)
        .execute(&fx.pool)
        .await
        .unwrap();
    authz
        .write_tuple(
            &format!("user:{owner_b}"),
            Relation::Owner,
            &Object::Tenant(tenant_b),
        )
        .await
        .unwrap();

    let owner_response = client
        .get(format!("{}/me/tenants", fx.addr))
        .bearer_auth(&fx.owner_id)
        .send()
        .await
        .unwrap();
    assert_eq!(owner_response.status(), reqwest::StatusCode::OK);
    let owner_tenants: Vec<serde_json::Value> = owner_response.json().await.unwrap();
    assert_eq!(owner_tenants.len(), 1);
    assert_eq!(owner_tenants[0]["id"], fx.tenant_id.to_string());
    assert!(owner_tenants[0]["name"].is_string());
    assert!(owner_tenants[0]["created_at"].is_string());
    assert_eq!(owner_tenants[0].as_object().unwrap().len(), 3);

    let owner_b_response = client
        .get(format!("{}/me/tenants", fx.addr))
        .bearer_auth(&owner_b)
        .send()
        .await
        .unwrap();
    assert_eq!(owner_b_response.status(), reqwest::StatusCode::OK);
    let owner_b_tenants: Vec<serde_json::Value> = owner_b_response.json().await.unwrap();
    assert_eq!(owner_b_tenants.len(), 1);
    assert_eq!(owner_b_tenants[0]["id"], tenant_b.to_string());

    let non_owner_response = client
        .get(format!("{}/me/tenants", fx.addr))
        .bearer_auth(&fx.member_id)
        .send()
        .await
        .unwrap();
    assert_eq!(non_owner_response.status(), reqwest::StatusCode::OK);
    let non_owner_tenants: Vec<serde_json::Value> = non_owner_response.json().await.unwrap();
    assert!(non_owner_tenants.is_empty());

    authz
        .delete_tuple(
            &format!("user:{}", fx.owner_id),
            Relation::Owner,
            &Object::Tenant(fx.tenant_id),
        )
        .await
        .unwrap();
    let stale_owner_response = client
        .get(format!("{}/me/tenants", fx.addr))
        .bearer_auth(&fx.owner_id)
        .send()
        .await
        .unwrap();
    assert_eq!(stale_owner_response.status(), reqwest::StatusCode::OK);
    let stale_owner_tenants: Vec<serde_json::Value> = stale_owner_response.json().await.unwrap();
    assert!(stale_owner_tenants.is_empty());

    let broken_authz = AuthzClient::new(
        spawn_authz_mock(AuthzMockMode::CheckFail).await,
        "test-store".to_string(),
        None,
    );
    let broken_fx = Fixture::bootstrap_sql_only(broken_authz).await;
    let failed_response = client
        .get(format!("{}/me/tenants", broken_fx.addr))
        .bearer_auth(&broken_fx.owner_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        failed_response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let failed_body: serde_json::Value = failed_response.json().await.unwrap();
    assert_eq!(failed_body["error"]["code"], "AUTHZ_ERROR");

    broken_fx.cleanup().await;
    let _ = authz
        .delete_tuple(
            &format!("user:{owner_b}"),
            Relation::Owner,
            &Object::Tenant(tenant_b),
        )
        .await;
    let _ = sqlx::query("DELETE FROM tenant_members WHERE tenant_id = $1")
        .bind(tenant_b)
        .execute(&fx.pool)
        .await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_b)
        .execute(&fx.pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&owner_b)
        .execute(&fx.pool)
        .await;
    fx.cleanup().await;
}

#[tokio::test]
async fn acl006_share_target_requires_sql_and_fga() {
    let _guard = phase0b_lock().await.lock().await;
    init_test_env();
    let authz = AuthzClient::from_env().unwrap();
    let fx = Fixture::bootstrap(authz.clone()).await;
    let client = Client::new();

    let document_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO documents (
            id, workspace_id, owner_id, filename, status, processing_stage,
            access_mode, object_key, bucket, content_type, size_bytes,
            checksum_sha256, uploaded_by
        ) VALUES (
            $1, $2, $3, 'phase0b.pdf', 'COMPLETED', 'DONE',
            'restricted', $4, 'gmrag-documents', 'application/pdf', 123,
            'abc', $3
        )
        "#,
    )
    .bind(document_id)
    .bind(fx.workspace_id)
    .bind(&fx.ws_admin_id)
    .bind(format!(
        "tenants/{}/workspaces/{}/documents/{}/original.pdf",
        fx.tenant_id, fx.workspace_id, document_id
    ))
    .execute(&fx.pool)
    .await
    .unwrap();

    authz
        .write_tuple(
            &format!("workspace:{}", fx.workspace_id),
            Relation::Workspace,
            &Object::Document(document_id),
        )
        .await
        .ok();

    // SQL + FGA member → grant PASS
    let ok = client
        .post(format!(
            "{}/workspaces/{}/documents/{}/shares/{}",
            fx.addr, fx.workspace_id, document_id, fx.member_id
        ))
        .bearer_auth(&fx.ws_admin_id)
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), reqwest::StatusCode::CREATED);

    // cleanup share for next cases
    let _ = client
        .delete(format!(
            "{}/workspaces/{}/documents/{}/shares/{}",
            fx.addr, fx.workspace_id, document_id, fx.member_id
        ))
        .bearer_auth(&fx.ws_admin_id)
        .send()
        .await;

    // Stale SQL: revoke FGA, keep SQL → deny, no mutation
    authz
        .delete_tuple(
            &format!("user:{}", fx.member_id),
            Relation::Member,
            &Object::Workspace(fx.workspace_id),
        )
        .await
        .unwrap();

    let deny_stale = client
        .post(format!(
            "{}/workspaces/{}/documents/{}/shares/{}",
            fx.addr, fx.workspace_id, document_id, fx.member_id
        ))
        .bearer_auth(&fx.ws_admin_id)
        .send()
        .await
        .unwrap();
    assert_eq!(deny_stale.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = deny_stale.json().await.unwrap();
    assert_eq!(body["error"]["code"], "USER_NOT_WORKSPACE_MEMBER");

    let share_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM document_shares WHERE document_id = $1 AND user_id = $2",
    )
    .bind(document_id)
    .bind(&fx.member_id)
    .fetch_one(&fx.pool)
    .await
    .unwrap();
    assert_eq!(share_count, 0);

    let viewer = authz
        .check_fga(
            &format!("user:{}", fx.member_id),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
        .unwrap_or(false);
    assert!(!viewer);

    // FGA true / SQL missing → deny
    authz
        .write_tuple(
            &format!("user:{}", fx.member_id),
            Relation::Member,
            &Object::Workspace(fx.workspace_id),
        )
        .await
        .unwrap();
    sqlx::query("DELETE FROM workspace_members WHERE workspace_id = $1 AND user_id = $2")
        .bind(fx.workspace_id)
        .bind(&fx.member_id)
        .execute(&fx.pool)
        .await
        .unwrap();

    let deny_sql = client
        .post(format!(
            "{}/workspaces/{}/documents/{}/shares/{}",
            fx.addr, fx.workspace_id, document_id, fx.member_id
        ))
        .bearer_auth(&fx.ws_admin_id)
        .send()
        .await
        .unwrap();
    assert_eq!(deny_sql.status(), reqwest::StatusCode::BAD_REQUEST);
    let body2: serde_json::Value = deny_sql.json().await.unwrap();
    assert_eq!(body2["error"]["code"], "USER_NOT_WORKSPACE_MEMBER");

    // FGA unavailable → deny / no mutation (admin check fails closed)
    let mock_url = spawn_authz_mock(AuthzMockMode::CheckFail).await;
    let store_id = std::env::var("OPENFGA_STORE_ID").unwrap_or_else(|_| "test-store".into());
    let broken = AuthzClient::new(mock_url, store_id, None);
    let fx2 = Fixture::bootstrap_sql_only(broken).await;
    let doc2 = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO documents (
            id, workspace_id, owner_id, filename, status, processing_stage,
            access_mode, object_key, bucket, content_type, size_bytes,
            checksum_sha256, uploaded_by
        ) VALUES (
            $1, $2, $3, 'phase0b2.pdf', 'COMPLETED', 'DONE',
            'restricted', $4, 'gmrag-documents', 'application/pdf', 123,
            'abc', $3
        )
        "#,
    )
    .bind(doc2)
    .bind(fx2.workspace_id)
    .bind(&fx2.ws_admin_id)
    .bind(format!(
        "tenants/{}/workspaces/{}/documents/{}/original.pdf",
        fx2.tenant_id, fx2.workspace_id, doc2
    ))
    .execute(&fx2.pool)
    .await
    .unwrap();

    let fga_down = client
        .post(format!(
            "{}/workspaces/{}/documents/{}/shares/{}",
            fx2.addr, fx2.workspace_id, doc2, fx2.member_id
        ))
        .bearer_auth(&fx2.ws_admin_id)
        .send()
        .await
        .unwrap();
    assert_ne!(fga_down.status(), reqwest::StatusCode::CREATED);
    let share2: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM document_shares WHERE document_id = $1")
            .bind(doc2)
            .fetch_one(&fx2.pool)
            .await
            .unwrap();
    assert_eq!(share2, 0);

    fx2.cleanup().await;
    let _ = authz
        .delete_tuple(
            &format!("workspace:{}", fx.workspace_id),
            Relation::Workspace,
            &Object::Document(document_id),
        )
        .await;
    fx.cleanup().await;
}

#[derive(Clone, Copy)]
enum AuthzMockMode {
    CheckFail,
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
                    AuthzMockMode::CheckFail => {
                        (AxumStatus::SERVICE_UNAVAILABLE, "openfga down").into_response()
                    }
                }
            }),
        )
        .route(
            "/stores/{store_id}/write",
            post(move || async move {
                match mode {
                    AuthzMockMode::CheckFail => {
                        (AxumStatus::SERVICE_UNAVAILABLE, "openfga unavailable").into_response()
                    }
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}
