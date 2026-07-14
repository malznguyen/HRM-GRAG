mod support;

use axum::{Json, Router, http::StatusCode, routing::post};
use gmrag_api::{
    auth::{
        authz::{AuthzClient, Object, Relation},
        keycloak::KeycloakClient,
    },
    workspace_admin_recovery::{
        RecoveryMode, RecoveryOutcome, RecoveryTarget, recover_workspace_admin,
    },
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn recovery_drill_is_dry_run_safe_idempotent_and_fail_closed() {
    init_test_env();
    let pool = setup_pool().await;
    let authz = AuthzClient::from_env().expect("OpenFGA config");
    let keycloak = KeycloakClient::from_env().expect("Keycloak test bypass");
    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Phase 4 recovery {tenant_id}"))
        .execute(&pool)
        .await
        .expect("tenant");
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind("Recovery drill")
        .execute(&pool)
        .await
        .expect("workspace");

    let target = RecoveryTarget::Email("recovery-target@phase0b.test".to_string());
    let dry_run = recover_workspace_admin(
        &pool,
        &authz,
        &keycloak,
        workspace_id,
        target.clone(),
        RecoveryMode::DryRun,
    )
    .await
    .expect("dry run");
    assert!(matches!(dry_run, RecoveryOutcome::WouldRecover { .. }));
    let member_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM workspace_members WHERE workspace_id = $1")
            .bind(workspace_id)
            .fetch_one(&pool)
            .await
            .expect("dry run must not mutate SQL");
    assert_eq!(member_count, 0);

    let applied = recover_workspace_admin(
        &pool,
        &authz,
        &keycloak,
        workspace_id,
        target.clone(),
        RecoveryMode::Apply,
    )
    .await
    .expect("apply");
    let target_user_id = match applied {
        RecoveryOutcome::Recovered { target_user_id } => target_user_id,
        other => panic!("unexpected recovery result: {other:?}"),
    };
    let role: String = sqlx::query_scalar(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(workspace_id)
    .bind(&target_user_id)
    .fetch_one(&pool)
    .await
    .expect("recovery SQL ADMIN row");
    assert_eq!(role, "ADMIN");
    assert!(
        authz
            .check_workspace_admin(&target_user_id, workspace_id)
            .await
            .expect("recovery OpenFGA tuple")
    );
    let audit_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE workspace_id = $1 AND event_type = 'workspace_admin_recovered'")
        .bind(workspace_id)
        .fetch_one(&pool)
        .await
        .expect("recovery audit");
    assert_eq!(audit_count, 1);

    let idempotent = recover_workspace_admin(
        &pool,
        &authz,
        &keycloak,
        workspace_id,
        target,
        RecoveryMode::Apply,
    )
    .await
    .expect("idempotent apply");
    assert_eq!(idempotent, RecoveryOutcome::AlreadyHealthy);

    let missing = recover_workspace_admin(
        &pool,
        &authz,
        &keycloak,
        Uuid::new_v4(),
        RecoveryTarget::Email("recovery-target@phase0b.test".to_string()),
        RecoveryMode::DryRun,
    )
    .await
    .expect("missing workspace is a normal outcome");
    assert_eq!(missing, RecoveryOutcome::WorkspaceNotFound);

    authz
        .delete_tuple(
            &format!("user:{target_user_id}"),
            Relation::Admin,
            &Object::Workspace(workspace_id),
        )
        .await
        .expect("test cleanup FGA");
    sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(&pool)
        .await
        .expect("test cleanup SQL");
}

#[tokio::test]
async fn recovery_rejects_unknown_identity_and_records_partial_fga_failure() {
    init_test_env();
    let pool = setup_pool().await;
    let keycloak = KeycloakClient::from_env().expect("Keycloak test bypass");
    let unknown = recover_workspace_admin(
        &pool,
        &AuthzClient::new("http://127.0.0.1:1".to_string(), "unused".to_string(), None),
        &keycloak,
        Uuid::new_v4(),
        RecoveryTarget::Email("unknown@example.invalid".to_string()),
        RecoveryMode::DryRun,
    )
    .await;
    assert!(matches!(
        unknown,
        Err(gmrag_api::workspace_admin_recovery::RecoveryError::IdentityNotVerified)
    ));

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Phase 4 partial {tenant_id}"))
        .execute(&pool)
        .await
        .expect("tenant");
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind("Partial recovery drill")
        .execute(&pool)
        .await
        .expect("workspace");
    let failing_authz = AuthzClient::new(
        spawn_failing_write_authz().await,
        "test-store".to_string(),
        None,
    );
    let result = recover_workspace_admin(
        &pool,
        &failing_authz,
        &keycloak,
        workspace_id,
        RecoveryTarget::Email("recovery-partial@phase0b.test".to_string()),
        RecoveryMode::Apply,
    )
    .await;
    assert!(matches!(
        result,
        Err(gmrag_api::workspace_admin_recovery::RecoveryError::PartialFailure)
    ));
    let outbox_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM authz_outbox WHERE event_type = 'tuple_write' AND status = 'PENDING' AND payload::text LIKE '%phase0b-recovery-partial%'")
        .fetch_one(&pool)
        .await
        .expect("partial recovery must enqueue tuple write");
    assert!(outbox_count >= 1);
    sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(&pool)
        .await
        .expect("test cleanup SQL");
}

#[tokio::test]
async fn concurrent_recovery_keeps_one_sql_admin_and_one_fga_relation() {
    init_test_env();
    let pool = setup_pool().await;
    let authz = AuthzClient::from_env().expect("OpenFGA config");
    let keycloak = KeycloakClient::from_env().expect("Keycloak test bypass");
    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Phase 4 concurrent {tenant_id}"))
        .execute(&pool)
        .await
        .expect("tenant");
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind("Concurrent recovery drill")
        .execute(&pool)
        .await
        .expect("workspace");
    let target = RecoveryTarget::Email("recovery-concurrent@phase0b.test".to_string());
    let (first, second) = tokio::join!(
        recover_workspace_admin(
            &pool,
            &authz,
            &keycloak,
            workspace_id,
            target.clone(),
            RecoveryMode::Apply
        ),
        recover_workspace_admin(
            &pool,
            &authz,
            &keycloak,
            workspace_id,
            target,
            RecoveryMode::Apply
        ),
    );
    assert!(first.is_ok());
    assert!(second.is_ok());
    let user_id = "phase0b-recovery-concurrent";
    let member_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(workspace_id)
    .bind(user_id)
    .fetch_one(&pool)
    .await
    .expect("one SQL membership");
    assert_eq!(member_count, 1);
    assert!(
        authz
            .check_workspace_admin(user_id, workspace_id)
            .await
            .expect("one FGA relation")
    );
    authz
        .delete_tuple(
            &format!("user:{user_id}"),
            Relation::Admin,
            &Object::Workspace(workspace_id),
        )
        .await
        .expect("test cleanup FGA");
    sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(tenant_id)
        .execute(&pool)
        .await
        .expect("test cleanup SQL");
}

async fn spawn_failing_write_authz() -> String {
    let app = Router::new()
        .route(
            "/stores/{store_id}/check",
            post(|| async { Json(json!({ "allowed": false })) }),
        )
        .route(
            "/stores/{store_id}/write",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("authz stub listener");
    let address = listener.local_addr().expect("authz stub address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("authz stub server");
    });
    format!("http://{address}")
}

fn init_test_env() {
    dotenvy::dotenv().ok();
    unsafe {
        std::env::set_var("APP_ENV", "test");
        std::env::set_var("TEST_BYPASS_JWT", "1");
        std::env::set_var("TEST_BYPASS_KEYCLOAK", "1");
    }
}

async fn setup_pool() -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&support::database_url().expect("DATABASE_URL"))
        .await
        .expect("PostgreSQL");
    match sqlx::migrate!("./migrations").run(&pool).await {
        Ok(_) | Err(sqlx::migrate::MigrateError::VersionMismatch(_)) => {}
        Err(error) => panic!("migration failed: {error}"),
    }
    pool
}
