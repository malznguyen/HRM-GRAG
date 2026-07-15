mod support;

use std::sync::{Arc, OnceLock};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::{post, put},
};
use reqwest::Client;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{Mutex, Semaphore};
use uuid::Uuid;

use gmrag_api::auth::authz::{Object, Relation};
use gmrag_api::auth::document_acl::backfill_document_workspace_relations;
use gmrag_api::ingestion::embedding::DEFAULT_EMBEDDING_DIM;
use gmrag_api::ingestion::graph::{GraphElement, GraphWriteBatch, bulk_upsert_graph};
use gmrag_api::state::AppState;

const EMBEDDING_DIM: usize = DEFAULT_EMBEDDING_DIM;
static PHASE2_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn phase2_test_lock() -> &'static Mutex<()> {
    PHASE2_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Default)]
struct QdrantMockInner {
    search_results: Vec<Uuid>,
    search_payloads: Vec<Value>,
}

#[derive(Clone, Default)]
struct QdrantMockState {
    inner: Arc<Mutex<QdrantMockInner>>,
}

impl QdrantMockState {
    async fn set_search_results(&self, results: Vec<Uuid>) {
        let mut guard = self.inner.lock().await;
        guard.search_results = results;
    }

    async fn latest_search_payload(&self) -> Option<Value> {
        let guard = self.inner.lock().await;
        guard.search_payloads.last().cloned()
    }
}

#[derive(Default)]
struct DeepseekMockInner {
    answer: String,
    requests: Vec<Value>,
}

#[derive(Clone, Default)]
struct DeepseekMockState {
    inner: Arc<Mutex<DeepseekMockInner>>,
}

impl DeepseekMockState {
    async fn set_answer(&self, answer: &str) {
        let mut guard = self.inner.lock().await;
        guard.answer = answer.to_string();
    }

    async fn latest_request(&self) -> Option<Value> {
        let guard = self.inner.lock().await;
        guard.requests.last().cloned()
    }
}

struct TestServer {
    addr: String,
    pool: sqlx::PgPool,
    state: AppState,
    qdrant_mock: QdrantMockState,
    deepseek_mock: DeepseekMockState,
}

impl TestServer {
    async fn bootstrap() -> Self {
        dotenvy::dotenv().ok();
        support::require_external_test_environment();

        let qdrant_mock = QdrantMockState::default();
        let deepseek_mock = DeepseekMockState::default();

        let qdrant_addr = spawn_mock_server(qdrant_router(qdrant_mock.clone())).await;
        let ollama_addr = spawn_mock_server(ollama_router()).await;
        let deepseek_addr = spawn_mock_server(deepseek_router(deepseek_mock.clone())).await;

        init_test_env(&qdrant_addr, &ollama_addr, &deepseek_addr);

        let database_url = support::database_url().expect("DATABASE_URL must be set");
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

        Self {
            addr,
            pool,
            state,
            qdrant_mock,
            deepseek_mock,
        }
    }
}

#[tokio::test]
async fn phase2_document_acl_and_qdrant_enforcement() {
    let _guard = phase2_test_lock().lock().await;
    let server = TestServer::bootstrap().await;

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let platform_admin = format!("phase2-platform-admin-{}", Uuid::new_v4());
    let tenant_owner = format!("phase2-tenant-owner-{}", Uuid::new_v4());
    let member_user = format!("phase2-member-{}", Uuid::new_v4());
    let explicit_viewer = format!("phase2-viewer-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Test Tenant {tenant_id}"))
        .execute(&server.pool)
        .await
        .unwrap();

    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("Test Workspace {workspace_id}"))
        .execute(&server.pool)
        .await
        .unwrap();

    for user in [
        &platform_admin,
        &tenant_owner,
        &member_user,
        &explicit_viewer,
    ] {
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user)
            .bind(format!("{user}@test.local"))
            .execute(&server.pool)
            .await
            .unwrap();
    }

    sqlx::query("INSERT INTO tenant_members (tenant_id, user_id, role) VALUES ($1, $2, 'OWNER')")
        .bind(tenant_id)
        .bind(&tenant_owner)
        .execute(&server.pool)
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'MEMBER')",
    )
    .bind(workspace_id)
    .bind(&member_user)
    .execute(&server.pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'MEMBER')",
    )
    .bind(workspace_id)
    .bind(&explicit_viewer)
    .execute(&server.pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
    )
    .bind(workspace_id)
    .bind(&tenant_owner)
    .execute(&server.pool)
    .await
    .unwrap();

    let first_provenance_doc_id = Uuid::new_v4();
    let second_provenance_doc_id = Uuid::new_v4();
    let (restricted_doc_id, public_doc_id) = if first_provenance_doc_id < second_provenance_doc_id {
        (first_provenance_doc_id, second_provenance_doc_id)
    } else {
        (second_provenance_doc_id, first_provenance_doc_id)
    };
    let legacy_public_doc_id = Uuid::new_v4();

    insert_document(
        &server.pool,
        workspace_id,
        public_doc_id,
        &member_user,
        "public-doc.pdf",
        "workspace_default",
    )
    .await;
    insert_document(
        &server.pool,
        workspace_id,
        legacy_public_doc_id,
        &member_user,
        "legacy-public-doc.pdf",
        "workspace_default",
    )
    .await;
    insert_document(
        &server.pool,
        workspace_id,
        restricted_doc_id,
        &member_user,
        "restricted-doc.pdf",
        "restricted",
    )
    .await;

    let public_chunk_id = Uuid::new_v4();
    let legacy_public_chunk_id = Uuid::new_v4();
    let restricted_chunk_id = Uuid::new_v4();

    insert_chunk(
        &server.pool,
        public_chunk_id,
        public_doc_id,
        workspace_id,
        0,
        "public alpha content",
        0.01,
    )
    .await;
    insert_chunk(
        &server.pool,
        legacy_public_chunk_id,
        legacy_public_doc_id,
        workspace_id,
        0,
        "legacy public content",
        0.02,
    )
    .await;
    insert_chunk(
        &server.pool,
        restricted_chunk_id,
        restricted_doc_id,
        workspace_id,
        0,
        "secret beta content",
        0.99,
    )
    .await;

    sqlx::query("INSERT INTO document_shares (document_id, user_id) VALUES ($1, $2)")
        .bind(restricted_doc_id)
        .bind(&explicit_viewer)
        .execute(&server.pool)
        .await
        .unwrap();

    seed_graph(&server.pool, workspace_id, public_doc_id, restricted_doc_id).await;

    seed_openfga(
        &server.state,
        tenant_id,
        workspace_id,
        public_doc_id,
        restricted_doc_id,
        &platform_admin,
        &tenant_owner,
        &member_user,
        &explicit_viewer,
    )
    .await;

    let client = Client::new();

    // List filtering and restricted allow/deny.
    let member_docs = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(member_docs.status(), reqwest::StatusCode::OK);
    let member_docs_json: Value = member_docs.json().await.unwrap();
    let member_doc_ids: Vec<String> = member_docs_json["documents"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();
    assert!(member_doc_ids.contains(&public_doc_id.to_string()));
    assert!(member_doc_ids.contains(&legacy_public_doc_id.to_string()));
    assert!(!member_doc_ids.contains(&restricted_doc_id.to_string()));

    let legacy_preview = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{legacy_public_doc_id}/preview",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(legacy_preview.status(), reqwest::StatusCode::OK);

    let legacy_chunk = client
        .get(format!(
            "{}/workspaces/{workspace_id}/chunks/{legacy_public_chunk_id}",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(legacy_chunk.status(), reqwest::StatusCode::OK);

    let viewer_docs = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents",
            server.addr
        ))
        .bearer_auth(&explicit_viewer)
        .send()
        .await
        .unwrap();
    assert_eq!(viewer_docs.status(), reqwest::StatusCode::OK);
    let viewer_docs_json: Value = viewer_docs.json().await.unwrap();
    let viewer_doc_ids: Vec<String> = viewer_docs_json["documents"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();
    assert!(viewer_doc_ids.contains(&restricted_doc_id.to_string()));

    let owner_docs = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents",
            server.addr
        ))
        .bearer_auth(&tenant_owner)
        .send()
        .await
        .unwrap();
    assert_eq!(owner_docs.status(), reqwest::StatusCode::OK);
    let owner_docs_json: Value = owner_docs.json().await.unwrap();
    let owner_doc_ids: Vec<String> = owner_docs_json["documents"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();
    assert!(owner_doc_ids.contains(&restricted_doc_id.to_string()));

    let platform_preview = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{restricted_doc_id}/preview",
            server.addr
        ))
        .bearer_auth(&platform_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(platform_preview.status(), reqwest::StatusCode::FORBIDDEN);

    let member_preview = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{restricted_doc_id}/preview",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(member_preview.status(), reqwest::StatusCode::NOT_FOUND);
    let member_preview_body = member_preview.text().await.unwrap();

    let unknown_document_id = Uuid::new_v4();
    let unknown_preview = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{unknown_document_id}/preview",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(unknown_preview.status(), reqwest::StatusCode::NOT_FOUND);
    let unknown_preview_body = unknown_preview.text().await.unwrap();
    assert_eq!(member_preview_body, unknown_preview_body);
    let preview_error: Value = serde_json::from_str(&member_preview_body).unwrap();
    assert_eq!(preview_error["error"]["code"], "RESOURCE_NOT_FOUND");
    assert!(preview_error["error"]["message"].is_string());

    let viewer_preview = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{restricted_doc_id}/preview",
            server.addr
        ))
        .bearer_auth(&explicit_viewer)
        .send()
        .await
        .unwrap();
    assert_eq!(viewer_preview.status(), reqwest::StatusCode::OK);

    let owner_preview = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{restricted_doc_id}/preview",
            server.addr
        ))
        .bearer_auth(&tenant_owner)
        .send()
        .await
        .unwrap();
    assert_eq!(owner_preview.status(), reqwest::StatusCode::OK);

    let member_chunk = client
        .get(format!(
            "{}/workspaces/{workspace_id}/chunks/{restricted_chunk_id}",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(member_chunk.status(), reqwest::StatusCode::NOT_FOUND);
    let member_chunk_body = member_chunk.text().await.unwrap();

    let unknown_chunk_id = Uuid::new_v4();
    let unknown_chunk = client
        .get(format!(
            "{}/workspaces/{workspace_id}/chunks/{unknown_chunk_id}",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(unknown_chunk.status(), reqwest::StatusCode::NOT_FOUND);
    let unknown_chunk_body = unknown_chunk.text().await.unwrap();
    assert_eq!(member_chunk_body, unknown_chunk_body);
    let chunk_error: Value = serde_json::from_str(&member_chunk_body).unwrap();
    assert_eq!(chunk_error["error"]["code"], "RESOURCE_NOT_FOUND");
    assert!(chunk_error["error"]["message"].is_string());

    let viewer_chunk = client
        .get(format!(
            "{}/workspaces/{workspace_id}/chunks/{restricted_chunk_id}",
            server.addr
        ))
        .bearer_auth(&explicit_viewer)
        .send()
        .await
        .unwrap();
    assert_eq!(viewer_chunk.status(), reqwest::StatusCode::OK);

    let owner_chunk = client
        .get(format!(
            "{}/workspaces/{workspace_id}/chunks/{restricted_chunk_id}",
            server.addr
        ))
        .bearer_auth(&tenant_owner)
        .send()
        .await
        .unwrap();
    assert_eq!(owner_chunk.status(), reqwest::StatusCode::OK);

    let member_graph = client
        .get(format!("{}/workspaces/{workspace_id}/graph", server.addr))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(member_graph.status(), reqwest::StatusCode::OK);
    let member_graph_json: Value = member_graph.json().await.unwrap();
    let member_shared_node = member_graph_json["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["entity_name"] == "SharedEntity")
        .unwrap();
    assert_eq!(member_shared_node["entity_type"], "public_type");
    assert_eq!(
        member_shared_node["description"],
        "public graph node description"
    );
    let member_shared_link = member_graph_json["links"]
        .as_array()
        .unwrap()
        .iter()
        .find(|link| link["relationship"] == "shared_relation")
        .unwrap();
    assert_eq!(
        member_shared_link["description"],
        "public graph edge description"
    );
    assert!(!member_graph_json.to_string().contains("restricted graph"));
    assert!(
        !member_graph_json
            .to_string()
            .contains("restricted_only_relation")
    );

    let viewer_graph = client
        .get(format!("{}/workspaces/{workspace_id}/graph", server.addr))
        .bearer_auth(&explicit_viewer)
        .send()
        .await
        .unwrap();
    assert_eq!(viewer_graph.status(), reqwest::StatusCode::OK);
    let viewer_graph_json: Value = viewer_graph.json().await.unwrap();
    let viewer_shared_node = viewer_graph_json["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["entity_name"] == "SharedEntity")
        .unwrap();
    assert_eq!(viewer_shared_node["entity_type"], "restricted_type");
    assert_eq!(
        viewer_shared_node["description"],
        "restricted graph node description"
    );
    let viewer_graph_text = viewer_graph_json.to_string();
    assert!(viewer_graph_text.contains("restricted graph edge description"));
    assert!(viewer_graph_text.contains("restricted_only_relation"));

    let platform_graph = client
        .get(format!("{}/workspaces/{workspace_id}/graph", server.addr))
        .bearer_auth(&platform_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(platform_graph.status(), reqwest::StatusCode::FORBIDDEN);

    let outsider_graph = client
        .get(format!("{}/workspaces/{workspace_id}/graph", server.addr))
        .bearer_auth(format!("phase2-outsider-{}", Uuid::new_v4()))
        .send()
        .await
        .unwrap();
    assert_eq!(outsider_graph.status(), reqwest::StatusCode::FORBIDDEN);

    // Citation-facing endpoint re-check for historical messages.
    let history_session_member = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO chat_sessions (id, workspace_id, user_id, title) VALUES ($1, $2, $3, $4)",
    )
    .bind(history_session_member)
    .bind(workspace_id)
    .bind(&member_user)
    .bind("acl-history")
    .execute(&server.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO chat_messages (session_id, role, content, citations) VALUES ($1, 'assistant', $2, $3)",
    )
    .bind(history_session_member)
    .bind(format!("restricted citation [chunk:{restricted_chunk_id}]"))
    .bind(json!([restricted_chunk_id]))
    .execute(&server.pool)
    .await
    .unwrap();

    let member_history = client
        .get(format!(
            "{}/workspaces/{workspace_id}/chat/history?session_id={history_session_member}",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(member_history.status(), reqwest::StatusCode::OK);
    let member_history_json: Value = member_history.json().await.unwrap();
    let member_first_citations = member_history_json
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|msg| msg.get("citations"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(member_first_citations.is_empty());

    // Chat path: filter-then-search payload + re-check before prompt assembly/citation output.
    server
        .qdrant_mock
        .set_search_results(vec![public_chunk_id, restricted_chunk_id])
        .await;
    server
        .deepseek_mock
        .set_answer("Safe answer from public doc [chunk:1]")
        .await;

    let live_session = Uuid::new_v4();
    let chat_resp = client
        .post(format!("{}/workspaces/{workspace_id}/chat", server.addr))
        .bearer_auth(&member_user)
        .json(&json!({
            "session_id": live_session,
            "message": "What is in our docs?"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(chat_resp.status(), reqwest::StatusCode::OK);
    let _ = chat_resp.text().await.unwrap();

    let deepseek_req = server.deepseek_mock.latest_request().await.unwrap();
    let system_prompt = deepseek_req["messages"][0]["content"]
        .as_str()
        .unwrap_or_default();
    assert!(system_prompt.contains("public alpha content"));
    assert!(!system_prompt.contains("secret beta content"));
    assert!(system_prompt.contains("public graph node description"));
    assert!(system_prompt.contains("public graph edge description"));
    assert!(!system_prompt.contains("restricted graph node description"));
    assert!(!system_prompt.contains("restricted graph edge description"));
    assert!(!system_prompt.contains("restricted_only_relation"));

    let qdrant_payload = server.qdrant_mock.latest_search_payload().await.unwrap();
    let must_conditions = qdrant_payload["filter"]["must"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let workspace_filter = must_conditions
        .iter()
        .find(|cond| cond.get("key").and_then(Value::as_str) == Some("workspace_id"))
        .cloned()
        .unwrap();
    assert_eq!(
        workspace_filter["match"]["value"].as_str(),
        Some(workspace_id.to_string().as_str())
    );

    let document_filter = must_conditions
        .iter()
        .find(|cond| cond.get("key").and_then(Value::as_str) == Some("document_id"))
        .cloned()
        .unwrap();
    let allowed_docs = document_filter["match"]["any"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| value.as_str().map(str::to_string))
        .collect::<Vec<_>>();
    assert!(allowed_docs.contains(&public_doc_id.to_string()));
    assert!(!allowed_docs.contains(&restricted_doc_id.to_string()));

    let live_history = client
        .get(format!(
            "{}/workspaces/{workspace_id}/chat/history?session_id={live_session}",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(live_history.status(), reqwest::StatusCode::OK);
    let live_history_json: Value = live_history.json().await.unwrap();
    let assistant_msg = live_history_json
        .as_array()
        .unwrap()
        .iter()
        .find(|msg| msg.get("role").and_then(Value::as_str) == Some("assistant"))
        .cloned()
        .unwrap();
    let citations = assistant_msg["citations"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| value.as_str().map(str::to_string))
        .collect::<Vec<_>>();
    assert_eq!(citations, vec![public_chunk_id.to_string()]);
    let assistant_content = assistant_msg["content"].as_str().unwrap_or_default();
    assert!(!assistant_content.contains("secret beta content"));
}

#[tokio::test]
async fn legacy_restricted_docs_need_backfill_for_owner_bypass() {
    let _guard = phase2_test_lock().lock().await;
    let server = TestServer::bootstrap().await;
    let client = Client::new();

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let tenant_owner = format!("phase2-legacy-owner-{}", Uuid::new_v4());
    let restricted_doc_id = Uuid::new_v4();
    let restricted_chunk_id = Uuid::new_v4();

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Legacy Tenant {tenant_id}"))
        .execute(&server.pool)
        .await
        .unwrap();

    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("Legacy Workspace {workspace_id}"))
        .execute(&server.pool)
        .await
        .unwrap();

    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&tenant_owner)
        .bind(format!("{tenant_owner}@test.local"))
        .execute(&server.pool)
        .await
        .unwrap();

    sqlx::query("INSERT INTO tenant_members (tenant_id, user_id, role) VALUES ($1, $2, 'OWNER')")
        .bind(tenant_id)
        .bind(&tenant_owner)
        .execute(&server.pool)
        .await
        .unwrap();

    insert_document(
        &server.pool,
        workspace_id,
        restricted_doc_id,
        &tenant_owner,
        "legacy-restricted.pdf",
        "restricted",
    )
    .await;

    insert_chunk(
        &server.pool,
        restricted_chunk_id,
        restricted_doc_id,
        workspace_id,
        0,
        "legacy restricted chunk",
        0.3,
    )
    .await;

    server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{tenant_owner}"),
            Relation::Owner,
            &Object::Tenant(tenant_id),
        )
        .await
        .unwrap();

    server
        .state
        .authz_client
        .write_tuple(
            &format!("tenant:{tenant_id}"),
            Relation::Tenant,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();

    // Legacy document has no workspace->document tuple yet.
    let before = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{restricted_doc_id}/preview",
            server.addr
        ))
        .bearer_auth(&tenant_owner)
        .send()
        .await
        .unwrap();
    assert_eq!(before.status(), reqwest::StatusCode::NOT_FOUND);

    let result = backfill_document_workspace_relations(&server.pool, &server.state.authz_client)
        .await
        .unwrap();
    assert!(result.inserted_relations >= 1);

    let after = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{restricted_doc_id}/preview",
            server.addr
        ))
        .bearer_auth(&tenant_owner)
        .send()
        .await
        .unwrap();
    assert_eq!(after.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn phase2_access_mode_and_share_endpoints_allow_and_deny() {
    let _guard = phase2_test_lock().lock().await;
    let server = TestServer::bootstrap().await;

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let platform_admin = format!("phase2-mgmt-platform-{}", Uuid::new_v4());
    let workspace_admin = format!("phase2-mgmt-admin-{}", Uuid::new_v4());
    let member_user = format!("phase2-mgmt-member-{}", Uuid::new_v4());
    let share_target = format!("phase2-mgmt-share-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Mgmt Tenant {tenant_id}"))
        .execute(&server.pool)
        .await
        .unwrap();

    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("Mgmt Workspace {workspace_id}"))
        .execute(&server.pool)
        .await
        .unwrap();

    for user in [
        &platform_admin,
        &workspace_admin,
        &member_user,
        &share_target,
    ] {
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user)
            .bind(format!("{user}@test.local"))
            .execute(&server.pool)
            .await
            .unwrap();
    }

    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'ADMIN')",
    )
    .bind(workspace_id)
    .bind(&workspace_admin)
    .execute(&server.pool)
    .await
    .unwrap();

    for member in [&member_user, &share_target] {
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'MEMBER')",
        )
        .bind(workspace_id)
        .bind(member)
        .execute(&server.pool)
        .await
        .unwrap();
    }

    let document_id = Uuid::new_v4();
    insert_document(
        &server.pool,
        workspace_id,
        document_id,
        &workspace_admin,
        "mgmt-doc.pdf",
        "workspace_default",
    )
    .await;

    let chunk_id = Uuid::new_v4();
    insert_chunk(
        &server.pool,
        chunk_id,
        document_id,
        workspace_id,
        0,
        "mgmt secret content",
        0.42,
    )
    .await;

    server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{platform_admin}"),
            Relation::Admin,
            &Object::Platform,
        )
        .await
        .unwrap();
    server
        .state
        .authz_client
        .write_tuple(
            "platform:system",
            Relation::Platform,
            &Object::Tenant(tenant_id),
        )
        .await
        .unwrap();
    server
        .state
        .authz_client
        .write_tuple(
            &format!("tenant:{tenant_id}"),
            Relation::Tenant,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();
    server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{workspace_admin}"),
            Relation::Admin,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();
    for member in [&member_user, &share_target] {
        server
            .state
            .authz_client
            .write_tuple(
                &format!("user:{member}"),
                Relation::Member,
                &Object::Workspace(workspace_id),
            )
            .await
            .unwrap();
    }
    write_tuple_idempotent(
        &server.state,
        &format!("workspace:{workspace_id}"),
        Relation::Workspace,
        &Object::Document(document_id),
    )
    .await;

    let client = Client::new();

    // Deny: member cannot patch access_mode.
    let member_patch = client
        .patch(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/access-mode",
            server.addr
        ))
        .bearer_auth(&member_user)
        .json(&json!({ "access_mode": "restricted" }))
        .send()
        .await
        .unwrap();
    assert_eq!(member_patch.status(), reqwest::StatusCode::FORBIDDEN);
    let member_patch_body: Value = member_patch.json().await.unwrap();
    assert_eq!(
        member_patch_body["error"]["code"],
        json!("WORKSPACE_ADMIN_REQUIRED")
    );

    // Allow: admin sets restricted; document becomes hidden from ordinary member.
    let admin_patch = client
        .patch(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/access-mode",
            server.addr
        ))
        .bearer_auth(&workspace_admin)
        .json(&json!({ "access_mode": "restricted" }))
        .send()
        .await
        .unwrap();
    assert_eq!(admin_patch.status(), reqwest::StatusCode::NO_CONTENT);

    let access_mode: String =
        sqlx::query_scalar("SELECT access_mode FROM documents WHERE id = $1 AND workspace_id = $2")
            .bind(document_id)
            .bind(workspace_id)
            .fetch_one(&server.pool)
            .await
            .unwrap();
    assert_eq!(access_mode, "restricted");

    let member_preview_before_share = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/preview",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(
        member_preview_before_share.status(),
        reqwest::StatusCode::NOT_FOUND
    );

    // Deny: member cannot share.
    let member_share = client
        .post(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/shares/{share_target}",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(member_share.status(), reqwest::StatusCode::FORBIDDEN);

    // Allow: admin shares with target member; target can preview.
    let admin_share = client
        .post(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/shares/{share_target}",
            server.addr
        ))
        .bearer_auth(&workspace_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(admin_share.status(), reqwest::StatusCode::CREATED);

    let share_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM document_shares WHERE document_id = $1 AND user_id = $2",
    )
    .bind(document_id)
    .bind(&share_target)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(share_count, 1);

    let shared_preview = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/preview",
            server.addr
        ))
        .bearer_auth(&share_target)
        .send()
        .await
        .unwrap();
    assert_eq!(shared_preview.status(), reqwest::StatusCode::OK);

    // Deny: member cannot revoke.
    let member_revoke = client
        .delete(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/shares/{share_target}",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(member_revoke.status(), reqwest::StatusCode::FORBIDDEN);

    // Allow: admin revokes; target can no longer preview.
    let admin_revoke = client
        .delete(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/shares/{share_target}",
            server.addr
        ))
        .bearer_auth(&workspace_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(admin_revoke.status(), reqwest::StatusCode::NO_CONTENT);

    let share_count_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM document_shares WHERE document_id = $1 AND user_id = $2",
    )
    .bind(document_id)
    .bind(&share_target)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(share_count_after, 0);

    let revoked_preview = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/preview",
            server.addr
        ))
        .bearer_auth(&share_target)
        .send()
        .await
        .unwrap();
    assert_eq!(revoked_preview.status(), reqwest::StatusCode::NOT_FOUND);

    // Gap 2: re-share, then revert to workspace_default — must clean shares + FGA tuples.
    let admin_reshare = client
        .post(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/shares/{share_target}",
            server.addr
        ))
        .bearer_auth(&workspace_admin)
        .send()
        .await
        .unwrap();
    assert_eq!(admin_reshare.status(), reqwest::StatusCode::CREATED);

    let admin_unrestrict = client
        .patch(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/access-mode",
            server.addr
        ))
        .bearer_auth(&workspace_admin)
        .json(&json!({ "access_mode": "workspace_default" }))
        .send()
        .await
        .unwrap();
    assert_eq!(admin_unrestrict.status(), reqwest::StatusCode::NO_CONTENT);

    let access_mode_after: String =
        sqlx::query_scalar("SELECT access_mode FROM documents WHERE id = $1 AND workspace_id = $2")
            .bind(document_id)
            .bind(workspace_id)
            .fetch_one(&server.pool)
            .await
            .unwrap();
    assert_eq!(access_mode_after, "workspace_default");

    let residual_shares: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM document_shares WHERE document_id = $1")
            .bind(document_id)
            .fetch_one(&server.pool)
            .await
            .unwrap();
    assert_eq!(residual_shares, 0);

    let explicit_still_allowed = server
        .state
        .authz_client
        .check_fga(
            &format!("user:{share_target}"),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
        .unwrap();
    assert!(
        !explicit_still_allowed,
        "explicit_viewer tuple must be removed when reverting to workspace_default"
    );

    let member_preview_after_unrestrict = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/preview",
            server.addr
        ))
        .bearer_auth(&member_user)
        .send()
        .await
        .unwrap();
    assert_eq!(
        member_preview_after_unrestrict.status(),
        reqwest::StatusCode::OK
    );
}

#[tokio::test]
async fn documents_list_fields_search_pagination_and_acl_total() {
    let _guard = phase2_test_lock().lock().await;
    let server = TestServer::bootstrap().await;

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let admin = format!("documents-list-admin-{}", Uuid::new_v4());
    let member = format!("documents-list-member-{}", Uuid::new_v4());
    let viewer = format!("documents-list-viewer-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Documents List Tenant {tenant_id}"))
        .execute(&server.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("Documents List Workspace {workspace_id}"))
        .execute(&server.pool)
        .await
        .unwrap();

    for user in [&admin, &member, &viewer] {
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user)
            .bind(format!("{user}@test.local"))
            .execute(&server.pool)
            .await
            .unwrap();
    }
    for (user, role) in [(&admin, "ADMIN"), (&member, "MEMBER"), (&viewer, "MEMBER")] {
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, $3)",
        )
        .bind(workspace_id)
        .bind(user)
        .bind(role)
        .execute(&server.pool)
        .await
        .unwrap();
    }

    server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{admin}"),
            Relation::Admin,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();
    for user in [&member, &viewer] {
        server
            .state
            .authz_client
            .write_tuple(
                &format!("user:{user}"),
                Relation::Member,
                &Object::Workspace(workspace_id),
            )
            .await
            .unwrap();
    }

    let public_alpha = Uuid::new_v4();
    let public_failed = Uuid::new_v4();
    let restricted_alpha = Uuid::new_v4();
    let restricted_hidden = Uuid::new_v4();
    insert_document(
        &server.pool,
        workspace_id,
        public_alpha,
        &admin,
        "Alpha Public.pdf",
        "workspace_default",
    )
    .await;
    insert_document(
        &server.pool,
        workspace_id,
        public_failed,
        &admin,
        "Beta Failed.pdf",
        "workspace_default",
    )
    .await;
    insert_document(
        &server.pool,
        workspace_id,
        restricted_alpha,
        &admin,
        "Alpha Restricted.pdf",
        "restricted",
    )
    .await;
    insert_document(
        &server.pool,
        workspace_id,
        restricted_hidden,
        &admin,
        "Hidden Processing.pdf",
        "restricted",
    )
    .await;

    sqlx::query(
        "UPDATE documents SET status = 'FAILED', processing_stage = 'FAILED', uploaded_by = $2, created_at = CURRENT_TIMESTAMP - INTERVAL '2 minutes' WHERE id = $1",
    )
    .bind(public_failed)
    .bind(format!("missing-uploader-{}", Uuid::new_v4()))
    .execute(&server.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE documents SET status = 'PROCESSING', processing_stage = 'QUEUED', created_at = CURRENT_TIMESTAMP - INTERVAL '3 minutes' WHERE id = $1",
    )
    .bind(restricted_hidden)
    .execute(&server.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE documents SET created_at = CURRENT_TIMESTAMP - INTERVAL '1 minute' WHERE id = $1",
    )
    .bind(restricted_alpha)
    .execute(&server.pool)
    .await
    .unwrap();

    server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{viewer}"),
            Relation::ExplicitViewer,
            &Object::Document(restricted_alpha),
        )
        .await
        .unwrap();

    let client = Client::new();
    let member_response = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents",
            server.addr
        ))
        .bearer_auth(&member)
        .send()
        .await
        .unwrap();
    assert_eq!(member_response.status(), reqwest::StatusCode::OK);
    let member_body: Value = member_response.json().await.unwrap();
    assert_eq!(member_body["total"], json!(2));
    assert_eq!(member_body["limit"], json!(20));
    assert_eq!(member_body["offset"], json!(0));
    assert_eq!(member_body["documents"].as_array().unwrap().len(), 2);
    let public_row = member_body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == public_alpha.to_string())
        .unwrap();
    assert_eq!(public_row["size_bytes"], json!(123));
    assert_eq!(public_row["access_mode"], json!("workspace_default"));
    assert_eq!(public_row["uploaded_by"], json!(admin));
    assert_eq!(
        public_row["uploaded_by_email"],
        json!(format!("{admin}@test.local"))
    );
    assert_eq!(public_row["content_type"], json!("application/pdf"));
    assert!(public_row.get("object_key").is_none());
    assert!(public_row.get("bucket").is_none());
    let failed_row = member_body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == public_failed.to_string())
        .unwrap();
    assert!(failed_row["uploaded_by_email"].is_null());

    let viewer_response = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents?q=alpha&status=COMPLETED&limit=1&offset=0",
            server.addr
        ))
        .bearer_auth(&viewer)
        .send()
        .await
        .unwrap();
    assert_eq!(viewer_response.status(), reqwest::StatusCode::OK);
    let viewer_body: Value = viewer_response.json().await.unwrap();
    assert_eq!(viewer_body["total"], json!(2));
    assert_eq!(viewer_body["documents"].as_array().unwrap().len(), 1);

    let member_filtered = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents?q=ALPHA&status=COMPLETED&limit=0&offset=0",
            server.addr
        ))
        .bearer_auth(&member)
        .send()
        .await
        .unwrap();
    let member_filtered_body: Value = member_filtered.json().await.unwrap();
    assert_eq!(member_filtered_body["total"], json!(1));
    assert_eq!(member_filtered_body["documents"], json!([]));

    for query in ["limit=-1", "limit=101", "offset=-1", "status=UNKNOWN"] {
        let response = client
            .get(format!(
                "{}/workspaces/{workspace_id}/documents?{query}",
                server.addr
            ))
            .bearer_auth(&member)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["error"]["code"], json!("INVALID_REQUEST"));
    }
}

#[tokio::test]
async fn document_shares_and_declarative_permissions_contract() {
    let _guard = phase2_test_lock().lock().await;
    let server = TestServer::bootstrap().await;

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let other_workspace_id = Uuid::new_v4();
    let admin = format!("permissions-admin-{}", Uuid::new_v4());
    let member = format!("permissions-member-{}", Uuid::new_v4());
    let old_viewer = format!("permissions-old-{}", Uuid::new_v4());
    let new_viewer = format!("permissions-new-{}", Uuid::new_v4());
    let outsider = format!("permissions-outsider-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Permissions Tenant {tenant_id}"))
        .execute(&server.pool)
        .await
        .unwrap();
    for (id, name) in [
        (workspace_id, "Permissions Workspace"),
        (other_workspace_id, "Other Workspace"),
    ] {
        sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(tenant_id)
            .bind(format!("{name} {id}"))
            .execute(&server.pool)
            .await
            .unwrap();
    }
    for user in [&admin, &member, &old_viewer, &new_viewer, &outsider] {
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user)
            .bind(format!("{user}@test.local"))
            .execute(&server.pool)
            .await
            .unwrap();
    }
    for (user, role) in [
        (&admin, "ADMIN"),
        (&member, "MEMBER"),
        (&old_viewer, "MEMBER"),
        (&new_viewer, "MEMBER"),
    ] {
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, $3)",
        )
        .bind(workspace_id)
        .bind(user)
        .bind(role)
        .execute(&server.pool)
        .await
        .unwrap();
    }

    server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{admin}"),
            Relation::Admin,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();
    for user in [&member, &old_viewer, &new_viewer] {
        server
            .state
            .authz_client
            .write_tuple(
                &format!("user:{user}"),
                Relation::Member,
                &Object::Workspace(workspace_id),
            )
            .await
            .unwrap();
    }

    let document_id = Uuid::new_v4();
    let other_document_id = Uuid::new_v4();
    insert_document(
        &server.pool,
        workspace_id,
        document_id,
        &admin,
        "permissions.pdf",
        "workspace_default",
    )
    .await;
    insert_document(
        &server.pool,
        other_workspace_id,
        other_document_id,
        &admin,
        "other.pdf",
        "workspace_default",
    )
    .await;
    sqlx::query("INSERT INTO document_shares (document_id, user_id) VALUES ($1, $2)")
        .bind(document_id)
        .bind(&old_viewer)
        .execute(&server.pool)
        .await
        .unwrap();
    server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{old_viewer}"),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
        .unwrap();
    sqlx::query("INSERT INTO document_shares (document_id, user_id) VALUES ($1, $2)")
        .bind(document_id)
        .bind(&member)
        .execute(&server.pool)
        .await
        .unwrap();
    server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{member}"),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
        .unwrap();

    let client = Client::new();
    let shares = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/shares",
            server.addr
        ))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(shares.status(), reqwest::StatusCode::OK);
    let shares_body: Value = shares.json().await.unwrap();
    assert_eq!(shares_body["document_id"], json!(document_id));
    assert_eq!(shares_body["access_mode"], json!("workspace_default"));
    assert_eq!(shares_body["shares"].as_array().unwrap().len(), 2);
    assert_eq!(shares_body["shares"][0]["user_id"], json!(member));
    assert_eq!(shares_body["shares"][1]["user_id"], json!(old_viewer));
    assert!(shares_body["shares"][0]["email"].is_string());
    assert!(shares_body["shares"][0]["shared_at"].is_string());

    server
        .state
        .authz_client
        .delete_tuple(
            &format!("user:{member}"),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
        .unwrap();
    sqlx::query("DELETE FROM document_shares WHERE document_id = $1 AND user_id = $2")
        .bind(document_id)
        .bind(&member)
        .execute(&server.pool)
        .await
        .unwrap();

    let denied_shares = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/shares",
            server.addr
        ))
        .bearer_auth(&member)
        .send()
        .await
        .unwrap();
    assert_eq!(denied_shares.status(), reqwest::StatusCode::FORBIDDEN);

    let wrong_workspace = client
        .get(format!(
            "{}/workspaces/{workspace_id}/documents/{other_document_id}/shares",
            server.addr
        ))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_workspace.status(), reqwest::StatusCode::NOT_FOUND);

    let invalid_target = client
        .put(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/permissions",
            server.addr
        ))
        .bearer_auth(&admin)
        .json(&json!({
            "access_mode": "restricted",
            "authorized_user_ids": [new_viewer, outsider]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_target.status(), reqwest::StatusCode::BAD_REQUEST);
    let invalid_target_body: Value = invalid_target.json().await.unwrap();
    assert_eq!(
        invalid_target_body["error"]["code"],
        json!("USER_NOT_WORKSPACE_MEMBER")
    );
    assert_eq!(
        invalid_target_body["error"]["details"]["user_id"],
        json!(outsider)
    );
    let unchanged_mode: String =
        sqlx::query_scalar("SELECT access_mode FROM documents WHERE id = $1")
            .bind(document_id)
            .fetch_one(&server.pool)
            .await
            .unwrap();
    assert_eq!(unchanged_mode, "workspace_default");
    let unchanged_old_share: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM document_shares WHERE document_id = $1 AND user_id = $2",
    )
    .bind(document_id)
    .bind(&old_viewer)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(unchanged_old_share, 1);

    let updated = client
        .put(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/permissions",
            server.addr
        ))
        .bearer_auth(&admin)
        .json(&json!({
            "access_mode": "restricted",
            "authorized_user_ids": [new_viewer]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), reqwest::StatusCode::OK);
    let updated_body: Value = updated.json().await.unwrap();
    assert_eq!(updated_body["access_mode"], json!("restricted"));
    assert_eq!(updated_body["shares"].as_array().unwrap().len(), 1);
    assert_eq!(updated_body["shares"][0]["user_id"], json!(new_viewer));

    let old_allowed = server
        .state
        .authz_client
        .check_fga(
            &format!("user:{old_viewer}"),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
        .unwrap();
    let new_allowed = server
        .state
        .authz_client
        .check_fga(
            &format!("user:{new_viewer}"),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
        .unwrap();
    assert!(!old_allowed);
    assert!(new_allowed);

    let audit_count_after_update: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_events WHERE document_id = $1 AND event_type = 'permissions_updated'",
    )
    .bind(document_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(audit_count_after_update, 1);
    let audit_metadata: Value = sqlx::query_scalar(
        "SELECT metadata FROM audit_events WHERE document_id = $1 AND event_type = 'permissions_updated' ORDER BY created_at DESC LIMIT 1",
    )
    .bind(document_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(audit_metadata["prev_mode"], json!("workspace_default"));
    assert_eq!(audit_metadata["new_mode_requested"], json!("restricted"));
    assert_eq!(audit_metadata["mode_applied"], json!("restricted"));
    assert_eq!(audit_metadata["granted_requested"], json!(1));
    assert_eq!(audit_metadata["granted_applied"], json!(1));
    assert_eq!(audit_metadata["revoked_requested"], json!(1));
    assert_eq!(audit_metadata["revoked_applied"], json!(1));
    assert_eq!(audit_metadata["completed"], json!(true));
    assert!(audit_metadata["failed_stage"].is_null());

    let repeated = client
        .put(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/permissions",
            server.addr
        ))
        .bearer_auth(&admin)
        .json(&json!({
            "access_mode": "restricted",
            "authorized_user_ids": [new_viewer]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(repeated.status(), reqwest::StatusCode::OK);
    let audit_count_after_noop: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_events WHERE document_id = $1 AND event_type = 'permissions_updated'",
    )
    .bind(document_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(audit_count_after_noop, audit_count_after_update);

    let invalid_default = client
        .put(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/permissions",
            server.addr
        ))
        .bearer_auth(&admin)
        .json(&json!({
            "access_mode": "workspace_default",
            "authorized_user_ids": [new_viewer]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_default.status(), reqwest::StatusCode::BAD_REQUEST);

    let reset = client
        .put(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/permissions",
            server.addr
        ))
        .bearer_auth(&admin)
        .json(&json!({
            "access_mode": "workspace_default",
            "authorized_user_ids": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(reset.status(), reqwest::StatusCode::OK);
    let reset_body: Value = reset.json().await.unwrap();
    assert_eq!(reset_body["access_mode"], json!("workspace_default"));
    assert_eq!(reset_body["shares"], json!([]));
    let new_allowed_after_reset = server
        .state
        .authz_client
        .check_fga(
            &format!("user:{new_viewer}"),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
        .unwrap();
    assert!(!new_allowed_after_reset);
}

#[tokio::test]
async fn permissions_cross_store_ordering_and_partial_audit() {
    let _guard = phase2_test_lock().lock().await;
    let server = TestServer::bootstrap().await;

    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let admin = format!("ordering-admin-{}", Uuid::new_v4());
    let target = format!("ordering-target-{}", Uuid::new_v4());
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Ordering Tenant {tenant_id}"))
        .execute(&server.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("Ordering Workspace {workspace_id}"))
        .execute(&server.pool)
        .await
        .unwrap();
    for user in [&admin, &target] {
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user)
            .bind(format!("{user}@test.local"))
            .execute(&server.pool)
            .await
            .unwrap();
    }
    for (user, role) in [(&admin, "ADMIN"), (&target, "MEMBER")] {
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, $3)",
        )
        .bind(workspace_id)
        .bind(user)
        .bind(role)
        .execute(&server.pool)
        .await
        .unwrap();
    }
    server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{admin}"),
            Relation::Admin,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();
    server
        .state
        .authz_client
        .write_tuple(
            &format!("user:{target}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();

    let document_id = Uuid::new_v4();
    insert_document(
        &server.pool,
        workspace_id,
        document_id,
        &admin,
        "ordering.pdf",
        "restricted",
    )
    .await;
    let client = Client::new();

    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION test_reject_document_share_insert()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'injected document_shares insert failure';
        END;
        $$
        "#,
    )
    .execute(&server.pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER test_reject_document_share_insert BEFORE INSERT ON document_shares FOR EACH ROW EXECUTE FUNCTION test_reject_document_share_insert()",
    )
    .execute(&server.pool)
    .await
    .unwrap();

    let failed_grant = client
        .put(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/permissions",
            server.addr
        ))
        .bearer_auth(&admin)
        .json(&json!({
            "access_mode": "restricted",
            "authorized_user_ids": [target]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        failed_grant.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let target_allowed_after_failed_sql: bool = server
        .state
        .authz_client
        .check_fga(
            &format!("user:{target}"),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
        .unwrap();
    assert!(!target_allowed_after_failed_sql);
    sqlx::query("DROP TRIGGER test_reject_document_share_insert ON document_shares")
        .execute(&server.pool)
        .await
        .unwrap();

    sqlx::query("INSERT INTO document_shares (document_id, user_id) VALUES ($1, $2)")
        .bind(document_id)
        .bind(&target)
        .execute(&server.pool)
        .await
        .unwrap();

    let recovered_grant = client
        .put(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/permissions",
            server.addr
        ))
        .bearer_auth(&admin)
        .json(&json!({
            "access_mode": "restricted",
            "authorized_user_ids": [target]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(recovered_grant.status(), reqwest::StatusCode::OK);
    let grant_recovery_metadata: Value = sqlx::query_scalar(
        "SELECT metadata FROM audit_events WHERE document_id = $1 AND event_type = 'permissions_updated' ORDER BY created_at DESC LIMIT 1",
    )
    .bind(document_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(grant_recovery_metadata["completed"], json!(true));
    assert_eq!(grant_recovery_metadata["granted_requested"], json!(1));
    assert_eq!(grant_recovery_metadata["granted_applied"], json!(1));

    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION test_reject_document_share_delete()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'injected document_shares delete failure';
        END;
        $$
        "#,
    )
    .execute(&server.pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER test_reject_document_share_delete BEFORE DELETE ON document_shares FOR EACH ROW EXECUTE FUNCTION test_reject_document_share_delete()",
    )
    .execute(&server.pool)
    .await
    .unwrap();

    let failed_revoke = client
        .put(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/permissions",
            server.addr
        ))
        .bearer_auth(&admin)
        .json(&json!({
            "access_mode": "restricted",
            "authorized_user_ids": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        failed_revoke.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let target_allowed_after_failed_revoke = server
        .state
        .authz_client
        .check_fga(
            &format!("user:{target}"),
            Relation::ExplicitViewer,
            &Object::Document(document_id),
        )
        .await
        .unwrap();
    assert!(!target_allowed_after_failed_revoke);
    let residual_share: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM document_shares WHERE document_id = $1 AND user_id = $2",
    )
    .bind(document_id)
    .bind(&target)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(residual_share, 1);
    let partial_metadata: Value = sqlx::query_scalar(
        "SELECT metadata FROM audit_events WHERE document_id = $1 AND event_type = 'permissions_updated' AND metadata->>'completed' = 'false' ORDER BY created_at DESC LIMIT 1",
    )
    .bind(document_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(partial_metadata["completed"], json!(false));
    assert_eq!(partial_metadata["failed_stage"], json!("revoke"));
    assert_eq!(partial_metadata["revoked_requested"], json!(1));
    assert_eq!(partial_metadata["revoked_applied"], json!(0));
    sqlx::query("DROP TRIGGER test_reject_document_share_delete ON document_shares")
        .execute(&server.pool)
        .await
        .unwrap();

    let recovered = client
        .put(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/permissions",
            server.addr
        ))
        .bearer_auth(&admin)
        .json(&json!({
            "access_mode": "restricted",
            "authorized_user_ids": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(recovered.status(), reqwest::StatusCode::OK);
    let recovery_metadata: Value = sqlx::query_scalar(
        "SELECT metadata FROM audit_events WHERE document_id = $1 AND event_type = 'permissions_updated' ORDER BY created_at DESC LIMIT 1",
    )
    .bind(document_id)
    .fetch_one(&server.pool)
    .await
    .unwrap();
    assert_eq!(recovery_metadata["completed"], json!(true));
    assert!(recovery_metadata["failed_stage"].is_null());
    assert_eq!(recovery_metadata["revoked_requested"], json!(1));
    assert_eq!(recovery_metadata["revoked_applied"], json!(1));

    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION test_reject_permissions_audit()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'injected permissions audit failure';
        END;
        $$
        "#,
    )
    .execute(&server.pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER test_reject_permissions_audit BEFORE INSERT ON audit_events FOR EACH ROW WHEN (NEW.event_type = 'permissions_updated') EXECUTE FUNCTION test_reject_permissions_audit()",
    )
    .execute(&server.pool)
    .await
    .unwrap();

    let audit_failure_does_not_fail_operation = client
        .put(format!(
            "{}/workspaces/{workspace_id}/documents/{document_id}/permissions",
            server.addr
        ))
        .bearer_auth(&admin)
        .json(&json!({
            "access_mode": "restricted",
            "authorized_user_ids": [target]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        audit_failure_does_not_fail_operation.status(),
        reqwest::StatusCode::OK
    );
    sqlx::query("DROP TRIGGER test_reject_permissions_audit ON audit_events")
        .execute(&server.pool)
        .await
        .unwrap();
}

async fn insert_document(
    pool: &sqlx::PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
    owner_id: &str,
    filename: &str,
    access_mode: &str,
) {
    let insert_full = sqlx::query(
        r#"
        INSERT INTO documents (
            id,
            workspace_id,
            owner_id,
            filename,
            status,
            processing_stage,
            access_mode,
            object_key,
            bucket,
            content_type,
            size_bytes,
            checksum_sha256,
            storage_etag,
            uploaded_by
        )
        VALUES ($1, $2, $3, $4, 'COMPLETED', 'DONE', $5, $6, $7, 'application/pdf', 123, 'abc', NULL, $8)
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .bind(owner_id)
    .bind(filename)
    .bind(access_mode)
    .bind(format!("tenants/test/workspaces/{workspace_id}/documents/{document_id}/original.pdf"))
    .bind("gmrag-documents")
    .bind(owner_id)
    .execute(pool)
    .await;

    match insert_full {
        Ok(_) => {}
        Err(err) if is_missing_column_error(&err, "object_key") => {
            sqlx::query(
                r#"
                INSERT INTO documents (
                    id,
                    workspace_id,
                    owner_id,
                    filename,
                    status,
                    processing_stage,
                    access_mode
                )
                VALUES ($1, $2, $3, $4, 'COMPLETED', 'DONE', $5)
                "#,
            )
            .bind(document_id)
            .bind(workspace_id)
            .bind(owner_id)
            .bind(filename)
            .bind(access_mode)
            .execute(pool)
            .await
            .unwrap();
        }
        Err(err) => panic!("failed to seed document: {err}"),
    }
}

async fn insert_chunk(
    pool: &sqlx::PgPool,
    chunk_id: Uuid,
    document_id: Uuid,
    workspace_id: Uuid,
    chunk_index: i32,
    text: &str,
    seed: f32,
) {
    let embedding_literal = format_pgvector_literal(seed);
    sqlx::query(
        r#"
        INSERT INTO document_chunks (id, document_id, workspace_id, chunk_index, original_text, embedding)
        VALUES ($1, $2, $3, $4, $5, $6::vector)
        "#,
    )
    .bind(chunk_id)
    .bind(document_id)
    .bind(workspace_id)
    .bind(chunk_index)
    .bind(text)
    .bind(embedding_literal)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_graph(
    pool: &sqlx::PgPool,
    workspace_id: Uuid,
    public_doc_id: Uuid,
    restricted_doc_id: Uuid,
) {
    let mut public_batch = GraphWriteBatch::from_extractions(&[(
        0,
        vec![
            GraphElement::Node {
                name: "SharedEntity".to_string(),
                entity_type: Some("public_type".to_string()),
                description: Some("public graph node description".to_string()),
            },
            GraphElement::Edge {
                relationship: "shared_relation".to_string(),
                source: "SharedEntity".to_string(),
                target: "SharedEntity".to_string(),
                description: Some("public graph edge description".to_string()),
            },
        ],
    )]);
    public_batch
        .attach_node_embeddings(vec![embedding_vec(1.0)])
        .unwrap();

    let mut public_tx = pool.begin().await.unwrap();
    bulk_upsert_graph(&mut public_tx, workspace_id, public_doc_id, &public_batch)
        .await
        .unwrap();
    public_tx.commit().await.unwrap();

    let mut restricted_batch = GraphWriteBatch::from_extractions(&[(
        0,
        vec![
            GraphElement::Node {
                name: "SharedEntity".to_string(),
                entity_type: Some("restricted_type".to_string()),
                description: Some("restricted graph node description".to_string()),
            },
            GraphElement::Edge {
                relationship: "shared_relation".to_string(),
                source: "SharedEntity".to_string(),
                target: "SharedEntity".to_string(),
                description: Some("restricted graph edge description".to_string()),
            },
            GraphElement::Edge {
                relationship: "restricted_only_relation".to_string(),
                source: "SharedEntity".to_string(),
                target: "SharedEntity".to_string(),
                description: Some("restricted-only graph edge description".to_string()),
            },
        ],
    )]);
    restricted_batch
        .attach_node_embeddings(vec![embedding_vec(0.5)])
        .unwrap();

    let mut restricted_tx = pool.begin().await.unwrap();
    bulk_upsert_graph(
        &mut restricted_tx,
        workspace_id,
        restricted_doc_id,
        &restricted_batch,
    )
    .await
    .unwrap();
    restricted_tx.commit().await.unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn seed_openfga(
    state: &AppState,
    tenant_id: Uuid,
    workspace_id: Uuid,
    public_doc_id: Uuid,
    restricted_doc_id: Uuid,
    platform_admin: &str,
    tenant_owner: &str,
    member_user: &str,
    explicit_viewer: &str,
) {
    state
        .authz_client
        .write_tuple(
            &format!("user:{platform_admin}"),
            Relation::Admin,
            &Object::Platform,
        )
        .await
        .unwrap();

    state
        .authz_client
        .write_tuple(
            "platform:system",
            Relation::Platform,
            &Object::Tenant(tenant_id),
        )
        .await
        .unwrap();

    state
        .authz_client
        .write_tuple(
            &format!("user:{tenant_owner}"),
            Relation::Owner,
            &Object::Tenant(tenant_id),
        )
        .await
        .unwrap();

    state
        .authz_client
        .write_tuple(
            &format!("tenant:{tenant_id}"),
            Relation::Tenant,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();

    state
        .authz_client
        .write_tuple(
            &format!("user:{member_user}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();

    state
        .authz_client
        .write_tuple(
            &format!("user:{explicit_viewer}"),
            Relation::Member,
            &Object::Workspace(workspace_id),
        )
        .await
        .unwrap();

    write_tuple_idempotent(
        state,
        &format!("workspace:{workspace_id}"),
        Relation::Workspace,
        &Object::Document(public_doc_id),
    )
    .await;

    write_tuple_idempotent(
        state,
        &format!("workspace:{workspace_id}"),
        Relation::Workspace,
        &Object::Document(restricted_doc_id),
    )
    .await;

    state
        .authz_client
        .write_tuple(
            &format!("user:{explicit_viewer}"),
            Relation::ExplicitViewer,
            &Object::Document(restricted_doc_id),
        )
        .await
        .unwrap();
}

async fn write_tuple_idempotent(state: &AppState, user: &str, relation: Relation, object: &Object) {
    match state.authz_client.write_tuple(user, relation, object).await {
        Ok(()) => {}
        Err(gmrag_api::auth::authz::AuthzError::OpenFga { body, .. })
            if body.to_ascii_lowercase().contains("already exists") => {}
        Err(err) => panic!("failed to write tuple: {err}"),
    }
}

fn init_test_env(qdrant_addr: &str, ollama_addr: &str, deepseek_addr: &str) {
    unsafe {
        std::env::set_var("APP_ENV", "test");
        std::env::set_var("TEST_BYPASS_JWT", "1");
        std::env::set_var("TEST_BYPASS_KEYCLOAK", "1");
        std::env::set_var("S3_ENDPOINT_URL", "http://localhost:9000");
        std::env::set_var("S3_REGION", "us-east-1");
        if std::env::var_os("S3_BUCKET").is_none() {
            std::env::set_var("S3_BUCKET", "gmrag-documents");
        }
        std::env::set_var("S3_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("S3_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("S3_FORCE_PATH_STYLE", "true");
        std::env::set_var("S3_PRESIGN_EXPIRY_SECS", "900");
        std::env::set_var("OLLAMA_EMBED_URL", format!("{ollama_addr}/api/embed"));
        std::env::set_var(
            "DEEPSEEK_API_URL",
            format!("{deepseek_addr}/chat/completions"),
        );
        std::env::set_var("DEEPSEEK_API_KEY", "test-key");
        std::env::set_var("QDRANT_URL", qdrant_addr);
        std::env::set_var("QDRANT_VECTOR_SIZE", EMBEDDING_DIM.to_string());
        std::env::set_var("QDRANT_TOP_K", "5");
        std::env::set_var("GMRAG_GRAPH_EXTRACTION_ENABLED", "false");
    }
}

fn qdrant_router(state: QdrantMockState) -> Router {
    Router::new()
        .route("/collections/{collection}", put(qdrant_create_collection))
        .route(
            "/collections/{collection}/points",
            put(qdrant_upsert_points),
        )
        .route(
            "/collections/{collection}/points/search",
            post(qdrant_search_points),
        )
        .with_state(state)
}

async fn qdrant_create_collection(Path(_collection): Path<String>) -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status":"ok","result":true})))
}

async fn qdrant_upsert_points(
    Path(_collection): Path<String>,
    Json(_payload): Json<Value>,
) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({"status":"ok","result":{"operation_id":1}})),
    )
}

async fn qdrant_search_points(
    State(state): State<QdrantMockState>,
    Path(_collection): Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let mut guard = state.inner.lock().await;
    guard.search_payloads.push(payload);
    let results = guard
        .search_results
        .iter()
        .map(|chunk_id| json!({"id": chunk_id.to_string(), "score": 0.9}))
        .collect::<Vec<_>>();

    (
        StatusCode::OK,
        Json(json!({"status":"ok","result":results})),
    )
}

fn ollama_router() -> Router {
    Router::new().route("/api/embed", post(ollama_embed))
}

async fn ollama_embed(Json(payload): Json<Value>) -> impl IntoResponse {
    let count = payload["input"].as_array().map_or(0, Vec::len);
    let embeddings = (0..count)
        .map(|idx| embedding_vec((idx as f32) + 1.0))
        .collect::<Vec<_>>();

    (StatusCode::OK, Json(json!({"embeddings": embeddings})))
}

fn deepseek_router(state: DeepseekMockState) -> Router {
    Router::new()
        .route("/chat/completions", post(deepseek_chat_completion))
        .with_state(state)
}

async fn deepseek_chat_completion(
    State(state): State<DeepseekMockState>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let mut guard = state.inner.lock().await;
    guard.requests.push(payload);
    let answer = guard.answer.clone();
    drop(guard);

    let chunk = json!({
        "choices": [
            {
                "delta": {
                    "content": answer
                }
            }
        ]
    });
    let body = format!("data: {chunk}\n\ndata: [DONE]\n\n");

    ([(header::CONTENT_TYPE, "text/event-stream")], body)
}

async fn spawn_mock_server(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    format!("http://{}", addr)
}

fn embedding_vec(seed: f32) -> Vec<f32> {
    let mut vector = vec![0.0_f32; EMBEDDING_DIM];
    vector[0] = seed;
    vector
}

fn format_pgvector_literal(seed: f32) -> String {
    let values = embedding_vec(seed)
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("[{values}]")
}

fn is_missing_column_error(err: &sqlx::Error, column_name: &str) -> bool {
    let sqlx::Error::Database(db_err) = err else {
        return false;
    };

    db_err.code().as_deref() == Some("42703") && db_err.message().contains(column_name)
}
