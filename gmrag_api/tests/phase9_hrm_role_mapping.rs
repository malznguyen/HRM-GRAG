use std::{
    error::Error,
    io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use gmrag_api::{
    auth::{
        authz::{AuthzClient, TupleKey},
        jwt::JwtValidator,
        keycloak::KeycloakClient,
    },
    retrieval::RetrievalClient,
    state::AppState,
    storage::{StorageClient, StorageConfig},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::{Client, Response, StatusCode, multipart};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::Semaphore;
use tokio::{task::JoinHandle, time::sleep};
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

struct RuntimeContext {
    base_url: String,
    client: Client,
    pool: PgPool,
    authz: AuthzClient,
    storage: StorageClient,
    retrieval: RetrievalClient,
    tenant_id: Uuid,
    workspace_id: Uuid,
    user_id: String,
    manager_token: String,
    hr_token: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the local HRM stack and PHASE9_RUNTIME_TEST=1"]
async fn manager_role_contract_end_to_end() {
    let (context, server) = start_runtime().await.expect("start Phase 9 runtime");

    let exercise = exercise_contract(&context).await;
    let cleanup = cleanup_and_verify(&context).await;
    server.abort();

    if let Err(error) = cleanup {
        panic!("Phase 9 cleanup failed: {error}");
    }
    if let Err(error) = exercise {
        panic!("Phase 9 contract failed: {error}");
    }
}

async fn start_runtime() -> TestResult<(RuntimeContext, JoinHandle<()>)> {
    let env_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.env");
    dotenvy::from_path(env_path)?;

    require_env_value("PHASE9_RUNTIME_TEST", "1")?;
    require_env_value("HRM_MODE", "true")?;
    require_env_value("JWT_ALG", "HS512")?;

    let configured_bind = required_env("API_BIND_ADDR")?
        .parse::<std::net::SocketAddr>()
        .map_err(|error| test_error(format!("invalid API_BIND_ADDR: {error}")))?;
    if !configured_bind.ip().is_loopback() {
        return Err(test_error("API_BIND_ADDR must remain loopback-only"));
    }

    let tenant_id = required_env("HRM_TENANT_ID")?.parse::<Uuid>()?;
    let workspace_id = required_env("HRM_WORKSPACE_ID")?.parse::<Uuid>()?;
    let database_url = required_env("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    ensure_empty_sql_corpus(&pool, workspace_id).await?;

    let jwt = JwtValidator::from_env()
        .map_err(|error| test_error(format!("JWT config failed: {error:?}")))?;
    let authz = AuthzClient::from_env().map_err(test_error)?;
    let storage = StorageClient::from_config(StorageConfig::from_env()?).await;
    let retrieval = RetrievalClient::from_env()?;

    let state = AppState {
        pool: pool.clone(),
        jwt,
        storage: storage.clone(),
        retrieval: retrieval.clone(),
        ingestion_limiter: Arc::new(Semaphore::new(1)),
        authz_client: authz.clone(),
        keycloak_client: KeycloakClient::disabled(),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, gmrag_api::app_router(state))
            .await
            .expect("Phase 9 test API");
    });

    let user_id = Uuid::new_v4().to_string();
    let manager_token = sign_token(&user_id, "MANAGER", &["CHATBOT_USE"])?;
    let hr_token = sign_token(&user_id, "HR", &["CHATBOT_USE", "CHATBOT_UPLOAD_DOCUMENT"])?;
    let client = Client::builder()
        .timeout(Duration::from_secs(180))
        .build()?;

    Ok((
        RuntimeContext {
            base_url: format!("http://{address}"),
            client,
            pool,
            authz,
            storage,
            retrieval,
            tenant_id,
            workspace_id,
            user_id,
            manager_token,
            hr_token,
        },
        server,
    ))
}

async fn exercise_contract(context: &RuntimeContext) -> TestResult<()> {
    let workspace_url = format!("{}/workspaces/hrm", context.base_url);

    let manager_list = context
        .client
        .get(format!("{workspace_url}/documents"))
        .bearer_auth(&context.manager_token)
        .send()
        .await?;
    expect_status(manager_list, StatusCode::OK).await?;
    expect_single_workspace_tuple(context, "member").await?;

    let manager_upload = upload(context, &context.manager_token).await?;
    let manager_upload_payload = expect_json_status(manager_upload, StatusCode::FORBIDDEN).await?;
    let upload_error_code = error_code(&manager_upload_payload)?;
    if upload_error_code != "CHATBOT_UPLOAD_PERMISSION_REQUIRED" {
        return Err(test_error(format!(
            "unexpected MANAGER upload code: {upload_error_code}"
        )));
    }

    let hr_upload = upload(context, &context.hr_token).await?;
    let hr_upload_payload = expect_json_status(hr_upload, StatusCode::ACCEPTED).await?;
    let document_id = hr_upload_payload["documents"][0]["document_id"]
        .as_str()
        .ok_or_else(|| test_error("HR upload response missing document_id"))?
        .parse::<Uuid>()?;
    expect_single_workspace_tuple(context, "admin").await?;

    wait_for_ingestion(context, document_id).await?;

    let manager_list_after_hr = context
        .client
        .get(format!("{workspace_url}/documents"))
        .bearer_auth(&context.manager_token)
        .send()
        .await?;
    expect_status(manager_list_after_hr, StatusCode::OK).await?;
    expect_single_workspace_tuple(context, "member").await?;

    let session_id = Uuid::new_v4();
    let chat = context
        .client
        .post(format!("{workspace_url}/chat"))
        .bearer_auth(&context.manager_token)
        .header("Accept", "text/event-stream")
        .json(&json!({
            "session_id": session_id,
            "message": "Giờ làm việc trong tài liệu kiểm thử là mấy giờ?"
        }))
        .send()
        .await?;
    let chat_status = chat.status();
    let chat_body = chat.text().await?;
    if chat_status != StatusCode::OK || !chat_body.contains("event: citations") {
        return Err(test_error(format!(
            "MANAGER chat expected 200 with citations event, got {chat_status}"
        )));
    }

    let manager_delete = context
        .client
        .delete(format!("{workspace_url}/documents/{document_id}"))
        .bearer_auth(&context.manager_token)
        .send()
        .await?;
    let manager_delete_payload = expect_json_status(manager_delete, StatusCode::NOT_FOUND).await?;
    if error_code(&manager_delete_payload)? != "RESOURCE_NOT_FOUND" {
        return Err(test_error("MANAGER delete must use RESOURCE_NOT_FOUND"));
    }

    let hr_delete = context
        .client
        .delete(format!("{workspace_url}/documents/{document_id}"))
        .bearer_auth(&context.hr_token)
        .send()
        .await?;
    expect_status(hr_delete, StatusCode::NO_CONTENT).await?;
    expect_single_workspace_tuple(context, "admin").await?;

    let manager_list_final = context
        .client
        .get(format!("{workspace_url}/documents"))
        .bearer_auth(&context.manager_token)
        .send()
        .await?;
    expect_status(manager_list_final, StatusCode::OK).await?;
    expect_single_workspace_tuple(context, "member").await?;

    println!(
        "PHASE9_RUNTIME manager_list=200 manager_tuple=member manager_chat=200 citations=true manager_upload=403 manager_upload_code={upload_error_code} manager_delete=404 manager_delete_code=RESOURCE_NOT_FOUND hr_upload=202 hr_delete=204"
    );

    Ok(())
}

async fn upload(context: &RuntimeContext, token: &str) -> TestResult<Response> {
    let file = multipart::Part::bytes(
        b"# Noi quy Phase 9\n\nGio lam viec kiem thu la tu 08:00 den 17:00.\n".to_vec(),
    )
    .file_name("phase9-role-contract.md")
    .mime_str("text/markdown")?;
    let form = multipart::Form::new().part("file", file);

    Ok(context
        .client
        .post(format!(
            "{}/workspaces/hrm/documents/upload",
            context.base_url
        ))
        .bearer_auth(token)
        .multipart(form)
        .send()
        .await?)
}

async fn wait_for_ingestion(context: &RuntimeContext, document_id: Uuid) -> TestResult<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    loop {
        let response = context
            .client
            .get(format!(
                "{}/workspaces/hrm/documents/{document_id}",
                context.base_url
            ))
            .bearer_auth(&context.hr_token)
            .send()
            .await?;
        let payload = expect_json_status(response, StatusCode::OK).await?;
        match payload["status"].as_str() {
            Some("COMPLETED") if payload["chunk_count"].as_i64().unwrap_or(0) > 0 => return Ok(()),
            Some("FAILED") => {
                return Err(test_error(format!(
                    "ingestion failed with code {}",
                    payload["failure_code"].as_str().unwrap_or("unknown")
                )));
            }
            _ if tokio::time::Instant::now() >= deadline => {
                return Err(test_error("ingestion did not complete within 300 seconds"));
            }
            _ => sleep(Duration::from_secs(2)).await,
        }
    }
}

async fn expect_single_workspace_tuple(
    context: &RuntimeContext,
    expected_relation: &str,
) -> TestResult<()> {
    let user = format!("user:{}", context.user_id);
    let object = format!("workspace:{}", context.workspace_id);
    let tuples: Vec<TupleKey> = context
        .authz
        .list_all_tuples()
        .await?
        .into_iter()
        .filter(|tuple| tuple.user == user && tuple.object == object)
        .collect();

    if tuples.len() != 1 || tuples[0].relation != expected_relation {
        return Err(test_error(format!(
            "expected exactly one {expected_relation} tuple, found {:?}",
            tuples
                .iter()
                .map(|tuple| tuple.relation.as_str())
                .collect::<Vec<_>>()
        )));
    }
    Ok(())
}

async fn cleanup_and_verify(context: &RuntimeContext) -> TestResult<()> {
    let document_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM documents WHERE workspace_id = $1 AND owner_id = $2")
            .bind(context.workspace_id)
            .bind(&context.user_id)
            .fetch_all(&context.pool)
            .await?;
    for document_id in document_ids {
        let response = context
            .client
            .delete(format!(
                "{}/workspaces/hrm/documents/{document_id}",
                context.base_url
            ))
            .bearer_auth(&context.hr_token)
            .send()
            .await?;
        if !matches!(
            response.status(),
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND
        ) {
            return Err(test_error(format!(
                "cleanup document returned {}",
                response.status()
            )));
        }
    }

    let session_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM chat_sessions WHERE workspace_id = $1 AND user_id = $2")
            .bind(context.workspace_id)
            .bind(&context.user_id)
            .fetch_all(&context.pool)
            .await?;
    for session_id in session_ids {
        let response = context
            .client
            .delete(format!(
                "{}/workspaces/hrm/chat/sessions/{session_id}",
                context.base_url
            ))
            .bearer_auth(&context.manager_token)
            .send()
            .await?;
        if !matches!(
            response.status(),
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND
        ) {
            return Err(test_error(format!(
                "cleanup chat session returned {}",
                response.status()
            )));
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let counts = corpus_counts(context).await?;
        if counts == (0, 0, 0, 0) {
            println!(
                "PHASE9_CLEANUP sql_documents=0 sql_chat_sessions=0 qdrant_points=0 minio_objects=0"
            );
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(test_error(format!(
                "corpus cleanup incomplete: documents={} sessions={} points={} objects={}",
                counts.0, counts.1, counts.2, counts.3
            )));
        }
        sleep(Duration::from_secs(2)).await;
    }

    let user = format!("user:{}", context.user_id);
    let object = format!("workspace:{}", context.workspace_id);
    let deletes = context
        .authz
        .list_all_tuples()
        .await?
        .into_iter()
        .filter(|tuple| tuple.user == user && tuple.object == object)
        .collect();
    context.authz.write_tuples(Vec::new(), deletes).await?;
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&context.user_id)
        .execute(&context.pool)
        .await?;

    Ok(())
}

async fn corpus_counts(context: &RuntimeContext) -> TestResult<(i64, i64, usize, usize)> {
    let documents: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM documents WHERE workspace_id = $1")
            .bind(context.workspace_id)
            .fetch_one(&context.pool)
            .await?;
    let sessions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM chat_sessions WHERE workspace_id = $1")
            .bind(context.workspace_id)
            .fetch_one(&context.pool)
            .await?;

    let mut qdrant_points = 0usize;
    let mut offset = None;
    loop {
        let page = context.retrieval.scroll_points_page(1000, offset).await?;
        qdrant_points += page
            .points
            .iter()
            .filter(|point| point.workspace_id == context.workspace_id)
            .count();
        let Some(next) = page.next_offset else {
            break;
        };
        offset = Some(next);
    }

    let prefix = format!(
        "tenants/{}/workspaces/{}/documents/",
        context.tenant_id, context.workspace_id
    );
    let minio_objects = context.storage.list_objects(Some(&prefix)).await?.len();
    Ok((documents, sessions, qdrant_points, minio_objects))
}

async fn ensure_empty_sql_corpus(pool: &PgPool, workspace_id: Uuid) -> TestResult<()> {
    let documents: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM documents WHERE workspace_id = $1")
            .bind(workspace_id)
            .fetch_one(pool)
            .await?;
    let sessions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM chat_sessions WHERE workspace_id = $1")
            .bind(workspace_id)
            .fetch_one(pool)
            .await?;
    if documents != 0 || sessions != 0 {
        return Err(test_error(format!(
            "Phase 9 runtime requires an empty SQL corpus; found documents={documents}, sessions={sessions}"
        )));
    }
    Ok(())
}

async fn expect_status(response: Response, expected: StatusCode) -> TestResult<()> {
    let actual = response.status();
    if actual != expected {
        let body = response.text().await.unwrap_or_default();
        return Err(test_error(format!(
            "expected HTTP {expected}, got {actual}: {}",
            truncate(&body)
        )));
    }
    Ok(())
}

async fn expect_json_status(response: Response, expected: StatusCode) -> TestResult<Value> {
    let actual = response.status();
    let body = response.text().await?;
    if actual != expected {
        return Err(test_error(format!(
            "expected HTTP {expected}, got {actual}: {}",
            truncate(&body)
        )));
    }
    Ok(serde_json::from_str(&body)?)
}

fn error_code(payload: &Value) -> TestResult<&str> {
    payload["error"]["code"]
        .as_str()
        .ok_or_else(|| test_error("response missing error.code"))
}

fn sign_token(user_id: &str, role: &str, permissions: &[&str]) -> TestResult<String> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let subject_claim = required_env("JWT_SUBJECT_CLAIM")?;
    let mut claims = json!({
        "iss": required_env("JWT_ISSUER")?,
        "iat": now,
        "exp": now + 3600,
        "email": format!("{user_id}@phase9.local"),
        "email_verified": true,
        "role": role,
        "permissions": permissions,
    });
    claims[&subject_claim] = Value::String(user_id.to_string());
    if let Ok(audience) = std::env::var("JWT_AUDIENCE") {
        if !audience.trim().is_empty() {
            claims["aud"] = Value::String(audience);
        }
    }

    Ok(encode(
        &Header::new(Algorithm::HS512),
        &claims,
        &EncodingKey::from_secret(required_env("JWT_HMAC_SECRET")?.as_bytes()),
    )?)
}

fn require_env_value(name: &str, expected: &str) -> TestResult<()> {
    let actual = required_env(name)?;
    if actual != expected {
        return Err(test_error(format!(
            "{name} must be exactly {expected:?}, got {actual:?}"
        )));
    }
    Ok(())
}

fn required_env(name: &str) -> TestResult<String> {
    std::env::var(name)
        .map(|value| value.trim().to_string())
        .map_err(|_| test_error(format!("{name} must be set")))
}

fn truncate(value: &str) -> String {
    value.chars().take(300).collect()
}

fn test_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
