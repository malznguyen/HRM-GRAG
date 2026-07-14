mod support;

use std::io::{Cursor, Write};

use axum::{Json, Router, http::StatusCode, routing::post};
use gmrag_api::document_format::{DOCX_CONTENT_TYPE, MARKDOWN_CONTENT_TYPE, TEXT_CONTENT_TYPE};
use gmrag_api::ingestion::embedding::EXPECTED_EMBEDDING_DIM;
use gmrag_api::ingestion::jobs::{
    IngestionWorkerConfig, JobFailure, claim_document_job, enqueue_job_tx, finish_job_failure,
};
use gmrag_api::ingestion::processor::{ProcessError, process_claimed_job};
use gmrag_api::retrieval::RetrievalClient;
use gmrag_api::storage::{StorageClient, StorageConfig, build_original_document_object_key};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;
use zip::{ZipWriter, write::SimpleFileOptions};

#[tokio::test]
async fn multi_format_ingestion_completes_and_uses_format_specific_terminal_failures() {
    dotenvy::dotenv().ok();
    let database_url = support::database_url().expect("DATABASE_URL must be set");
    let ollama_url = spawn_mock_ollama().await;
    unsafe {
        std::env::set_var("OLLAMA_EMBED_URL", format!("{ollama_url}/api/embed"));
        std::env::set_var("GMRAG_GRAPH_EXTRACTION_ENABLED", "false");
    }

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let storage = StorageClient::from_config(StorageConfig::from_env().unwrap()).await;
    let retrieval = RetrievalClient::from_env().unwrap();
    retrieval
        .search_chunk_ids(Uuid::nil(), &[Uuid::nil()], &mock_embedding(), 1)
        .await
        .unwrap();
    let (tenant_id, workspace_id, user_id) = seed_workspace(&pool).await;

    let fixtures = [
        (
            "format.docx",
            DOCX_CONTENT_TYPE,
            sample_docx_bytes("Nội dung DOCX ngắn &amp; hợp lệ"),
            "Nội dung DOCX ngắn & hợp lệ",
        ),
        (
            "format.txt",
            TEXT_CONTENT_TYPE,
            "Nội dung TXT UTF-8".as_bytes().to_vec(),
            "Nội dung TXT UTF-8",
        ),
        (
            "format.md",
            MARKDOWN_CONTENT_TYPE,
            b"# Tieu de\n\n**raw markdown**".to_vec(),
            "**raw markdown**",
        ),
    ];

    for (filename, content_type, bytes, expected_text) in fixtures {
        let document_id = seed_queued_document(
            &pool,
            &storage,
            tenant_id,
            workspace_id,
            &user_id,
            filename,
            content_type,
            &bytes,
        )
        .await;
        let worker_id = format!("multi-format-{document_id}");
        let config = IngestionWorkerConfig::from_env();
        let job = claim_document_job(&pool, &worker_id, config, document_id)
            .await
            .unwrap()
            .into_iter()
            .next()
            .expect("queued job must be claimable");

        process_claimed_job(
            pool.clone(),
            storage.clone(),
            retrieval.clone(),
            &job,
            &worker_id,
        )
        .await
        .unwrap();

        let state: (String, String, String) = sqlx::query_as(
            r#"
            SELECT d.status, d.processing_stage, j.status
            FROM documents d
            JOIN ingestion_jobs j ON j.document_id = d.id
            WHERE d.id = $1
            "#,
        )
        .bind(document_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            state,
            ("COMPLETED".into(), "DONE".into(), "SUCCEEDED".into())
        );

        let chunks: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT id, original_text FROM document_chunks WHERE document_id = $1 ORDER BY chunk_index",
        )
        .bind(document_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(!chunks.is_empty());
        assert!(chunks.iter().any(|(_, text)| text.contains(expected_text)));

        let point_ids = retrieval
            .search_chunk_ids(
                workspace_id,
                &[document_id],
                &mock_embedding(),
                chunks.len(),
            )
            .await
            .unwrap();
        assert!(!point_ids.is_empty());
        assert!(
            point_ids
                .iter()
                .all(|id| chunks.iter().any(|(chunk_id, _)| chunk_id == id))
        );
    }

    let broken_document_id = seed_queued_document(
        &pool,
        &storage,
        tenant_id,
        workspace_id,
        &user_id,
        "broken.docx",
        DOCX_CONTENT_TYPE,
        b"PK\x03\x04not-a-valid-archive",
    )
    .await;
    let worker_id = format!("broken-docx-{broken_document_id}");
    let config = IngestionWorkerConfig::from_env();
    let job = claim_document_job(&pool, &worker_id, config, broken_document_id)
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("broken DOCX job must be claimable");
    let error = process_claimed_job(pool.clone(), storage, retrieval, &job, &worker_id)
        .await
        .expect_err("broken DOCX must fail extraction");
    assert!(matches!(error, ProcessError::DocxParse(_)));

    let (code, message, retryable) = error.failure_kind();
    assert_eq!(code, "DOCX_PARSE_FAILED");
    assert!(!retryable);
    assert_eq!(
        finish_job_failure(
            &pool,
            &job,
            &worker_id,
            JobFailure {
                code,
                message,
                retryable,
            },
            config,
        )
        .await
        .unwrap(),
        Some(true)
    );
    let terminal: (String, String, Option<String>) = sqlx::query_as(
        "SELECT status, processing_stage, failure_code FROM documents WHERE id = $1",
    )
    .bind(broken_document_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        terminal,
        (
            "FAILED".to_string(),
            "FAILED".to_string(),
            Some("DOCX_PARSE_FAILED".to_string())
        )
    );
}

async fn seed_workspace(pool: &PgPool) -> (Uuid, Uuid, String) {
    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let user_id = format!("multi-format-{}", Uuid::new_v4());
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&user_id)
        .bind(format!("{user_id}@test.local"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("Multi-format tenant {tenant_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("Multi-format workspace {workspace_id}"))
        .execute(pool)
        .await
        .unwrap();
    (tenant_id, workspace_id, user_id)
}

#[allow(clippy::too_many_arguments)]
async fn seed_queued_document(
    pool: &PgPool,
    storage: &StorageClient,
    tenant_id: Uuid,
    workspace_id: Uuid,
    user_id: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
) -> Uuid {
    let document_id = Uuid::new_v4();
    let object_key = build_original_document_object_key(tenant_id, workspace_id, document_id);
    let upload = storage
        .put_original_document(&object_key, bytes, Some(content_type))
        .await
        .unwrap();
    let checksum = format!("{:x}", Sha256::digest(bytes));
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query(
        r#"
        INSERT INTO documents (
            id, workspace_id, owner_id, filename, status, processing_stage,
            object_key, bucket, content_type, size_bytes, checksum_sha256,
            storage_etag, uploaded_by
        )
        VALUES ($1, $2, $3, $4, 'PROCESSING', 'QUEUED', $5, $6, $7, $8, $9, $10, $3)
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .bind(user_id)
    .bind(filename)
    .bind(&object_key)
    .bind(storage.bucket())
    .bind(content_type)
    .bind(i64::try_from(bytes.len()).unwrap())
    .bind(checksum)
    .bind(upload.etag)
    .execute(&mut *transaction)
    .await
    .unwrap();
    enqueue_job_tx(
        &mut transaction,
        document_id,
        workspace_id,
        IngestionWorkerConfig::from_env().max_attempts,
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    document_id
}

async fn spawn_mock_ollama() -> String {
    let router = Router::new().route("/api/embed", post(mock_ollama_embed));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{address}")
}

async fn mock_ollama_embed(Json(payload): Json<Value>) -> (StatusCode, Json<Value>) {
    let count = payload["input"].as_array().map_or(1, Vec::len);
    let embeddings = (0..count).map(|_| mock_embedding()).collect::<Vec<_>>();
    (StatusCode::OK, Json(json!({ "embeddings": embeddings })))
}

fn mock_embedding() -> Vec<f32> {
    let mut embedding = vec![0.0; EXPECTED_EMBEDDING_DIM];
    embedding[0] = 1.0;
    embedding
}

fn sample_docx_bytes(text: &str) -> Vec<u8> {
    let document_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body><w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:body>
</w:document>"#
    );
    let mut output = Cursor::new(Vec::new());
    {
        let mut writer = ZipWriter::new(&mut output);
        let options = SimpleFileOptions::default();
        writer.start_file("[Content_Types].xml", options).unwrap();
        writer.write_all(b"<Types/>").unwrap();
        writer.start_file("word/document.xml", options).unwrap();
        writer.write_all(document_xml.as_bytes()).unwrap();
        writer.finish().unwrap();
    }
    output.into_inner()
}
