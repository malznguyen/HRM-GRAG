use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{StreamExt, stream};
use reqwest::Client;
use sqlx::{PgPool, Postgres, Transaction};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use super::chunking::{ChunkError, chunk_page_texts};
use super::embedding::{EmbedError, embed_texts, format_pgvector};
use super::graph::{
    GraphElement, GraphError, GraphWriteBatch, bulk_upsert_graph, extract_graph_elements,
};
use super::ocr::vision_ocr_fallback;
use super::pdf_parser::{PdfParseError, extract_pdf_from_bytes};
use crate::retrieval::{ChunkPoint, RetrievalClient, RetrievalError};
use crate::storage::{StorageClient, StorageError};

pub fn spawn_document_processing(
    pool: PgPool,
    storage: StorageClient,
    retrieval: RetrievalClient,
    workspace_id: Uuid,
    document_id: Uuid,
    limiter: Arc<Semaphore>,
) {
    tokio::spawn(async move {
        let _document_permit = match limiter.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                tracing::error!(
                    %workspace_id,
                    %document_id,
                    "Global ingestion limiter was closed before document processing started"
                );
                return;
            }
        };

        if let Err(err) = process_document(
            pool.clone(),
            storage.clone(),
            retrieval.clone(),
            workspace_id,
            document_id,
        )
        .await
        {
            tracing::error!(
                %workspace_id,
                %document_id,
                error = %err,
                "Document ingestion failed"
            );
            if let Err(db_err) = mark_document_failed(&pool, workspace_id, document_id).await {
                tracing::error!(
                    %workspace_id,
                    %document_id,
                    error = %db_err,
                    "Failed to update document status to FAILED"
                );
            }
        }
    });
}

async fn process_document(
    pool: PgPool,
    storage: StorageClient,
    retrieval: RetrievalClient,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<(), ProcessError> {
    let document_started = Instant::now();
    set_processing_stage(&pool, workspace_id, document_id, "PARSING").await?;

    let storage_metadata = fetch_document_storage_metadata(&pool, workspace_id, document_id)
        .await?
        .ok_or(ProcessError::DocumentNotFound { document_id })?;

    let pdf_bytes = storage
        .get_original_document(&storage_metadata.object_key)
        .await
        .map_err(|err| match err {
            StorageError::ObjectNotFound { .. } => ProcessError::DocumentObjectMissing {
                object_key: storage_metadata.object_key.clone(),
            },
            _ => ProcessError::Storage(err),
        })?;

    let parse_started = Instant::now();
    let parse_timeout = Duration::from_secs(pdf_parse_timeout_secs());

    tracing::info!(
        %workspace_id,
        %document_id,
        timeout_secs = parse_timeout.as_secs(),
        "PDF parse started"
    );

    let extracted = timeout(
        parse_timeout,
        tokio::task::spawn_blocking(move || extract_pdf_from_bytes(&pdf_bytes)),
    )
    .await
    .map_err(|_| ProcessError::ParseTimeout {
        timeout_secs: parse_timeout.as_secs(),
    })?
    .map_err(|_| ProcessError::Join)?
    .map_err(ProcessError::Parse)?;

    tracing::info!(
        %workspace_id,
        %document_id,
        pages = extracted.pages.len(),
        elapsed_ms = parse_started.elapsed().as_millis(),
        "PDF parse completed"
    );

    let mut page_texts: Vec<String> = Vec::with_capacity(extracted.pages.len());

    for page in extracted.pages {
        let mut text = page.text;

        if page.needs_ocr {
            tracing::warn!(
                %workspace_id,
                %document_id,
                page = page.page_number,
                char_count = text.chars().count(),
                "Page text below threshold; invoking vision OCR fallback"
            );
            let ocr_text = vision_ocr_fallback(&page.image_bytes).await;
            if !text.is_empty() && !ocr_text.is_empty() {
                text.push('\n');
            }
            text.push_str(&ocr_text);
        }

        page_texts.push(text);
    }

    let chunk_started = Instant::now();
    let chunks = chunk_page_texts(&page_texts).map_err(ProcessError::Chunk)?;
    tracing::info!(
        %workspace_id,
        %document_id,
        chunks = chunks.len(),
        elapsed_ms = chunk_started.elapsed().as_millis(),
        "PDF chunking completed"
    );
    if chunks.is_empty() {
        tracing::warn!(
            %workspace_id,
            %document_id,
            "No text chunks produced from document; marking COMPLETED"
        );
        set_processing_stage(&pool, workspace_id, document_id, "SAVING").await?;
        let mut tx = pool.begin().await.map_err(ProcessError::Database)?;
        mark_document_completed_tx(&mut tx, workspace_id, document_id).await?;
        tx.commit().await.map_err(ProcessError::Database)?;
        return Ok(());
    }

    set_processing_stage(&pool, workspace_id, document_id, "EMBEDDING").await?;

    let client = Client::new();
    let embedding_started = Instant::now();
    // ADR-21: cùng model với chat query (`embed_texts` / ollama_embed_model) — không hard-code model khác.
    tracing::info!(
        %workspace_id,
        %document_id,
        chunks = chunks.len(),
        batch_size = embedding_batch_size(),
        concurrency = embedding_concurrency(),
        timeout_secs = embedding_timeout_secs(),
        retries = embedding_retries(),
        "Batched embedding stage started (shared ADR-21 model)"
    );
    let embeddings = embed_chunks_batched(&client, &chunks).await?;
    tracing::info!(
        %workspace_id,
        %document_id,
        embeddings = embeddings.len(),
        elapsed_ms = embedding_started.elapsed().as_millis(),
        "Batched embedding stage completed"
    );

    let chunk_rows: Vec<ChunkRow> = chunks
        .into_iter()
        .zip(embeddings)
        .enumerate()
        .map(|(index, (text, embedding))| ChunkRow {
            index: index as i32,
            text,
            embedding,
        })
        .collect();

    set_processing_stage(&pool, workspace_id, document_id, "GRAPH_EXTRACTION").await?;

    let graph_started = Instant::now();
    let graph_results = if graph_extraction_enabled() {
        tracing::info!(
            %workspace_id,
            %document_id,
            chunks = chunk_rows.len(),
            configured_concurrency = graph_extraction_concurrency(),
            effective_concurrency = effective_graph_extraction_concurrency(chunk_rows.len()),
            timeout_secs = graph_extraction_timeout_secs(),
            retries = graph_extraction_retries(),
            stage_timeout_secs = graph_extraction_stage_timeout_secs(),
            "Graph extraction stage started"
        );
        extract_graph_for_chunks(client.clone(), workspace_id, document_id, &chunk_rows).await
    } else {
        tracing::warn!(
            %workspace_id,
            %document_id,
            "Graph extraction disabled; document chunks will still be persisted"
        );
        Vec::new()
    };
    let mut graph_batch = GraphWriteBatch::from_extractions(&graph_results);
    tracing::info!(
        %workspace_id,
        %document_id,
        chunks = graph_results.len(),
        graph_nodes = graph_batch.node_count(),
        graph_edges = graph_batch.edge_count(),
        elapsed_ms = graph_started.elapsed().as_millis(),
        "Graph extraction stage completed"
    );

    // Embed graph nodes trước SAVING để vector search trong chat không fallback ILIKE.
    // Cùng model chunk/query (ADR-21); hard-fail nếu Ollama lỗi (consistent chunk embed).
    if graph_batch.node_count() > 0 {
        let node_embed_started = Instant::now();
        let node_texts = graph_batch.node_texts_for_embedding();
        tracing::info!(
            %workspace_id,
            %document_id,
            graph_nodes = node_texts.len(),
            batch_size = embedding_batch_size(),
            concurrency = embedding_concurrency(),
            "Graph node embedding stage started (shared ADR-21 model)"
        );
        let node_embeddings = embed_chunks_batched(&client, &node_texts).await?;
        graph_batch
            .attach_node_embeddings(node_embeddings)
            .map_err(ProcessError::Graph)?;
        tracing::info!(
            %workspace_id,
            %document_id,
            graph_nodes = graph_batch.node_count(),
            elapsed_ms = node_embed_started.elapsed().as_millis(),
            "Graph node embedding stage completed"
        );
    }

    set_processing_stage(&pool, workspace_id, document_id, "SAVING").await?;

    let db_started = Instant::now();
    tracing::info!(
        %workspace_id,
        %document_id,
        chunks = chunk_rows.len(),
        graph_nodes = graph_batch.node_count(),
        graph_edges = graph_batch.edge_count(),
        "Bulk database transaction started"
    );
    let mut tx = pool.begin().await.map_err(ProcessError::Database)?;
    bulk_upsert_document_chunks(&mut tx, workspace_id, document_id, &chunk_rows).await?;
    bulk_upsert_graph(&mut tx, workspace_id, document_id, &graph_batch)
        .await
        .map_err(ProcessError::Graph)?;
    mark_document_completed_tx(&mut tx, workspace_id, document_id).await?;
    tx.commit().await.map_err(ProcessError::Database)?;
    tracing::info!(
        %workspace_id,
        %document_id,
        elapsed_ms = db_started.elapsed().as_millis(),
        "Bulk database transaction committed"
    );

    let points = build_chunk_points_for_retrieval(&pool, workspace_id, document_id, &chunk_rows)
        .await
        .map_err(ProcessError::Retrieval)?;

    // Race: user có thể DELETE document sau SQL commit chunks nhưng trước upsert Qdrant.
    // Upsert lúc đó tạo orphan points (SQL đã không còn document). Skip nếu row đã mất.
    if !document_still_exists(&pool, workspace_id, document_id).await? {
        tracing::warn!(
            %workspace_id,
            %document_id,
            "Document deleted during ingestion; skip Qdrant upsert to avoid orphan points"
        );
        return Ok(());
    }

    retrieval
        .upsert_chunk_points(&points)
        .await
        .map_err(ProcessError::Retrieval)?;

    tracing::info!(
        %workspace_id,
        %document_id,
        chunk_count = chunk_rows.len(),
        graph_nodes = graph_batch.node_count(),
        graph_edges = graph_batch.edge_count(),
        elapsed_ms = document_started.elapsed().as_millis(),
        "Document ingestion completed"
    );

    Ok(())
}

async fn embed_chunks_batched(
    client: &Client,
    chunks: &[String],
) -> Result<Vec<Vec<f32>>, ProcessError> {
    let batch_size = embedding_batch_size();
    let concurrency = embedding_concurrency();
    let timeout_duration = Duration::from_secs(embedding_timeout_secs());
    let retries = embedding_retries();
    let batches = chunks
        .chunks(batch_size)
        .enumerate()
        .map(|(batch_index, texts)| {
            (
                batch_index * batch_size,
                texts.iter().map(ToOwned::to_owned).collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();

    let results = stream::iter(batches.into_iter())
        .map(|(start_index, texts)| {
            let client = client.clone();

            async move {
                let embeddings =
                    embed_batch_with_retry(&client, &texts, timeout_duration, retries).await?;
                Ok::<_, ProcessError>((start_index, embeddings))
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    let mut ordered = vec![None; chunks.len()];
    for result in results {
        let (start_index, embeddings) = result?;
        for (offset, embedding) in embeddings.into_iter().enumerate() {
            let index = start_index + offset;
            if index < ordered.len() {
                ordered[index] = Some(embedding);
            }
        }
    }

    ordered
        .into_iter()
        .enumerate()
        .map(|(index, embedding)| {
            embedding.ok_or(ProcessError::MissingEmbedding { chunk_index: index })
        })
        .collect()
}

async fn embed_batch_with_retry(
    client: &Client,
    texts: &[String],
    timeout_duration: Duration,
    retries: usize,
) -> Result<Vec<Vec<f32>>, ProcessError> {
    let mut last_error = None;

    for attempt in 0..=retries {
        let result = timeout(timeout_duration, embed_texts(client, texts)).await;

        match result {
            Ok(Ok(embeddings)) => return Ok(embeddings),
            Ok(Err(err)) => {
                last_error = Some(ProcessError::Embed(err));
            }
            Err(_) => {
                last_error = Some(ProcessError::EmbedTimeout {
                    timeout_secs: timeout_duration.as_secs(),
                });
            }
        }

        if attempt < retries {
            let delay_ms = embedding_retry_backoff_ms()
                .saturating_mul(2_u64.saturating_pow(attempt.min(6) as u32));
            sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    Err(last_error.unwrap_or(ProcessError::EmbedTimeout {
        timeout_secs: timeout_duration.as_secs(),
    }))
}

async fn extract_graph_for_chunks(
    client: Client,
    workspace_id: Uuid,
    document_id: Uuid,
    chunks: &[ChunkRow],
) -> Vec<(i32, Vec<GraphElement>)> {
    let concurrency = effective_graph_extraction_concurrency(chunks.len());
    let timeout_duration = Duration::from_secs(graph_extraction_timeout_secs());
    let retries = graph_extraction_retries();
    let stage_timeout = Duration::from_secs(graph_extraction_stage_timeout_secs());
    let inputs = chunks
        .iter()
        .map(|chunk| (chunk.index, chunk.text.clone()))
        .collect::<Vec<_>>();

    let mut pending = stream::iter(inputs.into_iter().map(|(index, text)| {
        let client = client.clone();

        async move {
            let started = Instant::now();
            let text_chars = text.chars().count();
            let result = extract_graph_with_retry(&client, &text, timeout_duration, retries).await;

            (index, result, started.elapsed().as_millis(), text_chars)
        }
    }))
    .buffer_unordered(concurrency);

    let mut graph_results = Vec::with_capacity(chunks.len());
    let mut failures = 0usize;

    let completed = timeout(stage_timeout, async {
        while let Some((index, result, elapsed_ms, text_chars)) = pending.next().await {
            match result {
                Ok(elements) => {
                    if elements.is_empty() {
                        tracing::warn!(
                            %workspace_id,
                            %document_id,
                            chunk_index = index,
                            text_chars,
                            elapsed_ms,
                            "Graph extraction chunk produced zero items"
                        );
                    } else {
                        tracing::info!(
                            %workspace_id,
                            %document_id,
                            chunk_index = index,
                            graph_items = elements.len(),
                            elapsed_ms,
                            "Graph extraction chunk completed"
                        );
                    }
                    graph_results.push((index, elements));
                }
                Err(err) => {
                    failures += 1;
                    tracing::warn!(
                        %workspace_id,
                        %document_id,
                        chunk_index = index,
                        elapsed_ms,
                        error = %err,
                        "Graph extraction chunk skipped; continuing ingestion"
                    );
                }
            }
        }
    })
    .await;

    if completed.is_err() {
        tracing::warn!(
            %workspace_id,
            %document_id,
            completed_chunks = graph_results.len(),
            failed_chunks = failures,
            total_chunks = chunks.len(),
            timeout_secs = stage_timeout.as_secs(),
            "Graph extraction stage timed out; persisting partial graph results"
        );
    }

    graph_results.sort_by_key(|(index, _)| *index);

    graph_results
}

async fn extract_graph_with_retry(
    client: &Client,
    text: &str,
    timeout_duration: Duration,
    retries: usize,
) -> Result<Vec<GraphElement>, ProcessError> {
    let mut last_error = None;

    for attempt in 0..=retries {
        let result = timeout(timeout_duration, extract_graph_elements(client, text)).await;

        match result {
            Ok(Ok(elements)) => return Ok(elements),
            Ok(Err(GraphError::MissingApiKey)) => {
                return Err(ProcessError::Graph(GraphError::MissingApiKey));
            }
            Ok(Err(err)) => {
                last_error = Some(ProcessError::Graph(err));
            }
            Err(_) => {
                last_error = Some(ProcessError::GraphTimeout {
                    timeout_secs: timeout_duration.as_secs(),
                });
            }
        }

        if attempt < retries {
            let delay_ms = graph_extraction_retry_backoff_ms()
                .saturating_mul(2_u64.saturating_pow(attempt.min(6) as u32));
            sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    Err(last_error.unwrap_or(ProcessError::GraphTimeout {
        timeout_secs: timeout_duration.as_secs(),
    }))
}

async fn bulk_upsert_document_chunks(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    document_id: Uuid,
    rows: &[ChunkRow],
) -> Result<(), ProcessError> {
    if rows.is_empty() {
        return Ok(());
    }

    let indexes: Vec<i32> = rows.iter().map(|row| row.index).collect();
    let texts: Vec<String> = rows.iter().map(|row| row.text.clone()).collect();
    let embeddings: Vec<String> = rows
        .iter()
        .map(|row| format_pgvector(&row.embedding))
        .collect();

    sqlx::query(
        r#"
        INSERT INTO document_chunks (document_id, workspace_id, chunk_index, original_text, embedding)
        SELECT $1, $2, chunk.chunk_index, chunk.original_text, chunk.embedding::vector
        FROM UNNEST($3::int[], $4::text[], $5::text[])
            AS chunk(chunk_index, original_text, embedding)
        ON CONFLICT (workspace_id, document_id, chunk_index)
        DO UPDATE SET
            original_text = EXCLUDED.original_text,
            embedding = EXCLUDED.embedding
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .bind(&indexes)
    .bind(&texts)
    .bind(&embeddings)
    .execute(&mut **tx)
    .await
    .map_err(ProcessError::Database)?;

    Ok(())
}

#[derive(sqlx::FromRow)]
struct PersistedChunkRow {
    id: Uuid,
    chunk_index: i32,
}

async fn build_chunk_points_for_retrieval(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
    rows: &[ChunkRow],
) -> Result<Vec<ChunkPoint>, RetrievalError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let persisted_rows: Vec<PersistedChunkRow> = sqlx::query_as(
        r#"
        SELECT id, chunk_index
        FROM document_chunks
        WHERE workspace_id = $1 AND document_id = $2
        "#,
    )
    .bind(workspace_id)
    .bind(document_id)
    .fetch_all(pool)
    .await?;

    let mut embeddings_by_index: std::collections::HashMap<i32, Vec<f32>> = rows
        .iter()
        .map(|row| (row.index, row.embedding.clone()))
        .collect();

    let mut points = Vec::with_capacity(persisted_rows.len());
    for persisted in persisted_rows {
        if let Some(embedding) = embeddings_by_index.remove(&persisted.chunk_index) {
            points.push(ChunkPoint {
                chunk_id: persisted.id,
                workspace_id,
                document_id,
                chunk_index: persisted.chunk_index,
                embedding,
            });
        }
    }

    Ok(points)
}

#[derive(sqlx::FromRow)]
struct DocumentStorageMetadata {
    object_key: String,
}

async fn document_still_exists(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<bool, ProcessError> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM documents
            WHERE id = $1 AND workspace_id = $2
        )
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_one(pool)
    .await
    .map_err(ProcessError::Database)?;
    Ok(exists)
}

#[cfg(test)]
mod race_guard_tests {
    use super::document_still_exists;
    use sqlx::postgres::PgPoolOptions;
    use uuid::Uuid;

    /// Fix High #4: sau khi document bị xoá, race guard phải thấy `exists=false`.
    #[tokio::test]
    async fn document_still_exists_false_after_delete() {
        dotenvy::dotenv().ok();
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skip: DATABASE_URL not set");
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .expect("connect");
        let _ = sqlx::migrate!("./migrations").run(&pool).await;

        let tenant_id = Uuid::new_v4();
        let workspace_id = Uuid::new_v4();
        let document_id = Uuid::new_v4();
        let user_id = format!("race-guard-{}", Uuid::new_v4());

        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(&user_id)
            .bind(format!("{user_id}@test.local"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
            .bind(tenant_id)
            .bind(format!("race-tenant-{tenant_id}"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
            .bind(workspace_id)
            .bind(tenant_id)
            .bind(format!("race-ws-{workspace_id}"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO documents (
                id, workspace_id, owner_id, filename, status, processing_stage,
                object_key, bucket, uploaded_by
            )
            VALUES (
                $1, $2, $3, 'race.pdf', 'PROCESSING', 'EMBEDDING',
                'tmp/race.pdf', 'gmrag-documents', $3
            )
            "#,
        )
        .bind(document_id)
        .bind(workspace_id)
        .bind(&user_id)
        .execute(&pool)
        .await
        .unwrap();

        assert!(
            document_still_exists(&pool, workspace_id, document_id)
                .await
                .unwrap()
        );

        sqlx::query("DELETE FROM documents WHERE id = $1")
            .bind(document_id)
            .execute(&pool)
            .await
            .unwrap();

        assert!(
            !document_still_exists(&pool, workspace_id, document_id)
                .await
                .unwrap(),
            "after SQL delete, upsert race guard must report missing document"
        );

        sqlx::query("DELETE FROM workspaces WHERE id = $1")
            .bind(workspace_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM tenants WHERE id = $1")
            .bind(tenant_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(&user_id)
            .execute(&pool)
            .await
            .ok();
    }
}

async fn fetch_document_storage_metadata(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<Option<DocumentStorageMetadata>, ProcessError> {
    sqlx::query_as(
        r#"
        SELECT object_key
        FROM documents
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .fetch_optional(pool)
    .await
    .map_err(ProcessError::Database)
}

async fn set_processing_stage(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
    stage: &str,
) -> Result<(), ProcessError> {
    sqlx::query(
        r#"
        UPDATE documents
        SET processing_stage = $3
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .bind(stage)
    .execute(pool)
    .await
    .map_err(ProcessError::Database)?;
    Ok(())
}

async fn mark_document_completed_tx(
    tx: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<(), ProcessError> {
    sqlx::query(
        r#"
        UPDATE documents
        SET status = 'COMPLETED', processing_stage = 'DONE'
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .execute(&mut **tx)
    .await
    .map_err(ProcessError::Database)?;
    Ok(())
}

async fn mark_document_failed(
    pool: &PgPool,
    workspace_id: Uuid,
    document_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE documents
        SET status = 'FAILED'
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn embedding_batch_size() -> usize {
    env_usize("GMRAG_EMBEDDING_BATCH_SIZE", 32, 1)
}

fn embedding_concurrency() -> usize {
    env_usize("GMRAG_EMBEDDING_CONCURRENCY", 2, 1)
}

fn embedding_timeout_secs() -> u64 {
    env_u64("GMRAG_EMBEDDING_TIMEOUT_SECS", 120, 1)
}

fn embedding_retries() -> usize {
    env_usize("GMRAG_EMBEDDING_RETRIES", 1, 0)
}

fn embedding_retry_backoff_ms() -> u64 {
    env_u64("GMRAG_EMBEDDING_RETRY_BACKOFF_MS", 250, 1)
}

fn pdf_parse_timeout_secs() -> u64 {
    env_u64("GMRAG_PDF_PARSE_TIMEOUT_SECS", 120, 1)
}

fn graph_extraction_enabled() -> bool {
    env_bool("GMRAG_GRAPH_EXTRACTION_ENABLED", true)
}

fn graph_extraction_concurrency() -> usize {
    env_usize("GMRAG_GRAPH_EXTRACTION_CONCURRENCY", 12, 1)
}

fn effective_graph_extraction_concurrency(chunk_count: usize) -> usize {
    graph_extraction_concurrency().min(chunk_count.max(1))
}

fn graph_extraction_timeout_secs() -> u64 {
    env_u64("GMRAG_GRAPH_EXTRACTION_TIMEOUT_SECS", 20, 1)
}

fn graph_extraction_retries() -> usize {
    env_usize("GMRAG_GRAPH_EXTRACTION_RETRIES", 0, 0)
}

fn graph_extraction_stage_timeout_secs() -> u64 {
    env_u64("GMRAG_GRAPH_EXTRACTION_STAGE_TIMEOUT_SECS", 30, 1)
}

fn graph_extraction_retry_backoff_ms() -> u64 {
    env_u64("GMRAG_GRAPH_EXTRACTION_RETRY_BACKOFF_MS", 250, 1)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize, min: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
        .max(min)
}

fn env_u64(name: &str, default: u64, min: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
        .max(min)
}

struct ChunkRow {
    index: i32,
    text: String,
    embedding: Vec<f32>,
}

#[derive(Debug)]
enum ProcessError {
    Join,
    DocumentNotFound { document_id: Uuid },
    DocumentObjectMissing { object_key: String },
    Storage(StorageError),
    Parse(PdfParseError),
    ParseTimeout { timeout_secs: u64 },
    Chunk(ChunkError),
    Embed(EmbedError),
    EmbedTimeout { timeout_secs: u64 },
    MissingEmbedding { chunk_index: usize },
    Graph(GraphError),
    GraphTimeout { timeout_secs: u64 },
    Retrieval(RetrievalError),
    Database(sqlx::Error),
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessError::Join => write!(f, "background task panicked"),
            ProcessError::DocumentNotFound { document_id } => {
                write!(f, "document not found in database: {document_id}")
            }
            ProcessError::DocumentObjectMissing { object_key } => {
                write!(f, "document object missing in storage: {object_key}")
            }
            ProcessError::Storage(err) => write!(f, "storage error: {err}"),
            ProcessError::Parse(e) => write!(f, "{e}"),
            ProcessError::ParseTimeout { timeout_secs } => {
                write!(f, "PDF parsing timed out after {timeout_secs}s")
            }
            ProcessError::Chunk(e) => write!(f, "{e}"),
            ProcessError::Embed(e) => write!(f, "{e}"),
            ProcessError::EmbedTimeout { timeout_secs } => {
                write!(
                    f,
                    "ollama embedding request timed out after {timeout_secs}s"
                )
            }
            ProcessError::MissingEmbedding { chunk_index } => {
                write!(f, "missing embedding for chunk {chunk_index}")
            }
            ProcessError::Graph(e) => write!(f, "{e}"),
            ProcessError::GraphTimeout { timeout_secs } => {
                write!(f, "graph extraction timed out after {timeout_secs}s")
            }
            ProcessError::Retrieval(err) => write!(f, "retrieval error: {err}"),
            ProcessError::Database(e) => write!(f, "database error: {e}"),
        }
    }
}

impl std::error::Error for ProcessError {}
