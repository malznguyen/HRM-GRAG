//! Operator backfill: điền `graph_nodes.embedding` cho node legacy (`embedding IS NULL`).
//!
//! Chỉ chạy qua binary `backfill-graph-node-embeddings`. Mặc định dry-run.
//! Text embed tái dùng `node_text_for_embedding` (cùng forward-path); vector qua ADR-21 `embed_texts`.

use std::fmt;
use std::future::Future;

use reqwest::Client;
use sqlx::PgPool;
use uuid::Uuid;

use super::embedding::{EmbedError, embed_text, embed_texts, format_pgvector};
use super::graph::node_text_for_embedding;

const DEFAULT_BATCH_SIZE: usize = 50;
const MAX_ERROR_SAMPLES: usize = 10;

/// Cờ vận hành: mặc định không ghi; cần `allow_apply` để UPDATE embedding.
#[derive(Debug, Clone)]
pub struct BackfillGraphNodeEmbeddingsOptions {
    pub allow_apply: bool,
    pub workspace_id: Option<Uuid>,
    pub batch_size: usize,
}

impl Default for BackfillGraphNodeEmbeddingsOptions {
    fn default() -> Self {
        Self {
            allow_apply: false,
            workspace_id: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

impl BackfillGraphNodeEmbeddingsOptions {
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    pub fn with_workspace_id(mut self, workspace_id: Option<Uuid>) -> Self {
        self.workspace_id = workspace_id;
        self
    }

    pub fn with_allow_apply(mut self, allow_apply: bool) -> Self {
        self.allow_apply = allow_apply;
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct WorkspaceNullEmbeddingCount {
    pub workspace_id: Uuid,
    pub null_count: i64,
}

#[derive(Debug, Clone, Default)]
pub struct BackfillGraphNodeEmbeddingsReport {
    pub applied: bool,
    pub workspace_filter: Option<Uuid>,
    pub batch_size: usize,
    /// Tổng node `embedding IS NULL` tại thời điểm quét ban đầu (dry-run và apply).
    pub nodes_found: usize,
    pub counts_by_workspace: Vec<WorkspaceNullEmbeddingCount>,
    /// Số node UPDATE embedding thành công (chỉ > 0 khi apply).
    pub nodes_updated: usize,
    /// Node SELECT được nhưng UPDATE không đổi (đã có embedding giữa chừng).
    pub nodes_skipped_already_embedded: usize,
    pub error_count: usize,
    /// Vài lỗi đầu (node_id + message) — không chứa entity_name/description.
    pub error_samples: Vec<String>,
}

#[derive(Debug)]
pub enum BackfillGraphNodeEmbeddingsError {
    Database(sqlx::Error),
}

impl fmt::Display for BackfillGraphNodeEmbeddingsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackfillGraphNodeEmbeddingsError::Database(err) => {
                write!(f, "database error: {err}")
            }
        }
    }
}

impl std::error::Error for BackfillGraphNodeEmbeddingsError {}

impl From<sqlx::Error> for BackfillGraphNodeEmbeddingsError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct NullEmbeddingNode {
    id: Uuid,
    /// Giữ trong SELECT để log/debug theo workspace nếu cần; hiện không dùng trong UPDATE.
    #[allow(dead_code)]
    workspace_id: Uuid,
    entity_name: String,
    description: Option<String>,
}

/// Backfill production: gọi Ollama qua `embed_texts` / `embed_text` (ADR-21).
pub async fn backfill_graph_node_embeddings(
    pool: &PgPool,
    client: &Client,
    options: BackfillGraphNodeEmbeddingsOptions,
) -> Result<BackfillGraphNodeEmbeddingsReport, BackfillGraphNodeEmbeddingsError> {
    backfill_graph_node_embeddings_with_embedder(pool, options, |texts| {
        let client = client.clone();
        async move { embed_batch_with_fallback(&client, &texts).await }
    })
    .await
}

/// Core logic; `embed_batch` nhận text đã format bởi `node_text_for_embedding`.
///
/// Mỗi phần tử `Ok(vector)` hoặc `Err(message)` — lỗi 1 node không chặn node khác.
pub async fn backfill_graph_node_embeddings_with_embedder<E, Fut>(
    pool: &PgPool,
    options: BackfillGraphNodeEmbeddingsOptions,
    mut embed_batch: E,
) -> Result<BackfillGraphNodeEmbeddingsReport, BackfillGraphNodeEmbeddingsError>
where
    E: FnMut(Vec<String>) -> Fut,
    Fut: Future<Output = Result<Vec<Result<Vec<f32>, String>>, String>>,
{
    let batch_size = options.batch_size.max(1);
    let counts = count_null_embeddings_by_workspace(pool, options.workspace_id).await?;
    let nodes_found: usize = counts.iter().map(|row| row.null_count as usize).sum();

    let mut report = BackfillGraphNodeEmbeddingsReport {
        applied: options.allow_apply,
        workspace_filter: options.workspace_id,
        batch_size,
        nodes_found,
        counts_by_workspace: counts,
        nodes_updated: 0,
        nodes_skipped_already_embedded: 0,
        error_count: 0,
        error_samples: Vec::new(),
    };

    // Dry-run: chỉ báo cáo counts, không gọi Ollama, không UPDATE.
    if !options.allow_apply {
        return Ok(report);
    }

    if nodes_found == 0 {
        return Ok(report);
    }

    // Node lỗi trong lần chạy này bị exclude khỏi SELECT tiếp theo (tránh spin vô hạn).
    let mut exclude_ids: Vec<Uuid> = Vec::new();

    loop {
        let batch = fetch_null_embedding_batch(
            pool,
            options.workspace_id,
            batch_size as i64,
            &exclude_ids,
        )
        .await?;
        if batch.is_empty() {
            break;
        }

        let texts: Vec<String> = batch
            .iter()
            .map(|node| node_text_for_embedding(&node.entity_name, node.description.as_deref()))
            .collect();

        match embed_batch(texts).await {
            Ok(per_node) => {
                if per_node.len() != batch.len() {
                    for node in &batch {
                        record_node_error(
                            &mut report,
                            &mut exclude_ids,
                            node.id,
                            &format!(
                                "embedder returned {} results for {} texts",
                                per_node.len(),
                                batch.len()
                            ),
                        );
                    }
                    continue;
                }

                for (node, result) in batch.iter().zip(per_node) {
                    match result {
                        Ok(embedding) => {
                            match update_node_embedding(pool, node.id, &embedding).await {
                                Ok(updated) => {
                                    if updated {
                                        report.nodes_updated += 1;
                                    } else {
                                        report.nodes_skipped_already_embedded += 1;
                                    }
                                }
                                Err(err) => {
                                    record_node_error(
                                        &mut report,
                                        &mut exclude_ids,
                                        node.id,
                                        &format!("database update failed: {err}"),
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            record_node_error(&mut report, &mut exclude_ids, node.id, &err);
                        }
                    }
                }
            }
            Err(err) => {
                for node in &batch {
                    record_node_error(&mut report, &mut exclude_ids, node.id, &err);
                }
            }
        }
    }

    Ok(report)
}

/// Gọi Ollama batch; nếu fail, fallback từng text — lỗi 1 node không chặn node khác.
async fn embed_batch_with_fallback(
    client: &Client,
    texts: &[String],
) -> Result<Vec<Result<Vec<f32>, String>>, String> {
    if texts.is_empty() {
        return Ok(Vec::new());
    }

    match embed_texts(client, texts).await {
        Ok(embeddings) => {
            if embeddings.len() != texts.len() {
                return Ok(embed_each(client, texts).await);
            }
            Ok(embeddings.into_iter().map(Ok).collect())
        }
        Err(batch_err) => {
            tracing::warn!(
                error = %batch_err,
                text_count = texts.len(),
                "Batch embed failed; falling back to per-node embed"
            );
            Ok(embed_each(client, texts).await)
        }
    }
}

async fn embed_each(client: &Client, texts: &[String]) -> Vec<Result<Vec<f32>, String>> {
    let mut results = Vec::with_capacity(texts.len());
    for text in texts {
        match embed_text(client, text).await {
            Ok(embedding) => results.push(Ok(embedding)),
            Err(err) => results.push(Err(format_embed_error(&err))),
        }
    }
    results
}

fn format_embed_error(err: &EmbedError) -> String {
    err.to_string()
}

fn record_node_error(
    report: &mut BackfillGraphNodeEmbeddingsReport,
    exclude_ids: &mut Vec<Uuid>,
    node_id: Uuid,
    message: &str,
) {
    report.error_count += 1;
    exclude_ids.push(node_id);
    tracing::warn!(%node_id, error = %message, "Graph node embedding backfill failed for node");
    if report.error_samples.len() < MAX_ERROR_SAMPLES {
        report
            .error_samples
            .push(format!("node_id={node_id}: {message}"));
    }
}

async fn count_null_embeddings_by_workspace(
    pool: &PgPool,
    workspace_id: Option<Uuid>,
) -> Result<Vec<WorkspaceNullEmbeddingCount>, BackfillGraphNodeEmbeddingsError> {
    let rows = sqlx::query_as::<_, (Uuid, i64)>(
        r#"
        SELECT workspace_id, COUNT(*)::bigint AS null_count
        FROM graph_nodes
        WHERE embedding IS NULL
          AND ($1::uuid IS NULL OR workspace_id = $1)
        GROUP BY workspace_id
        ORDER BY workspace_id
        "#,
    )
    .bind(workspace_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(workspace_id, null_count)| WorkspaceNullEmbeddingCount {
            workspace_id,
            null_count,
        })
        .collect())
}

async fn fetch_null_embedding_batch(
    pool: &PgPool,
    workspace_id: Option<Uuid>,
    limit: i64,
    exclude_ids: &[Uuid],
) -> Result<Vec<NullEmbeddingNode>, BackfillGraphNodeEmbeddingsError> {
    let rows = sqlx::query_as::<_, NullEmbeddingNode>(
        r#"
        SELECT id, workspace_id, entity_name, description
        FROM graph_nodes
        WHERE embedding IS NULL
          AND ($1::uuid IS NULL OR workspace_id = $1)
          AND NOT (id = ANY($2::uuid[]))
        ORDER BY workspace_id, id
        LIMIT $3
        "#,
    )
    .bind(workspace_id)
    .bind(exclude_ids)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Chỉ UPDATE khi vẫn NULL — idempotent và không ghi đè embedding đã có.
async fn update_node_embedding(
    pool: &PgPool,
    node_id: Uuid,
    embedding: &[f32],
) -> Result<bool, sqlx::Error> {
    let literal = format_pgvector(embedding);
    let result = sqlx::query(
        r#"
        UPDATE graph_nodes
        SET embedding = $2::vector
        WHERE id = $1
          AND embedding IS NULL
        "#,
    )
    .bind(node_id)
    .bind(literal)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::graph::node_text_for_embedding;

    #[test]
    fn default_options_are_dry_run() {
        let options = BackfillGraphNodeEmbeddingsOptions::default();
        assert!(!options.allow_apply);
        assert!(options.workspace_id.is_none());
        assert_eq!(options.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn batch_size_clamped_to_at_least_one() {
        let options = BackfillGraphNodeEmbeddingsOptions::default().with_batch_size(0);
        assert_eq!(options.batch_size, 1);
    }

    #[test]
    fn reuses_node_text_for_embedding_shape() {
        // Guard: backfill phải cùng format text với forward-path.
        assert_eq!(
            node_text_for_embedding("ICU", Some("Intensive care")),
            "ICU\nIntensive care"
        );
        assert_eq!(node_text_for_embedding("ICU", None), "ICU");
    }
}
