use std::collections::HashSet;
use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RetrievalConfig {
    pub qdrant_url: String,
    pub collection_name: String,
    pub vector_size: usize,
    pub top_k: usize,
    pub api_key: Option<String>,
}

impl RetrievalConfig {
    pub fn from_env() -> Result<Self, RetrievalConfigError> {
        let qdrant_url = std::env::var("QDRANT_URL")
            .unwrap_or_else(|_| "http://localhost:6333".to_string())
            .trim_end_matches('/')
            .to_string();

        if qdrant_url.is_empty() {
            return Err(RetrievalConfigError::MissingQdrantUrl);
        }

        let collection_name = std::env::var("QDRANT_COLLECTION")
            .unwrap_or_else(|_| "gmrag_document_chunks".to_string())
            .trim()
            .to_string();

        if collection_name.is_empty() {
            return Err(RetrievalConfigError::MissingCollectionName);
        }

        let vector_size = std::env::var("QDRANT_VECTOR_SIZE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(768)
            .max(1);

        let top_k = std::env::var("QDRANT_TOP_K")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(5)
            .max(1);

        let api_key = std::env::var("QDRANT_API_KEY")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        Ok(Self {
            qdrant_url,
            collection_name,
            vector_size,
            top_k,
            api_key,
        })
    }
}

#[derive(Debug)]
pub enum RetrievalConfigError {
    MissingQdrantUrl,
    MissingCollectionName,
}

impl fmt::Display for RetrievalConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetrievalConfigError::MissingQdrantUrl => write!(f, "QDRANT_URL must not be empty"),
            RetrievalConfigError::MissingCollectionName => {
                write!(f, "QDRANT_COLLECTION must not be empty")
            }
        }
    }
}

impl std::error::Error for RetrievalConfigError {}

#[derive(Debug)]
pub enum RetrievalError {
    Http(reqwest::Error),
    Api {
        status: StatusCode,
        body: String,
        operation: &'static str,
    },
    InvalidPointId {
        raw_id: Value,
    },
    InvalidEmbeddingLiteral {
        literal: String,
    },
    Database(sqlx::Error),
}

impl fmt::Display for RetrievalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetrievalError::Http(err) => write!(f, "Qdrant HTTP request failed: {err}"),
            RetrievalError::Api {
                status,
                body,
                operation,
            } => write!(
                f,
                "Qdrant API error during {operation}: status={status}, body={body}"
            ),
            RetrievalError::InvalidPointId { raw_id } => {
                write!(f, "invalid Qdrant point id: {raw_id}")
            }
            RetrievalError::InvalidEmbeddingLiteral { literal } => {
                write!(f, "invalid pgvector literal: {literal}")
            }
            RetrievalError::Database(err) => write!(f, "database error: {err}"),
        }
    }
}

impl std::error::Error for RetrievalError {}

impl From<reqwest::Error> for RetrievalError {
    fn from(value: reqwest::Error) -> Self {
        RetrievalError::Http(value)
    }
}

impl From<sqlx::Error> for RetrievalError {
    fn from(value: sqlx::Error) -> Self {
        RetrievalError::Database(value)
    }
}

#[derive(Debug, Clone)]
pub struct ChunkPoint {
    pub chunk_id: Uuid,
    pub workspace_id: Uuid,
    pub document_id: Uuid,
    pub chunk_index: i32,
    pub embedding: Vec<f32>,
}

#[derive(Clone)]
pub struct RetrievalClient {
    http: reqwest::Client,
    config: RetrievalConfig,
    collection_ready: Arc<AtomicBool>,
    collection_guard: Arc<Mutex<()>>,
    backfilled_workspaces: Arc<Mutex<HashSet<Uuid>>>,
}

impl RetrievalClient {
    pub fn from_config(config: RetrievalConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
            collection_ready: Arc::new(AtomicBool::new(false)),
            collection_guard: Arc::new(Mutex::new(())),
            backfilled_workspaces: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn from_env() -> Result<Self, RetrievalConfigError> {
        let config = RetrievalConfig::from_env()?;
        Ok(Self::from_config(config))
    }

    pub fn top_k(&self) -> usize {
        self.config.top_k
    }

    pub async fn upsert_chunk_points(&self, points: &[ChunkPoint]) -> Result<(), RetrievalError> {
        if points.is_empty() {
            return Ok(());
        }

        self.ensure_collection().await?;

        let qdrant_points: Vec<QdrantPointUpsert> = points
            .iter()
            .map(|point| QdrantPointUpsert {
                id: point.chunk_id.to_string(),
                vector: point.embedding.clone(),
                payload: QdrantChunkPayload {
                    workspace_id: point.workspace_id.to_string(),
                    document_id: point.document_id.to_string(),
                    chunk_index: point.chunk_index,
                },
            })
            .collect();

        let url = format!(
            "{}/collections/{}/points?wait=true",
            self.config.qdrant_url, self.config.collection_name
        );

        let request = self.http.put(url).json(&QdrantUpsertRequest {
            points: qdrant_points,
        });

        let response = self.with_auth_header(request).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(RetrievalError::Api {
                status,
                body,
                operation: "points_upsert",
            });
        }

        Ok(())
    }

    pub async fn search_chunk_ids(
        &self,
        workspace_id: Uuid,
        allowed_document_ids: &[Uuid],
        query_vector: &[f32],
        limit: usize,
    ) -> Result<Vec<Uuid>, RetrievalError> {
        if allowed_document_ids.is_empty() {
            return Ok(Vec::new());
        }

        self.ensure_collection().await?;

        let document_ids: Vec<String> = allowed_document_ids
            .iter()
            .map(|id| id.to_string())
            .collect();

        let payload = QdrantSearchRequest {
            vector: query_vector.to_vec(),
            limit,
            with_payload: false,
            with_vector: false,
            filter: QdrantFilter {
                must: vec![
                    QdrantCondition {
                        key: "workspace_id".to_string(),
                        r#match: QdrantMatch {
                            value: Some(workspace_id.to_string()),
                            any: None,
                        },
                    },
                    QdrantCondition {
                        key: "document_id".to_string(),
                        r#match: QdrantMatch {
                            value: None,
                            any: Some(document_ids),
                        },
                    },
                ],
            },
        };

        let url = format!(
            "{}/collections/{}/points/search",
            self.config.qdrant_url, self.config.collection_name
        );

        let request = self.http.post(url).json(&payload);
        let response = self.with_auth_header(request).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(RetrievalError::Api {
                status,
                body,
                operation: "points_search",
            });
        }

        let body: QdrantSearchResponse = response.json().await?;
        body.result
            .into_iter()
            .map(|point| parse_point_id(point.id))
            .collect()
    }

    pub async fn ensure_workspace_backfilled(
        &self,
        pool: &PgPool,
        workspace_id: Uuid,
    ) -> Result<(), RetrievalError> {
        {
            let guard = self.backfilled_workspaces.lock().await;
            if guard.contains(&workspace_id) {
                return Ok(());
            }
        }

        let rows: Vec<BackfillChunkRow> = sqlx::query_as(
            r#"
            SELECT
                id,
                document_id,
                chunk_index,
                embedding::text AS embedding_literal
            FROM document_chunks
            WHERE workspace_id = $1
              AND embedding IS NOT NULL
            "#,
        )
        .bind(workspace_id)
        .fetch_all(pool)
        .await?;

        let mut points = Vec::with_capacity(rows.len());
        for row in rows {
            points.push(ChunkPoint {
                chunk_id: row.id,
                workspace_id,
                document_id: row.document_id,
                chunk_index: row.chunk_index,
                embedding: parse_pgvector_literal(&row.embedding_literal)?,
            });
        }

        for batch in points.chunks(256) {
            self.upsert_chunk_points(batch).await?;
        }

        let mut guard = self.backfilled_workspaces.lock().await;
        guard.insert(workspace_id);
        Ok(())
    }

    fn with_auth_header(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(api_key) = &self.config.api_key {
            return request.header("api-key", api_key);
        }
        request
    }

    async fn ensure_collection(&self) -> Result<(), RetrievalError> {
        if self.collection_ready.load(Ordering::Relaxed) {
            return Ok(());
        }

        let _guard = self.collection_guard.lock().await;
        if self.collection_ready.load(Ordering::Relaxed) {
            return Ok(());
        }

        let url = format!(
            "{}/collections/{}",
            self.config.qdrant_url, self.config.collection_name
        );

        let request = self.http.put(url).json(&QdrantCreateCollectionRequest {
            vectors: QdrantVectorsConfig {
                size: self.config.vector_size,
                distance: "Cosine",
            },
        });

        let response = self.with_auth_header(request).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(RetrievalError::Api {
                status,
                body,
                operation: "create_collection",
            });
        }

        self.collection_ready.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Serialize)]
struct QdrantCreateCollectionRequest<'a> {
    vectors: QdrantVectorsConfig<'a>,
}

#[derive(Serialize)]
struct QdrantVectorsConfig<'a> {
    size: usize,
    distance: &'a str,
}

#[derive(Serialize)]
struct QdrantUpsertRequest {
    points: Vec<QdrantPointUpsert>,
}

#[derive(Serialize)]
struct QdrantPointUpsert {
    id: String,
    vector: Vec<f32>,
    payload: QdrantChunkPayload,
}

#[derive(Serialize)]
struct QdrantChunkPayload {
    workspace_id: String,
    document_id: String,
    chunk_index: i32,
}

#[derive(Serialize)]
struct QdrantSearchRequest {
    vector: Vec<f32>,
    limit: usize,
    with_payload: bool,
    with_vector: bool,
    filter: QdrantFilter,
}

#[derive(Serialize)]
struct QdrantFilter {
    must: Vec<QdrantCondition>,
}

#[derive(Serialize)]
struct QdrantCondition {
    key: String,
    r#match: QdrantMatch,
}

#[derive(Serialize)]
struct QdrantMatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    any: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct QdrantSearchResponse {
    result: Vec<QdrantSearchPoint>,
}

#[derive(Deserialize)]
struct QdrantSearchPoint {
    id: Value,
}

#[derive(sqlx::FromRow)]
struct BackfillChunkRow {
    id: Uuid,
    document_id: Uuid,
    chunk_index: i32,
    embedding_literal: String,
}

fn parse_point_id(raw_id: Value) -> Result<Uuid, RetrievalError> {
    let Some(raw_id) = raw_id.as_str() else {
        return Err(RetrievalError::InvalidPointId { raw_id });
    };

    Uuid::parse_str(raw_id).map_err(|_| RetrievalError::InvalidPointId {
        raw_id: Value::String(raw_id.to_string()),
    })
}

fn parse_pgvector_literal(literal: &str) -> Result<Vec<f32>, RetrievalError> {
    let trimmed = literal.trim();
    let Some(body) = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
    else {
        return Err(RetrievalError::InvalidEmbeddingLiteral {
            literal: literal.to_string(),
        });
    };

    if body.trim().is_empty() {
        return Ok(Vec::new());
    }

    body.split(',')
        .map(|token| {
            token
                .trim()
                .parse::<f32>()
                .map_err(|_| RetrievalError::InvalidEmbeddingLiteral {
                    literal: literal.to_string(),
                })
        })
        .collect()
}
