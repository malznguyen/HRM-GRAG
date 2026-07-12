pub mod cleanup;
pub mod outbox;
pub mod outbox_health;

use std::collections::HashSet;
use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::Mutex;
use tracing::warn;
use uuid::Uuid;

/// Timeout ngắn mặc định cho HTTP delete path (giây) — tránh treo API request.
const DEFAULT_DELETE_REQUEST_TIMEOUT_SECS: u64 = 5;
/// Timeout dài mặc định cho outbox worker / operator cleanup (giây).
const DEFAULT_DELETE_WORKER_TIMEOUT_SECS: u64 = 60;
/// Trần an toàn cho timeout delete (giây) — tránh cấu hình cực lớn treo process.
const MAX_DELETE_TIMEOUT_SECS: u64 = 300;

#[derive(Debug, Clone)]
pub struct RetrievalConfig {
    pub qdrant_url: String,
    pub collection_name: String,
    pub vector_size: usize,
    pub top_k: usize,
    pub api_key: Option<String>,
    /// Timeout ngắn cho DELETE HTTP handler (sync best-effort rồi enqueue).
    pub delete_request_timeout_secs: u64,
    /// Timeout dài cho `process-qdrant-outbox` / `cleanup-qdrant-orphans`.
    pub delete_worker_timeout_secs: u64,
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

        // Default matches ADR-21 / `document_chunks.embedding vector(768)`.
        let vector_size = std::env::var("QDRANT_VECTOR_SIZE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(crate::ingestion::embedding::EXPECTED_EMBEDDING_DIM)
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

        // Request path phải short-circuit; worker được chờ lâu hơn (collection lớn).
        let delete_request_timeout_secs = parse_timeout_secs(
            "QDRANT_DELETE_REQUEST_TIMEOUT_SECS",
            DEFAULT_DELETE_REQUEST_TIMEOUT_SECS,
        );

        // Ưu tiên env worker mới; fallback `QDRANT_DELETE_TIMEOUT_SECS` (tên cũ) cho ops đã cấu hình.
        let delete_worker_timeout_secs = std::env::var("QDRANT_DELETE_WORKER_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| {
                std::env::var("QDRANT_DELETE_TIMEOUT_SECS")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
            })
            .unwrap_or(DEFAULT_DELETE_WORKER_TIMEOUT_SECS)
            .clamp(1, MAX_DELETE_TIMEOUT_SECS);

        Ok(Self {
            qdrant_url,
            collection_name,
            vector_size,
            top_k,
            api_key,
            delete_request_timeout_secs,
            delete_worker_timeout_secs,
        })
    }
}

fn parse_timeout_secs(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
        .clamp(1, MAX_DELETE_TIMEOUT_SECS)
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
    /// Delete `wait=true` vượt quá timeout client — caller nên enqueue outbox.
    Timeout {
        operation: &'static str,
        timeout_secs: u64,
    },
    /// `delete_points_by_tenant` không resolve được workspace nào (tenant đã cascade / không tồn tại).
    EmptyTenantWorkspaceList {
        tenant_id: Uuid,
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
            RetrievalError::Timeout {
                operation,
                timeout_secs,
            } => write!(
                f,
                "Qdrant {operation} timed out after {timeout_secs}s (wait=true not confirmed)"
            ),
            RetrievalError::EmptyTenantWorkspaceList { tenant_id } => write!(
                f,
                "no workspaces found for tenant_id={tenant_id} (already cascaded? capture workspace ids before SQL delete, or use outbox/cleanup with explicit ids)"
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

        // Point id ổn định nên replay toàn bộ batch sau timeout/an unknown response vẫn idempotent.
        for batch in points.chunks(256) {
            self.upsert_chunk_point_batch(batch).await?;
        }
        Ok(())
    }

    async fn upsert_chunk_point_batch(&self, points: &[ChunkPoint]) -> Result<(), RetrievalError> {
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

    /// Timeout ngắn cho HTTP delete handler (`QDRANT_DELETE_REQUEST_TIMEOUT_SECS`).
    pub fn delete_request_timeout_secs(&self) -> u64 {
        self.config.delete_request_timeout_secs
    }

    /// Timeout dài cho outbox/cleanup worker (`QDRANT_DELETE_WORKER_TIMEOUT_SECS`).
    pub fn delete_worker_timeout_secs(&self) -> u64 {
        self.config.delete_worker_timeout_secs
    }

    /// Xoá points document — timeout **worker** (outbox / operator).
    ///
    /// Idempotent: document không có point vẫn Ok khi API reachable.
    /// Không gọi `ensure_collection` — tránh tạo collection rỗng chỉ để xoá.
    pub async fn delete_points_by_document(
        &self,
        workspace_id: Uuid,
        document_id: Uuid,
    ) -> Result<(), RetrievalError> {
        self.delete_points_by_document_with_timeout(
            workspace_id,
            document_id,
            self.config.delete_worker_timeout_secs,
        )
        .await
    }

    /// Xoá points document trên **HTTP request path** — timeout ngắn.
    ///
    /// Lý do tách timeout: DELETE API không được block tới 60s khi Qdrant chậm.
    /// Short-circuit → caller enqueue `qdrant_outbox` → worker retry với timeout dài.
    pub async fn delete_points_by_document_for_request(
        &self,
        workspace_id: Uuid,
        document_id: Uuid,
    ) -> Result<(), RetrievalError> {
        self.delete_points_by_document_with_timeout(
            workspace_id,
            document_id,
            self.config.delete_request_timeout_secs,
        )
        .await
    }

    async fn delete_points_by_document_with_timeout(
        &self,
        workspace_id: Uuid,
        document_id: Uuid,
        timeout_secs: u64,
    ) -> Result<(), RetrievalError> {
        // Lọc kép workspace + document để tránh xoá nhầm cross-tenant nếu payload lệch.
        self.delete_points_by_filter(
            vec![
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
                        value: Some(document_id.to_string()),
                        any: None,
                    },
                },
            ],
            timeout_secs,
        )
        .await
    }

    /// Xoá points workspace — timeout **worker**.
    pub async fn delete_points_by_workspace(
        &self,
        workspace_id: Uuid,
    ) -> Result<(), RetrievalError> {
        self.delete_points_by_workspaces_with_timeout(
            &[workspace_id],
            self.config.delete_worker_timeout_secs,
        )
        .await
    }

    /// Xoá points workspace trên **HTTP request path** — timeout ngắn (xem document counterpart).
    pub async fn delete_points_by_workspace_for_request(
        &self,
        workspace_id: Uuid,
    ) -> Result<(), RetrievalError> {
        self.delete_points_by_workspaces_with_timeout(
            &[workspace_id],
            self.config.delete_request_timeout_secs,
        )
        .await
    }

    /// Xoá points theo danh sách workspace — timeout **worker**.
    ///
    /// Payload Qdrant **không** có `tenant_id`; caller phải capture workspace ids
    /// **trước** SQL cascade khi xoá tenant.
    ///
    /// - Slice rỗng → Ok ngay (idempotent no-op cho caller đã biết list rỗng).
    /// - Tenant path dùng `delete_points_by_tenant` — rỗng sẽ **hard fail**.
    pub async fn delete_points_by_workspaces(
        &self,
        workspace_ids: &[Uuid],
    ) -> Result<(), RetrievalError> {
        self.delete_points_by_workspaces_with_timeout(
            workspace_ids,
            self.config.delete_worker_timeout_secs,
        )
        .await
    }

    async fn delete_points_by_workspaces_with_timeout(
        &self,
        workspace_ids: &[Uuid],
        timeout_secs: u64,
    ) -> Result<(), RetrievalError> {
        if workspace_ids.is_empty() {
            return Ok(());
        }

        // Một id: match value đơn giản; nhiều id: match.any trong một request.
        let condition = if workspace_ids.len() == 1 {
            QdrantCondition {
                key: "workspace_id".to_string(),
                r#match: QdrantMatch {
                    value: Some(workspace_ids[0].to_string()),
                    any: None,
                },
            }
        } else {
            QdrantCondition {
                key: "workspace_id".to_string(),
                r#match: QdrantMatch {
                    value: None,
                    any: Some(workspace_ids.iter().map(|id| id.to_string()).collect()),
                },
            }
        };

        self.delete_points_by_filter(vec![condition], timeout_secs)
            .await
    }

    /// Thin wrapper tenant cleanup: load workspace ids từ SQL rồi filter-delete.
    ///
    /// Payload **không** có `tenant_id` — bắt buộc resolve workspace list khi rows **còn**
    /// trong SQL. Sau cascade, dùng `delete_points_by_workspaces` với ids đã capture trước.
    ///
    /// Danh sách rỗng → **Err** (không silent no-op): empty thường nghĩa tenant đã cascade
    /// hoặc id sai; caller phải có workspace ids tường minh.
    pub async fn delete_points_by_tenant(
        &self,
        pool: &PgPool,
        tenant_id: Uuid,
    ) -> Result<(), RetrievalError> {
        let workspace_ids: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT id
            FROM workspaces
            WHERE tenant_id = $1
            "#,
        )
        .bind(tenant_id)
        .fetch_all(pool)
        .await?;

        if workspace_ids.is_empty() {
            return Err(RetrievalError::EmptyTenantWorkspaceList { tenant_id });
        }

        self.delete_points_by_workspaces(&workspace_ids).await
    }

    /// Gọi Qdrant `/points/delete?wait=true` với filter `must` và timeout tường minh.
    ///
    /// Timeout = không confirm success → caller phải enqueue `qdrant_outbox` (không coi Ok).
    /// Client cancel không huỷ job phía Qdrant; retry filter-delete vẫn idempotent.
    async fn delete_points_by_filter(
        &self,
        must: Vec<QdrantCondition>,
        timeout_secs: u64,
    ) -> Result<(), RetrievalError> {
        let payload = QdrantDeleteByFilterRequest {
            filter: QdrantFilter { must },
        };

        let url = format!(
            "{}/collections/{}/points/delete?wait=true",
            self.config.qdrant_url, self.config.collection_name
        );

        let timeout = Duration::from_secs(timeout_secs.max(1));
        let send_future = async {
            let request = self.http.post(&url).json(&payload);
            let response = self.with_auth_header(request).send().await?;
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(RetrievalError::Api {
                    status,
                    body,
                    operation: "points_delete",
                });
            }
            Ok(())
        };

        match tokio::time::timeout(timeout, send_future).await {
            Ok(result) => result,
            Err(_) => Err(RetrievalError::Timeout {
                operation: "points_delete",
                timeout_secs: timeout_secs.max(1),
            }),
        }
    }

    /// Scroll một trang point (payload only) — dùng cho orphan full-scan operator.
    pub async fn scroll_points_page(
        &self,
        limit: usize,
        offset: Option<Value>,
    ) -> Result<ScrollPage, RetrievalError> {
        let limit = limit.clamp(1, 1000);
        let body = QdrantScrollRequest {
            limit,
            with_payload: true,
            with_vector: false,
            offset,
        };

        let url = format!(
            "{}/collections/{}/points/scroll",
            self.config.qdrant_url, self.config.collection_name
        );

        let request = self.http.post(url).json(&body);
        let response = self.with_auth_header(request).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(RetrievalError::Api {
                status,
                body,
                operation: "points_scroll",
            });
        }

        let parsed: QdrantScrollResponse = response.json().await?;
        let mut points = Vec::with_capacity(parsed.result.points.len());
        for point in parsed.result.points {
            let Some(payload) = point.payload else {
                continue;
            };
            let workspace_id = payload
                .get("workspace_id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok());
            let document_id = payload
                .get("document_id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok());
            let Some(workspace_id) = workspace_id else {
                continue;
            };
            let Some(document_id) = document_id else {
                continue;
            };
            points.push(ScrolledPoint {
                workspace_id,
                document_id,
            });
        }

        Ok(ScrollPage {
            points,
            next_offset: parsed.result.next_page_offset,
        })
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
        let status = response.status();
        // 409 = collection đã tồn tại — coi như ensure thành công (idempotent).
        if !status.is_success() && status != StatusCode::CONFLICT {
            let body = response.text().await.unwrap_or_default();
            return Err(RetrievalError::Api {
                status,
                body,
                operation: "create_collection",
            });
        }

        // Index là tối ưu filter-delete/search; lỗi không chặn collection ready
        // (mock test / Qdrant cũ vẫn dùng được, chỉ chậm hơn).
        if let Err(err) = self.ensure_payload_indexes().await {
            warn!(
                error = %err,
                "Failed to ensure Qdrant payload indexes; filter deletes may be slower"
            );
        }

        self.collection_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Tạo keyword payload index trên `workspace_id` và `document_id` (idempotent).
    async fn ensure_payload_indexes(&self) -> Result<(), RetrievalError> {
        self.create_payload_index("workspace_id").await?;
        self.create_payload_index("document_id").await?;
        Ok(())
    }

    async fn create_payload_index(&self, field_name: &str) -> Result<(), RetrievalError> {
        let url = format!(
            "{}/collections/{}/index",
            self.config.qdrant_url, self.config.collection_name
        );

        let request = self.http.put(url).json(&QdrantCreateIndexRequest {
            field_name,
            field_schema: "keyword",
        });

        let response = self.with_auth_header(request).send().await?;
        let status = response.status();
        // 409 / already-exists body: index đã có — idempotent success.
        if status.is_success() || status == StatusCode::CONFLICT {
            return Ok(());
        }

        let body = response.text().await.unwrap_or_default();
        let body_lower = body.to_ascii_lowercase();
        if body_lower.contains("already") || body_lower.contains("exist") {
            return Ok(());
        }

        Err(RetrievalError::Api {
            status,
            body,
            operation: "create_payload_index",
        })
    }
}

/// Một point đã scroll (chỉ metadata ACL dùng cho orphan scan).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScrolledPoint {
    pub workspace_id: Uuid,
    pub document_id: Uuid,
}

#[derive(Debug)]
pub struct ScrollPage {
    pub points: Vec<ScrolledPoint>,
    pub next_offset: Option<Value>,
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
struct QdrantCreateIndexRequest<'a> {
    field_name: &'a str,
    field_schema: &'a str,
}

#[derive(Serialize)]
struct QdrantScrollRequest {
    limit: usize,
    with_payload: bool,
    with_vector: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<Value>,
}

#[derive(Deserialize)]
struct QdrantScrollResponse {
    result: QdrantScrollResult,
}

#[derive(Deserialize)]
struct QdrantScrollResult {
    points: Vec<QdrantScrollPoint>,
    #[serde(default)]
    next_page_offset: Option<Value>,
}

#[derive(Deserialize)]
struct QdrantScrollPoint {
    #[serde(default)]
    payload: Option<Value>,
}

#[derive(Serialize)]
struct QdrantUpsertRequest {
    points: Vec<QdrantPointUpsert>,
}

#[derive(Serialize)]
struct QdrantDeleteByFilterRequest {
    filter: QdrantFilter,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(qdrant_url: &str) -> RetrievalConfig {
        RetrievalConfig {
            qdrant_url: qdrant_url.trim_end_matches('/').to_string(),
            collection_name: "gmrag_document_chunks_test".to_string(),
            vector_size: crate::ingestion::embedding::EXPECTED_EMBEDDING_DIM,
            top_k: 5,
            api_key: None,
            delete_request_timeout_secs: DEFAULT_DELETE_REQUEST_TIMEOUT_SECS,
            delete_worker_timeout_secs: DEFAULT_DELETE_WORKER_TIMEOUT_SECS,
        }
    }

    #[tokio::test]
    async fn delete_points_by_document_fails_when_qdrant_unreachable() {
        // Port 1 thường không có listener — kiểm tra lỗi mạng không panic.
        let client = RetrievalClient::from_config(test_config("http://127.0.0.1:1"));
        let result = client
            .delete_points_by_document(Uuid::new_v4(), Uuid::new_v4())
            .await;

        assert!(matches!(result, Err(RetrievalError::Http(_))));
    }

    #[tokio::test]
    async fn delete_points_by_workspace_fails_when_qdrant_unreachable() {
        let client = RetrievalClient::from_config(test_config("http://127.0.0.1:1"));
        let result = client.delete_points_by_workspace(Uuid::new_v4()).await;

        assert!(matches!(result, Err(RetrievalError::Http(_))));
    }

    #[tokio::test]
    async fn delete_points_by_workspaces_fails_when_qdrant_unreachable() {
        let client = RetrievalClient::from_config(test_config("http://127.0.0.1:1"));
        let result = client
            .delete_points_by_workspaces(&[Uuid::new_v4(), Uuid::new_v4()])
            .await;

        assert!(matches!(result, Err(RetrievalError::Http(_))));
    }

    #[tokio::test]
    async fn delete_points_by_workspaces_empty_slice_is_noop() {
        // Không gọi Qdrant khi không có workspace — vẫn Ok (idempotent).
        let client = RetrievalClient::from_config(test_config("http://127.0.0.1:1"));
        client
            .delete_points_by_workspaces(&[])
            .await
            .expect("empty workspace list must short-circuit without network");
    }

    #[test]
    fn empty_tenant_workspace_list_error_display_is_actionable() {
        let tenant_id = Uuid::nil();
        let err = RetrievalError::EmptyTenantWorkspaceList { tenant_id };
        let msg = err.to_string();
        assert!(msg.contains("no workspaces found"));
        assert!(msg.contains("capture workspace ids"));
    }

    /// TCP accept rồi treo — mô phỏng Qdrant chậm để assert timeout client.
    async fn spawn_hanging_http_peer() -> String {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind hanging peer");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                // Giữ socket mở, không trả HTTP — client phải timeout.
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(300)).await;
                    drop(stream);
                });
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn delete_for_request_times_out_with_short_request_timeout() {
        // Fix High #1: HTTP path dùng timeout ngắn, không chờ worker timeout.
        let base = spawn_hanging_http_peer().await;
        let client = RetrievalClient::from_config(RetrievalConfig {
            qdrant_url: base,
            collection_name: "gmrag_document_chunks_test".to_string(),
            vector_size: crate::ingestion::embedding::EXPECTED_EMBEDDING_DIM,
            top_k: 5,
            api_key: None,
            delete_request_timeout_secs: 1,
            delete_worker_timeout_secs: 30,
        });

        let started = std::time::Instant::now();
        let result = client
            .delete_points_by_document_for_request(Uuid::new_v4(), Uuid::new_v4())
            .await;
        let elapsed = started.elapsed();

        assert!(
            matches!(
                result,
                Err(RetrievalError::Timeout {
                    operation: "points_delete",
                    timeout_secs: 1
                })
            ),
            "expected Timeout(1s), got {result:?}"
        );
        assert!(
            elapsed.as_secs_f64() < 5.0,
            "request-path delete must short-circuit well under worker timeout, elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    async fn delete_for_request_uses_request_timeout_not_worker_timeout() {
        let base = spawn_hanging_http_peer().await;
        let client = RetrievalClient::from_config(RetrievalConfig {
            qdrant_url: base,
            collection_name: "gmrag_document_chunks_test".to_string(),
            vector_size: crate::ingestion::embedding::EXPECTED_EMBEDDING_DIM,
            top_k: 5,
            api_key: None,
            delete_request_timeout_secs: 1,
            delete_worker_timeout_secs: 8,
        });

        let started = std::time::Instant::now();
        let result = client
            .delete_points_by_workspace_for_request(Uuid::new_v4())
            .await;
        let elapsed = started.elapsed();

        assert!(matches!(
            result,
            Err(RetrievalError::Timeout {
                timeout_secs: 1,
                ..
            })
        ));
        // Nếu nhầm dùng worker timeout (8s) thì elapsed sẽ ~8s.
        assert!(
            elapsed.as_secs_f64() < 4.0,
            "for_request must use 1s timeout, not worker 8s; elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    async fn delete_points_by_document_is_idempotent_when_no_matching_points() {
        dotenvy::dotenv().ok();
        let qdrant_url =
            std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6333".to_string());
        let client = RetrievalClient::from_config(test_config(&qdrant_url));

        // Collection có thể chưa tồn tại: tạo qua upsert rỗng không được; dùng ensure qua upsert 1 point rồi xoá.
        let workspace_id = Uuid::new_v4();
        let document_id = Uuid::new_v4();
        let chunk_id = Uuid::new_v4();
        let dim = crate::ingestion::embedding::EXPECTED_EMBEDDING_DIM;
        let embedding = vec![0.01_f32; dim];

        client
            .upsert_chunk_points(&[ChunkPoint {
                chunk_id,
                workspace_id,
                document_id,
                chunk_index: 0,
                embedding: embedding.clone(),
            }])
            .await
            .expect("upsert should succeed against local Qdrant");

        client
            .delete_points_by_document(workspace_id, document_id)
            .await
            .expect("first delete should succeed");

        // Lần 2 không còn point — filter delete vẫn Ok (idempotent).
        client
            .delete_points_by_document(workspace_id, document_id)
            .await
            .expect("second delete should remain idempotent");

        let remaining = client
            .search_chunk_ids(workspace_id, &[document_id], &embedding, 5)
            .await
            .expect("search after delete should succeed");
        assert!(
            remaining.is_empty(),
            "no points should remain for deleted document"
        );
    }

    #[tokio::test]
    async fn delete_points_by_workspace_is_idempotent_and_removes_all_workspace_points() {
        dotenvy::dotenv().ok();
        let qdrant_url =
            std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6333".to_string());
        let client = RetrievalClient::from_config(test_config(&qdrant_url));

        let workspace_id = Uuid::new_v4();
        let other_workspace_id = Uuid::new_v4();
        let document_a = Uuid::new_v4();
        let document_b = Uuid::new_v4();
        let other_document = Uuid::new_v4();
        let dim = crate::ingestion::embedding::EXPECTED_EMBEDDING_DIM;
        let embedding = vec![0.02_f32; dim];

        client
            .upsert_chunk_points(&[
                ChunkPoint {
                    chunk_id: Uuid::new_v4(),
                    workspace_id,
                    document_id: document_a,
                    chunk_index: 0,
                    embedding: embedding.clone(),
                },
                ChunkPoint {
                    chunk_id: Uuid::new_v4(),
                    workspace_id,
                    document_id: document_b,
                    chunk_index: 0,
                    embedding: embedding.clone(),
                },
                // Point workspace khác — không được bị xoá.
                ChunkPoint {
                    chunk_id: Uuid::new_v4(),
                    workspace_id: other_workspace_id,
                    document_id: other_document,
                    chunk_index: 0,
                    embedding: embedding.clone(),
                },
            ])
            .await
            .expect("upsert should succeed against local Qdrant");

        client
            .delete_points_by_workspace(workspace_id)
            .await
            .expect("first workspace delete should succeed");

        client
            .delete_points_by_workspace(workspace_id)
            .await
            .expect("second workspace delete should remain idempotent");

        let remaining_target = client
            .search_chunk_ids(workspace_id, &[document_a, document_b], &embedding, 5)
            .await
            .expect("search deleted workspace should succeed");
        assert!(
            remaining_target.is_empty(),
            "no points should remain for deleted workspace"
        );

        let remaining_other = client
            .search_chunk_ids(other_workspace_id, &[other_document], &embedding, 5)
            .await
            .expect("search other workspace should succeed");
        assert_eq!(
            remaining_other.len(),
            1,
            "points outside deleted workspace must stay"
        );
    }

    #[tokio::test]
    async fn delete_points_by_workspaces_is_idempotent_and_removes_tenant_workspace_points() {
        // Mô phỏng tenant cleanup: xoá points của nhiều workspace thuộc cùng tenant
        // trong một filter `match.any`, không đụng workspace ngoài danh sách.
        dotenvy::dotenv().ok();
        let qdrant_url =
            std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6333".to_string());
        let client = RetrievalClient::from_config(test_config(&qdrant_url));

        let workspace_a = Uuid::new_v4();
        let workspace_b = Uuid::new_v4();
        let outside_workspace = Uuid::new_v4();
        let document_a = Uuid::new_v4();
        let document_b = Uuid::new_v4();
        let outside_document = Uuid::new_v4();
        let dim = crate::ingestion::embedding::EXPECTED_EMBEDDING_DIM;
        let embedding = vec![0.04_f32; dim];

        client
            .upsert_chunk_points(&[
                ChunkPoint {
                    chunk_id: Uuid::new_v4(),
                    workspace_id: workspace_a,
                    document_id: document_a,
                    chunk_index: 0,
                    embedding: embedding.clone(),
                },
                ChunkPoint {
                    chunk_id: Uuid::new_v4(),
                    workspace_id: workspace_b,
                    document_id: document_b,
                    chunk_index: 0,
                    embedding: embedding.clone(),
                },
                ChunkPoint {
                    chunk_id: Uuid::new_v4(),
                    workspace_id: outside_workspace,
                    document_id: outside_document,
                    chunk_index: 0,
                    embedding: embedding.clone(),
                },
            ])
            .await
            .expect("upsert should succeed against local Qdrant");

        let tenant_workspace_ids = [workspace_a, workspace_b];

        client
            .delete_points_by_workspaces(&tenant_workspace_ids)
            .await
            .expect("first tenant-level delete should succeed");

        // Lần 2 — filter delete rỗng vẫn Ok (idempotent).
        client
            .delete_points_by_workspaces(&tenant_workspace_ids)
            .await
            .expect("second tenant-level delete should remain idempotent");

        let remaining_a = client
            .search_chunk_ids(workspace_a, &[document_a], &embedding, 5)
            .await
            .expect("search workspace_a after tenant cleanup");
        let remaining_b = client
            .search_chunk_ids(workspace_b, &[document_b], &embedding, 5)
            .await
            .expect("search workspace_b after tenant cleanup");
        assert!(
            remaining_a.is_empty() && remaining_b.is_empty(),
            "all points for tenant workspaces must be removed"
        );

        let remaining_outside = client
            .search_chunk_ids(outside_workspace, &[outside_document], &embedding, 5)
            .await
            .expect("search outside workspace after tenant cleanup");
        assert_eq!(
            remaining_outside.len(),
            1,
            "points outside tenant workspace list must stay"
        );
    }
}
