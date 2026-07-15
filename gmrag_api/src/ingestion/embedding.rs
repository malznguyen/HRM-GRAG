use reqwest::Client;
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};

const DEFAULT_OLLAMA_URL: &str = "http://localhost:11434/api/embed";

// ---------------------------------------------------------------------------
// ADR-21 — mô hình embedding được ghim (pinned)
// ---------------------------------------------------------------------------
// Cả ingestion (embed chunk) và chat (embed query) PHẢI dùng cùng một model
// qua `ollama_embed_model()` / `embed_texts()`. Không được hard-code model
// khác ở call site — lệch model sẽ làm similarity search im lặng hỏng.
//
// Model khuyến nghị / mặc định: AITeamVN/Vietnamese_Embedding (1024 dims),
// phục vụ qua Ollama. `QDRANT_VECTOR_SIZE` là nguồn cấu hình chiều vector dùng
// chung cho guard embedding và Qdrant; mặc định 1024.
//
// Override: `OLLAMA_EMBED_MODEL` (tương thích ngược, ví dụ nomic-embed-text).
// Dùng model khác ADR-21 sẽ làm giảm chất lượng retrieval tiếng Việt và
// có thể lệch chiều vector so với dữ liệu đã index — bắt buộc re-embed backfill
// nếu đổi model sau khi đã ingest.
// ---------------------------------------------------------------------------

/// Model embedding được ghim theo ADR-21 (ingestion + chat query).
pub const PINNED_EMBED_MODEL: &str = "AITeamVN/Vietnamese_Embedding";

/// Default khi không set `OLLAMA_EMBED_MODEL` — luôn trùng `PINNED_EMBED_MODEL`.
const DEFAULT_EMBED_MODEL: &str = PINNED_EMBED_MODEL;

/// Chiều vector mặc định của model ADR-21 khi không set `QDRANT_VECTOR_SIZE`.
pub const DEFAULT_EMBEDDING_DIM: usize = 1024;

/// Log cảnh báo dim mismatch một lần mỗi process (tránh spam log).
static DIM_MISMATCH_LOGGED: AtomicBool = AtomicBool::new(false);

pub fn ollama_embed_url() -> String {
    if let Ok(url) = std::env::var("OLLAMA_EMBED_URL") {
        return url;
    }

    std::env::var("OLLAMA_EMBEDDINGS_URL")
        .map(|url| legacy_embeddings_url_to_embed_url(&url))
        .unwrap_or_else(|_| DEFAULT_OLLAMA_URL.to_string())
}

/// Model Ollama dùng cho **cả** ingestion và chat query embedding.
///
/// Ưu tiên `OLLAMA_EMBED_MODEL`; nếu không set thì dùng `PINNED_EMBED_MODEL`
/// (`AITeamVN/Vietnamese_Embedding`) theo ADR-21.
pub fn ollama_embed_model() -> String {
    std::env::var("OLLAMA_EMBED_MODEL").unwrap_or_else(|_| DEFAULT_EMBED_MODEL.to_string())
}

/// Chiều vector runtime dùng chung cho embedding guard và Qdrant.
pub fn configured_embedding_dimension() -> usize {
    std::env::var("QDRANT_VECTOR_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_EMBEDDING_DIM)
}

/// Gọi lúc startup: log model đang dùng và cảnh báo nếu lệch ADR-21.
///
/// Không fail process — vẫn cho phép override (môi trường legacy), nhưng
/// làm rõ rủi ro retrieval quality / re-embed.
pub fn log_embedding_config_on_startup() {
    let model = ollama_embed_model();
    let url = ollama_embed_url();
    let expected_dims = configured_embedding_dimension();

    tracing::info!(
        embed_model = %model,
        embed_url = %url,
        expected_dims,
        pinned_model = PINNED_EMBED_MODEL,
        "Embedding config (ADR-21): shared model for ingestion + chat query"
    );

    if model != PINNED_EMBED_MODEL {
        tracing::warn!(
            configured_model = %model,
            recommended_model = PINNED_EMBED_MODEL,
            expected_dims,
            "OLLAMA_EMBED_MODEL is not the ADR-21 pinned model \
             (AITeamVN/Vietnamese_Embedding). Using a different model will \
             degrade Vietnamese retrieval quality and may be incompatible with \
             already-indexed vectors — re-embed all chunks after any model change."
        );
    }
}

pub async fn embed_text(client: &Client, text: &str) -> Result<Vec<f32>, EmbedError> {
    let mut embeddings = embed_texts(client, &[text.to_string()]).await?;
    embeddings.pop().ok_or(EmbedError::Empty)
}

pub async fn embed_texts(client: &Client, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
    if texts.is_empty() {
        return Ok(Vec::new());
    }

    let model = ollama_embed_model();
    let body = OllamaEmbedBatchRequest {
        model: model.clone(),
        input: texts,
    };

    let response = client
        .post(ollama_embed_url())
        .json(&body)
        .send()
        .await
        .map_err(EmbedError::Http)?
        .error_for_status()
        .map_err(EmbedError::Http)?;

    let payload: OllamaEmbedBatchResponse = response.json().await.map_err(EmbedError::Http)?;

    if payload.embeddings.len() != texts.len() {
        return Err(EmbedError::CountMismatch {
            expected: texts.len(),
            actual: payload.embeddings.len(),
        });
    }

    if payload.embeddings.iter().any(Vec::is_empty) {
        return Err(EmbedError::Empty);
    }

    // Guard dùng đúng cấu hình QDRANT_VECTOR_SIZE để Postgres/Qdrant không lệch nhau.
    // Sai dim → lỗi cứng (không ghi vector lệch vào DB/Qdrant).
    validate_embedding_dimensions(&payload.embeddings, &model)?;

    Ok(payload.embeddings)
}

/// Kiểm tra mọi embedding có đúng `QDRANT_VECTOR_SIZE` (mặc định 1024).
///
/// Trong môi trường non-test: log error rõ ràng (một lần) rồi trả `DimensionMismatch`.
/// Trong test: chỉ trả error (không panic) để mock/assert ổn định.
pub fn validate_embedding_dimensions(
    embeddings: &[Vec<f32>],
    model: &str,
) -> Result<(), EmbedError> {
    let expected = configured_embedding_dimension();
    for embedding in embeddings {
        let actual = embedding.len();
        if actual != expected {
            if !cfg!(test) && !DIM_MISMATCH_LOGGED.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    model = %model,
                    expected_dims = expected,
                    actual_dims = actual,
                    pinned_model = PINNED_EMBED_MODEL,
                    "Embedding dimension mismatch — refusing to use this vector. \
                     ADR-21 requires AITeamVN/Vietnamese_Embedding at its native dimension. \
                     Check OLLAMA_EMBED_MODEL, QDRANT_VECTOR_SIZE, and the imported Ollama model. \
                     Do not mix models without a full re-embedding backfill."
                );
            }

            // Production-safety: hard fail (không panic process — request/ingestion fail rõ ràng).
            // Panic sẽ kéo sập worker; Result + error log đủ an toàn và dễ quan sát.
            return Err(EmbedError::DimensionMismatch {
                expected,
                actual,
                model: model.to_string(),
            });
        }
    }
    Ok(())
}

pub fn format_pgvector(values: &[f32]) -> String {
    let body = values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("[{body}]")
}

fn legacy_embeddings_url_to_embed_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    trimmed
        .strip_suffix("/api/embeddings")
        .map(|base| format!("{base}/api/embed"))
        .unwrap_or_else(|| url.to_string())
}

#[derive(Debug, serde::Serialize)]
struct OllamaEmbedBatchRequest<'a> {
    model: String,
    input: &'a [String],
}

#[derive(Debug, Deserialize)]
struct OllamaEmbedBatchResponse {
    embeddings: Vec<Vec<f32>>,
}

#[derive(Debug)]
pub enum EmbedError {
    Http(reqwest::Error),
    Empty,
    CountMismatch {
        expected: usize,
        actual: usize,
    },
    /// Vector length khác `QDRANT_VECTOR_SIZE` — schema/Qdrant không tương thích.
    DimensionMismatch {
        expected: usize,
        actual: usize,
        model: String,
    },
}

impl std::fmt::Display for EmbedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmbedError::Http(err) => write!(f, "ollama embedding request failed: {err}"),
            EmbedError::Empty => write!(f, "ollama returned an empty embedding"),
            EmbedError::CountMismatch { expected, actual } => write!(
                f,
                "ollama returned {actual} embeddings for {expected} requested texts"
            ),
            EmbedError::DimensionMismatch {
                expected,
                actual,
                model,
            } => write!(
                f,
                "embedding dimension mismatch for model `{model}`: got {actual}, expected {expected} \
                 (ADR-21 pinned model is `{PINNED_EMBED_MODEL}`; \
                 verify QDRANT_VECTOR_SIZE and the imported Ollama model)"
            ),
        }
    }
}

impl std::error::Error for EmbedError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    /// Serialize env mutations so parallel tests do not race on process env.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// ADR-21 pin: model name + dimension must not silently regress.
    #[test]
    fn adr21_pinned_model_name_and_dimension() {
        assert_eq!(
            PINNED_EMBED_MODEL, "AITeamVN/Vietnamese_Embedding",
            "ADR-21 pinned embedding model name must not change without a full re-embed plan"
        );
        assert_eq!(
            DEFAULT_EMBED_MODEL, PINNED_EMBED_MODEL,
            "default model must equal the ADR-21 pin"
        );
        assert_eq!(
            DEFAULT_EMBEDDING_DIM, 1024,
            "schema/Qdrant default to native 1024-d vectors for the pinned model"
        );
        // Guard against the pre-fix default that caused the silent mismatch.
        assert_ne!(
            DEFAULT_EMBED_MODEL, "nomic-embed-text",
            "default must not regress to nomic-embed-text"
        );
    }

    /// Khôi phục biến môi trường sau test; caller phải giữ `env_lock()`.
    fn restore_env(name: &str, previous: Option<String>) {
        // SAFETY: env_lock tuần tự hóa các test này và không còn reader song song.
        match previous {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
    }

    #[test]
    fn ollama_embed_model_defaults_to_pinned_when_env_unset() {
        let _guard = env_lock();
        let previous = std::env::var("OLLAMA_EMBED_MODEL").ok();
        // SAFETY: đang giữ env_lock và sẽ khôi phục trước khi nhả khóa.
        unsafe { std::env::remove_var("OLLAMA_EMBED_MODEL") };

        assert_eq!(ollama_embed_model(), PINNED_EMBED_MODEL);
        assert_eq!(ollama_embed_model(), "AITeamVN/Vietnamese_Embedding");

        restore_env("OLLAMA_EMBED_MODEL", previous);
    }

    #[test]
    fn ollama_embed_model_respects_env_override() {
        let _guard = env_lock();
        let previous = std::env::var("OLLAMA_EMBED_MODEL").ok();
        // SAFETY: đang giữ env_lock và sẽ khôi phục trước khi nhả khóa.
        unsafe { std::env::set_var("OLLAMA_EMBED_MODEL", "nomic-embed-text") };

        assert_eq!(ollama_embed_model(), "nomic-embed-text");

        restore_env("OLLAMA_EMBED_MODEL", previous);
    }

    #[test]
    fn validate_dimensions_accepts_configured_size() {
        let _guard = env_lock();
        let previous = std::env::var("QDRANT_VECTOR_SIZE").ok();
        unsafe { std::env::remove_var("QDRANT_VECTOR_SIZE") };
        let vecs = vec![vec![0.0_f32; DEFAULT_EMBEDDING_DIM], vec![1.0_f32; 1024]];
        assert!(validate_embedding_dimensions(&vecs, PINNED_EMBED_MODEL).is_ok());
        restore_env("QDRANT_VECTOR_SIZE", previous);
    }

    #[test]
    fn validate_dimensions_rejects_wrong_size() {
        let _guard = env_lock();
        let previous = std::env::var("QDRANT_VECTOR_SIZE").ok();
        unsafe { std::env::remove_var("QDRANT_VECTOR_SIZE") };
        let vecs = vec![vec![0.0_f32; 384]];
        let err = validate_embedding_dimensions(&vecs, "some-model").unwrap_err();
        match err {
            EmbedError::DimensionMismatch {
                expected,
                actual,
                model,
            } => {
                assert_eq!(expected, DEFAULT_EMBEDDING_DIM);
                assert_eq!(expected, 1024);
                assert_eq!(actual, 384);
                assert_eq!(model, "some-model");
            }
            other => panic!("unexpected error: {other}"),
        }
        restore_env("QDRANT_VECTOR_SIZE", previous);
    }

    #[test]
    fn dimension_mismatch_display_mentions_pinned_model_and_dim() {
        let err = EmbedError::DimensionMismatch {
            expected: DEFAULT_EMBEDDING_DIM,
            actual: 512,
            model: "wrong-model".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("512"), "actual dim in message: {msg}");
        assert!(msg.contains("1024"), "expected dim in message: {msg}");
        assert!(
            msg.contains(PINNED_EMBED_MODEL),
            "pinned model guidance in message: {msg}"
        );
        assert!(
            msg.contains("wrong-model"),
            "configured model in message: {msg}"
        );
    }

    #[test]
    fn legacy_embeddings_url_is_normalized() {
        assert_eq!(
            legacy_embeddings_url_to_embed_url("http://localhost:11434/api/embeddings"),
            "http://localhost:11434/api/embed"
        );
        assert_eq!(
            legacy_embeddings_url_to_embed_url("http://localhost:11434/api/embed"),
            "http://localhost:11434/api/embed"
        );
    }

    #[test]
    fn format_pgvector_round_trips_shape() {
        let values = vec![1.0, 2.5, -0.5];
        assert_eq!(format_pgvector(&values), "[1,2.5,-0.5]");
    }
}
