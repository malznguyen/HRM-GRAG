use reqwest::Client;
use serde::Deserialize;

const DEFAULT_OLLAMA_URL: &str = "http://localhost:11434/api/embed";
const DEFAULT_EMBED_MODEL: &str = "nomic-embed-text";

pub fn ollama_embed_url() -> String {
    if let Ok(url) = std::env::var("OLLAMA_EMBED_URL") {
        return url;
    }

    std::env::var("OLLAMA_EMBEDDINGS_URL")
        .map(|url| legacy_embeddings_url_to_embed_url(&url))
        .unwrap_or_else(|_| DEFAULT_OLLAMA_URL.to_string())
}

pub fn ollama_embed_model() -> String {
    std::env::var("OLLAMA_EMBED_MODEL").unwrap_or_else(|_| DEFAULT_EMBED_MODEL.to_string())
}

pub async fn embed_text(client: &Client, text: &str) -> Result<Vec<f32>, EmbedError> {
    let mut embeddings = embed_texts(client, &[text.to_string()]).await?;
    embeddings.pop().ok_or(EmbedError::Empty)
}

pub async fn embed_texts(client: &Client, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
    if texts.is_empty() {
        return Ok(Vec::new());
    }

    let body = OllamaEmbedBatchRequest {
        model: ollama_embed_model(),
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

    Ok(payload.embeddings)
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
    CountMismatch { expected: usize, actual: usize },
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
        }
    }
}

impl std::error::Error for EmbedError {}
