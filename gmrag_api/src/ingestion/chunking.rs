use text_splitter::{ChunkConfig, TextSplitter};
use tiktoken_rs::cl100k_base;

const CHUNK_SIZE_TOKENS: usize = 1200;
const CHUNK_OVERLAP_TOKENS: usize = 100;

pub fn chunk_page_texts(page_texts: &[String]) -> Result<Vec<String>, ChunkError> {
    let full_text = page_texts
        .iter()
        .map(|page| page.trim())
        .filter(|page| !page.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");

    if full_text.is_empty() {
        return Ok(Vec::new());
    }

    let tokenizer = cl100k_base().map_err(|err| ChunkError::Tokenizer(err.to_string()))?;
    let config = ChunkConfig::new(CHUNK_SIZE_TOKENS)
        .with_sizer(tokenizer)
        .with_overlap(CHUNK_OVERLAP_TOKENS)
        .map_err(|err| ChunkError::Config(err.to_string()))?;

    let splitter = TextSplitter::new(config);
    Ok(splitter
        .chunks(&full_text)
        .map(str::trim)
        .filter(|chunk| !chunk.is_empty())
        .map(str::to_owned)
        .collect())
}

#[derive(Debug)]
pub enum ChunkError {
    Tokenizer(String),
    Config(String),
}

impl std::fmt::Display for ChunkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkError::Tokenizer(msg) => write!(f, "tokenizer init failed: {msg}"),
            ChunkError::Config(msg) => write!(f, "invalid chunk config: {msg}"),
        }
    }
}

impl std::error::Error for ChunkError {}
