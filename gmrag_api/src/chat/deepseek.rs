use futures::StreamExt;
use reqwest::{Client, StatusCode};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

const DEFAULT_DEEPSEEK_CHAT_URL: &str = "https://api.deepseek.com/chat/completions";
const DEFAULT_DEEPSEEK_CHAT_MODEL: &str = "deepseek-v4-flash";

pub fn deepseek_chat_url() -> String {
    std::env::var("DEEPSEEK_API_URL").unwrap_or_else(|_| DEFAULT_DEEPSEEK_CHAT_URL.to_string())
}

pub fn deepseek_chat_model() -> String {
    std::env::var("DEEPSEEK_MODEL").unwrap_or_else(|_| DEFAULT_DEEPSEEK_CHAT_MODEL.to_string())
}

pub async fn stream_chat_completion(
    client: &Client,
    system_prompt: &str,
    messages: &[ChatMessage],
) -> Result<reqwest::Response, DeepseekStreamError> {
    let api_key =
        std::env::var("DEEPSEEK_API_KEY").map_err(|_| DeepseekStreamError::MissingApiKey)?;

    let mut api_messages = Vec::with_capacity(messages.len() + 1);
    api_messages.push(DeepseekApiMessage {
        role: "system",
        content: system_prompt,
    });

    for message in messages {
        api_messages.push(DeepseekApiMessage {
            role: message.role.as_str(),
            content: message.content.as_str(),
        });
    }

    let body = DeepseekStreamRequest {
        model: deepseek_chat_model(),
        messages: api_messages,
        stream: true,
        temperature: 0.2,
    };

    let response = client
        .post(deepseek_chat_url())
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .map_err(DeepseekStreamError::Http)?;

    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Err(DeepseekStreamError::UpstreamStatus { status, body })
}

pub struct DeepseekTokenParser {
    line_buffer: String,
    pending_tokens: Vec<String>,
    done: bool,
}

impl DeepseekTokenParser {
    pub fn new() -> Self {
        Self {
            line_buffer: String::new(),
            pending_tokens: Vec::new(),
            done: false,
        }
    }

    pub fn push_chunk(&mut self, chunk: &str) {
        self.line_buffer.push_str(chunk);

        while let Some(newline_idx) = self.line_buffer.find('\n') {
            let line = self.line_buffer[..newline_idx].trim().to_string();
            self.line_buffer.drain(..=newline_idx);
            self.parse_line(&line);
        }
    }

    pub fn pop_token(&mut self) -> Option<String> {
        if self.pending_tokens.is_empty() {
            None
        } else {
            Some(self.pending_tokens.remove(0))
        }
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    fn parse_line(&mut self, line: &str) {
        if line.is_empty() || !line.starts_with("data:") {
            return;
        }

        let data = line.trim_start_matches("data:").trim();
        if data == "[DONE]" {
            self.done = true;
            return;
        }

        let Ok(parsed) = serde_json::from_str::<DeepseekStreamChunk>(data) else {
            return;
        };

        for choice in parsed.choices {
            if let Some(content) = choice.delta.content {
                if !content.is_empty() {
                    self.pending_tokens.push(content);
                }
            }
        }
    }
}

pub async fn next_stream_token(
    byte_stream: &mut (impl futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin),
    parser: &mut DeepseekTokenParser,
) -> Option<Result<String, DeepseekStreamError>> {
    loop {
        if let Some(token) = parser.pop_token() {
            return Some(Ok(token));
        }

        if parser.is_done() {
            return None;
        }

        match byte_stream.next().await {
            Some(Ok(bytes)) => {
                parser.push_chunk(&String::from_utf8_lossy(&bytes));
            }
            Some(Err(err)) => return Some(Err(DeepseekStreamError::Http(err))),
            None => return None,
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct DeepseekStreamRequest<'a> {
    model: String,
    messages: Vec<DeepseekApiMessage<'a>>,
    stream: bool,
    temperature: f32,
}

#[derive(Debug, serde::Serialize)]
struct DeepseekApiMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Debug, Deserialize)]
struct DeepseekStreamChunk {
    choices: Vec<DeepseekStreamChoice>,
}

#[derive(Debug, Deserialize)]
struct DeepseekStreamChoice {
    delta: DeepseekStreamDelta,
}

#[derive(Debug, Deserialize)]
struct DeepseekStreamDelta {
    content: Option<String>,
}

#[derive(Debug)]
pub enum DeepseekStreamError {
    MissingApiKey,
    Http(reqwest::Error),
    UpstreamStatus { status: StatusCode, body: String },
}

impl std::fmt::Display for DeepseekStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeepseekStreamError::MissingApiKey => write!(f, "DEEPSEEK_API_KEY is not set"),
            DeepseekStreamError::Http(err) => write!(f, "deepseek chat request failed: {err}"),
            DeepseekStreamError::UpstreamStatus { status, body } => {
                if body.trim().is_empty() {
                    write!(f, "deepseek returned status {status}")
                } else {
                    write!(f, "deepseek returned status {status}: {body}")
                }
            }
        }
    }
}

impl std::error::Error for DeepseekStreamError {}
