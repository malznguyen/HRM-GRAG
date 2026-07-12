use futures::StreamExt;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

const DEFAULT_DEEPSEEK_CHAT_URL: &str = "https://api.deepseek.com/chat/completions";
const DEFAULT_DEEPSEEK_CHAT_MODEL: &str = "deepseek-v4-flash";
const DEFAULT_DEEPSEEK_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_DEEPSEEK_STREAM_IDLE_TIMEOUT_SECS: u64 = 30;

pub fn deepseek_chat_url() -> String {
    std::env::var("DEEPSEEK_API_URL").unwrap_or_else(|_| DEFAULT_DEEPSEEK_CHAT_URL.to_string())
}

pub fn deepseek_chat_model() -> String {
    std::env::var("DEEPSEEK_MODEL").unwrap_or_else(|_| DEFAULT_DEEPSEEK_CHAT_MODEL.to_string())
}

pub fn deepseek_request_timeout() -> Duration {
    Duration::from_secs(parse_u64_env(
        "DEEPSEEK_REQUEST_TIMEOUT_SECS",
        DEFAULT_DEEPSEEK_REQUEST_TIMEOUT_SECS,
        1,
    ))
}

pub fn deepseek_stream_idle_timeout() -> Duration {
    Duration::from_secs(parse_u64_env(
        "DEEPSEEK_STREAM_IDLE_TIMEOUT_SECS",
        DEFAULT_DEEPSEEK_STREAM_IDLE_TIMEOUT_SECS,
        1,
    ))
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

    let request_timeout = deepseek_request_timeout();
    let response = tokio::time::timeout(
        request_timeout,
        client
            .post(deepseek_chat_url())
            .bearer_auth(api_key)
            .json(&body)
            .send(),
    )
    .await
    .map_err(|_| DeepseekStreamError::RequestTimeout {
        timeout_secs: request_timeout.as_secs(),
    })
    .and_then(|result| result.map_err(DeepseekStreamError::Http))?;

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
    idle_timeout: Duration,
) -> Option<Result<String, DeepseekStreamError>> {
    loop {
        if let Some(token) = parser.pop_token() {
            return Some(Ok(token));
        }

        if parser.is_done() {
            return None;
        }

        match tokio::time::timeout(idle_timeout, byte_stream.next()).await {
            Err(_) => {
                return Some(Err(DeepseekStreamError::StreamIdleTimeout {
                    timeout_secs: idle_timeout.as_secs(),
                }));
            }
            Ok(Some(Ok(bytes))) => {
                parser.push_chunk(&String::from_utf8_lossy(&bytes));
            }
            Ok(Some(Err(err))) => return Some(Err(DeepseekStreamError::Http(err))),
            Ok(None) => return None,
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
    RequestTimeout { timeout_secs: u64 },
    StreamIdleTimeout { timeout_secs: u64 },
    Http(reqwest::Error),
    UpstreamStatus { status: StatusCode, body: String },
}

impl std::fmt::Display for DeepseekStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeepseekStreamError::MissingApiKey => write!(f, "DEEPSEEK_API_KEY is not set"),
            DeepseekStreamError::RequestTimeout { timeout_secs } => {
                write!(f, "deepseek request timed out after {timeout_secs}s")
            }
            DeepseekStreamError::StreamIdleTimeout { timeout_secs } => {
                write!(f, "deepseek stream idle timeout after {timeout_secs}s")
            }
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

fn parse_u64_env(name: &str, default: u64, min: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
        .max(min)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Mutex;

    static DEEPSEEK_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn deepseek_test_lock() -> &'static Mutex<()> {
        DEEPSEEK_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    #[tokio::test]
    async fn request_timeout_fails_when_upstream_hangs_before_headers() {
        let _guard = deepseek_test_lock().lock().await;
        let base_url = spawn_hanging_http_peer().await;

        unsafe {
            std::env::set_var("DEEPSEEK_API_KEY", "test-api-key");
            std::env::set_var("DEEPSEEK_API_URL", &base_url);
            std::env::set_var("DEEPSEEK_REQUEST_TIMEOUT_SECS", "1");
        }

        let client = Client::new();
        let result = stream_chat_completion(
            &client,
            "system",
            &[ChatMessage {
                role: "user".to_string(),
                content: "hello".to_string(),
            }],
        )
        .await;

        assert!(matches!(
            result,
            Err(DeepseekStreamError::RequestTimeout { timeout_secs: 1 })
        ));
    }

    #[tokio::test]
    async fn stream_idle_timeout_fails_when_no_chunks_arrive() {
        let _guard = deepseek_test_lock().lock().await;
        let base_url = spawn_idle_stream_server().await;

        unsafe {
            std::env::set_var("DEEPSEEK_API_KEY", "test-api-key");
            std::env::set_var("DEEPSEEK_API_URL", &base_url);
            std::env::set_var("DEEPSEEK_STREAM_IDLE_TIMEOUT_SECS", "1");
        }

        let client = Client::new();
        let response = stream_chat_completion(
            &client,
            "system",
            &[ChatMessage {
                role: "user".to_string(),
                content: "hello".to_string(),
            }],
        )
        .await
        .expect("stream request should succeed before idle timeout kicks in");

        let mut byte_stream = response.bytes_stream();
        let mut parser = DeepseekTokenParser::new();
        let token = next_stream_token(
            &mut byte_stream,
            &mut parser,
            deepseek_stream_idle_timeout(),
        )
        .await;

        assert!(matches!(
            token,
            Some(Err(DeepseekStreamError::StreamIdleTimeout {
                timeout_secs: 1
            }))
        ));
    }

    #[tokio::test]
    async fn dropping_stream_closes_upstream_connection() {
        let _guard = deepseek_test_lock().lock().await;
        let (base_url, disconnected) = spawn_single_chunk_stream_server().await;

        unsafe {
            std::env::set_var("DEEPSEEK_API_KEY", "test-api-key");
            std::env::set_var("DEEPSEEK_API_URL", &base_url);
            std::env::set_var("DEEPSEEK_STREAM_IDLE_TIMEOUT_SECS", "2");
        }

        let client = Client::new();
        let response = stream_chat_completion(
            &client,
            "system",
            &[ChatMessage {
                role: "user".to_string(),
                content: "hello".to_string(),
            }],
        )
        .await
        .expect("stream request should succeed");

        let mut byte_stream = response.bytes_stream();
        let mut parser = DeepseekTokenParser::new();
        let token = next_stream_token(
            &mut byte_stream,
            &mut parser,
            deepseek_stream_idle_timeout(),
        )
        .await;
        assert!(matches!(token, Some(Ok(_))));

        drop(byte_stream);

        let disconnect_result = tokio::time::timeout(Duration::from_secs(2), disconnected)
            .await
            .expect("server should observe stream disconnect");
        disconnect_result
            .expect("disconnect channel should remain open")
            .expect("disconnect signal should resolve successfully");
    }

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
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(300)).await;
                    drop(stream);
                });
            }
        });
        format!("http://{addr}")
    }

    async fn spawn_idle_stream_server() -> String {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind idle stream server");
        let addr = listener.local_addr().expect("idle stream server addr");

        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };

            let mut request_buf = [0_u8; 2048];
            let _ = socket.read(&mut request_buf).await;

            let response_head =
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n";
            if socket.write_all(response_head).await.is_err() {
                return;
            }

            tokio::time::sleep(Duration::from_secs(300)).await;
        });

        format!("http://{addr}")
    }

    async fn spawn_single_chunk_stream_server() -> (
        String,
        tokio::sync::oneshot::Receiver<Result<(), std::io::Error>>,
    ) {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind single chunk stream server");
        let addr = listener
            .local_addr()
            .expect("single chunk stream server addr");
        let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                let _ = disconnect_tx.send(Err(std::io::Error::other("accept failed")));
                return;
            };

            let mut request_buf = [0_u8; 2048];
            let _ = socket.read(&mut request_buf).await;

            let response_head =
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n";
            if socket.write_all(response_head).await.is_err() {
                let _ = disconnect_tx.send(Err(std::io::Error::other("write head failed")));
                return;
            }

            let data = "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n";
            let chunk = format!("{:X}\r\n{}\r\n", data.len(), data);
            if socket.write_all(chunk.as_bytes()).await.is_err() {
                let _ = disconnect_tx.send(Err(std::io::Error::other("write chunk failed")));
                return;
            }

            let mut read_buf = [0_u8; 256];
            loop {
                match socket.read(&mut read_buf).await {
                    Ok(0) => {
                        let _ = disconnect_tx.send(Ok(()));
                        break;
                    }
                    Ok(_) => continue,
                    Err(err) => {
                        let _ = disconnect_tx.send(Err(err));
                        break;
                    }
                }
            }
        });

        (format!("http://{addr}"), disconnect_rx)
    }
}
