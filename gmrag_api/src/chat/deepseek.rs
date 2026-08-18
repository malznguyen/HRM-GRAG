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
    let body_len = response.text().await.map(|body| body.len()).unwrap_or(0);
    tracing::warn!(
        %status,
        upstream_body_len = body_len,
        "DeepSeek returned a non-success status"
    );
    Err(DeepseekStreamError::UpstreamStatus { status })
}

/// Provider-side generation metadata captured off the stream for observability.
/// Metadata-only: no prompt, chunk, answer, or reasoning text — only identifiers
/// and a count of reasoning deltas. Logged, never persisted or surfaced (Stage 1a).
#[derive(Debug, Default, Clone)]
pub struct StreamMetadata {
    pub model: Option<String>,
    pub system_fingerprint: Option<String>,
    pub finish_reason: Option<String>,
    /// Number of streamed deltas carrying `reasoning_content`. >0 means the
    /// provider served the request in thinking mode; 0 means non-thinking.
    pub reasoning_delta_count: u32,
}

/// Giữ byte UTF-8 dở dang ở cuối chunk mạng, ghép vào chunk kế trước khi decode.
/// `from_utf8_lossy` từng chunk độc lập sẽ biến nửa ký tự tiếng Việt thành U+FFFD.
#[derive(Debug, Default)]
struct IncrementalUtf8 {
    pending: Vec<u8>,
}

impl IncrementalUtf8 {
    fn push(&mut self, incoming: &[u8]) -> String {
        self.pending.extend_from_slice(incoming);
        self.take_valid(false)
    }

    fn flush(&mut self) -> String {
        self.take_valid(true)
    }

    fn take_valid(&mut self, flush_incomplete: bool) -> String {
        let mut output = String::new();
        loop {
            match std::str::from_utf8(&self.pending) {
                Ok(valid) => {
                    output.push_str(valid);
                    self.pending.clear();
                    break;
                }
                Err(err) => {
                    let valid_up_to = err.valid_up_to();
                    if valid_up_to > 0 {
                        let valid = std::str::from_utf8(&self.pending[..valid_up_to])
                            .expect("valid_up_to marks a UTF-8 prefix");
                        output.push_str(valid);
                        self.pending.drain(..valid_up_to);
                        continue;
                    }
                    match err.error_len() {
                        Some(error_len) => {
                            output.push(char::REPLACEMENT_CHARACTER);
                            self.pending.drain(..error_len);
                        }
                        None if flush_incomplete => {
                            output.push(char::REPLACEMENT_CHARACTER);
                            self.pending.clear();
                            break;
                        }
                        None => break,
                    }
                }
            }
        }
        output
    }
}

pub struct DeepseekTokenParser {
    line_buffer: String,
    pending_tokens: Vec<String>,
    done: bool,
    metadata: StreamMetadata,
    utf8: IncrementalUtf8,
}

impl Default for DeepseekTokenParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DeepseekTokenParser {
    pub fn new() -> Self {
        Self {
            line_buffer: String::new(),
            pending_tokens: Vec::new(),
            done: false,
            metadata: StreamMetadata::default(),
            utf8: IncrementalUtf8::default(),
        }
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) {
        let chunk = self.utf8.push(bytes);
        if !chunk.is_empty() {
            self.push_chunk(&chunk);
        }
    }

    pub fn flush_utf8(&mut self) {
        let chunk = self.utf8.flush();
        if !chunk.is_empty() {
            self.push_chunk(&chunk);
        }
    }

    /// Provider-returned generation metadata accumulated so far.
    pub fn metadata(&self) -> &StreamMetadata {
        &self.metadata
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

        if self.metadata.model.is_none() {
            self.metadata.model = parsed.model;
        }
        if self.metadata.system_fingerprint.is_none() {
            self.metadata.system_fingerprint = parsed.system_fingerprint;
        }

        for choice in parsed.choices {
            if choice.finish_reason.is_some() {
                self.metadata.finish_reason = choice.finish_reason;
            }
            if choice.delta.reasoning_content.is_some() {
                self.metadata.reasoning_delta_count += 1;
            }
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
                parser.push_bytes(&bytes);
            }
            Ok(Some(Err(err))) => return Some(Err(DeepseekStreamError::Http(err))),
            Ok(None) => {
                parser.flush_utf8();
                return None;
            }
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
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    system_fingerprint: Option<String>,
    choices: Vec<DeepseekStreamChoice>,
}

#[derive(Debug, Deserialize)]
struct DeepseekStreamChoice {
    delta: DeepseekStreamDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeepseekStreamDelta {
    content: Option<String>,
    // Present only while the model is in thinking mode; absent when thinking is
    // disabled. Captured as a metadata-only signal (count of reasoning deltas),
    // never surfaced or persisted — see StreamMetadata.
    #[serde(default)]
    reasoning_content: Option<String>,
}

#[derive(Debug)]
pub enum DeepseekStreamError {
    MissingApiKey,
    RequestTimeout { timeout_secs: u64 },
    StreamIdleTimeout { timeout_secs: u64 },
    Http(reqwest::Error),
    UpstreamStatus { status: StatusCode },
}

impl DeepseekStreamError {
    pub fn client_message(&self) -> &'static str {
        "Generation service unavailable"
    }
}

impl std::fmt::Display for DeepseekStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.client_message())
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

    #[test]
    fn incremental_utf8_does_not_replace_vietnamese_split_across_chunks() {
        let text = "Tôi không tìm thấy";
        let bytes = text.as_bytes();
        assert_eq!(
            &bytes[1..3],
            &[0xc3, 0xb4],
            "ô in Tôi is the two-byte sequence c3 b4"
        );

        let mut decoder = IncrementalUtf8::default();
        let first = decoder.push(&bytes[..2]);
        let second = decoder.push(&bytes[2..]);
        let assembled = format!("{first}{second}");

        assert_eq!(first, "T");
        assert_eq!(assembled, text);
        assert!(
            !assembled.contains('\u{FFFD}'),
            "split multi-byte Vietnamese must not become U+FFFD"
        );
    }

    #[test]
    fn parser_reassembles_vietnamese_token_split_across_network_chunks() {
        let payload = "data: {\"choices\":[{\"delta\":{\"content\":\"Tôi\"}}]}\n";
        let bytes = payload.as_bytes();
        let split_at = bytes
            .iter()
            .position(|&b| b == 0xc3)
            .expect("payload contains ô")
            + 1;

        let mut parser = DeepseekTokenParser::new();
        parser.push_bytes(&bytes[..split_at]);
        assert!(
            parser.pop_token().is_none(),
            "incomplete UTF-8 must wait for the next chunk"
        );
        parser.push_bytes(&bytes[split_at..]);

        let token = parser.pop_token().expect("reassembled token");
        assert_eq!(token, "Tôi");
        assert!(!token.contains('\u{FFFD}'));
    }

    #[test]
    fn lossy_per_chunk_decode_is_exactly_the_bug() {
        let text = "Tôi";
        let bytes = text.as_bytes();
        let broken = format!(
            "{}{}",
            String::from_utf8_lossy(&bytes[..2]),
            String::from_utf8_lossy(&bytes[2..])
        );
        assert!(
            broken.contains('\u{FFFD}'),
            "document the old from_utf8_lossy-per-chunk failure mode"
        );
        assert_ne!(broken, text);
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

    #[tokio::test]
    async fn upstream_error_body_is_not_exposed_in_client_message() {
        let _guard = deepseek_test_lock().lock().await;
        let base_url = spawn_error_response_server("provider detail api_key=fake-secret").await;

        unsafe {
            std::env::set_var("DEEPSEEK_API_KEY", "test-api-key");
            std::env::set_var("DEEPSEEK_API_URL", &base_url);
        }

        let result = stream_chat_completion(
            &Client::new(),
            "system",
            &[ChatMessage {
                role: "user".to_string(),
                content: "hello".to_string(),
            }],
        )
        .await;

        let error = result.expect_err("non-success upstream response should fail");
        assert!(matches!(
            &error,
            DeepseekStreamError::UpstreamStatus {
                status: StatusCode::BAD_GATEWAY
            }
        ));
        assert_eq!(error.client_message(), "Generation service unavailable");
        assert!(!error.to_string().contains("fake-secret"));
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

    async fn spawn_error_response_server(body: &'static str) -> String {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind error response server");
        let addr = listener.local_addr().expect("error response server addr");

        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };

            let mut request_buf = [0_u8; 2048];
            let _ = socket.read(&mut request_buf).await;
            let response = format!(
                "HTTP/1.1 502 Bad Gateway\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });

        format!("http://{addr}")
    }
}
