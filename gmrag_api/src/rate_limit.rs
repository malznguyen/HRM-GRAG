use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{MatchedPath, Request};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use serde::Serialize;
use tokio::sync::Mutex;

static GLOBAL_RATE_LIMITERS: OnceLock<RateLimiters> = OnceLock::new();

#[derive(Clone, Copy)]
enum RateLimitCategory {
    Chat,
    Upload,
    AuthSensitive,
}

#[derive(Clone, Copy)]
enum RateLimitScope {
    User,
    Tenant,
}

#[derive(Clone, Copy)]
struct RateLimitRule {
    category: RateLimitCategory,
    scope: RateLimitScope,
}

#[derive(Serialize)]
struct RateLimitErrorEnvelope {
    error: RateLimitErrorPayload,
}

#[derive(Serialize)]
struct RateLimitErrorPayload {
    code: &'static str,
    message: &'static str,
}

pub async fn enforce_rate_limits(request: Request, next: Next) -> Response {
    let matched_path = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str);

    let Some(rule) = classify_rule(request.method(), matched_path) else {
        return next.run(request).await;
    };

    let Some(scope_key) = resolve_scope_key(&request, rule.scope) else {
        return next.run(request).await;
    };

    let allowed = global_rate_limiters()
        .allow(rule.category, &scope_key)
        .await;
    if allowed {
        return next.run(request).await;
    }

    let payload = RateLimitErrorEnvelope {
        error: RateLimitErrorPayload {
            code: "RATE_LIMITED",
            message: "Too many requests. Please retry later.",
        },
    };
    (StatusCode::TOO_MANY_REQUESTS, Json(payload)).into_response()
}

fn classify_rule(method: &Method, matched_path: Option<&str>) -> Option<RateLimitRule> {
    let path = matched_path?;
    match (method, path) {
        (&Method::POST, "/workspaces/{workspace_id}/chat") => Some(RateLimitRule {
            category: RateLimitCategory::Chat,
            scope: RateLimitScope::User,
        }),
        (&Method::POST, "/workspaces/{workspace_id}/documents/upload") => Some(RateLimitRule {
            category: RateLimitCategory::Upload,
            scope: RateLimitScope::User,
        }),
        (&Method::POST, "/users/sync")
        | (&Method::POST, "/tenants")
        | (&Method::POST, "/workspaces/{workspace_id}/members")
        | (&Method::PATCH, "/workspaces/{workspace_id}/members/{member_id}")
        | (&Method::DELETE, "/workspaces/{workspace_id}/members/{member_id}") => {
            Some(RateLimitRule {
                category: RateLimitCategory::AuthSensitive,
                scope: RateLimitScope::User,
            })
        }
        (&Method::POST, "/tenants/{tenant_id}/owners") => Some(RateLimitRule {
            category: RateLimitCategory::AuthSensitive,
            scope: RateLimitScope::Tenant,
        }),
        _ => None,
    }
}

fn resolve_scope_key(request: &Request, scope: RateLimitScope) -> Option<String> {
    match scope {
        RateLimitScope::User => resolve_user_scope_key(request),
        RateLimitScope::Tenant => resolve_tenant_scope_key(request),
    }
}

fn resolve_user_scope_key(request: &Request) -> Option<String> {
    let token = bearer_token(request)?;
    let key = parse_jwt_sub(token).unwrap_or_else(|| token.to_string());
    Some(format!("user:{key}"))
}

fn resolve_tenant_scope_key(request: &Request) -> Option<String> {
    let path = request.uri().path();
    let mut segments = path.trim_start_matches('/').split('/');
    let resource = segments.next()?;
    if resource != "tenants" {
        return None;
    }
    let tenant_id = segments.next()?;
    if tenant_id.is_empty() {
        return None;
    }
    Some(format!("tenant:{tenant_id}"))
}

fn bearer_token(request: &Request) -> Option<&str> {
    let header = request.headers().get(axum::http::header::AUTHORIZATION)?;
    let value = header.to_str().ok()?;
    let value = value.trim();
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

fn parse_jwt_sub(token: &str) -> Option<String> {
    let mut parts = token.split('.');
    let _header = parts.next()?;
    let payload = parts.next()?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let value: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    value
        .get("sub")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|sub| !sub.is_empty())
        .map(|sub| sub.to_string())
}

fn global_rate_limiters() -> &'static RateLimiters {
    GLOBAL_RATE_LIMITERS.get_or_init(RateLimiters::from_env)
}

struct RateLimiters {
    chat: SlidingWindowRateLimiter,
    upload: SlidingWindowRateLimiter,
    auth_sensitive: SlidingWindowRateLimiter,
}

impl RateLimiters {
    fn from_env() -> Self {
        let window_secs = parse_u64_env("RATE_LIMIT_WINDOW_SECS", 60).max(1);
        let window = Duration::from_secs(window_secs);

        Self {
            chat: SlidingWindowRateLimiter::new(
                parse_u32_env("RATE_LIMIT_CHAT_PER_WINDOW", 30).max(1),
                window,
            ),
            upload: SlidingWindowRateLimiter::new(
                parse_u32_env("RATE_LIMIT_UPLOAD_PER_WINDOW", 10).max(1),
                window,
            ),
            auth_sensitive: SlidingWindowRateLimiter::new(
                parse_u32_env("RATE_LIMIT_AUTH_PER_WINDOW", 20).max(1),
                window,
            ),
        }
    }

    async fn allow(&self, category: RateLimitCategory, scope_key: &str) -> bool {
        match category {
            RateLimitCategory::Chat => self.chat.allow(scope_key).await,
            RateLimitCategory::Upload => self.upload.allow(scope_key).await,
            RateLimitCategory::AuthSensitive => self.auth_sensitive.allow(scope_key).await,
        }
    }
}

struct SlidingWindowRateLimiter {
    max_requests: u32,
    window: Duration,
    state: Mutex<HashMap<String, VecDeque<Instant>>>,
}

impl SlidingWindowRateLimiter {
    fn new(max_requests: u32, window: Duration) -> Self {
        Self {
            max_requests,
            window,
            state: Mutex::new(HashMap::new()),
        }
    }

    async fn allow(&self, scope_key: &str) -> bool {
        let mut guard = self.state.lock().await;
        let queue = guard
            .entry(scope_key.to_string())
            .or_insert_with(VecDeque::new);

        let now = Instant::now();
        while queue
            .front()
            .is_some_and(|seen_at| now.saturating_duration_since(*seen_at) >= self.window)
        {
            queue.pop_front();
        }

        if queue.len() >= self.max_requests as usize {
            return false;
        }

        queue.push_back(now);
        true
    }
}

fn parse_u32_env(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn parse_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sliding_window_limiter_rejects_requests_after_limit() {
        let limiter = SlidingWindowRateLimiter::new(2, Duration::from_secs(60));

        assert!(limiter.allow("user:test").await);
        assert!(limiter.allow("user:test").await);
        assert!(!limiter.allow("user:test").await);
        assert!(limiter.allow("user:another").await);
    }

    #[test]
    fn tenant_scope_key_extracts_id_from_path() {
        let request = Request::builder()
            .uri("/tenants/tenant-123/owners")
            .body(axum::body::Body::empty())
            .expect("request builder");
        assert_eq!(
            resolve_tenant_scope_key(&request).as_deref(),
            Some("tenant:tenant-123")
        );
    }
}
