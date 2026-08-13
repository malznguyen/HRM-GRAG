use axum::{
    Json,
    body::{Body, to_bytes},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::Value;

#[derive(Serialize)]
pub struct ApiErrorPayload {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Serialize)]
pub struct ApiErrorResponse {
    pub error: ApiErrorPayload,
}

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn from_status(status: StatusCode) -> Self {
        let (code, message) = match status {
            StatusCode::BAD_REQUEST => ("INVALID_REQUEST", "The request is invalid"),
            StatusCode::UNAUTHORIZED => ("UNAUTHORIZED", "Authentication is required"),
            StatusCode::FORBIDDEN => ("FORBIDDEN", "Access denied"),
            StatusCode::NOT_FOUND => ("RESOURCE_NOT_FOUND", "Resource not found"),
            StatusCode::METHOD_NOT_ALLOWED => ("METHOD_NOT_ALLOWED", "Method not allowed"),
            StatusCode::CONFLICT => ("CONFLICT", "The request conflicts with current state"),
            StatusCode::GONE => ("RESOURCE_GONE", "Resource is no longer available"),
            StatusCode::PAYLOAD_TOO_LARGE => ("PAYLOAD_TOO_LARGE", "Request payload is too large"),
            StatusCode::UNSUPPORTED_MEDIA_TYPE => {
                ("UNSUPPORTED_MEDIA_TYPE", "Unsupported media type")
            }
            StatusCode::UNPROCESSABLE_ENTITY => {
                ("UNPROCESSABLE_ENTITY", "Request cannot be processed")
            }
            StatusCode::TOO_MANY_REQUESTS => {
                ("RATE_LIMITED", "Too many requests. Please retry later.")
            }
            StatusCode::BAD_GATEWAY => (
                "GENERATION_SERVICE_UNAVAILABLE",
                "Generation service unavailable",
            ),
            StatusCode::SERVICE_UNAVAILABLE => ("SERVICE_UNAVAILABLE", "Service unavailable"),
            _ => ("INTERNAL_ERROR", "Internal server error"),
        };
        Self::new(status, code, message)
    }

    pub fn into_response_with_details(self, details: Value) -> Response {
        (
            self.status,
            Json(ApiErrorResponse {
                error: ApiErrorPayload {
                    code: self.code,
                    message: self.message,
                    details: Some(details),
                },
            }),
        )
            .into_response()
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ApiErrorResponse {
                error: ApiErrorPayload {
                    code: self.code,
                    message: self.message,
                    details: None,
                },
            }),
        )
            .into_response()
    }
}

/// Chuẩn hoá mọi lỗi HTTP trước khi response rời API, kể cả rejection của Axum.
pub async fn normalize_error_responses(request: axum::extract::Request, next: Next) -> Response {
    let response = next.run(request).await;
    if response.status().is_success() || response.status().is_redirection() {
        return response;
    }

    let status = response.status();
    let (parts, body) = response.into_parts();
    let body_bytes = match to_bytes(body, 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(_) => return ApiError::from_status(status).into_response(),
    };

    if is_error_envelope(&body_bytes) {
        return Response::from_parts(parts, Body::from(body_bytes));
    }

    if status == StatusCode::SERVICE_UNAVAILABLE {
        if let Ok(value) = serde_json::from_slice::<Value>(&body_bytes) {
            if value.get("failed_dependencies").is_some() {
                return (
                    status,
                    Json(ApiErrorResponse {
                        error: ApiErrorPayload {
                            code: "SERVICE_UNAVAILABLE",
                            message: "Service unavailable".to_string(),
                            details: Some(value),
                        },
                    }),
                )
                    .into_response();
            }
        }
    }
    ApiError::from_status(status).into_response()
}

fn is_error_envelope(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("error").cloned())
        .is_some_and(|error| error.is_object())
}

pub async fn not_found() -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "RESOURCE_NOT_FOUND",
        "Resource not found",
    )
}

pub async fn method_not_allowed() -> ApiError {
    ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "METHOD_NOT_ALLOWED",
        "Method not allowed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, http::Request, routing::get};
    use tower::ServiceExt;

    #[test]
    fn status_mapping_uses_stable_safe_codes() {
        let error = ApiError::from_status(StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(error.code, "INTERNAL_ERROR");
        assert_eq!(error.message, "Internal server error");
    }

    /// Xác nhận hành vi thật của middleware trên một handler trả `StatusCode` thô
    /// (không đi qua `ApiError`) — đây là mẫu hình `get_document_preview` dùng cho
    /// nhánh 409 trước Phase 17.2. Nếu middleware đã tự bọc JSON, việc sửa 17.2 chỉ
    /// còn là đổi `code` generic "CONFLICT" thành mã ổn định theo ngữ nghĩa, không
    /// phải thêm JSON body từ chỗ không có gì.
    #[tokio::test]
    async fn normalize_error_responses_wraps_a_bare_status_code_response() {
        let app = Router::new()
            .route(
                "/bare-conflict",
                get(|| async { StatusCode::CONFLICT.into_response() }),
            )
            .layer(axum::middleware::from_fn(normalize_error_responses));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/bare-conflict")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&body).expect(
            "bare StatusCode response must still be normalized into a JSON error envelope",
        );
        assert_eq!(value["error"]["code"], "CONFLICT");
        assert_eq!(value["error"]["message"], "The request conflicts with current state");
    }
}
