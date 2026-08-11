use std::sync::{Arc, LazyLock};

use axum::{
    Router,
    extract::Path,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use utoipa_swagger_ui::Config;

const OPENAPI_YAML: &str = include_str!("../../docs/api/openapi.yaml");
const DOCS_BASE: &str = r#"<base href="/docs/">"#;

static SWAGGER_CONFIG: LazyLock<Arc<Config<'static>>> =
    LazyLock::new(|| Arc::new(Config::from("/openapi.yaml")));

pub fn enabled_from_env() -> bool {
    enabled_value(std::env::var("DOCS_ENABLED").ok().as_deref())
}

fn enabled_value(value: Option<&str>) -> bool {
    !matches!(
        value.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("false" | "0" | "off" | "no")
    )
}

pub fn router<S>(enabled: bool) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    if !enabled {
        return Router::new();
    }

    Router::new()
        .route("/openapi.yaml", get(openapi_yaml))
        .route("/docs", get(swagger_ui_index))
        .route("/docs/", get(swagger_ui_index))
        .route("/docs/{*asset}", get(swagger_ui_asset))
}

async fn openapi_yaml() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/yaml; charset=utf-8")],
        OPENAPI_YAML,
    )
}

async fn swagger_ui_index() -> Response {
    swagger_ui_response("")
}

async fn swagger_ui_asset(Path(asset): Path<String>) -> Response {
    swagger_ui_response(&asset)
}

fn swagger_ui_response(path: &str) -> Response {
    match utoipa_swagger_ui::serve(path, SWAGGER_CONFIG.clone()) {
        Ok(Some(file)) => {
            let bytes = if path.is_empty() || path == "/" || path == "index.html" {
                inject_docs_base(file.bytes.as_ref())
            } else {
                file.bytes.into_owned()
            };

            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, file.content_type)],
                bytes,
            )
                .into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            tracing::error!(error = %error, path, "Failed to serve embedded Swagger UI asset");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn inject_docs_base(index: &[u8]) -> Vec<u8> {
    let index = String::from_utf8_lossy(index);
    index
        .replacen("<head>", &format!("<head>\n    {DOCS_BASE}"), 1)
        .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    #[test]
    fn docs_are_enabled_by_default_and_accept_explicit_false_values() {
        assert!(enabled_value(None));
        assert!(enabled_value(Some("true")));
        assert!(enabled_value(Some("unexpected")));
        assert!(!enabled_value(Some("false")));
        assert!(!enabled_value(Some(" OFF ")));
        assert!(!enabled_value(Some("0")));
    }

    #[tokio::test]
    async fn enabled_router_serves_embedded_spec_ui_and_assets() {
        let app = router::<()>(true);

        let spec = app
            .clone()
            .oneshot(Request::get("/openapi.yaml").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(spec.status(), StatusCode::OK);
        assert_eq!(
            spec.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/yaml; charset=utf-8"
        );
        let spec_body = axum::body::to_bytes(spec.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(spec_body.as_ref(), OPENAPI_YAML.as_bytes());

        let docs = app
            .clone()
            .oneshot(Request::get("/docs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(docs.status(), StatusCode::OK);
        let docs_body = axum::body::to_bytes(docs.into_body(), usize::MAX)
            .await
            .unwrap();
        let docs_html = String::from_utf8(docs_body.to_vec()).unwrap();
        assert!(docs_html.contains("<div id=\"swagger-ui\"></div>"));
        assert!(docs_html.contains(DOCS_BASE));

        let asset = app
            .oneshot(
                Request::get("/docs/swagger-ui.css")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(asset.status(), StatusCode::OK);
        assert_eq!(
            asset.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/css"
        );
    }

    #[tokio::test]
    async fn disabled_router_exposes_no_documentation_routes() {
        let response = router::<()>(false)
            .oneshot(Request::get("/docs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
