use std::sync::OnceLock;
use std::time::Duration;

use axum::extract::MatchedPath;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use sqlx::PgPool;

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Khởi tạo global metrics recorder đúng một lần cho toàn process.
pub fn init_metrics_recorder() -> &'static PrometheusHandle {
    PROMETHEUS_HANDLE.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("Failed to install Prometheus metrics recorder")
    })
}

pub fn metrics_handle() -> &'static PrometheusHandle {
    init_metrics_recorder()
}

pub async fn http_metrics_middleware(request: Request<axum::body::Body>, next: Next) -> Response {
    let method = request.method().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("unknown")
        .to_string();

    let response = next.run(request).await;
    let status = response.status().as_u16().to_string();

    metrics::counter!(
        "gmrag_http_requests_total",
        "method" => method,
        "route" => route,
        "status" => status,
    )
    .increment(1);

    response
}

pub fn record_model_latency(provider: &'static str, operation: &'static str, elapsed: Duration) {
    metrics::histogram!(
        "gmrag_model_latency_seconds",
        "provider" => provider,
        "operation" => operation,
    )
    .record(elapsed.as_secs_f64());
}

pub async fn refresh_operational_metrics(pool: &PgPool) -> Result<(), sqlx::Error> {
    let avg_ingestion_latency_secs: Option<f64> = sqlx::query_scalar(
        r#"
        SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))::float8
        FROM ingestion_jobs
        WHERE status = 'SUCCEEDED'
          AND started_at IS NOT NULL
          AND completed_at IS NOT NULL
        "#,
    )
    .fetch_one(pool)
    .await?;

    metrics::gauge!("gmrag_ingestion_latency_seconds", "aggregation" => "avg")
        .set(avg_ingestion_latency_secs.unwrap_or(0.0).max(0.0));

    let max_ingestion_latency_secs: Option<f64> = sqlx::query_scalar(
        r#"
        SELECT MAX(EXTRACT(EPOCH FROM (completed_at - started_at)))::float8
        FROM ingestion_jobs
        WHERE status = 'SUCCEEDED'
          AND started_at IS NOT NULL
          AND completed_at IS NOT NULL
        "#,
    )
    .fetch_one(pool)
    .await?;

    metrics::gauge!("gmrag_ingestion_latency_seconds", "aggregation" => "max")
        .set(max_ingestion_latency_secs.unwrap_or(0.0).max(0.0));

    let ingestion_failure_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::bigint FROM ingestion_jobs WHERE status = 'DEAD'")
            .fetch_one(pool)
            .await?;

    metrics::gauge!("gmrag_ingestion_failure_count").set(ingestion_failure_count as f64);

    refresh_outbox_depth_metrics(pool).await?;

    Ok(())
}

async fn refresh_outbox_depth_metrics(pool: &PgPool) -> Result<(), sqlx::Error> {
    refresh_single_outbox_depth_metric(
        pool,
        "authz",
        "SELECT COUNT(*)::bigint FROM authz_outbox WHERE status = $1",
        &["PENDING", "FAILED", "PROCESSED"],
    )
    .await?;

    refresh_single_outbox_depth_metric(
        pool,
        "qdrant",
        "SELECT COUNT(*)::bigint FROM qdrant_outbox WHERE status = $1",
        &["PENDING", "FAILED", "PROCESSED", "DEAD"],
    )
    .await?;

    refresh_single_outbox_depth_metric(
        pool,
        "storage",
        "SELECT COUNT(*)::bigint FROM storage_outbox WHERE status = $1",
        &["PENDING", "FAILED", "PROCESSED", "DEAD"],
    )
    .await?;

    Ok(())
}

async fn refresh_single_outbox_depth_metric(
    pool: &PgPool,
    outbox: &'static str,
    query: &'static str,
    statuses: &[&'static str],
) -> Result<(), sqlx::Error> {
    for status in statuses {
        let count: i64 = sqlx::query_scalar(query)
            .bind(*status)
            .fetch_one(pool)
            .await?;

        metrics::gauge!(
            "gmrag_outbox_depth",
            "outbox" => outbox,
            "status" => status_label(status),
        )
        .set(count as f64);
    }

    Ok(())
}

fn status_label(status: &str) -> &'static str {
    match status {
        "PENDING" => "pending",
        "FAILED" => "failed",
        "PROCESSED" => "processed",
        "DEAD" => "dead",
        _ => "unknown",
    }
}
