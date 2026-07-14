mod support;

use std::sync::OnceLock;

use gmrag_api::retrieval::outbox_health::{
    QdrantOutboxHealthOptions, QdrantOutboxHealthReport, check_qdrant_outbox_health,
};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::Mutex;

static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn test_lock() -> &'static Mutex<()> {
    TEST_LOCK.get_or_init(|| Mutex::new(()))
}

async fn pool_or_skip() -> Option<PgPool> {
    dotenvy::dotenv().ok();
    let database_url = support::database_url().ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .ok()?;
    match sqlx::migrate!("./migrations").run(&pool).await {
        Ok(_) | Err(sqlx::migrate::MigrateError::VersionMismatch(_)) => Some(pool),
        Err(_) => None,
    }
}

async fn run_fixture(
    rows: &[(&str, i32, &str)],
    options: QdrantOutboxHealthOptions,
) -> QdrantOutboxHealthReport {
    let pool = pool_or_skip()
        .await
        .expect("DATABASE_URL must be available");
    let mut transaction = pool.begin().await.unwrap();

    sqlx::query(
        "CREATE TEMP TABLE qdrant_outbox AS SELECT * FROM public.qdrant_outbox WITH NO DATA",
    )
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query("SET LOCAL search_path TO pg_temp, public")
        .execute(&mut *transaction)
        .await
        .unwrap();

    for (status, retry_count, age_sql) in rows {
        sqlx::query(
            "INSERT INTO qdrant_outbox (
                event_type, payload, status, retry_count, next_attempt_at, created_at, updated_at
            ) VALUES (
                $1, '{}'::jsonb, $2, $3,
                CURRENT_TIMESTAMP - $4::interval,
                CURRENT_TIMESTAMP - $4::interval,
                CURRENT_TIMESTAMP - $4::interval
            )",
        )
        .bind(format!("ops005-{status}-{retry_count}"))
        .bind(*status)
        .bind(*retry_count)
        .bind(*age_sql)
        .execute(&mut *transaction)
        .await
        .unwrap();
    }

    check_qdrant_outbox_health(&mut *transaction, options)
        .await
        .unwrap()
}

fn assert_text_and_json(report: &QdrantOutboxHealthReport) {
    let text = report.format_text();
    assert!(text.contains(&format!("status={}", report.status)));
    assert!(text.contains(&format!("dead_count={}", report.dead_count)));
    assert!(text.contains(&format!("failed_retryable={}", report.failed_retryable)));
    assert!(text.contains(&format!("high_retry_count={}", report.high_retry_count)));
    assert!(text.contains(&format!("max_dead={}", report.max_dead)));
    assert!(text.contains(&format!("max_failed={}", report.max_failed)));
    assert!(text.contains(&format!("max_age_minutes={}", report.max_age_minutes)));

    let json = serde_json::to_value(report).unwrap();
    assert_eq!(json["status"], Value::String(report.status.to_string()));
    assert_eq!(json["dead_count"], report.dead_count);
    assert_eq!(json["failed_retryable"], report.failed_retryable);
    assert_eq!(json["high_retry_count"], report.high_retry_count);
    assert_eq!(
        json["oldest_age_minutes"],
        report
            .oldest_age_minutes
            .map_or(Value::Null, |age| Value::from(age))
    );
    assert_eq!(json["max_dead"], report.max_dead);
    assert_eq!(json["max_failed"], report.max_failed);
    assert_eq!(json["max_age_minutes"], report.max_age_minutes);
    assert_eq!(json["max_retries"], report.max_retries);
    assert_eq!(
        json["alert_reasons"],
        Value::Array(
            report
                .alert_reasons
                .iter()
                .map(|reason| Value::String((*reason).to_string()))
                .collect()
        )
    );
}

fn default_options() -> QdrantOutboxHealthOptions {
    QdrantOutboxHealthOptions {
        max_dead: 0,
        max_failed: 0,
        max_age_minutes: 60,
        max_retries: 5,
    }
}

#[tokio::test]
async fn healthy_pending_is_ok_in_text_and_json_reports() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-005 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(&[("PENDING", 0, "1 second")], default_options()).await;

    assert_eq!(report.dead_count, 0);
    assert_eq!(report.failed_retryable, 0);
    assert_eq!(report.high_retry_count, 0);
    assert_eq!(report.oldest_age_minutes, Some(1));
    assert_eq!(report.status, "OK");
    assert_eq!(report.exit_code(), 0);
    assert!(report.alert_reasons.is_empty());
    assert_text_and_json(&report);
}

#[tokio::test]
async fn dead_rows_alert_in_text_and_json_reports() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-005 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(&[("DEAD", 5, "1 second")], default_options()).await;

    assert_eq!(report.dead_count, 1);
    assert_eq!(report.failed_retryable, 0);
    assert_eq!(report.high_retry_count, 0);
    // DEAD không vào open-backlog age.
    assert_eq!(report.oldest_age_minutes, None);
    assert_eq!(report.exit_code(), 1);
    assert_eq!(report.alert_reasons, vec!["dead_above_threshold"]);
    assert_text_and_json(&report);
}

#[tokio::test]
async fn retryable_failed_rows_alert_in_text_and_json_reports() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-005 integration test: DATABASE_URL unavailable");
        return;
    }

    // retry_count=1: FAILED retryable nhưng chưa high_retry (threshold = 4 khi max=5).
    let report = run_fixture(&[("FAILED", 1, "1 second")], default_options()).await;

    assert_eq!(report.dead_count, 0);
    assert_eq!(report.failed_retryable, 1);
    assert_eq!(report.high_retry_count, 0);
    assert_eq!(report.oldest_age_minutes, Some(1));
    assert_eq!(report.exit_code(), 1);
    assert_eq!(
        report.alert_reasons,
        vec!["failed_retryable_above_threshold"]
    );
    assert_text_and_json(&report);
}

#[tokio::test]
async fn high_retry_count_proxy_alerts_in_text_and_json_reports() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-005 integration test: DATABASE_URL unavailable");
        return;
    }

    // retry_count=4: proxy high_retry (max_retries-1) + vẫn failed_retryable (4 < 5).
    let report = run_fixture(&[("FAILED", 4, "1 second")], default_options()).await;

    assert_eq!(report.dead_count, 0);
    assert_eq!(report.failed_retryable, 1);
    assert_eq!(report.high_retry_count, 1);
    assert_eq!(report.oldest_age_minutes, Some(1));
    assert_eq!(report.exit_code(), 1);
    assert_eq!(
        report.alert_reasons,
        vec![
            "failed_retryable_above_threshold",
            "high_retry_count_present"
        ]
    );
    assert_text_and_json(&report);
}

#[tokio::test]
async fn old_pending_row_alerts_in_text_and_json_reports() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-005 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(&[("PENDING", 0, "121 minutes")], default_options()).await;

    assert_eq!(report.dead_count, 0);
    assert_eq!(report.failed_retryable, 0);
    assert_eq!(report.high_retry_count, 0);
    assert_eq!(report.oldest_age_minutes, Some(121));
    assert_eq!(report.exit_code(), 1);
    assert_eq!(report.alert_reasons, vec!["oldest_age_above_threshold"]);
    assert_text_and_json(&report);
}

#[tokio::test]
async fn database_error_surfaces_as_sqlx_error_for_exit_code_two() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-005 integration test: DATABASE_URL unavailable");
        return;
    }

    let pool = pool_or_skip()
        .await
        .expect("DATABASE_URL must be available");
    let mut transaction = pool.begin().await.unwrap();

    // Chỉ search pg_temp (trống) → relation qdrant_outbox không resolve được.
    sqlx::query("SET LOCAL search_path TO pg_temp")
        .execute(&mut *transaction)
        .await
        .unwrap();

    let err = check_qdrant_outbox_health(&mut *transaction, default_options())
        .await
        .expect_err("missing relation must fail the health query");
    assert!(
        matches!(err, sqlx::Error::Database(_)),
        "expected database error, got {err:?}"
    );
}
