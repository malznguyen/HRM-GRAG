use std::sync::OnceLock;

use gmrag_api::auth::outbox_health::{
    AuthzOutboxHealthOptions, AuthzOutboxHealthReport, check_authz_outbox_health,
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
    let database_url = std::env::var("DATABASE_URL").ok()?;
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
    options: AuthzOutboxHealthOptions,
) -> AuthzOutboxHealthReport {
    let pool = pool_or_skip()
        .await
        .expect("DATABASE_URL must be available");
    let mut transaction = pool.begin().await.unwrap();

    sqlx::query("CREATE TEMP TABLE authz_outbox AS SELECT * FROM public.authz_outbox WITH NO DATA")
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("SET LOCAL search_path TO pg_temp, public")
        .execute(&mut *transaction)
        .await
        .unwrap();

    for (status, retry_count, age_sql) in rows {
        sqlx::query(
            "INSERT INTO authz_outbox (event_type, payload, status, retry_count, created_at, updated_at) VALUES ($1, '{}'::jsonb, $2, $3, CURRENT_TIMESTAMP - $4::interval, CURRENT_TIMESTAMP - $4::interval)",
        )
        .bind(format!("ops004-{status}-{retry_count}"))
        .bind(*status)
        .bind(*retry_count)
        .bind(*age_sql)
        .execute(&mut *transaction)
        .await
        .unwrap();
    }

    check_authz_outbox_health(&mut *transaction, options)
        .await
        .unwrap()
}

fn assert_text_and_json(report: &AuthzOutboxHealthReport) {
    let text = report.format_text();
    assert!(text.contains(&format!("status={}", report.status)));
    assert!(text.contains(&format!("failed_retryable={}", report.failed_retryable)));
    assert!(text.contains(&format!("exhausted={}", report.exhausted)));
    assert!(text.contains(&format!("max_failed={}", report.max_failed)));
    assert!(text.contains(&format!("max_age_minutes={}", report.max_age_minutes)));

    let json = serde_json::to_value(report).unwrap();
    assert_eq!(json["status"], Value::String(report.status.to_string()));
    assert_eq!(json["failed_retryable"], report.failed_retryable);
    assert_eq!(json["exhausted"], report.exhausted);
    assert_eq!(
        json["oldest_age_minutes"],
        report
            .oldest_age_minutes
            .map_or(Value::Null, |age| Value::from(age))
    );
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

#[tokio::test]
async fn normal_backlog_is_healthy_in_text_and_json_reports() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-004 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(
        &[("PENDING", 0, "1 second")],
        AuthzOutboxHealthOptions {
            max_failed: 0,
            max_age_minutes: 60,
            max_retries: 5,
        },
    )
    .await;

    assert_eq!(report.failed_retryable, 0);
    assert_eq!(report.exhausted, 0);
    assert_eq!(report.oldest_age_minutes, Some(1));
    assert_eq!(report.status, "OK");
    assert_eq!(report.exit_code(), 0);
    assert_text_and_json(&report);
}

#[tokio::test]
async fn retryable_failed_rows_alert_in_text_and_json_reports() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-004 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(
        &[("FAILED", 4, "1 second")],
        AuthzOutboxHealthOptions {
            max_failed: 0,
            max_age_minutes: 60,
            max_retries: 5,
        },
    )
    .await;

    assert_eq!(report.failed_retryable, 1);
    assert_eq!(report.exhausted, 0);
    assert_eq!(report.oldest_age_minutes, Some(1));
    assert_eq!(report.exit_code(), 1);
    assert_eq!(
        report.alert_reasons,
        vec!["failed_retryable_above_threshold"]
    );
    assert_text_and_json(&report);
}

#[tokio::test]
async fn exhausted_failed_rows_alert_in_text_and_json_reports() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-004 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(
        &[("FAILED", 5, "1 second")],
        AuthzOutboxHealthOptions {
            max_failed: 0,
            max_age_minutes: 60,
            max_retries: 5,
        },
    )
    .await;

    assert_eq!(report.failed_retryable, 0);
    assert_eq!(report.exhausted, 1);
    assert_eq!(report.oldest_age_minutes, Some(1));
    assert_eq!(report.exit_code(), 1);
    assert_eq!(report.alert_reasons, vec!["exhausted_retries"]);
    assert_text_and_json(&report);
}

#[tokio::test]
async fn old_pending_row_alerts_in_text_and_json_reports() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-004 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(
        &[("PENDING", 0, "121 minutes")],
        AuthzOutboxHealthOptions {
            max_failed: 0,
            max_age_minutes: 60,
            max_retries: 5,
        },
    )
    .await;

    assert_eq!(report.failed_retryable, 0);
    assert_eq!(report.exhausted, 0);
    assert_eq!(report.oldest_age_minutes, Some(121));
    assert_eq!(report.exit_code(), 1);
    assert_eq!(report.alert_reasons, vec!["oldest_age_above_threshold"]);
    assert_text_and_json(&report);
}
