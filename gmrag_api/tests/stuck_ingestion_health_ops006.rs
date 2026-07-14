mod support;

use std::sync::OnceLock;

use gmrag_api::ingestion::stuck_health::{
    StuckIngestionHealthOptions, StuckIngestionHealthReport, check_stuck_ingestion_health,
};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::Mutex;
use uuid::Uuid;

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

struct FixtureDoc {
    status: &'static str,
    age_sql: &'static str,
    /// None = không có active job; Some(status) = insert job QUEUED|PROCESSING.
    active_job_status: Option<&'static str>,
}

async fn run_fixture(
    docs: &[FixtureDoc],
    options: StuckIngestionHealthOptions,
) -> StuckIngestionHealthReport {
    let pool = pool_or_skip()
        .await
        .expect("DATABASE_URL must be available");
    let mut transaction = pool.begin().await.unwrap();

    sqlx::query("CREATE TEMP TABLE documents AS SELECT * FROM public.documents WITH NO DATA")
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TEMP TABLE ingestion_jobs AS SELECT * FROM public.ingestion_jobs WITH NO DATA",
    )
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query("SET LOCAL search_path TO pg_temp, public")
        .execute(&mut *transaction)
        .await
        .unwrap();

    let workspace_id = Uuid::new_v4();

    for doc in docs {
        let document_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO documents (
                id, workspace_id, filename, status, processing_stage,
                object_key, bucket, uploaded_by, created_at
            ) VALUES (
                $1, $2, $3, $4, 'QUEUED',
                'ops006/object.pdf', 'test', 'ops006-uploader',
                CURRENT_TIMESTAMP - $5::interval
            )",
        )
        .bind(document_id)
        .bind(workspace_id)
        .bind(format!("ops006-{}.pdf", document_id))
        .bind(doc.status)
        .bind(doc.age_sql)
        .execute(&mut *transaction)
        .await
        .unwrap();

        if let Some(job_status) = doc.active_job_status {
            sqlx::query(
                "INSERT INTO ingestion_jobs (
                    document_id, workspace_id, status, attempt_count, max_attempts
                ) VALUES ($1, $2, $3, 0, 5)",
            )
            .bind(document_id)
            .bind(workspace_id)
            .bind(job_status)
            .execute(&mut *transaction)
            .await
            .unwrap();
        }
    }

    check_stuck_ingestion_health(&mut *transaction, options)
        .await
        .unwrap()
}

fn assert_text_and_json(report: &StuckIngestionHealthReport) {
    let text = report.format_text();
    assert!(text.contains(&format!("status={}", report.status)));
    assert!(text.contains(&format!("stuck_count={}", report.stuck_count)));
    assert!(text.contains(&format!(
        "stuck_without_active_job_count={}",
        report.stuck_without_active_job_count
    )));
    assert!(text.contains(&format!("max_age_minutes={}", report.max_age_minutes)));

    let json = serde_json::to_value(report).unwrap();
    assert_eq!(json["status"], Value::String(report.status.to_string()));
    assert_eq!(json["stuck_count"], report.stuck_count);
    assert_eq!(
        json["stuck_without_active_job_count"],
        report.stuck_without_active_job_count
    );
    assert_eq!(
        json["oldest_age_minutes"],
        report
            .oldest_age_minutes
            .map_or(Value::Null, |age| Value::from(age))
    );
    assert_eq!(json["max_age_minutes"], report.max_age_minutes);
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

fn default_options() -> StuckIngestionHealthOptions {
    StuckIngestionHealthOptions {
        max_age_minutes: 15,
    }
}

#[tokio::test]
async fn fresh_processing_with_active_job_is_healthy() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-006 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(
        &[FixtureDoc {
            status: "PROCESSING",
            age_sql: "1 minute",
            active_job_status: Some("PROCESSING"),
        }],
        default_options(),
    )
    .await;

    assert_eq!(report.stuck_count, 0);
    assert_eq!(report.stuck_without_active_job_count, 0);
    assert_eq!(report.oldest_age_minutes, Some(1));
    assert_eq!(report.status, "OK");
    assert_eq!(report.exit_code(), 0);
    assert!(report.alert_reasons.is_empty());
    assert_text_and_json(&report);
}

#[tokio::test]
async fn old_processing_with_active_job_alerts_normally() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-006 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(
        &[FixtureDoc {
            status: "PROCESSING",
            age_sql: "30 minutes",
            active_job_status: Some("PROCESSING"),
        }],
        default_options(),
    )
    .await;

    assert_eq!(report.stuck_count, 1);
    assert_eq!(report.stuck_without_active_job_count, 0);
    assert_eq!(report.oldest_age_minutes, Some(30));
    assert_eq!(report.exit_code(), 1);
    assert_eq!(
        report.alert_reasons,
        vec!["stuck_processing_above_threshold"]
    );
    assert_text_and_json(&report);
}

#[tokio::test]
async fn old_processing_without_active_job_alerts_higher_severity() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-006 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(
        &[FixtureDoc {
            status: "PROCESSING",
            age_sql: "30 minutes",
            active_job_status: None,
        }],
        default_options(),
    )
    .await;

    assert_eq!(report.stuck_count, 1);
    assert_eq!(report.stuck_without_active_job_count, 1);
    assert_eq!(report.oldest_age_minutes, Some(30));
    assert_eq!(report.exit_code(), 1);
    assert_eq!(
        report.alert_reasons,
        vec![
            "stuck_processing_above_threshold",
            "stuck_without_active_job"
        ]
    );
    assert_text_and_json(&report);
}

#[tokio::test]
async fn completed_document_is_not_stuck() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-006 integration test: DATABASE_URL unavailable");
        return;
    }

    let report = run_fixture(
        &[FixtureDoc {
            status: "COMPLETED",
            age_sql: "120 minutes",
            active_job_status: None,
        }],
        default_options(),
    )
    .await;

    assert_eq!(report.stuck_count, 0);
    assert_eq!(report.stuck_without_active_job_count, 0);
    assert_eq!(report.oldest_age_minutes, None);
    assert_eq!(report.status, "OK");
    assert_eq!(report.exit_code(), 0);
    assert_text_and_json(&report);
}

#[tokio::test]
async fn database_error_surfaces_as_sqlx_error_for_exit_code_two() {
    let _guard = test_lock().lock().await;
    if pool_or_skip().await.is_none() {
        eprintln!("skip OPS-006 integration test: DATABASE_URL unavailable");
        return;
    }

    let pool = pool_or_skip()
        .await
        .expect("DATABASE_URL must be available");
    let mut transaction = pool.begin().await.unwrap();

    // Chỉ search pg_temp (trống) → relation documents không resolve được.
    sqlx::query("SET LOCAL search_path TO pg_temp")
        .execute(&mut *transaction)
        .await
        .unwrap();

    let err = check_stuck_ingestion_health(&mut *transaction, default_options())
        .await
        .expect_err("missing relation must fail the health query");
    assert!(
        matches!(err, sqlx::Error::Database(_)),
        "expected database error, got {err:?}"
    );
}
