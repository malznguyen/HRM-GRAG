use chrono::NaiveDateTime;
use serde::Serialize;
use sqlx::{Executor, FromRow, Postgres};

/// 3 × `INGESTION_JOB_LEASE_SECS` (default 300s) — lớn hơn lease để tránh false alert.
pub const DEFAULT_MAX_AGE_MINUTES: i64 = 15;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StuckIngestionHealthOptions {
    pub max_age_minutes: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct StuckIngestionHealthReport {
    pub status: &'static str,
    pub stuck_count: i64,
    pub stuck_without_active_job_count: i64,
    pub oldest_age_minutes: Option<i64>,
    pub max_age_minutes: i64,
    pub alert_reasons: Vec<&'static str>,
}

impl StuckIngestionHealthReport {
    /// Trả exit code theo kết quả health-check: 0 khoẻ, 1 cảnh báo.
    pub fn exit_code(&self) -> i32 {
        if self.status == "ALERT" { 1 } else { 0 }
    }

    /// Render report text với cùng tên field như JSON report.
    pub fn format_text(&self) -> String {
        let oldest_age = self
            .oldest_age_minutes
            .map_or_else(|| "null".to_string(), |age| age.to_string());
        let alert_reasons = if self.alert_reasons.is_empty() {
            "none".to_string()
        } else {
            self.alert_reasons.join(",")
        };

        format!(
            "status={}\nstuck_count={}\nstuck_without_active_job_count={}\noldest_age_minutes={}\nmax_age_minutes={}\nalert_reasons={}\n",
            self.status,
            self.stuck_count,
            self.stuck_without_active_job_count,
            oldest_age,
            self.max_age_minutes,
            alert_reasons,
        )
    }
}

#[derive(Debug, FromRow)]
struct StuckIngestionHealthSnapshot {
    stuck_count: i64,
    stuck_without_active_job_count: i64,
    oldest_created_at: Option<NaiveDateTime>,
    database_now: NaiveDateTime,
}

/// Đọc snapshot document PROCESSING kẹt TTL — không mutate documents/ingestion_jobs.
pub async fn check_stuck_ingestion_health<'e, E>(
    executor: E,
    options: StuckIngestionHealthOptions,
) -> Result<StuckIngestionHealthReport, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    // Age dùng documents.created_at — bảng documents không có updated_at/status_changed_at.
    // Active job = QUEUED|PROCESSING (cùng định nghĩa recover-stale-ingestion-jobs).
    let snapshot = sqlx::query_as::<_, StuckIngestionHealthSnapshot>(
        r#"
        SELECT
            COUNT(*) FILTER (
                WHERE document.status = 'PROCESSING'
                  AND document.created_at
                      < CURRENT_TIMESTAMP - ($1::double precision * INTERVAL '1 minute')
            )::bigint AS stuck_count,
            COUNT(*) FILTER (
                WHERE document.status = 'PROCESSING'
                  AND document.created_at
                      < CURRENT_TIMESTAMP - ($1::double precision * INTERVAL '1 minute')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM ingestion_jobs job
                      WHERE job.document_id = document.id
                        AND job.status IN ('QUEUED', 'PROCESSING')
                  )
            )::bigint AS stuck_without_active_job_count,
            MIN(document.created_at) FILTER (
                WHERE document.status = 'PROCESSING'
            ) AS oldest_created_at,
            CURRENT_TIMESTAMP::timestamp AS database_now
        FROM documents document
        "#,
    )
    .bind(options.max_age_minutes as f64)
    .fetch_one(executor)
    .await?;

    let oldest_age_minutes = snapshot
        .oldest_created_at
        .map(|oldest| age_minutes(oldest, snapshot.database_now));

    let mut alert_reasons = Vec::new();
    if snapshot.stuck_count > 0 {
        alert_reasons.push("stuck_processing_above_threshold");
    }
    if snapshot.stuck_without_active_job_count > 0 {
        alert_reasons.push("stuck_without_active_job");
    }

    Ok(StuckIngestionHealthReport {
        status: if alert_reasons.is_empty() {
            "OK"
        } else {
            "ALERT"
        },
        stuck_count: snapshot.stuck_count,
        stuck_without_active_job_count: snapshot.stuck_without_active_job_count,
        oldest_age_minutes,
        max_age_minutes: options.max_age_minutes,
        alert_reasons,
    })
}

fn age_minutes(oldest: NaiveDateTime, now: NaiveDateTime) -> i64 {
    let age_seconds = (now - oldest).num_seconds();
    if age_seconds <= 0 {
        return 0;
    }

    (age_seconds + 59) / 60
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn age_rounds_up_and_clamps_future_timestamps() {
        let now = NaiveDateTime::default();
        assert_eq!(age_minutes(now - Duration::seconds(1), now), 1);
        assert_eq!(age_minutes(now - Duration::minutes(15), now), 15);
        assert_eq!(age_minutes(now + Duration::seconds(1), now), 0);
    }

    #[test]
    fn text_report_uses_null_and_none_for_healthy() {
        let report = StuckIngestionHealthReport {
            status: "OK",
            stuck_count: 0,
            stuck_without_active_job_count: 0,
            oldest_age_minutes: None,
            max_age_minutes: 15,
            alert_reasons: Vec::new(),
        };

        assert!(report.format_text().contains("oldest_age_minutes=null"));
        assert!(report.format_text().contains("alert_reasons=none"));
        assert_eq!(report.exit_code(), 0);
    }
}
