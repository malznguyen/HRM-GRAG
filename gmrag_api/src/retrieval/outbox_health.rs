use chrono::NaiveDateTime;
use serde::Serialize;
use sqlx::{Executor, FromRow, Postgres};

pub const DEFAULT_MAX_DEAD: i64 = 0;
pub const DEFAULT_MAX_FAILED: i64 = 0;
pub const DEFAULT_MAX_AGE_MINUTES: i64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QdrantOutboxHealthOptions {
    pub max_dead: i64,
    pub max_failed: i64,
    pub max_age_minutes: i64,
    pub max_retries: i32,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct QdrantOutboxHealthReport {
    pub status: &'static str,
    pub dead_count: i64,
    pub failed_retryable: i64,
    pub high_retry_count: i64,
    pub oldest_age_minutes: Option<i64>,
    pub max_dead: i64,
    pub max_failed: i64,
    pub max_age_minutes: i64,
    pub max_retries: i32,
    pub alert_reasons: Vec<&'static str>,
}

impl QdrantOutboxHealthReport {
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
            "status={}\ndead_count={}\nfailed_retryable={}\nhigh_retry_count={}\noldest_age_minutes={}\nmax_dead={}\nmax_failed={}\nmax_age_minutes={}\nmax_retries={}\nalert_reasons={}\n",
            self.status,
            self.dead_count,
            self.failed_retryable,
            self.high_retry_count,
            oldest_age,
            self.max_dead,
            self.max_failed,
            self.max_age_minutes,
            self.max_retries,
            alert_reasons,
        )
    }
}

#[derive(Debug, FromRow)]
struct QdrantOutboxHealthSnapshot {
    dead_count: i64,
    failed_retryable: i64,
    high_retry_count: i64,
    oldest_created_at: Option<NaiveDateTime>,
    database_now: NaiveDateTime,
}

/// Đọc snapshot qdrant outbox và không thực hiện bất kỳ mutation nào.
pub async fn check_qdrant_outbox_health<'e, E>(
    executor: E,
    options: QdrantOutboxHealthOptions,
) -> Result<QdrantOutboxHealthReport, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    // high_retry_count: proxy "gần DEAD" — không phải retry rate theo thời gian.
    // Ngưỡng: retry_count >= max_retries - 1 (còn 1 fail nữa là terminal).
    let high_retry_threshold = options.max_retries.saturating_sub(1).max(0);

    let snapshot = sqlx::query_as::<_, QdrantOutboxHealthSnapshot>(
        r#"
        SELECT
            COUNT(*) FILTER (
                WHERE status = 'DEAD'
            )::bigint AS dead_count,
            COUNT(*) FILTER (
                WHERE status = 'FAILED' AND retry_count < $1
            )::bigint AS failed_retryable,
            COUNT(*) FILTER (
                WHERE status IN ('PENDING', 'FAILED')
                  AND retry_count >= $2
            )::bigint AS high_retry_count,
            MIN(created_at) FILTER (
                WHERE status IN ('PENDING', 'FAILED')
            ) AS oldest_created_at,
            CURRENT_TIMESTAMP::timestamp AS database_now
        FROM qdrant_outbox
        "#,
    )
    .bind(options.max_retries)
    .bind(high_retry_threshold)
    .fetch_one(executor)
    .await?;

    let oldest_age_minutes = snapshot
        .oldest_created_at
        .map(|oldest| age_minutes(oldest, snapshot.database_now));

    let mut alert_reasons = Vec::new();
    if snapshot.dead_count > options.max_dead {
        alert_reasons.push("dead_above_threshold");
    }
    if snapshot.failed_retryable > options.max_failed {
        alert_reasons.push("failed_retryable_above_threshold");
    }
    if snapshot.high_retry_count > 0 {
        alert_reasons.push("high_retry_count_present");
    }
    if oldest_age_minutes.is_some_and(|age| age > options.max_age_minutes) {
        alert_reasons.push("oldest_age_above_threshold");
    }

    Ok(QdrantOutboxHealthReport {
        status: if alert_reasons.is_empty() {
            "OK"
        } else {
            "ALERT"
        },
        dead_count: snapshot.dead_count,
        failed_retryable: snapshot.failed_retryable,
        high_retry_count: snapshot.high_retry_count,
        oldest_age_minutes,
        max_dead: options.max_dead,
        max_failed: options.max_failed,
        max_age_minutes: options.max_age_minutes,
        max_retries: options.max_retries,
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
        assert_eq!(age_minutes(now - Duration::minutes(60), now), 60);
        assert_eq!(age_minutes(now + Duration::seconds(1), now), 0);
    }

    #[test]
    fn text_report_uses_null_and_none_for_empty_backlog() {
        let report = QdrantOutboxHealthReport {
            status: "OK",
            dead_count: 0,
            failed_retryable: 0,
            high_retry_count: 0,
            oldest_age_minutes: None,
            max_dead: 0,
            max_failed: 0,
            max_age_minutes: 60,
            max_retries: 5,
            alert_reasons: Vec::new(),
        };

        assert!(report.format_text().contains("oldest_age_minutes=null"));
        assert!(report.format_text().contains("alert_reasons=none"));
        assert_eq!(report.exit_code(), 0);
    }
}
