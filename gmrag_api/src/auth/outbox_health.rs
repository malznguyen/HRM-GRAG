use chrono::NaiveDateTime;
use serde::Serialize;
use sqlx::{Executor, FromRow, Postgres};

pub const DEFAULT_MAX_FAILED: i64 = 0;
pub const DEFAULT_MAX_AGE_MINUTES: i64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthzOutboxHealthOptions {
    pub max_failed: i64,
    pub max_age_minutes: i64,
    pub max_retries: i32,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AuthzOutboxHealthReport {
    pub status: &'static str,
    pub failed_retryable: i64,
    pub exhausted: i64,
    pub oldest_age_minutes: Option<i64>,
    pub max_failed: i64,
    pub max_age_minutes: i64,
    pub max_retries: i32,
    pub alert_reasons: Vec<&'static str>,
}

impl AuthzOutboxHealthReport {
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
            "status={}\nfailed_retryable={}\nexhausted={}\noldest_age_minutes={}\nmax_failed={}\nmax_age_minutes={}\nmax_retries={}\nalert_reasons={}\n",
            self.status,
            self.failed_retryable,
            self.exhausted,
            oldest_age,
            self.max_failed,
            self.max_age_minutes,
            self.max_retries,
            alert_reasons,
        )
    }
}

#[derive(Debug, FromRow)]
struct AuthzOutboxHealthSnapshot {
    failed_retryable: i64,
    exhausted: i64,
    oldest_created_at: Option<NaiveDateTime>,
    database_now: NaiveDateTime,
}

/// Đọc snapshot authz outbox và không thực hiện bất kỳ mutation nào.
pub async fn check_authz_outbox_health<'e, E>(
    executor: E,
    options: AuthzOutboxHealthOptions,
) -> Result<AuthzOutboxHealthReport, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let snapshot = sqlx::query_as::<_, AuthzOutboxHealthSnapshot>(
        r#"
        SELECT
            COUNT(*) FILTER (
                WHERE status = 'FAILED' AND retry_count < $1
            )::bigint AS failed_retryable,
            COUNT(*) FILTER (
                WHERE status = 'FAILED' AND retry_count >= $1
            )::bigint AS exhausted,
            MIN(created_at) FILTER (
                WHERE status IN ('PENDING', 'FAILED')
            ) AS oldest_created_at,
            CURRENT_TIMESTAMP::timestamp AS database_now
        FROM authz_outbox
        "#,
    )
    .bind(options.max_retries)
    .fetch_one(executor)
    .await?;

    let oldest_age_minutes = snapshot
        .oldest_created_at
        .map(|oldest| age_minutes(oldest, snapshot.database_now));

    let mut alert_reasons = Vec::new();
    if snapshot.failed_retryable > options.max_failed {
        alert_reasons.push("failed_retryable_above_threshold");
    }
    if snapshot.exhausted > 0 {
        alert_reasons.push("exhausted_retries");
    }
    if oldest_age_minutes.is_some_and(|age| age > options.max_age_minutes) {
        alert_reasons.push("oldest_age_above_threshold");
    }

    Ok(AuthzOutboxHealthReport {
        status: if alert_reasons.is_empty() {
            "OK"
        } else {
            "ALERT"
        },
        failed_retryable: snapshot.failed_retryable,
        exhausted: snapshot.exhausted,
        oldest_age_minutes,
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
        let report = AuthzOutboxHealthReport {
            status: "OK",
            failed_retryable: 0,
            exhausted: 0,
            oldest_age_minutes: None,
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
