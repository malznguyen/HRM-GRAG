//! Logic dùng chung cho outbox processor (claim lease, exponential backoff, poison).
//!
//! Hiện qdrant_outbox dùng module này; authz_outbox có thể tái sử dụng sau
//! mà không bắt buộc đổi schema/authz trong cùng PR.

use std::time::Duration;

/// Status terminal: hết retry hoặc lỗi không thể tự phục hồi (payload/event hỏng).
pub const STATUS_DEAD: &str = "DEAD";
pub const STATUS_FAILED: &str = "FAILED";
pub const STATUS_PENDING: &str = "PENDING";
pub const STATUS_PROCESSED: &str = "PROCESSED";

/// Cấu hình backoff + lease khi claim row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxBackoffConfig {
    /// Base delay (giây) cho lần fail đầu: delay = base * 2^(retry_count-1).
    pub base_backoff_secs: i64,
    /// Trần delay (giây) — tránh chờ quá lâu khi retry_count lớn.
    pub max_backoff_secs: i64,
    /// Lease (giây) gắn vào `next_attempt_at` lúc claim — worker crash thì row
    /// tự “nhả” sau lease để worker khác claim lại.
    pub claim_lease_secs: i64,
}

impl Default for OutboxBackoffConfig {
    fn default() -> Self {
        Self {
            base_backoff_secs: 2,
            max_backoff_secs: 300,
            claim_lease_secs: 120,
        }
    }
}

/// Kết quả quyết định sau một lần xử lý fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureDisposition {
    /// Còn retry được — status FAILED + schedule next_attempt_at.
    Retryable { next_retry_count: i32, backoff_secs: i64 },
    /// Poison / hết retry — status DEAD, không claim lại.
    Dead { next_retry_count: i32 },
}

/// Tính delay backoff: `min(base * 2^(retry_count_after_fail - 1), max)`.
///
/// `retry_count_after_fail` là giá trị *sau* khi tăng (lần fail đầu = 1 → delay = base).
pub fn compute_backoff_secs(
    retry_count_after_fail: i32,
    base_backoff_secs: i64,
    max_backoff_secs: i64,
) -> i64 {
    let base = base_backoff_secs.max(0);
    let max = max_backoff_secs.max(0);
    if base == 0 || max == 0 {
        return 0;
    }

    // Cap exponent để tránh overflow khi shift; 2^30 * base đã rất lớn.
    let exponent = retry_count_after_fail.saturating_sub(1).clamp(0, 30) as u32;
    let multiplier = 1i64.checked_shl(exponent).unwrap_or(i64::MAX);
    let delay = base.saturating_mul(multiplier);
    delay.min(max)
}

/// Quyết định FAILED (retry + backoff) hay DEAD (poison / hết quota).
///
/// - `permanent_error`: lỗi không bao giờ tự hết (payload/event_type sai) → DEAD ngay.
/// - `retry_count` hiện tại *trước* lần fail này; hàm tự +1.
pub fn disposition_after_failure(
    current_retry_count: i32,
    max_retries: i32,
    permanent_error: bool,
    backoff: OutboxBackoffConfig,
) -> FailureDisposition {
    let next_retry_count = current_retry_count.saturating_add(1);

    if permanent_error || next_retry_count >= max_retries {
        return FailureDisposition::Dead { next_retry_count };
    }

    let backoff_secs = compute_backoff_secs(
        next_retry_count,
        backoff.base_backoff_secs,
        backoff.max_backoff_secs,
    );

    FailureDisposition::Retryable {
        next_retry_count,
        backoff_secs,
    }
}

/// `Duration` từ số giây backoff (dùng khi bind SQL interval nếu cần từ Rust).
pub fn backoff_duration(secs: i64) -> Duration {
    Duration::from_secs(secs.max(0) as u64)
}

pub fn parse_env_i64(name: &str, default: i64, min: i64, max: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

pub fn parse_env_i32(name: &str, default: i32, min: i32, max: i32) -> i32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_first_fail_uses_base() {
        // retry_count sau fail = 1 → 2^(0) * base = base
        assert_eq!(compute_backoff_secs(1, 2, 300), 2);
        assert_eq!(compute_backoff_secs(2, 2, 300), 4);
        assert_eq!(compute_backoff_secs(3, 2, 300), 8);
        assert_eq!(compute_backoff_secs(4, 2, 300), 16);
    }

    #[test]
    fn backoff_caps_at_max() {
        assert_eq!(compute_backoff_secs(20, 2, 60), 60);
        assert_eq!(compute_backoff_secs(1, 100, 50), 50);
    }

    #[test]
    fn backoff_zero_base_or_max() {
        assert_eq!(compute_backoff_secs(1, 0, 300), 0);
        assert_eq!(compute_backoff_secs(1, 5, 0), 0);
    }

    #[test]
    fn disposition_retryable_under_max() {
        let backoff = OutboxBackoffConfig {
            base_backoff_secs: 2,
            max_backoff_secs: 300,
            claim_lease_secs: 120,
        };
        match disposition_after_failure(0, 5, false, backoff) {
            FailureDisposition::Retryable {
                next_retry_count,
                backoff_secs,
            } => {
                assert_eq!(next_retry_count, 1);
                assert_eq!(backoff_secs, 2);
            }
            other => panic!("expected Retryable, got {other:?}"),
        }
    }

    #[test]
    fn disposition_dead_when_retries_exhausted() {
        let backoff = OutboxBackoffConfig::default();
        // max_retries=5: sau fail thứ 5 next_retry=5 → DEAD
        match disposition_after_failure(4, 5, false, backoff) {
            FailureDisposition::Dead { next_retry_count } => {
                assert_eq!(next_retry_count, 5);
            }
            other => panic!("expected Dead, got {other:?}"),
        }
    }

    #[test]
    fn disposition_dead_on_permanent_error_immediately() {
        let backoff = OutboxBackoffConfig::default();
        match disposition_after_failure(0, 5, true, backoff) {
            FailureDisposition::Dead { next_retry_count } => {
                assert_eq!(next_retry_count, 1);
            }
            other => panic!("expected Dead, got {other:?}"),
        }
    }

    #[test]
    fn retry_count_saturates() {
        let backoff = OutboxBackoffConfig::default();
        match disposition_after_failure(i32::MAX, 5, true, backoff) {
            FailureDisposition::Dead { next_retry_count } => {
                assert_eq!(next_retry_count, i32::MAX);
            }
            other => panic!("expected Dead, got {other:?}"),
        }
    }
}
