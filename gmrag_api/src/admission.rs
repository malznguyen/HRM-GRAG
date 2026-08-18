//! Giới hạn số request chat được xử lý đồng thời (admission control).
//!
//! # Vì sao cần
//!
//! Bước embed câu hỏi tốn CPU và Ollama chỉ xử lý một request tại một thời điểm
//! (`OLLAMA_NUM_PARALLEL=1`). Khi có quá nhiều request cùng lúc, CPU dành phần
//! lớn thời gian chuyển ngữ cảnh thay vì tính toán, và **thông lượng giảm tuyệt
//! đối** — đo trên server 1 vCPU (2026-08-14):
//!
//! | Đồng thời | Thông lượng |
//! |---|---|
//! | 5  | 1,30 req/s |
//! | 10 | 0,59 req/s |
//! | 20 | 0,19 req/s |
//! | 40 | 0,044 req/s |
//!
//! Ở mức 20 request đồng thời, **100% trả 502** — Qdrant bị đói CPU và timeout.
//! Không ai nhận được câu trả lời, kể cả người gửi đầu tiên. Toàn bộ công sức
//! embed đã bỏ ra bị vứt đi.
//!
//! Cho ít request vào cùng lúc thì tổng số câu trả lời hoàn thành LỚN HƠN.
//!
//! # Khác gì `rate_limit`
//!
//! [`crate::rate_limit`] giới hạn **số request mỗi phút của từng user**. Module
//! này giới hạn **số request đang chạy cùng lúc của toàn hệ thống**. Chúng bắt
//! các trường hợp khác nhau: 40 user khác nhau, mỗi người gửi đúng 1 câu, thì
//! rate limit cho qua hết — nhưng vẫn là 40 request đồng thời.
//!
//! # Hàng đợi phải có đáy
//!
//! Chỉ chặn mà cho xếp hàng vô hạn thì chỉ **dời** chỗ sập: người thứ 200 chờ
//! vài phút rồi client tự timeout — vẫn hỏng, chỉ chậm hơn. Nên có ba tầng:
//!
//! 1. `concurrency` request được chạy;
//! 2. thêm `queue_depth` request được chờ, tối đa `wait_timeout`;
//! 3. quá ngưỡng đó → từ chối NGAY với 503 + `Retry-After`.
//!
//! Từ chối nhanh và trung thực tốt hơn bắt người dùng chờ rồi vẫn báo lỗi.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Số chat được xử lý đồng thời.
///
/// **Bắt buộc chỉnh theo số lõi CPU thật của máy.** 10 là mặc định cho máy có
/// vài lõi; máy 1 vCPU phải hạ xuống 5 — đo trên server 1 vCPU với 20 người hỏi
/// cùng lúc:
///
/// | `GMRAG_CHAT_CONCURRENCY` | Trả lời hoàn chỉnh | Lỗi cứng 502 | p50 |
/// |---|---|---|---|
/// | 10 | 5/20 | 5 | 44,4s |
/// | **5** | **16/20** | **0** | **12,1s** |
///
/// Đặt cao hơn sức máy KHÔNG làm phục vụ được nhiều hơn — nó chỉ khiến các
/// request đang chạy cùng đói CPU rồi cùng timeout ở Qdrant.
pub const DEFAULT_CHAT_CONCURRENCY: usize = 10;
/// Số request được phép nằm chờ ngoài cửa. Quá ngưỡng này là từ chối ngay.
pub const DEFAULT_CHAT_QUEUE_DEPTH: usize = 20;
/// Thời gian chờ tối đa trong hàng đợi trước khi bỏ cuộc.
pub const DEFAULT_CHAT_QUEUE_WAIT_SECS: u64 = 15;
/// Trần cứng, chặn cấu hình sai kiểu `GMRAG_CHAT_QUEUE_WAIT_SECS=3600`.
const MAX_CHAT_QUEUE_WAIT_SECS: u64 = 120;

/// Lý do một request bị từ chối. Cả hai đều ra 503, nhưng tách ra để log và
/// metric phân biệt được "quá tải tức thời" với "chờ mãi không tới lượt".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejection {
    /// Hàng đợi đã đầy — từ chối ngay, không bắt chờ.
    QueueFull,
    /// Đã chờ hết `wait_timeout` mà vẫn chưa tới lượt.
    WaitTimeout,
}

impl AdmissionRejection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QueueFull => "queue_full",
            Self::WaitTimeout => "wait_timeout",
        }
    }
}

/// Giảm bộ đếm hàng đợi kể cả khi future bị huỷ giữa chừng.
///
/// Nếu client ngắt kết nối trong lúc đang chờ, future bị drop và đoạn code sau
/// `.await` KHÔNG bao giờ chạy. Không có guard này thì bộ đếm chỉ tăng không
/// giảm, và sau một thời gian mọi request đều bị từ chối oan.
struct WaitingGuard(Arc<AtomicUsize>);

impl Drop for WaitingGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct ChatAdmission {
    semaphore: Arc<Semaphore>,
    waiting: Arc<AtomicUsize>,
    queue_depth: usize,
    wait_timeout: Duration,
}

impl Default for ChatAdmission {
    /// Dùng đúng bộ mặc định của [`ChatAdmission::from_env`] nhưng không đọc
    /// biến môi trường — để test dựng `AppState` mà không phụ thuộc env.
    fn default() -> Self {
        Self::new(
            DEFAULT_CHAT_CONCURRENCY,
            DEFAULT_CHAT_QUEUE_DEPTH,
            Duration::from_secs(DEFAULT_CHAT_QUEUE_WAIT_SECS),
        )
    }
}

impl ChatAdmission {
    pub fn new(concurrency: usize, queue_depth: usize, wait_timeout: Duration) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(concurrency.max(1))),
            waiting: Arc::new(AtomicUsize::new(0)),
            queue_depth,
            wait_timeout,
        }
    }

    pub fn from_env() -> Self {
        let concurrency =
            parse_usize_env("GMRAG_CHAT_CONCURRENCY", DEFAULT_CHAT_CONCURRENCY).max(1);
        let queue_depth = parse_usize_env("GMRAG_CHAT_QUEUE_DEPTH", DEFAULT_CHAT_QUEUE_DEPTH);
        let wait_secs = parse_u64_env("GMRAG_CHAT_QUEUE_WAIT_SECS", DEFAULT_CHAT_QUEUE_WAIT_SECS)
            .clamp(1, MAX_CHAT_QUEUE_WAIT_SECS);

        Self::new(concurrency, queue_depth, Duration::from_secs(wait_secs))
    }

    /// Số giây gợi ý cho header `Retry-After`.
    pub fn retry_after_secs(&self) -> u64 {
        self.wait_timeout.as_secs().max(1)
    }

    pub fn log_config_on_startup(&self) {
        tracing::info!(
            chat_concurrency = self.semaphore.available_permits(),
            chat_queue_depth = self.queue_depth,
            chat_queue_wait_secs = self.wait_timeout.as_secs(),
            "Chat admission control configured"
        );
    }

    /// Xin một suất xử lý. Permit trả về phải được GIỮ trong suốt thời gian
    /// request còn chạy — thả sớm là mất tác dụng giới hạn.
    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, AdmissionRejection> {
        // Đường nhanh: còn chỗ trống thì vào thẳng, không đụng tới hàng đợi.
        if let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_owned() {
            record_outcome("admitted");
            return Ok(permit);
        }

        // Nhận chỗ trong hàng đợi trước khi kiểm tra, để hai request vào cùng
        // lúc không cùng thấy "còn chỗ" rồi cùng chen vào.
        let previous = self.waiting.fetch_add(1, Ordering::SeqCst);
        let _guard = WaitingGuard(Arc::clone(&self.waiting));
        if previous >= self.queue_depth {
            record_outcome(AdmissionRejection::QueueFull.as_str());
            return Err(AdmissionRejection::QueueFull);
        }

        match tokio::time::timeout(
            self.wait_timeout,
            Arc::clone(&self.semaphore).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => {
                record_outcome("admitted_after_wait");
                Ok(permit)
            }
            // Semaphore bị đóng: không xảy ra vì không nơi nào gọi `close()`.
            // Fail-closed thay vì cho qua không giới hạn.
            Ok(Err(_)) => {
                record_outcome(AdmissionRejection::QueueFull.as_str());
                Err(AdmissionRejection::QueueFull)
            }
            Err(_) => {
                record_outcome(AdmissionRejection::WaitTimeout.as_str());
                Err(AdmissionRejection::WaitTimeout)
            }
        }
    }

    #[cfg(test)]
    fn waiting_count(&self) -> usize {
        self.waiting.load(Ordering::SeqCst)
    }
}

fn record_outcome(outcome: &'static str) {
    metrics::counter!("gmrag_chat_admission_total", "outcome" => outcome).increment(1);
}

fn parse_usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn parse_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn admits_up_to_concurrency_without_waiting() {
        let admission = ChatAdmission::new(3, 5, Duration::from_secs(1));

        let permits: Vec<_> = futures::future::join_all((0..3).map(|_| admission.acquire()))
            .await
            .into_iter()
            .map(|result| result.expect("trong giới hạn thì phải được nhận"))
            .collect();

        assert_eq!(permits.len(), 3);
        assert_eq!(admission.waiting_count(), 0, "chưa ai phải xếp hàng");
    }

    #[tokio::test]
    async fn rejects_immediately_when_queue_is_full() {
        // 1 chỗ chạy, 0 chỗ chờ: request thứ hai phải bị từ chối NGAY.
        let admission = ChatAdmission::new(1, 0, Duration::from_secs(30));
        let _held = admission.acquire().await.expect("request đầu được nhận");

        let started = std::time::Instant::now();
        let rejection = admission.acquire().await.expect_err("phải bị từ chối");

        assert_eq!(rejection, AdmissionRejection::QueueFull);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "queue_full phải trả về ngay, không được chờ hết wait_timeout"
        );
    }

    #[tokio::test]
    async fn waits_then_times_out_when_permit_never_frees() {
        let admission = ChatAdmission::new(1, 4, Duration::from_millis(120));
        let _held = admission.acquire().await.expect("request đầu được nhận");

        let rejection = admission.acquire().await.expect_err("phải hết giờ chờ");

        assert_eq!(rejection, AdmissionRejection::WaitTimeout);
        assert_eq!(
            admission.waiting_count(),
            0,
            "bộ đếm hàng đợi phải được trả lại sau khi hết giờ"
        );
    }

    #[tokio::test]
    async fn queued_request_is_admitted_once_a_permit_frees() {
        let admission = ChatAdmission::new(1, 4, Duration::from_secs(5));
        let held = admission.acquire().await.expect("request đầu được nhận");

        let waiter = {
            let admission = admission.clone();
            tokio::spawn(async move { admission.acquire().await })
        };

        // Nhường lượt để `waiter` kịp vào hàng đợi trước khi permit được thả.
        tokio::task::yield_now().await;
        drop(held);

        let result = waiter.await.expect("task không được panic");
        assert!(
            result.is_ok(),
            "người đang chờ phải được nhận khi có chỗ trống"
        );
        assert_eq!(admission.waiting_count(), 0);
    }

    #[tokio::test]
    async fn waiting_counter_is_released_when_caller_gives_up() {
        let admission = ChatAdmission::new(1, 4, Duration::from_secs(30));
        let _held = admission.acquire().await.expect("request đầu được nhận");

        {
            // Mô phỏng client ngắt kết nối: future bị drop giữa lúc đang chờ.
            let pending = admission.acquire();
            tokio::pin!(pending);
            let poll = tokio::time::timeout(Duration::from_millis(50), &mut pending).await;
            assert!(poll.is_err(), "vẫn đang chờ, chưa có kết quả");
        }

        assert_eq!(
            admission.waiting_count(),
            0,
            "future bị huỷ vẫn phải trả lại chỗ trong hàng đợi, nếu không bộ đếm rò rỉ"
        );
    }

    #[tokio::test]
    async fn permit_is_returned_to_the_pool_on_drop() {
        let admission = ChatAdmission::new(1, 0, Duration::from_secs(1));

        {
            let _permit = admission.acquire().await.expect("nhận được");
            assert!(admission.acquire().await.is_err(), "đang đầy");
        }

        assert!(
            admission.acquire().await.is_ok(),
            "permit đã drop thì chỗ phải được trả lại"
        );
    }
}

