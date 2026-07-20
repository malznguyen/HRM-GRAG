use std::{future::Future, io, time::Duration};

use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    Sigterm,
    Sigint,
}

impl ShutdownSignal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sigterm => "SIGTERM",
            Self::Sigint => "SIGINT",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ShutdownState {
    count: u64,
    first: Option<ShutdownSignal>,
    latest: Option<ShutdownSignal>,
}

#[derive(Clone)]
pub struct Shutdown {
    state: watch::Receiver<ShutdownState>,
}

impl Shutdown {
    /// Đăng ký bộ lắng nghe tín hiệu dùng chung cho toàn bộ vòng đời process.
    pub fn install() -> io::Result<Self> {
        let (sender, state) = watch::channel(ShutdownState::default());
        spawn_signal_monitor(sender)?;
        Ok(Self { state })
    }

    pub fn received(&self) -> bool {
        self.state.borrow().count >= 1
    }

    async fn wait_for_count(&mut self, count: u64) -> ShutdownSignal {
        loop {
            let state = *self.state.borrow_and_update();
            if state.count >= count {
                return if count == 1 {
                    state.first.expect("first shutdown signal must be recorded")
                } else {
                    state
                        .latest
                        .expect("latest shutdown signal must be recorded")
                };
            }

            self.state
                .changed()
                .await
                .expect("shutdown signal monitor stopped unexpectedly");
        }
    }
}

/// Chờ tín hiệu dừng đầu tiên từ SIGTERM hoặc SIGINT.
pub async fn shutdown_signal(mut shutdown: Shutdown) -> ShutdownSignal {
    shutdown.wait_for_count(1).await
}

/// Chờ tín hiệu dừng thứ hai để operator buộc process thoát ngay.
pub async fn second_shutdown_signal(mut shutdown: Shutdown) -> ShutdownSignal {
    shutdown.wait_for_count(2).await
}

#[derive(Debug, PartialEq, Eq)]
pub enum DrainOutcome<T> {
    Completed(T),
    DeadlineElapsed,
    SecondSignal(ShutdownSignal),
}

/// Chờ phần việc đang chạy hoàn tất, hết hạn drain, hoặc nhận tín hiệu thứ hai.
pub async fn drain_with_deadline<F>(
    in_flight: F,
    shutdown: Shutdown,
    deadline: Duration,
) -> DrainOutcome<F::Output>
where
    F: Future,
{
    tokio::pin!(in_flight);

    tokio::select! {
        output = &mut in_flight => DrainOutcome::Completed(output),
        _ = tokio::time::sleep(deadline) => DrainOutcome::DeadlineElapsed,
        signal = second_shutdown_signal(shutdown) => DrainOutcome::SecondSignal(signal),
    }
}

/// Hoàn tất phần việc đã nhận, trừ khi operator gửi tín hiệu dừng lần hai.
pub async fn drain_or_second_signal<F>(
    in_flight: F,
    shutdown: Shutdown,
) -> Result<F::Output, ShutdownSignal>
where
    F: Future,
{
    tokio::pin!(in_flight);

    tokio::select! {
        output = &mut in_flight => Ok(output),
        signal = second_shutdown_signal(shutdown) => Err(signal),
    }
}

#[cfg(unix)]
fn spawn_signal_monitor(sender: watch::Sender<ShutdownState>) -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;

    tokio::spawn(async move {
        loop {
            let received = select_shutdown_signal(
                async {
                    terminate.recv().await;
                },
                async {
                    interrupt.recv().await;
                },
            )
            .await;

            if !publish_signal(&sender, received) {
                break;
            }
        }
    });

    Ok(())
}

#[cfg(not(unix))]
fn spawn_signal_monitor(sender: watch::Sender<ShutdownState>) -> io::Result<()> {
    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_err() {
                break;
            }

            if !publish_signal(&sender, ShutdownSignal::Sigint) {
                break;
            }
        }
    });

    Ok(())
}

fn publish_signal(sender: &watch::Sender<ShutdownState>, signal: ShutdownSignal) -> bool {
    let mut state = *sender.borrow();
    state.count += 1;
    state.first.get_or_insert(signal);
    state.latest = Some(signal);
    sender.send(state).is_ok()
}

#[cfg(any(unix, test))]
async fn select_shutdown_signal<Terminate, Interrupt>(
    terminate: Terminate,
    interrupt: Interrupt,
) -> ShutdownSignal
where
    Terminate: Future<Output = ()>,
    Interrupt: Future<Output = ()>,
{
    tokio::pin!(terminate);
    tokio::pin!(interrupt);

    tokio::select! {
        _ = &mut terminate => ShutdownSignal::Sigterm,
        _ = &mut interrupt => ShutdownSignal::Sigint,
    }
}

#[cfg(test)]
mod tests {
    use std::future::{pending, ready};

    use super::*;

    fn test_shutdown() -> (watch::Sender<ShutdownState>, Shutdown) {
        let (sender, state) = watch::channel(ShutdownState::default());
        (sender, Shutdown { state })
    }

    #[tokio::test]
    async fn signal_future_resolves_on_sigterm() {
        let signal = select_shutdown_signal(ready(()), pending()).await;

        assert_eq!(signal, ShutdownSignal::Sigterm);
    }

    #[tokio::test]
    async fn signal_future_resolves_on_sigint() {
        let signal = select_shutdown_signal(pending(), ready(())).await;

        assert_eq!(signal, ShutdownSignal::Sigint);
    }

    #[tokio::test]
    async fn never_ending_drain_is_bounded_by_deadline() {
        let (_sender, shutdown) = test_shutdown();

        let outcome =
            drain_with_deadline(pending::<()>(), shutdown, Duration::from_millis(10)).await;

        assert_eq!(outcome, DrainOutcome::DeadlineElapsed);
    }

    #[tokio::test]
    async fn second_signal_short_circuits_drain() {
        let (sender, shutdown) = test_shutdown();
        assert!(publish_signal(&sender, ShutdownSignal::Sigterm));
        assert!(publish_signal(&sender, ShutdownSignal::Sigint));

        let outcome = drain_with_deadline(pending::<()>(), shutdown, Duration::from_secs(1)).await;

        assert_eq!(outcome, DrainOutcome::SecondSignal(ShutdownSignal::Sigint));
    }
}
