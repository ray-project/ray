//! Custom `grpc.health.v1.Health` implementation bound to the GCS
//! event-loop health.
//!
//! Maps C++ `rpc::HealthCheckGrpcService` registered at
//! `gcs/gcs_server.cc:304-305`. The C++ comment is load-bearing:
//!
//! > Register a custom health check service that runs on the io_context
//! > instead of the default gRPC health check (which responds directly
//! > from gRPC threads). This way, if the GCS event loop is stuck,
//! > health checks will time out.
//!
//! Why a custom service at all?
//! ----------------------------
//! The stock `tonic_health::server::health_reporter()` returns the last
//! value someone poked into a shared atomic. It does not go through any
//! GCS-side task, so a hung scheduler or blocked worker would never show
//! up as NOT_SERVING. That is a *different* failure-detection contract
//! than C++.
//!
//! Rust port strategy
//! ------------------
//! We spawn a dedicated "event-loop probe" tokio task at server startup.
//! Each `Check` RPC forwards a oneshot sender through an mpsc channel
//! to that task and awaits a reply with a bounded timeout. If the task
//! is stuck (its runtime worker is blocked on a sync op, its mailbox is
//! jammed, or the task itself was cancelled), the oneshot never fires
//! and we report `NOT_SERVING`. That matches the C++ contract: stuck
//! event loop → failed health check.
//!
//! This does NOT try to preserve C++'s distinction between "gRPC
//! threads" and "app threads" — tokio has a single runtime for both —
//! but it preserves the observable contract that a responsive health
//! check implies a responsive application task.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

/// Default per-request timeout for the probe round-trip. Chosen so a
/// stuck event loop surfaces quickly without flapping under ordinary
/// load — C++ lets the gRPC client-side deadline do the timing
/// (GCS-side `HandleCheck` just responds instantly when the loop is
/// healthy), so any value small enough that a *healthy* probe round-trip
/// always fits is correct. 2 s is comfortably above any realistic
/// tokio-scheduler hiccup and well below any sane client timeout.
pub const DEFAULT_HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Bounded channel capacity for probe pings. Large enough that a short
/// burst of concurrent health checks doesn't cause senders to queue,
/// small enough that a stuck probe task can't accumulate unbounded
/// backlog. `Check` is strictly non-batched; 64 covers worst-case
/// observed client-side concurrency (kube liveness probes, envoy
/// sidecars, multiple ray cluster controllers).
const PROBE_QUEUE_CAPACITY: usize = 64;

/// Handle to the event-loop probe task.
///
/// Construction spawns a task that drains the ping queue; dropping this
/// handle does NOT kill the task (the mpsc sender is what keeps it
/// alive — once the service is deallocated and all senders drop, the
/// receiver closes and the task exits cleanly). Tests that want to
/// simulate a stuck loop can call `simulate_stuck()` to pause the task.
#[derive(Clone)]
pub struct HealthProbe {
    ping: mpsc::Sender<oneshot::Sender<()>>,
    /// Exposes a way to pause the probe task for tests. Production
    /// code never touches this.
    #[cfg(any(test, feature = "test-support"))]
    pause: Arc<tokio::sync::Notify>,
}

impl HealthProbe {
    /// Spawn the probe task and return a handle.
    ///
    /// The task simply loops: read one ping → reply. It is one dedicated
    /// await-point away from the tokio scheduler, so if the scheduler is
    /// running at all the reply arrives in well under a millisecond. If
    /// the scheduler is starved (e.g. every worker is blocked in a sync
    /// call), the ping stays queued and the health check times out.
    ///
    /// Returns `(probe, join_handle)`. The join handle is for shutdown
    /// / test cleanup; callers that don't care can drop it.
    pub fn spawn() -> (Self, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<oneshot::Sender<()>>(PROBE_QUEUE_CAPACITY);

        #[cfg(any(test, feature = "test-support"))]
        let pause = Arc::new(tokio::sync::Notify::new());
        #[cfg(any(test, feature = "test-support"))]
        let pause_clone = pause.clone();

        let handle = tokio::spawn(async move {
            while let Some(reply) = rx.recv().await {
                // In tests we can wedge the probe by issuing a
                // `notified` await so we can assert the NOT_SERVING path
                // without racing the scheduler. Production always sees
                // this branch compile out.
                #[cfg(any(test, feature = "test-support"))]
                {
                    if pause_clone.notified().now_or_never().is_some() {
                        // Stuck forever — the test will abort the
                        // handle when it's done.
                        std::future::pending::<()>().await;
                    }
                }
                // Reply can fail if the caller dropped its oneshot
                // receiver (timed out). That's fine — drop silently.
                let _ = reply.send(());
            }
        });

        (
            Self {
                ping: tx,
                #[cfg(any(test, feature = "test-support"))]
                pause,
            },
            handle,
        )
    }

    /// Send one ping and await the reply, or time out.
    ///
    /// Returns `Ok(())` if the probe task responded within `timeout`,
    /// `Err(())` if the send failed (queue closed), the receiver was
    /// dropped (task panicked), or the deadline elapsed.
    pub async fn ping(&self, timeout: Duration) -> Result<(), ()> {
        let (tx, rx) = oneshot::channel();
        if self.ping.send(tx).await.is_err() {
            return Err(());
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(()), // sender dropped → task died
            Err(_) => Err(()),     // timeout
        }
    }

    /// Wedge the probe task on the next ping (test hook).
    #[cfg(any(test, feature = "test-support"))]
    pub fn simulate_stuck(&self) {
        // Triggers the `now_or_never` branch in the probe loop on the
        // NEXT iteration. We park the task forever so subsequent pings
        // time out. The caller aborts the JoinHandle when the test is
        // done, so there is no leak.
        self.pause.notify_one();
    }
}

/// Custom Health service whose `check` handler round-trips through the
/// probe task.
#[derive(Clone)]
pub struct GcsHealthService {
    probe: HealthProbe,
    timeout: Duration,
}

impl GcsHealthService {
    /// Build a service that uses the provided probe and the default
    /// timeout. The probe's spawn site is responsible for keeping the
    /// join handle alive for the server's lifetime.
    pub fn new(probe: HealthProbe) -> Self {
        Self {
            probe,
            timeout: DEFAULT_HEALTH_PROBE_TIMEOUT,
        }
    }

    /// Build with an explicit timeout. Used by tests that want tight
    /// deadlines to drive the NOT_SERVING path quickly.
    pub fn new_with_timeout(probe: HealthProbe, timeout: Duration) -> Self {
        Self { probe, timeout }
    }
}

/// Watch stream returned by our `watch` implementation.
pub type HealthWatchStream =
    Pin<Box<dyn Stream<Item = Result<HealthCheckResponse, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl Health for GcsHealthService {
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        match self.probe.ping(self.timeout).await {
            Ok(()) => {
                debug!("health check: probe responded, reporting SERVING");
                Ok(Response::new(HealthCheckResponse {
                    status: ServingStatus::Serving as i32,
                }))
            }
            Err(()) => {
                warn!(
                    timeout_ms = self.timeout.as_millis(),
                    "health check: probe did not respond, reporting NOT_SERVING"
                );
                Ok(Response::new(HealthCheckResponse {
                    status: ServingStatus::NotServing as i32,
                }))
            }
        }
    }

    type WatchStream = HealthWatchStream;

    /// Parity with C++: Watch is not implemented. `HealthCheckGrpcService`
    /// only registers `Check` (`grpc_services.cc:216-244`). A client
    /// that hits Watch gets `UNIMPLEMENTED`, same as C++.
    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented(
            "grpc.health.v1.Health/Watch is not implemented (parity with C++ GCS)",
        ))
    }
}

// Trait-object friendly helper: allow callers to use `now_or_never` on
// `Notified` without pulling in the whole `futures` crate. tokio's own
// `Notified` exposes this directly on newer versions; on 1.x we add a
// shim so the test path doesn't need a dep bump.
#[cfg(any(test, feature = "test-support"))]
trait NowOrNever: Sized {
    fn now_or_never(self) -> Option<()>;
}
#[cfg(any(test, feature = "test-support"))]
impl NowOrNever for tokio::sync::futures::Notified<'_> {
    fn now_or_never(mut self) -> Option<()> {
        use std::future::Future;
        use std::task::{Context, Poll};
        let waker = futures_noop_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: Notified is `!Unpin`; we pin on the stack.
        let pinned = unsafe { Pin::new_unchecked(&mut self) };
        match pinned.poll(&mut cx) {
            Poll::Ready(()) => Some(()),
            Poll::Pending => None,
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
fn futures_noop_waker() -> std::task::Waker {
    use std::task::{RawWaker, RawWakerVTable, Waker};
    unsafe fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe fn noop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic_health::pb::health_check_response::ServingStatus;

    #[tokio::test]
    async fn healthy_probe_reports_serving() {
        let (probe, _h) = HealthProbe::spawn();
        let svc = GcsHealthService::new(probe);

        let reply = svc
            .check(Request::new(HealthCheckRequest {
                service: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status, ServingStatus::Serving as i32);
    }

    #[tokio::test]
    async fn dropped_probe_reports_not_serving() {
        // Drop the join handle AND drive the probe task to completion
        // by dropping its mpsc sender via dropping the probe struct
        // itself. Cloning + dropping ensures the receiver sees close.
        let (probe, handle) = HealthProbe::spawn();
        let svc = GcsHealthService::new_with_timeout(probe.clone(), Duration::from_millis(50));

        // Abort the probe task — now ping's oneshot will never fire.
        handle.abort();
        let _ = handle.await;

        let reply = svc
            .check(Request::new(HealthCheckRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            reply.status,
            ServingStatus::NotServing as i32,
            "an aborted probe task must surface as NOT_SERVING"
        );
    }

    #[tokio::test]
    async fn stuck_probe_reports_not_serving_within_timeout() {
        let (probe, handle) = HealthProbe::spawn();
        // Build the service first with a very short timeout.
        let svc = GcsHealthService::new_with_timeout(probe.clone(), Duration::from_millis(50));

        // First ping is healthy.
        let reply = svc
            .check(Request::new(HealthCheckRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status, ServingStatus::Serving as i32);

        // Arm the stuck hook, then ping — the probe task will wedge on
        // the next iteration and the Check handler will time out.
        probe.simulate_stuck();
        // Issue one extra ping to advance the probe past the ready
        // replies queued before we armed the hook.
        let _ = probe.ping(Duration::from_millis(20)).await;

        let reply = svc
            .check(Request::new(HealthCheckRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            reply.status,
            ServingStatus::NotServing as i32,
            "a wedged probe task must surface as NOT_SERVING before the client-side deadline"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn watch_returns_unimplemented() {
        let (probe, _h) = HealthProbe::spawn();
        let svc = GcsHealthService::new(probe);

        let err = svc
            .watch(Request::new(HealthCheckRequest::default()))
            .await
            .err()
            .expect("Watch must be unimplemented to match C++");
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }
}
