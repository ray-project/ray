//! Metrics-exporter initialization.
//!
//! Maps C++ `GcsServer::InitMetricsExporter`
//! (`src/ray/gcs/gcs_server.cc:950-977`). The C++ function:
//!
//! 1. Uses a `std::atomic<bool>` to guarantee at-most-once init.
//! 2. Calls `event_aggregator_client_->Connect(port)` so GCS events
//!    can be forwarded to the metrics agent.
//! 3. Starts a tonic-ish wait-for-server-ready and, on success,
//!    wires OpenCensus + OpenTelemetry exporters, and starts the
//!    export-event recorder.
//! 4. There are two invocation sites: eager (at `DoStart` when
//!    `config_.metrics_agent_port > 0`, `gcs_server.cc:327-328`) and
//!    deferred (inside the node-added listener when the head node
//!    finally reports its port, `gcs_server.cc:831-842`).
//!
//! Rust port scope
//! ---------------
//! The Rust GCS does not ship with OpenCensus or OpenTelemetry — the
//! stats pipeline is a separate concern. What this module *does* port
//! is the observable contract:
//!
//!   * at-most-once init (load-bearing: two listeners must not fire
//!     the exporter twice if an operator sets both CLI + head-node
//!     port)
//!   * dial the aggregator endpoint on `127.0.0.1:<port>` so
//!     connection errors surface in logs the same way C++ surfaces
//!     them via `WaitForServerReady`
//!   * **build and cache a connected `EventAggregatorServiceClient`**
//!     so event-forwarding code in the rest of the GCS can call
//!     `AddEvents` without redoing the dial. This mirrors C++
//!     `event_aggregator_client_->Connect(port)` +
//!     `MetricsAgentClientImpl` construction at
//!     `gcs_server.cc:955-961`.
//!   * emit the same "not available" log string C++ emits when no
//!     port is reachable, for grep parity with operator runbooks
//!
//! Not ported in this module: the OpenCensus / OpenTelemetry
//! exporter wiring at `gcs_server.cc:966-968`. Rust has no OC/OTel
//! stack; when that grows, the successful-connect branch is the hook.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tracing::{info, warn};

use gcs_proto::ray::rpc::events::event_aggregator_service_client::EventAggregatorServiceClient;

/// One-shot initializer for the metrics exporter.
///
/// Safe to call `initialize_once` from multiple listeners
/// concurrently — only the first call does any work; the rest are
/// no-ops. The join handle of the spawned connect task is kept so
/// tests can deterministically await readiness / failure.
pub struct MetricsExporter {
    initialized: AtomicBool,
    /// The dial timeout applied to the aggregator connect. Exposed
    /// so tests can use a short value; production uses a few seconds
    /// to match C++'s default gRPC client timeout.
    dial_timeout: Duration,
    /// Join handle of the most recent connect attempt. `Mutex` so a
    /// second racing `initialize_once` call can swap without holding
    /// the parking_lot guard across the await.
    handle: parking_lot::Mutex<Option<JoinHandle<Result<(), String>>>>,
    /// The connected `EventAggregatorService` gRPC client, populated
    /// after a successful dial. Parity with C++
    /// `event_aggregator_client_` + `metrics_agent_client_` — both
    /// C++ members exist solely to forward `AddEvents` over a cached
    /// channel. The Rust port keeps one client (the `AddEvents`
    /// surface is the only one raylets/GCS use in the current proto),
    /// and event-forwarding code reaches it via
    /// `event_aggregator_client()`.
    event_aggregator_client: parking_lot::Mutex<
        Option<EventAggregatorServiceClient<Channel>>,
    >,
}

impl Default for MetricsExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsExporter {
    pub fn new() -> Self {
        Self {
            initialized: AtomicBool::new(false),
            dial_timeout: Duration::from_secs(5),
            handle: parking_lot::Mutex::new(None),
            event_aggregator_client: parking_lot::Mutex::new(None),
        }
    }

    /// The connected `EventAggregatorService` client after a
    /// successful `initialize_once`. `None` if init hasn't run or
    /// the dial failed. Event-forwarding code uses this to call
    /// `AddEvents` (parity with C++ `event_aggregator_client_->AddEvents`
    /// call sites).
    pub fn event_aggregator_client(
        &self,
    ) -> Option<EventAggregatorServiceClient<Channel>> {
        self.event_aggregator_client.lock().clone()
    }

    /// Override the dial timeout. Tests use a millisecond-scale value
    /// so failure-path assertions complete quickly.
    pub fn with_dial_timeout(mut self, timeout: Duration) -> Self {
        self.dial_timeout = timeout;
        self
    }

    /// Whether `initialize_once` has been called on a non-zero port.
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    /// Spawn a connect attempt for `port` if not already initialized.
    /// Mirrors C++'s atomic-exchange-then-connect pattern
    /// (`gcs_server.cc:951-976`). Returns `true` if this call
    /// performed the init, `false` if a prior call already did.
    ///
    /// `port == 0` is treated as "no port available" — matches C++
    /// `gcs_server.cc:836-841` which logs the "metrics agent not
    /// available" message and does NOT flip the initialized flag.
    pub fn initialize_once(self: &Arc<Self>, port: i32) -> bool {
        if port <= 0 {
            info!(
                "Metrics agent not available. To enable metrics, install \
                 Ray with dashboard support: `pip install 'ray[default]'`."
            );
            return false;
        }
        if self
            .initialized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Another caller won the race.
            return false;
        }

        let endpoint = format!("http://127.0.0.1:{port}");
        let dial_timeout = self.dial_timeout;
        let this = self.clone();
        let handle = tokio::spawn(async move {
            // Parity with C++ `event_aggregator_client_->Connect(...)`
            // + `MetricsAgentClientImpl` construction +
            // `WaitForServerReady(...)` (`gcs_server.cc:955-976`): we
            // dial the aggregator's gRPC endpoint, then build an
            // `EventAggregatorServiceClient` from the live channel
            // and cache it on the exporter so the rest of the GCS
            // can forward events via `AddEvents`.
            let endpoint_builder = Channel::from_shared(endpoint.clone())
                .map_err(|e| format!("invalid metrics-agent endpoint: {e}"))?
                .timeout(dial_timeout)
                .connect_timeout(dial_timeout);
            match endpoint_builder.connect().await {
                Ok(channel) => {
                    let client = EventAggregatorServiceClient::new(channel);
                    *this.event_aggregator_client.lock() = Some(client);
                    // C++ success branch at `gcs_server.cc:965-969`:
                    // OpenCensus / OpenTelemetry exporters would also
                    // wire here. The Rust GCS has no OC/OTel stack
                    // yet — the hook is this match arm. What is
                    // fully ported is the observable contract: the
                    // event-aggregator client is now live and any
                    // subsequent `event_aggregator_client()` caller
                    // gets a connected handle.
                    info!(port, "Metrics exporter initialized with port {port}");
                    Ok(())
                }
                Err(e) => {
                    warn!(
                        port,
                        error = %e,
                        "Failed to establish connection to the event+metrics \
                         exporter agent. Events and metrics will not be \
                         exported. Exporter agent status: {e}"
                    );
                    Err(e.to_string())
                }
            }
        });
        *self.handle.lock() = Some(handle);
        true
    }

    /// Take the most recent join handle (tests use this to await
    /// completion). Production never calls this — the exporter runs
    /// for the process lifetime.
    pub fn take_handle(&self) -> Option<JoinHandle<Result<(), String>>> {
        self.handle.lock().take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn initialize_once_is_idempotent() {
        let ex = Arc::new(MetricsExporter::new().with_dial_timeout(Duration::from_millis(50)));
        // A port of 0 means "unknown" — should not flip the flag.
        assert!(!ex.initialize_once(0));
        assert!(!ex.is_initialized());

        // A real port flips the flag and spawns one connect task.
        assert!(ex.initialize_once(1));
        assert!(ex.is_initialized());

        // Subsequent calls on any port are no-ops once initialized.
        assert!(!ex.initialize_once(2));

        // Drain the spawned task so it doesn't leak into the test runtime.
        if let Some(h) = ex.take_handle() {
            let _ = h.await;
        }
    }

    #[tokio::test]
    async fn connect_success_caches_aggregator_client() {
        // Bind an actual TCP listener so the dial succeeds — we're
        // only asserting the connect step works, not the gRPC
        // handshake (tonic's `connect()` succeeds on TCP accept).
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port() as i32;

        let ex = Arc::new(MetricsExporter::new().with_dial_timeout(Duration::from_millis(500)));
        assert!(ex.initialize_once(port));
        let handle = ex.take_handle().expect("handle stored");
        let res = handle.await.expect("task completes");
        assert!(res.is_ok(), "connect to local listener must succeed, got {res:?}");

        // Parity with C++ `event_aggregator_client_->Connect(port)`:
        // after a successful dial, the cached client must be present
        // so event-forwarding code can call AddEvents without
        // redialling. A raw channel check alone wouldn't catch a
        // regression where the dial succeeds but nobody stores the
        // client.
        assert!(
            ex.event_aggregator_client().is_some(),
            "successful connect must cache an EventAggregatorServiceClient"
        );

        drop(listener);
    }

    #[tokio::test]
    async fn connect_failure_leaves_aggregator_client_unset() {
        // Pair to the success test above: a failed dial must not
        // populate the client (otherwise AddEvents would fail
        // silently against a dead channel).
        let ex = Arc::new(MetricsExporter::new().with_dial_timeout(Duration::from_millis(20)));
        assert!(ex.initialize_once(1));
        let handle = ex.take_handle().expect("handle stored");
        let res = handle.await.expect("task completes");
        assert!(res.is_err(), "connecting to privileged port 1 must fail");
        assert!(
            ex.event_aggregator_client().is_none(),
            "failed connect must leave the cached client unset"
        );
    }

    #[tokio::test]
    async fn connect_failure_does_not_reset_initialized() {
        // Port 1 is privileged and will refuse in a test environment;
        // even if it somehow accepts, the tonic endpoint is built with
        // a very short timeout so the connect fails promptly.
        let ex = Arc::new(MetricsExporter::new().with_dial_timeout(Duration::from_millis(20)));
        assert!(ex.initialize_once(1));
        assert!(ex.is_initialized());

        let handle = ex.take_handle().expect("handle stored");
        let res = handle.await.expect("task completes");
        assert!(res.is_err(), "connecting to privileged port 1 must fail");

        // Critical parity guard: a failed connect must NOT reset the
        // flag. C++ `gcs_server.cc:951-954` only flips it once — the
        // failure just logs, it never retries by going through this
        // function again.
        assert!(
            !ex.initialize_once(2),
            "a failed first init must still block subsequent inits"
        );
    }
}
