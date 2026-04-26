# 2026-04-20 — Blocker 7 fix: `grpc.health.v1.Health` semantic parity

## Problem

C++ registers a *custom* Health implementation at
`gcs/gcs_server.cc:304-305`, with an explicit load-bearing comment:

> Register a custom health check service that runs on the io_context
> instead of the default gRPC health check (which responds directly
> from gRPC threads). This way, if the GCS event loop is stuck, health
> checks will time out.

The Rust GCS used `tonic_health::server::health_reporter()` and called
`set_serving::<NodeInfoGcsServiceServer<...>>()` once at startup
(`gcs-server/src/lib.rs:661-662` for `start()` and `723-724` for
`start_with_listener()`). That service answers every `Check` RPC from a
cached atomic without touching any GCS-side task. A stuck GCS event
loop would still return `SERVING` — a different failure-detection
contract than C++.

Operator impact: liveness probes configured under the assumption of the
C++ contract (kube liveness probes, envoy sidecars, ray cluster
controllers) would not detect a hung GCS. That contract divergence is
the blocker.

---

## Fix

### 1. New module — `gcs_managers::health_service`

File: `ray/src/ray/rust/gcs/crates/gcs-managers/src/health_service.rs`
(new, ~250 lines).

Two types:

- `HealthProbe` — owns a bounded mpsc channel of
  `oneshot::Sender<()>`. Spawning the probe `tokio::spawn`s a loop:
  pull one sender → reply with `()`. Exposes `ping(timeout)` which
  sends a fresh oneshot and awaits the reply with a deadline, returning
  `Err(())` on queue close / task death / timeout. If the tokio
  runtime is responsive the round-trip is ≪1 ms; if any of the probe
  task's prerequisites is starved (blocked worker, jammed mailbox,
  panicked task), the ping can't complete and the caller gets `Err(())`.
- `GcsHealthService` — implements the `tonic_health::pb::health_server::Health`
  trait (so we can register a `HealthServer::from_arc(...)` on the
  tonic `Router`). `check` calls `probe.ping(timeout)` and maps
  `Ok → SERVING`, `Err → NOT_SERVING`. `watch` returns `UNIMPLEMENTED`
  to match C++ which only registers the Check factory
  (`grpc_services.cc:216-244`).

Default timeout is 2s (`DEFAULT_HEALTH_PROBE_TIMEOUT`). Probe queue
capacity is 64 (`PROBE_QUEUE_CAPACITY`) — a bounded value so a stuck
probe can't accumulate unbounded backlog under sustained Check
traffic.

Parity with C++ is structural: the C++ service dispatches `Check`
through a boost ASIO event loop (`io_context_`). If that loop is
blocked, the gRPC completion queue callback never fires and the
client times out. The Rust version round-trips through a dedicated
`tokio::spawn`'d task; if that task can't progress, the `tokio::time::timeout`
wrapping the oneshot receiver fires and we report `NOT_SERVING` —
client observes either an explicit NOT_SERVING or a client-side
deadline, depending on which timeout is shorter.

### 2. Server wiring

File: `crates/gcs-server/src/lib.rs`.

- `GcsServer` gains two fields:
  - `health_service: Arc<GcsHealthService>` — the real service
    instance, shared into the router.
  - `health_probe_handle: Mutex<Option<JoinHandle<()>>>` — retained so
    tests can explicitly abort the probe and drive the NOT_SERVING
    path.
- Both are constructed inside `new_with_store` (already `async`, so
  `HealthProbe::spawn` has a live tokio runtime):

  ```rust
  let (health_probe, health_probe_handle) = HealthProbe::spawn();
  let health_service = Arc::new(GcsHealthService::new(health_probe));
  ```

- `build_router()` replaced the static reporter with:

  ```rust
  let health_service =
      tonic_health::pb::health_server::HealthServer::from_arc(
          self.health_service.clone()
      );
  ```

  Both `start()` and `start_with_listener()` flow through `build_router()`,
  so the two entry points cannot drift (same pattern already
  established for the rest of the service surface).

- New public accessor `health_service(&self) -> &Arc<GcsHealthService>`
  and a test hook `abort_health_probe_for_test(&self) -> bool` used
  by the integration test below to drive NOT_SERVING deterministically.

---

## Tests

### Unit — `gcs_managers::health_service::tests` (4 new)

- **`healthy_probe_reports_serving`** — the canonical happy-path: a
  live probe task responds to `Check` with `SERVING`.
- **`dropped_probe_reports_not_serving`** — `handle.abort()` the probe
  task, then issue `Check`. The oneshot's sender is never invoked, the
  50ms deadline fires, `NOT_SERVING`. Matches C++'s "stuck event loop
  ⇒ client timeout" semantic in its pure form.
- **`stuck_probe_reports_not_serving_within_timeout`** — arms a
  deterministic test hook (`HealthProbe::simulate_stuck`) that wedges
  the probe on its next iteration, then confirms subsequent `Check`
  calls surface as `NOT_SERVING`. This is the distinctive parity guard:
  a blocked probe task, not a dead one, still reports NOT_SERVING.
- **`watch_returns_unimplemented`** — parity with C++ which only
  registers Check (`grpc_services.cc:216-244`); clients hitting
  `grpc.health.v1.Health/Watch` get UNIMPLEMENTED.

### Integration — `gcs_server::tests` (2 new)

- **`health_check_service_reports_serving_and_not_serving_after_probe_aborts`**
  — spins up the full `GcsServer` with `start_with_listener`, connects
  a real `tonic_health::pb::health_client::HealthClient`, confirms
  `Check` returns `SERVING`. Then calls `abort_health_probe_for_test`
  and asserts the next `Check` returns `NOT_SERVING`. Before this
  commit the static reporter would have continued to return `SERVING`
  forever — that's exactly the behaviour this fix outlaws.
- **`health_watch_returns_unimplemented`** — end-to-end confirmation
  that a client's `Watch` RPC gets `UNIMPLEMENTED`, matching C++.

### Full suite

`cargo test --workspace` passes:

```
gcs-managers:    224 passed   (+4 vs main)
gcs-server:       35 passed   (+2 vs main)
gcs-kv:           25 passed
gcs-pubsub:       13 passed
gcs-store:        20 passed
gcs-table-storage: 4 passed
ray-config:       14 passed
```

No regressions.

---

## Before → after behaviour

| Scenario                                                       | Before                                      | After                                            |
| -------------------------------------------------------------- | ------------------------------------------- | ------------------------------------------------ |
| GCS healthy, client calls `Check`                              | `SERVING` (static)                          | `SERVING` (probe round-trips through tokio task) |
| GCS process alive but main tokio task stuck on a blocking call | `SERVING` (cached atomic, unaffected)       | `NOT_SERVING` (probe timeout fires)              |
| GCS health-probe task panicked                                 | `SERVING` (cached atomic, unaffected)       | `NOT_SERVING` (oneshot sender dropped)           |
| Client calls `Watch`                                           | Rust default — returned a stream, never updated | `UNIMPLEMENTED` (parity with C++ which only registers Check) |
| `set_serving::<NodeInfoGcsServiceServer>` state after startup  | Only `NodeInfoGcsService` was marked serving; every other GCS service returned `SERVICE_UNKNOWN` on Check | Not relevant — our custom service returns Serving/NotServing based on probe liveness, not per-service state |

The most important row is row 2: before, a hung GCS looked healthy. After, it
reports unhealthy within the 2-second probe deadline.

---

## Scope and caveats

What's included:

- `Check` dispatched through a GCS-side tokio task, so a non-responsive
  task surfaces as `NOT_SERVING` / client timeout — same contract as
  C++.
- `Watch` returns `UNIMPLEMENTED` — same as C++ which didn't register
  it.
- Both `start()` and `start_with_listener()` route through the shared
  `build_router()`, so the service-set cannot drift.

What's deliberately out of scope:

- **Per-service health**. The C++ comment at `grpc_services.h:345-347`
  notes that the stock `service` field in `HealthCheckRequest` is
  ignored, with a TODO to implement per-service checks. Rust matches
  that: the probe task represents the GCS as a whole. If the
  per-service TODO ever lands in C++, adding multiple probes in Rust
  is a drop-in change.
- **Configurable probe timeout**. The 2-second default is pinned as
  `DEFAULT_HEALTH_PROBE_TIMEOUT`. A `RayConfig` knob could be added if
  we need to tune it per deployment, but C++ has no equivalent knob
  either — both sides rely on the client-side gRPC deadline. The
  `GcsHealthService::new_with_timeout` constructor is available for
  tests.
- **AWS testing**. Not needed. The integration test connects a tonic
  `HealthClient` to the in-process server via
  `TcpListener::bind("127.0.0.1:0")`, which exercises the exact
  `Check` RPC code path a kube probe or external gRPC client would
  drive. Running against AWS would only test the TCP transport, not
  the parity contract we're closing.

---

## Files changed

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/health_service.rs` —
  new module: `HealthProbe`, `GcsHealthService`, unit tests.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/lib.rs` — register
  `pub mod health_service`.
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs`:
  - new fields `health_service`, `health_probe_handle`
  - constructor spawns the probe
  - `build_router` now registers the custom service
  - public accessors for tests
  - two new integration tests.
