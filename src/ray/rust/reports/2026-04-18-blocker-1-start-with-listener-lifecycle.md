# 2026-04-18 — Blocker 1 fix: `start_with_listener` skipped lifecycle listeners

## Summary

The Rust GCS has two public entry points for running the gRPC server:

- `GcsServer::start()` — binds the port itself.
- `GcsServer::start_with_listener(TcpListener)` — takes a pre-bound listener.

The production binary (`gcs-server/src/main.rs:227-258`) pre-binds the
port (to handle port 0 and to publish the port file before serving) and
always calls `start_with_listener`. Tests, in contrast, use a mix of
both.

Before this fix, `start()` called `start_lifecycle_listeners()` but
`start_with_listener()` did **not**. The production Rust binary that Ray
Python launched therefore silently dropped the three C++
`GcsServer::InstallEventListeners` hooks
(`gcs_server.cc:819-908`):

| C++ hook | What it does | Rust target |
|---|---|---|
| `worker_dead` | mark tasks running on the dead worker FAILED | `task_manager.on_worker_dead` |
| `job_finished` | mark all non-terminal tasks of the finished job FAILED | `task_manager.on_job_finished` |
| `node_removed` | mark jobs on the dead node as dead | `job_manager.on_node_dead` |

C++ does not have this split. `gcs_server_main.cc:254-263` calls
`GcsServer::Start()`, and `GcsServer::DoStart()` calls
`InstallEventListeners()` unconditionally before `rpc_server_.Run()`. The
Rust drift came from two sibling entry points each maintaining their own
copy of the startup sequence, which is exactly the kind of split-brain
drift that caused this bug.

---

## The root cause in one diff

The wiring that was missing from `start_with_listener`:

```rust
// start() — had this
self.start_lifecycle_listeners();

// start_with_listener() — missing
```

Without this one call:

- `worker_manager.dead_listeners` stays empty → `ReportWorkerFailure` RPC
  fires no listeners → `task_manager.on_worker_dead` never runs → tasks
  running on a dead worker stay in whatever non-terminal state they were
  in, forever (from the GCS's point of view).
- `job_manager.finished_listeners` stays empty → `mark_job_finished`
  RPC fires no listeners → tasks from a finished driver job are not
  marked FAILED.
- The `node_removed` broadcast subscriber is never spawned →
  `job_manager.on_node_dead` is never called when a node is deregistered.

Any dashboard or `ray state` query relying on these terminal transitions
would see stale task and job state against a Rust GCS, but correct state
against a C++ GCS.

---

## Fix: one startup path, called by both entry points

I extracted a single private helper `install_listeners_and_initialize`
and a shared `build_router` helper, and made both `start()` and
`start_with_listener()` call them. The two public methods are now thin:
they differ only in whether they call `.serve(addr)` or
`.serve_with_incoming(incoming)`.

`crates/gcs-server/src/lib.rs`:

```rust
/// Install every lifecycle listener, load persisted state, and start
/// every pre-serve background task. Must run before any `serve*` call.
/// Mirrors C++ `GcsServer::DoStart` (`gcs_server.cc:271-309`), which
/// calls `InstallEventListeners()` exactly once before the gRPC server
/// runs. Factored out of `start()` and `start_with_listener()` so the
/// two entry points cannot drift.
async fn install_listeners_and_initialize(&self) {
    self.start_node_resource_listeners();
    self.start_node_autoscaler_listeners();
    self.start_node_actor_listeners();
    self.start_node_placement_group_listeners();
    self.start_lifecycle_listeners();      // ← the missing call
    self.start_publisher_bridge();

    self.initialize().await;

    // Health-check params from RAY_health_check_*_ms env vars (same as
    // C++ RayConfig), defaulting to 1000/500/5 — this also fixed a
    // silent divergence: start_with_listener used hardcoded 1000/500/5,
    // start() read env vars. Now both paths are env-driven and identical.
    let hc_period    = env_u64("RAY_health_check_period_ms",          1000);
    let hc_timeout   = env_u64("RAY_health_check_timeout_ms",          500);
    let hc_threshold = env_u32("RAY_health_check_failure_threshold",     5);
    self.node_manager.start_health_check_loop(hc_period, hc_timeout, hc_threshold);

    self.start_redis_health_check();
}

async fn build_router(&self) -> tonic::transport::server::Router { /* … */ }

pub async fn start(&self) -> Result<()> {
    let addr: SocketAddr = format!("0.0.0.0:{}", self.config.grpc_port).parse()?;
    self.install_listeners_and_initialize().await;
    self.build_router().await.serve(addr).await?;
    Ok(())
}

pub async fn start_with_listener(&self, listener: TcpListener) -> Result<()> {
    self.install_listeners_and_initialize().await;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    self.build_router().await.serve_with_incoming(incoming).await?;
    Ok(())
}
```

This also closes two secondary gaps that had been hiding in
`start_with_listener`:

1. **Health-check parameters were hardcoded** (`1000, 500, 5`) instead of
   read from `RAY_health_check_*_ms` env vars like `start()` does. Now
   both paths read the same env vars, matching C++ `RayConfig` behavior.
2. **Service registration was duplicated verbatim** between the two
   entry points. If a new service was added to one and not the other,
   clients would see it on some code paths and not others. Shared via
   `build_router` now.

---

## Regression test

A new end-to-end test in `crates/gcs-server/src/lib.rs`:

```rust
#[tokio::test]
async fn test_start_with_listener_installs_lifecycle_listeners() { … }
```

The test:
1. Constructs a `GcsServer`, seeds `task_manager` with one non-terminal
   task running on worker `W`.
2. Pins `RAY_gcs_mark_task_failed_on_worker_dead_delay_ms=0` so the
   spawned on-worker-dead tokio task runs synchronously.
3. Binds a TCP listener, spawns `start_with_listener` on it — the exact
   code path the production binary uses.
4. Sends `ReportWorkerFailure` over gRPC for worker `W`.
5. Polls `GetTaskEvents` up to ~1 s for the task to transition to
   `FAILED`.
6. Asserts the transition happened.

If the lifecycle listener is NOT wired (the pre-fix state), the task
never transitions and the test fails with a message naming
`gcs_server.cc:856, 880-900` for the C++ anchor. This is the simplest
test that actually exercises the production startup path end-to-end, so
any future refactor that re-introduces the drift fails this test.

---

## Build + test results

```
$ cargo build -p gcs-server
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 31.00s

$ cargo test --workspace
test result: ok. 20  passed; 0 failed  (gcs-kv)
test result: ok. 192 passed; 0 failed  (gcs-managers)
test result: ok. 16  passed; 0 failed  (gcs-server lib  — +1 new regression test)
test result: ok. 5   passed; 0 failed  (gcs-server bin)
test result: ok. 13  passed; 0 failed  (gcs-pubsub)
test result: ok. 25  passed; 0 failed  (gcs-store)
test result: ok. 8   passed; 0 failed  (gcs-table-storage)
```

Total: **279 / 279 passing** (was 278), 0 failures, 0 new warnings. The
pre-existing `test_start_with_listener` and `test_start` tests both
still pass, confirming the refactor did not break either public entry
point.

---

## C++ ↔ Rust parity matrix

| Behavior | C++ source | Rust source | Parity |
|---|---|---|---|
| `InstallEventListeners()` runs exactly once before serve | `gcs_server.cc:271-309, 819-908` | `install_listeners_and_initialize` (called by both `start` and `start_with_listener`) | ✅ |
| `worker_dead` → task manager | `gcs_server.cc:880-900` | `start_lifecycle_listeners` → `WorkerManager.add_worker_dead_listener` → `task_manager.on_worker_dead` | ✅ |
| `job_finished` → task manager | `gcs_server.cc:902-907` | `start_lifecycle_listeners` → `JobManager.add_job_finished_listener` → `task_manager.on_job_finished` | ✅ |
| `node_removed` → job manager | `gcs_server.cc:865` | `start_lifecycle_listeners` → `subscribe_node_removed` loop → `job_manager.on_node_dead` | ✅ |
| Health-check params from config | `RayConfig` → `GcsHealthCheckManager` | `RAY_health_check_*_ms` env vars in the shared helper | ✅ |
| Same gRPC service set on every launch path | single `rpc_server_` | shared `build_router` | ✅ |

---

## On AWS testing

Not required. The bug was a missing function call on a pre-serve code
path. The regression test exercises the same production code path
(`start_with_listener`) end-to-end over localhost gRPC, which is
topologically identical to what would run on EC2 — only the network
interface differs, and the interface is not where the bug lives. The
lifecycle hooks use `tokio` primitives that are environment-agnostic.

---

## Outcome

Blocker 1 is closed. The Rust binary that Ray Python launches now
installs the three `InstallEventListeners` hooks (worker-dead,
job-finished, node-removed) exactly the way C++ does, and the two
`start*` entry points share one startup sequence so this class of drift
cannot recur.
