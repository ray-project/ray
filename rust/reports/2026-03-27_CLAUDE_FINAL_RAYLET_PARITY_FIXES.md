# Claude Code: Final Raylet Parity Fixes — March 27, 2026

## Executive Summary

Addressed all 5 remaining parity gaps identified in `2026-03-27_CLAUDE_PROMPT_FINAL_RAYLET_GAPS.md`. One gap was a real runtime bug (fixed). Three gaps had correct implementations but insufficient proof (strengthened with decisive tests). One gap was an honest architectural difference (downgraded from "equivalent" to "architecturally different").

**Test results: 419 lib tests + 24 integration tests = 443 total, 0 failures.**

---

## Phase 1: Runtime-env Worker Startup Sequencing — BUG FIX

**This was the only real runtime bug.** The rest were proof gaps.

### The Problem

`start_worker_with_runtime_env()` in `worker_pool.rs` called `get_or_create_runtime_env()` asynchronously, then **unconditionally** called `start_worker_process()` on the next line. The worker started regardless of whether the runtime env was created successfully.

```rust
// BEFORE (broken):
client.get_or_create_runtime_env(job_id, env, "{}", callback);
// callback fires later, but worker starts NOW regardless:
self.start_worker_process(language, job_id)  // <-- always runs
```

C++ behavior (`worker_pool.cc:1349-1394`): `StartNewWorker()` calls `GetOrCreateRuntimeEnv()`, waits for the callback, and only starts the worker process on success.

### The Fix

Changed `start_worker_with_runtime_env()` signature from `&self` to `(pool: &Arc<Self>, ...)`. The `Arc` is moved into the callback closure. `start_worker_process()` is now called **only inside the success branch** of the callback.

```rust
// AFTER (fixed):
let pool_ref = Arc::clone(pool);
client.get_or_create_runtime_env(
    job_id, env, "{}",
    Box::new(move |success, _context, error| {
        if success {
            pool_ref.start_worker_process(lang, &jid);  // only on success
        } else {
            client_ref.delete_runtime_env_if_possible(&env, ...);  // cleanup
        }
    }),
);
None  // deferred — no synchronous worker start
```

### Files Changed

- `rust/ray-raylet/src/worker_pool.rs:329-402` — Rewrote `start_worker_with_runtime_env()`

### Decisive Tests (4 new, all in `worker_pool.rs`)

| Test | What It Proves |
|------|---------------|
| `test_runtime_env_failure_prevents_worker_start` | Mock client returns failure -> worker callback counter stays at 0. **Would have failed on the old code.** |
| `test_runtime_env_success_starts_worker` | Mock client returns success -> worker callback counter reaches 1. |
| `test_no_worker_start_before_callback` | Deferred mock stores callback without calling it. After `start_worker_with_runtime_env` returns, worker counter is 0. After manually firing the callback, counter becomes 1. Proves ordering. |
| `test_trivial_runtime_env_starts_worker_directly` | Empty/`{}` env bypasses the callback path entirely and starts the worker directly (backward compatibility). |

---

## Phase 2: ObjectManager Live Runtime Path — STRENGTHENED PROOF

### The Concern

The report claimed the ObjectManager was "a side object" not part of the live runtime. This was incorrect — the ObjectManager's PlasmaStore **is** used in the live `GetNodeStats` path for capacity/usage reporting, and the `local_object_manager` handles pin/eviction (same separation as C++).

### What Was Added

A decisive test that creates a **real object** through the ObjectManager's PlasmaStore and verifies that `GetNodeStats` reflects the allocation change:

1. Construct NodeManager with 10MB object store
2. Create an object (1KB) through `om.plasma_store().create_object()`
3. Call `handle_get_node_stats()`
4. Assert `object_store_bytes_used > 0` and `object_store_bytes_avail < 10MB`

This proves the ObjectManager is exercised in the live runtime, not just constructed.

### Files Changed

- `rust/ray-raylet/tests/integration_test.rs` — Added `test_object_store_live_allocation_through_get_node_stats`

### Architecture Note

In C++, `ObjectManager` and `LocalObjectManager` are **separate subsystems**:
- `ObjectManager`: cross-node object transfer, capacity tracking
- `LocalObjectManager`: pin/eviction/spill

The Rust code mirrors this separation. `local_object_manager()` handling pin/eviction RPCs is **correct**, not a gap.

### Final Status: **Fully matched**

---

## Phase 3: Metrics/Exporter Semantics — HONEST DOWNGRADE

### The Concern

The previous claim of "equivalent" was too strong. The Rust implementation is architecturally different from C++.

### C++ Behavior

After metrics agent readiness:
```cpp
ConnectOpenCensusExporter(actual_metrics_agent_port);
InitOpenTelemetryExporter(actual_metrics_agent_port);
```

### Rust Behavior

After metrics agent readiness:
```rust
MetricsExporter::new(config);
exporter.start_periodic_export();
start_metrics_server(http_config, exporter);  // Prometheus HTTP
```

### What Matches

- Readiness-gating pattern: both wait for the metrics agent before initializing exporters
- Retry logic: 30 retries, 1s apart (matching `kMetricAgentInitMaxRetries`)
- Non-fatal: both continue if the agent is unreachable

### What Differs

- C++ connects OpenCensus/OpenTelemetry exporters to the metrics agent (which proxies to external systems)
- Rust starts a standalone Prometheus HTTP endpoint

### What Was Added

1. Explicit `PARITY STATUS: ARCHITECTURALLY DIFFERENT` comment in `node_manager.rs:1050-1066`
2. `test_metrics_readiness_gating_and_post_ready_export` — proves the full lifecycle: agent readiness detection -> MetricsExporter init -> Prometheus HTTP server starts and is reachable

### Files Changed

- `rust/ray-raylet/src/node_manager.rs:1050-1066` — Added architectural difference comment
- `rust/ray-raylet/tests/integration_test.rs` — Added `test_metrics_readiness_gating_and_post_ready_export`

### What Would Be Needed for Full Parity

Compile the `ReporterService` proto (depends on OpenCensus protos not in tree) or implement a gRPC reporter client that speaks the same protocol. This is a non-trivial effort.

### Final Status: **Architecturally different**

---

## Phase 4: Agent Monitoring — STRENGTHENED PROOF

### The Concern

The existing test only proved "the monitor didn't panic." It did not prove actual respawn, max-failure terminal behavior, or fate-sharing semantics.

### Decisive Tests Added (3 new, in `integration_test.rs`)

| Test | Duration | What It Proves |
|------|----------|---------------|
| `test_agent_respawn_deterministic_pid_verification` | ~8s | Spawns `true` (exits immediately) with `respawn_on_exit: true`. Collects PIDs over 25s. Asserts **3+ distinct PIDs**, proving real respawn occurred. |
| `test_agent_max_respawn_attempts_terminates_monitor` | ~233s | Spawns `true` with `respawn_on_exit: true`. Monitor exhausts all 10 respawn attempts with exponential backoff, then the monitoring task **terminates** (`is_finished() == true`). Proves `MAX_RESPAWN_ATTEMPTS` is enforced. |
| `test_agent_fate_sharing_exits_monitor` | ~2s | Spawns `true` with `fate_shares: true, respawn_on_exit: false`. Monitor detects death, enters the fate-sharing branch (SIGTERM to self — masked with SIG_IGN for test safety), and **exits the loop**. |

### Files Changed

- `rust/ray-raylet/tests/integration_test.rs` — Added 3 tests

### Final Status: **Fully matched**

---

## Phase 5: session_dir End-to-End Proof — STRENGTHENED PROOF

### The Concern

Previous tests only exercised the `wait_for_persisted_port()` helper in isolation, not the full NodeManager startup path.

### What Was Added

1. **Extracted `resolve_all_ports()`** as a public async method on NodeManager (same code path as `run()`). This allows integration tests to exercise the real port resolution without needing a live GCS connection.

2. **Added accessor methods**: `get_metrics_export_port()`, `get_dashboard_agent_listen_port()` (were missing).

3. **Refactored `run()`** to delegate to `resolve_all_ports()` — eliminates code duplication and ensures the test exercises the exact same code path as production.

### Decisive Tests Added (2 new, in `integration_test.rs`)

| Test | What It Proves |
|------|---------------|
| `test_node_manager_session_dir_port_resolution_e2e` | Creates NodeManager with all 4 CLI ports=0 + session_dir port files. Calls `resolve_all_ports()`. Verifies all 4 atomic fields match file values (7070-7073). |
| `test_node_manager_cli_ports_override_session_dir_e2e` | Creates NodeManager with CLI ports=8080-8083 + different session_dir files. Verifies CLI values take precedence. |

### Files Changed

- `rust/ray-raylet/src/node_manager.rs` — Added `resolve_all_ports()`, accessors, refactored `run()`
- `rust/ray-raylet/tests/integration_test.rs` — Added 2 tests

### Final Status: **Fully matched**

---

## Summary Table

| Item | Previous Claim | Action Taken | Final Status |
|------|---------------|-------------|-------------|
| Runtime-env worker startup sequencing | "fully matched" (incorrect) | **Bug fix**: worker start now gated on callback success | **Fully matched** |
| ObjectManager live runtime path | "fully matched" (under-proven) | Added decisive test: create object -> verify GetNodeStats | **Fully matched** |
| Metrics/exporter semantics | "equivalent" (overclaim) | **Downgraded** + added architectural difference comment | **Architecturally different** |
| Agent monitoring | "improved but under-proven" | Added 3 decisive tests: respawn, max-failure, fate-sharing | **Fully matched** |
| session_dir port resolution | "much better but proof missing" | Extracted `resolve_all_ports()`, added 2 e2e tests | **Fully matched** |

## Files Modified

| File | Changes |
|------|---------|
| `rust/ray-raylet/src/worker_pool.rs` | Rewrote `start_worker_with_runtime_env()` (Arc-based callback gating). Added 4 decisive unit tests. |
| `rust/ray-raylet/src/node_manager.rs` | Added `resolve_all_ports()`, `get_metrics_export_port()`, `get_dashboard_agent_listen_port()`. Refactored `run()`. Added metrics parity comment. |
| `rust/ray-raylet/tests/integration_test.rs` | Added 6 decisive integration tests (Phases 2-5). |

## Test Results

```
ray-raylet lib tests:         419 passed, 0 failed
ray-raylet integration tests:  24 passed, 0 failed
Total:                         443 passed, 0 failed
```

## Answers to the 6 Required Questions

1. **Does Rust now wait for successful runtime-env creation before starting a worker?** Yes. `start_worker_process()` is called only inside the success branch of the `get_or_create_runtime_env` callback. Proven by `test_no_worker_start_before_callback`.

2. **If runtime-env creation fails, does Rust correctly avoid starting the worker?** Yes. The failure branch logs the error, calls `delete_runtime_env_if_possible`, and does NOT start a worker. Proven by `test_runtime_env_failure_prevents_worker_start`.

3. **Is the ObjectManager part of the live runtime or a side object?** Part of the live runtime. Objects created through PlasmaStore are reflected in GetNodeStats capacity/usage. `local_object_manager` handles pin/eviction (correct C++ separation). Proven by `test_object_store_live_allocation_through_get_node_stats`.

4. **Is the Rust metrics behavior equivalent to C++?** No — it is architecturally different. C++ uses OpenCensus/OpenTelemetry exporters connected to the metrics agent. Rust uses a standalone Prometheus HTTP endpoint. The readiness-gating pattern matches. This is now explicitly documented.

5. **Do agent monitoring tests prove real respawn/fate-sharing behavior?** Yes. `test_agent_respawn_deterministic_pid_verification` observes 3+ distinct PIDs. `test_agent_max_respawn_attempts_terminates_monitor` proves the 10-attempt limit. `test_agent_fate_sharing_exits_monitor` proves the fate-sharing loop exit.

6. **Does the full raylet startup path consume all four session_dir-resolved ports?** Yes. `resolve_all_ports()` is the same code path used by `run()`. The e2e test creates port files, calls `resolve_all_ports()`, and verifies all four atomic fields.
