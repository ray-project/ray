# Claude Corrective Pass — Response to Codex Re-Audit

Date: 2026-03-28
Branch: `cc-to-rust-experimental`
Subject: Address all recommendations from `2026-03-27_CODEX_REAUDIT_CLAUDE_FINAL_RAYLET_PARITY_FIXES.md`

## Summary of Changes

Three items fixed, one honestly downgraded.

**423 lib tests pass, 0 failures.**

---

## Finding 1 (HIGH): Runtime-env-gated worker start now wired into production path

### Problem

The fixed `start_worker_with_runtime_env()` was only called from tests. No production call site used it. The `handle_prestart_workers` RPC handler called `start_worker_process()` directly, bypassing runtime-env gating.

### Fix

1. **Added `pop_or_start_worker_with_runtime_env()`** — a new production API on `WorkerPool` that combines `pop_worker()` + runtime-env-gated worker start. This is the Rust equivalent of C++ `PopWorker()` in `worker_pool.cc:1349-1394`.

   - If an idle worker exists, returns it immediately (no env creation needed).
   - If a new worker is needed AND `serialized_runtime_env` is non-trivial, calls `get_or_create_runtime_env()` and defers worker start to the success callback.
   - If env is trivial (`""` or `"{}"`), starts the worker directly.
   - Accepts an `on_failure` callback for `RuntimeEnvCreationFailed` propagation.

   Source: `worker_pool.rs:591-697`

2. **Wired `handle_prestart_workers` to use the new API** — the `PrestartWorkers` RPC handler now:
   - Extracts `runtime_env_info.serialized_runtime_env` from the proto request
   - Calls `handle_job_started_with_runtime_env()` when env is non-trivial
   - Uses `pop_or_start_worker_with_runtime_env()` instead of `start_worker_process()`
   - Passes an `on_failure` callback that logs `RuntimeEnvCreationFailed`

   Source: `grpc_service.rs:310-370`

### Decisive Tests (4 new)

| Test | What It Proves |
|------|---------------|
| `test_pop_or_start_with_runtime_env_success_starts_worker` | Production API starts worker after successful env creation |
| `test_pop_or_start_with_runtime_env_failure_propagates_status` | Production API invokes `on_failure(RuntimeEnvCreationFailed)` on env failure, does NOT start worker, cleans up env |
| `test_pop_or_start_with_trivial_env_starts_directly` | Empty/`{}` env uses direct start (backward compat) |
| `test_pop_or_start_with_runtime_env_pops_idle_first` | Idle workers are returned without env creation (correct optimization) |

---

## Finding 2 (HIGH): RuntimeEnvCreationFailed propagation implemented

### Problem

C++ propagates `PopWorkerStatus::RuntimeEnvCreationFailed` through the worker-start flow. Rust had the enum variant but never set it.

### Fix

`pop_or_start_worker_with_runtime_env()` accepts an `on_failure: Option<Box<dyn FnOnce(PopWorkerStatus) + Send>>` callback. When `get_or_create_runtime_env` returns failure, the callback is invoked with `PopWorkerStatus::RuntimeEnvCreationFailed`.

The `handle_prestart_workers` RPC handler passes a concrete `on_failure` that logs the failure status.

Source: `worker_pool.rs:660-696` (internal callback), `grpc_service.rs:356-362` (production call site)

### Decisive Test

`test_pop_or_start_with_runtime_env_failure_propagates_status` — asserts the `on_failure` callback receives `PopWorkerStatus::RuntimeEnvCreationFailed`, worker counter stays at 0, and env cleanup occurs.

---

## Finding 3 (HIGH): Object-store claims explicitly downgraded to PARTIALLY MATCHED

### Problem

The ObjectManager/PlasmaStore is used for stats reporting in `GetNodeStats`, but the actual object-management RPC handling (PinObjectIDs, FreeObjectsInObjectStore) routes through `local_object_manager()` — an in-memory pin/eviction tracker not backed by PlasmaStore.

### Action

Explicitly downgraded with `PARITY STATUS: PARTIALLY MATCHED` comments:

1. **`grpc_service.rs`** (near `handle_pin_object_ids`): Documents that pin/eviction RPCs use `local_object_manager`, not the ObjectManager/PlasmaStore. Lists steps needed for full parity.

2. **`node_manager.rs`** (near `object_manager()` accessor): Documents that ObjectManager is a stats sidecar, not the live-object runtime.

3. **`integration_test.rs`**: Updated test comments to clarify they prove stats-path integration, not full live-path parity.

### What Would Be Needed for Full Parity

- PinObjectIDs: retrieve objects from PlasmaStore, hold allocation handles, release on owner-death
- FreeObjectsInObjectStore: delete objects from PlasmaStore
- SpillObjects/RestoreSpilledObjects: integrate with PlasmaStore allocation

---

## Finding 4 (MEDIUM): Metrics row kept at ARCHITECTURALLY DIFFERENT

No change from previous pass. The honest downgrade stands:
- C++: `ConnectOpenCensusExporter` + `InitOpenTelemetryExporter`
- Rust: `MetricsExporter` + Prometheus HTTP server
- Readiness-gating pattern matches; export protocol differs

---

## Finding 5 (MEDIUM): session_dir — already closed

No additional changes. Previous e2e tests confirmed.

---

## Finding 6 (MEDIUM): Agent monitoring — already closed

No additional changes. Previous decisive tests confirmed.

---

## Updated Status Table

| Item | Previous Status | Action | New Status |
|------|----------------|--------|-----------|
| Runtime-env sequencing (helper) | Fixed but test-only | Wired into `handle_prestart_workers` via `pop_or_start_worker_with_runtime_env` | **Fully matched** (production path) |
| RuntimeEnvCreationFailed propagation | Enum existed, never set | `on_failure` callback in production API | **Fully matched** |
| Object-store live runtime | Claimed "fully matched" | Explicitly downgraded with PARTIALLY MATCHED comments | **Partially matched** (stats-only) |
| Metrics/exporter | Architecturally different | No change | **Architecturally different** |
| session_dir | Closed | No change | **Closed** |
| Agent monitoring | Closed | No change | **Closed** |

## Test Results

```
ray-raylet lib tests:  423 passed, 0 failed  (+4 new production-path tests)
```

## Files Modified

| File | Changes |
|------|---------|
| `worker_pool.rs` | Added `pop_or_start_worker_with_runtime_env()`, `start_worker_with_runtime_env_and_callback()`. Added 4 decisive tests. |
| `grpc_service.rs` | Rewired `handle_prestart_workers` to extract runtime_env_info and use `pop_or_start_worker_with_runtime_env`. Added `PARTIALLY MATCHED` comment on pin handler. |
| `node_manager.rs` | Added `PARTIALLY MATCHED` comment on `object_manager()` accessor. |
| `integration_test.rs` | Updated object-store test comments to clarify stats-only scope. |
