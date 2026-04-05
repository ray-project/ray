# Claude Session Report: IO-Worker Subsystem Implementation

**Date:** 2026-04-01
**Branch:** `cc-to-rust-experimental`
**For:** Codex re-audit

---

## Context

All prior Codex audit rounds (V1-V10) identified the IO-worker subsystem as the largest remaining parity gap. The Rust raylet had `WorkerType` enum variants and `--worker-type` flag emission but no actual pool management, no `TryStartIOWorkers`, and no `LocalObjectManager` integration. This session implements the full subsystem.

---

## What Was Implemented

### 1. Config (`rust/ray-common/src/config.rs`)

Added two new fields to `RayConfig`:
- `object_spilling_config: String` — JSON config passed to IO workers (default: "")
- `max_io_workers: i32` — max concurrent IO workers per type (default: 4)

Both support JSON parsing and `RAY_` environment variable overrides.

### 2. Worker Spawner (`rust/ray-raylet/src/worker_spawner.rs`)

- Added `object_spilling_config: String` to `WorkerSpawnerConfig`
- Replaced all 3 TODO comments with actual `--object-spilling-config=<base64>` emission via `append_object_spilling_config()` helper
- IO workers (spill/restore/delete) now receive the base64-encoded spilling config in their argv, matching C++ `worker_pool.cc:355-360`

### 3. Worker Pool — IO Worker State (`rust/ray-raylet/src/worker_pool.rs`)

This is the core change. Added the full C++ `IOWorkerState` equivalent:

**New types:**
- `IOWorkerCallback` — `Box<dyn FnOnce(WorkerID) + Send + 'static>`
- `IOWorkerState` — per-worker-type state with `idle_io_workers`, `pending_io_tasks` (callback queue), `started_io_workers`, `num_starting_io_workers`
- `LanguageIOState` — groups spill and restore `IOWorkerState` per language
- `IOWorkerPool` trait — 6-method interface for `LocalObjectManager` integration

**New WorkerPool fields:**
- `max_io_workers: i32`
- `io_states: Mutex<HashMap<Language, LanguageIOState>>`

**New public APIs (matching C++ worker_pool.cc:995-1084):**

| Method | C++ Equivalent | Behavior |
|--------|---------------|----------|
| `pop_spill_worker(callback)` | `PopSpillWorker` | Return idle worker or queue callback |
| `push_spill_worker(worker_id)` | `PushSpillWorker` | Return worker to pool, drain pending |
| `pop_restore_worker(callback)` | `PopRestoreWorker` | Return idle worker or queue callback |
| `push_restore_worker(worker_id)` | `PushRestoreWorker` | Return worker to pool, drain pending |
| `pop_delete_worker(callback)` | `PopDeleteWorker` | Pick from pool with more idle workers |
| `push_delete_worker(worker_id)` | `PushDeleteWorker` | Route to actual worker type's pool |

**Private helpers:**
- `pop_io_worker_internal` — idle → callback immediately; else queue + `try_start_io_workers`
- `push_io_worker_internal` — if pending queue empty → add to idle; else pop callback and invoke
- `try_start_io_workers` — spawn on demand up to `max_io_workers` (matching C++ `TryStartIOWorkers` at worker_pool.cc:1757-1786)
- `get_io_worker_state` — select spill or restore state by worker type

**Registration/disconnect updates:**
- `register_worker()` — IO workers added to `started_io_workers`, `num_starting_io_workers` decremented, triggers `try_start_io_workers` for pending
- `disconnect_worker()` — IO workers removed from `started_io_workers` and `idle_io_workers`

**Concurrency:** Callbacks invoked outside lock to prevent deadlocks (callback may call back into WorkerPool).

### 4. LocalObjectManager Integration (`rust/ray-raylet/src/local_object_manager.rs`)

- Added `io_worker_pool: Option<Arc<dyn IOWorkerPool>>` field
- Added `set_io_worker_pool()` method
- Added `spill_objects()` — selects objects via `select_objects_to_spill()`, marks pending, pops spill worker
- Added `restore_object(object_id, url)` — pops restore worker
- Added `delete_spilled_objects(urls)` — pops delete worker

IO worker RPC dispatch (SpillObjects/RestoreSpilledObjects/DeleteSpilledObjects) is fully implemented — see Section 6 below.

### 5. NodeManager Wiring (`rust/ray-raylet/src/node_manager.rs`)

- `WorkerPool::new()` now receives `config.ray_config.max_io_workers`
- `WorkerSpawnerConfig` now receives `config.ray_config.object_spilling_config`
- `LocalObjectManager` receives the `WorkerPool` as `Arc<dyn IOWorkerPool>` via `set_io_worker_pool()`

---

## C++ Parity Mapping

| C++ Component | Rust Equivalent | Status |
|--------------|-----------------|--------|
| `IOWorkerState` struct | `IOWorkerState` + `LanguageIOState` | **Implemented** |
| `PopSpillWorker` / `PushSpillWorker` | `pop_spill_worker` / `push_spill_worker` | **Implemented** |
| `PopRestoreWorker` / `PushRestoreWorker` | `pop_restore_worker` / `push_restore_worker` | **Implemented** |
| `PopDeleteWorker` (smart routing) | `pop_delete_worker` (picks larger pool) | **Implemented** |
| `PushDeleteWorker` (type-based routing) | `push_delete_worker` (routes by actual type) | **Implemented** |
| `TryStartIOWorkers` (worker_pool.cc:1757-1786) | `try_start_io_workers` (worker_pool.rs:1077-1116) | **Implemented** — see detailed mapping below |
| `OnWorkerStarted` (IO worker registration) | `register_worker` IO branch | **Implemented** |
| Worker disconnect cleanup | `disconnect_worker` IO branch | **Implemented** |
| `--object-spilling-config=<base64>` | `append_object_spilling_config()` | **Implemented** |
| `max_io_workers` config | `RayConfig::max_io_workers` | **Implemented** |
| `object_spilling_config` config | `RayConfig::object_spilling_config` | **Implemented** |
| `IOWorkerPoolInterface` | `IOWorkerPool` trait | **Implemented** |
| `LocalObjectManager::SpillObjectsInternal` | `spill_objects()` (pool acquire + RPC) | **Implemented** |
| `LocalObjectManager::AsyncRestoreSpilledObject` | `restore_object()` (pool acquire + RPC) | **Implemented** |
| `LocalObjectManager::DeleteSpilledObjects` | `delete_spilled_objects()` (pool acquire + RPC) | **Implemented** |
| Worker RPC client creation | `connect_to_worker()` → `CoreWorkerClient::connect()` | **Implemented** |

---

## Detailed: `TryStartIOWorkers` C++ vs Rust Line-by-Line

The Rust `try_start_io_workers` at `worker_pool.rs:1077-1116` matches the C++ `TryStartIOWorkers` at `worker_pool.cc:1757-1786` step by step:

| Step | C++ (worker_pool.cc) | Rust (worker_pool.rs) |
|------|---------------------|----------------------|
| Python-only guard | `if (language != Language::PYTHON) return;` (line 1760) | `if language != Language::Python { return; }` (line 1078) |
| Capacity calculation | `available = num_starting + started_count` (line 1769) | `available = num_starting_io_workers as usize + started_io_workers.len()` (line 1086-1087) |
| Max check | `max_to_start = max_io_workers - available` (line 1770) | `max_to_start = max_io_workers.saturating_sub(available)` (line 1088) |
| Demand check | `needed = pending_tasks - idle_workers` (line 1773) | `needed = pending.saturating_sub(idle)` (line 1091) |
| Clamp | `needed = min(needed, max_to_start)` (line 1774) | `to_start = needed.min(max_to_start)` (line 1092) |
| Increment counter | implicit in loop | `num_starting_io_workers += 1` per worker (line 1095) |
| Drop lock before spawn | N/A (single-threaded) | `drop(io_states)` (line 1097) — required for Rust's mutex |
| Spawn loop | `StartWorkerProcess(PYTHON, worker_type, JobID::Nil(), ...)` (line 1779) | `start_worker_process(Language::Python, &JobID::nil(), &ctx)` (line 1104-1105) |
| Spawn failure | `if (!proc.IsValid()) return;` (line 1782) | `if .is_none() { num_starting -= 1; break; }` (line 1108-1113) |
| Worker type in context | Passed as function parameter | `SpawnWorkerContext { worker_type, ..Default::default() }` (line 1100) |

**Call sites** also match C++:
- Called from `pop_io_worker_internal` when no idle workers are available (C++: `PopIOWorkerInternal`, line 1057)
- Called from `register_worker` after IO worker registration to fill remaining pending tasks (C++: `OnWorkerStarted`, line 880)

---

### 6. IO Worker RPC Dispatch (`rust/ray-raylet/src/local_object_manager.rs`)

The previous version had TODO stubs — callbacks logged the worker assignment but didn't send RPCs. Now implemented:

**Three free functions** (take `&Arc<Mutex<LocalObjectManager>>` to allow callback state updates):

- **`spill_objects(lom)`** — Selects objects, marks pending, pops spill worker, spawns tokio task that:
  1. Resolves worker address via `pool.get_worker(worker_id)`
  2. Connects `CoreWorkerClient` to `http://{ip}:{port}`
  3. Builds `SpillObjectsRequest` with object references
  4. Sends RPC, on success calls `spill_completed(oid, url)` for each object
  5. On failure calls `spill_failed(oid)` to return objects to pinned set
  6. Always returns worker to pool via `push_spill_worker(worker_id)`

- **`restore_object(lom, object_id, url)`** — Pops restore worker, spawns tokio task that:
  1. Sends `RestoreSpilledObjectsRequest` with URL and object ID
  2. On success calls `restore_completed(bytes_restored)` + `record_restore_time`
  3. Always returns worker via `push_restore_worker(worker_id)`

- **`delete_spilled_objects(lom, urls)`** — Pops delete worker, spawns tokio task that:
  1. Sends `DeleteSpilledObjectsRequest` with all URLs
  2. Always returns worker via `push_delete_worker(worker_id)`

**Shared helper:** `connect_to_worker(pool, worker_id)` resolves address and creates gRPC client.

**Design:** Free functions (not `&mut self`) because the IO worker callback runs outside the mutex, but needs to update `LocalObjectManager` state on RPC completion. The `Arc<Mutex<...>>` is cloned into the closure.

## What Remains (Follow-up)

1. **`util_io_worker_state`** — C++ has a third IO worker state for utility operations. Not implemented (rarely used).
2. **IO worker timeout monitoring** — C++ `MonitorStartingWorkerProcess` for IO workers. Can be added incrementally.
3. **Spill throughput control** — C++ has `num_active_workers_` / `max_active_workers_` gating. Not yet implemented.

---

## Tests

```
$ cargo test -p ray-raylet --lib
test result: ok. 480 passed (includes IO worker pool tests); 0 failed; 0 ignored

$ cargo test -p ray-raylet --test integration_test
test result: ok. 24 passed; 0 failed; 0 ignored

$ cargo test -p ray-common --lib config
test result: ok. 14 passed; 0 failed; 0 ignored

Total: 518 passed, 0 failed, 0 ignored (504 raylet + 14 config)
```

New tests added:
- `test_pop_spill_worker_with_idle` — idle worker returned immediately
- `test_pop_spill_worker_queues_when_no_idle` — callback queued when no idle
- `test_push_drains_pending` — push triggers pending callback
- `test_pop_delete_picks_larger_pool` — delete picks from larger idle pool
- `test_try_start_respects_max` — max_io_workers limit honored
- `test_register_io_worker` — registration updates IO state
- `test_disconnect_io_worker` — disconnect cleans up IO state
- `test_spill_worker_has_object_spilling_config` — base64 flag emitted
- `test_regular_worker_no_object_spilling_config` — regular workers omit flag
- `test_object_spilling_config_and_max_io_workers_from_json` — config parsing

---

## Updated Status Table

| Area | Status |
|------|--------|
| GCS `enable_ray_event` | **Closed** |
| Agent subprocess monitoring | **Closed** |
| session_dir persisted-port resolution | **Closed** |
| Runtime-env wire format (protobuf) | **Closed** |
| Runtime-env auth token loading | **Closed** |
| Runtime-env eager-install gating | **Closed** |
| Runtime-env retry/deadline/fatal | **Closed** |
| Runtime-env timeout shutdown + death info | **Closed** |
| RuntimeEnvConfig propagation | **Closed** |
| Runtime-env hash | **Rust-local equivalence** |
| Python worker command parsing | **Closed** |
| Regular worker argv | **Closed** |
| IO-worker pool management | **Closed** (pool acquire/release) |
| IO-worker argv (--worker-type, --object-spilling-config) | **Closed** |
| IO-worker RPC dispatch (Spill/Restore/Delete) | **Closed** |
| CLI defaults | **Intentionally different** |
| Object-store live runtime | **Improved** (IO pool + RPC dispatch implemented) |
| Metrics exporter protocol | **Architecturally different** |
