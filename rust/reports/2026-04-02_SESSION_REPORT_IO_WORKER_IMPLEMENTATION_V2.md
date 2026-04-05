# Claude Session Report: IO-Worker Subsystem Implementation V2

**Date:** 2026-04-02
**Branch:** `cc-to-rust-experimental`
**Previous version:** `2026-04-01_SESSION_REPORT_IO_WORKER_IMPLEMENTATION.md`
**For:** Codex re-audit

---

## Changes Since V1

V2 fixes two bugs found during Codex review of V1:

### Bug Fix 1: `pop_delete_worker` silently drops callback (worker_pool.rs:993)

**Problem:** When no Python IO state existed yet, `pop_delete_worker` silently dropped the callback instead of queuing demand and starting a worker — unlike C++ `worker_pool.cc:1072` which always proceeds.

**Root cause:** The `if let Some(io_lang) = io_states.get(&Language::Python)` check returned early with no `else` branch, causing the callback to be dropped.

**Fix:** When no Python IO state exists, default to `(0, 0)` idle counts and fall through to `pop_io_worker_internal(RestoreWorker, callback)`. This creates the state on demand, queues the callback, and triggers `try_start_io_workers`.

```rust
// BEFORE (bug):
if let Some(io_lang) = io_states.get(&Language::Python) {
    // ... route callback
}
// callback silently dropped if no Python state

// AFTER (fixed):
let (num_spill, num_restore) = if let Some(io_lang) = io_states.get(&Language::Python) {
    (spill_idle_count, restore_idle_count)
} else {
    (0, 0)  // No state yet — will be created on demand
};
drop(io_states);
// Always routes to pop_io_worker_internal, never drops callback
```

### Bug Fix 2: Restore deduplication missing (local_object_manager.rs)

**Problem:** Rust `restore_object()` did not deduplicate repeated restore requests for the same object. C++ checks `objects_pending_restore_.count(object_id)` and returns early on duplicates (`local_object_manager.cc:469-474`), also tracking `num_bytes_pending_restore_`.

**Fix:** Added full restore deduplication matching C++:

**New fields in `LocalObjectManager`:**
- `objects_pending_restore: HashSet<ObjectID>` — dedup set (C++ `objects_pending_restore_`)
- `num_bytes_pending_restore: u64` — byte tracking (C++ `num_bytes_pending_restore_`)

**New methods:**
- `mark_pending_restore(object_id, object_size) -> bool` — returns false if already pending (dedup)
- `clear_pending_restore(object_id, object_size)` — called on RPC completion or failure
- `is_restore_pending(object_id) -> bool` — query method
- `num_objects_pending_restore() -> usize` — for metrics
- `num_bytes_pending_restore() -> u64` — for metrics

**Updated `restore_object()` signature:** Now takes `object_size: u64` parameter for byte tracking.

**Updated `restore_object()` flow:**
1. Lock LOM, call `mark_pending_restore(object_id, object_size)` — returns false on duplicate → early return
2. Pop restore worker, send RPC
3. On success: `clear_pending_restore` + `restore_completed` + `record_restore_time`
4. On failure: `clear_pending_restore` (object can be retried later)
5. Always: `push_restore_worker(worker_id)`

**Updated `LocalObjectManagerStats`:** Added `num_pending_restore` and `bytes_pending_restore` fields.

---

## C++ Parity Mapping (Updated)

| C++ Component | Rust Equivalent | Status |
|--------------|-----------------|--------|
| `IOWorkerState` struct | `IOWorkerState` + `LanguageIOState` | **Implemented** |
| `PopSpillWorker` / `PushSpillWorker` | `pop_spill_worker` / `push_spill_worker` | **Implemented** |
| `PopRestoreWorker` / `PushRestoreWorker` | `pop_restore_worker` / `push_restore_worker` | **Implemented** |
| `PopDeleteWorker` (smart routing) | `pop_delete_worker` (picks larger pool, never drops callback) | **Fixed in V2** |
| `PushDeleteWorker` (type-based routing) | `push_delete_worker` (routes by actual type) | **Implemented** |
| `TryStartIOWorkers` (worker_pool.cc:1757-1786) | `try_start_io_workers` (worker_pool.rs:1077-1116) | **Implemented** |
| `OnWorkerStarted` (IO worker registration) | `register_worker` IO branch | **Implemented** |
| Worker disconnect cleanup | `disconnect_worker` IO branch | **Implemented** |
| `--object-spilling-config=<base64>` | `append_object_spilling_config()` | **Implemented** |
| `max_io_workers` config | `RayConfig::max_io_workers` | **Implemented** |
| `object_spilling_config` config | `RayConfig::object_spilling_config` | **Implemented** |
| `IOWorkerPoolInterface` | `IOWorkerPool` trait (+ `get_worker` method) | **Implemented** |
| `SpillObjectsInternal` → `SpillObjects` RPC | `spill_objects()` free function | **Implemented** |
| `AsyncRestoreSpilledObject` → `RestoreSpilledObjects` RPC | `restore_object()` free function | **Implemented** |
| `DeleteSpilledObjects` → `DeleteSpilledObjects` RPC | `delete_spilled_objects()` free function | **Implemented** |
| `objects_pending_restore_` deduplication | `objects_pending_restore` HashSet + `mark_pending_restore` | **Fixed in V2** |
| `num_bytes_pending_restore_` tracking | `num_bytes_pending_restore` u64 | **Fixed in V2** |
| Worker RPC client creation | `connect_to_worker()` → `CoreWorkerClient::connect()` | **Implemented** |

---

## Detailed: `TryStartIOWorkers` C++ vs Rust Line-by-Line

| Step | C++ (worker_pool.cc) | Rust (worker_pool.rs) |
|------|---------------------|----------------------|
| Python-only guard | `if (language != Language::PYTHON) return;` (line 1760) | `if language != Language::Python { return; }` (line 1078) |
| Capacity calculation | `available = num_starting + started_count` (line 1769) | `available = num_starting_io_workers as usize + started_io_workers.len()` (line 1086-1087) |
| Max check | `max_to_start = max_io_workers - available` (line 1770) | `max_to_start = max_io_workers.saturating_sub(available)` (line 1088) |
| Demand check | `needed = pending_tasks - idle_workers` (line 1773) | `needed = pending.saturating_sub(idle)` (line 1091) |
| Clamp | `needed = min(needed, max_to_start)` (line 1774) | `to_start = needed.min(max_to_start)` (line 1092) |
| Drop lock before spawn | N/A (single-threaded) | `drop(io_states)` (line 1097) |
| Spawn loop | `StartWorkerProcess(PYTHON, worker_type, JobID::Nil())` (line 1779) | `start_worker_process(Language::Python, &JobID::nil(), &ctx)` (line 1104-1105) |
| Spawn failure | `if (!proc.IsValid()) return;` (line 1782) | `if .is_none() { num_starting -= 1; break; }` (line 1108-1113) |

---

## What Remains

1. **`util_io_worker_state`** — C++ has a third IO worker state for utility operations. Not implemented (rarely used).
2. **IO worker timeout monitoring** — C++ `MonitorStartingWorkerProcess` for IO workers. Can be added incrementally.
3. **Spill throughput control** — C++ has `num_active_workers_` / `max_active_workers_` gating. Not yet implemented.

---

## Tests

```
$ cargo test -p ray-raylet --lib
test result: ok. 480 passed; 0 failed; 0 ignored

$ cargo test -p ray-raylet --test integration_test
test result: ok. 24 passed; 0 failed; 0 ignored

$ cargo test -p ray-common --lib config
test result: ok. 14 passed; 0 failed; 0 ignored

Total: 518 passed, 0 failed, 0 ignored
```
