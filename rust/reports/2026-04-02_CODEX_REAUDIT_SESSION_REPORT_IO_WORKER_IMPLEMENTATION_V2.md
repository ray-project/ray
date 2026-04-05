# Codex Re-Audit of IO Worker Implementation V2

**Date:** 2026-04-02
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-04-01_SESSION_REPORT_IO_WORKER_IMPLEMENTATION.md`
**Scope:** Source-level verification of Claude's IO-worker implementation against current Rust and C++ raylet code.

## Executive Verdict

Claude implemented the previously missing IO-worker RPC dispatch. That is real progress and closes the earlier blocker I had identified.

However, the IO-worker subsystem is **still not fully closed**. I found two concrete remaining gaps:

1. The new delete-worker path can silently drop the callback when no Python IO state exists yet.
2. Restore-state deduplication and pending-restore accounting still do not match C++ `LocalObjectManager`.

## Findings

### 1. `pop_delete_worker()` can drop the callback without starting any worker

**Severity:** HIGH

Rust `pop_delete_worker()` currently does:

- check `io_states.get(&Language::Python)`
- if present, choose spill vs restore pool
- if absent, do nothing

See:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L993)

That means if delete is requested before a Python IO state entry exists, the callback is lost and no worker is queued or started.

This is not just weaker parity. It is a correctness bug in the new Rust implementation.

By contrast, C++ `PopDeleteWorker()` always routes into `PopSpillWorker()` or `PopRestoreWorker()`, both of which eventually queue demand and trigger IO-worker startup:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1072)

#### Required fix

`pop_delete_worker()` must never drop the callback. If there is no existing IO language state yet, it should still route to a concrete worker-type path and let that path create state and start workers.

### 2. Restore deduplication and pending-restore tracking are still missing

**Severity:** HIGH

C++ `LocalObjectManager::AsyncRestoreSpilledObject()` explicitly:

1. deduplicates repeated restore requests for the same object
2. tracks `objects_pending_restore_`
3. tracks `num_bytes_pending_restore_`
4. clears that state on completion

See:

- [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L464)

The current Rust `LocalObjectManager` does not contain equivalent restore-pending structures. A source search shows no `pending_restore` state in the Rust local object manager, and the restore path simply sends the RPC and updates cumulative stats:

- [local_object_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L630)

This is a real semantic gap, not just a monitoring omission. Without deduplication, repeated restore requests for the same object can behave differently from C++.

#### Required fix

Rust needs explicit restore-pending state comparable to C++:

1. `objects_pending_restore`
2. `num_bytes_pending_restore`
3. request deduplication
4. cleanup on completion/failure

### 3. Additional parity gaps remain in IO-worker lifecycle behavior

**Severity:** MEDIUM

Claude’s report lists some of these as follow-up, and that is fair, but they are still real remaining gaps:

1. no `util_io_worker_state`
2. no throughput gating equivalent to C++ `num_active_workers_` / `max_active_workers_`
3. no delete retry behavior equivalent to C++ `RetryDeleteSpilledObjects`

Relevant C++ references:

- [worker_pool.cc](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1814)
- [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L579)
- [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L169)

These are secondary relative to the two issues above, but they mean the subsystem is not yet parity-closed.

## Confirmed Progress

The following IO-worker work does appear genuinely implemented now:

### Pool management and startup

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1037)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1077)

### LocalObjectManager wiring

- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L469)

### Actual spill/restore/delete RPC dispatch

- [local_object_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L532)
- [local_object_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L630)
- [local_object_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L704)

### `--object-spilling-config` argv emission

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L223)

## Prescriptive Plan

### Priority 1: fix `pop_delete_worker()` so callbacks are never dropped

Implement:

1. a deterministic fallback routing path when no Python IO state exists yet
2. coverage for the first-delete-request case

Acceptance criteria:

1. delete callback is always either queued or immediately assigned
2. first delete request can start IO workers from an empty state

### Priority 2: add restore deduplication and pending-restore tracking

Implement:

1. `objects_pending_restore`
2. `num_bytes_pending_restore`
3. duplicate-request suppression
4. cleanup on success and failure

Acceptance criteria:

1. repeated restore requests for the same object do not double-dispatch
2. pending-restore counters behave like C++

### Priority 3: close the remaining lifecycle gaps

Implement:

1. delete retry behavior
2. spill throughput gating
3. optional `util_io_worker_state` if required by actual workloads

## Merge Recommendation

My recommendation is firm:

- Do **not** declare the IO-worker subsystem fully closed yet
- Do **not** claim there are no remaining gaps

The accurate status is:

- IO-worker pool and RPC dispatch: substantially implemented
- delete-worker startup path: still has a correctness bug
- restore dedup/accounting: still not parity-complete
- secondary lifecycle parity items: still open

That is the technically defensible reading of the current Rust and C++ code.
