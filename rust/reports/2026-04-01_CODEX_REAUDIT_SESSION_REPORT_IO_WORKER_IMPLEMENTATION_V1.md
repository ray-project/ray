# Codex Re-Audit of IO Worker Implementation V1

**Date:** 2026-04-01
**Branch:** `cc-to-rust-experimental`
**Input reviewed:** `rust/reports/2026-04-01_SESSION_REPORT_IO_WORKER_IMPLEMENTATION.md`
**Scope:** Source-level verification of Claude's IO-worker implementation report against current Rust and C++ raylet code.

## Executive Verdict

Claude implemented meaningful IO-worker infrastructure in Rust. The gap is materially smaller than before.

However, the subsystem is **not fully closed**. The remaining gap is substantive: Rust now acquires and recycles IO workers, but it still does not dispatch the actual spill/restore/delete RPCs that C++ uses to perform the work.

## Findings

### 1. IO-worker RPC execution is still missing

**Severity:** HIGH

This is the main remaining gap.

Rust `LocalObjectManager` now:

1. selects objects / URLs
2. acquires spill/restore/delete workers from the new pool
3. logs assignment

But it explicitly does **not** send the actual RPCs yet:

- [local_object_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L447)
- [local_object_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L470)
- [local_object_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L497)

The code comments are explicit:

- `"actual RPC is a follow-up"`
- TODOs to send `SpillObjects`, `RestoreSpilledObject`, and `DeleteSpilledObjects`

By contrast, C++ `LocalObjectManager` actually builds and sends those RPCs:

- [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L327)

This is not a minor omission. Without the RPC dispatch, the newly implemented Rust pool management does not yet complete the real spill/restore/delete workflow.

### 2. IO-worker pool/argv/config plumbing is real

**Severity:** RESOLVED AREA

Claude’s implementation in these areas appears real:

1. `WorkerPool` now has IO-worker state and acquire/release APIs
2. `try_start_io_workers` exists
3. `LocalObjectManager` is wired to an `IOWorkerPool`
4. `WorkerSpawner` now appends `--object-spilling-config`
5. `RayConfig` now includes `object_spilling_config` and `max_io_workers`

Relevant source:

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1037)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1077)
- [local_object_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L420)
- [node_manager.rs](/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs#L469)
- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L223)
- [config.rs](/Users/istoica/src/ray/rust/ray-common/src/config.rs#L74)

This is real progress. It should be credited.

### 3. The report’s status table is still too optimistic

**Severity:** MEDIUM

Claude marks:

- `IO-worker pool management` as closed
- `IO-worker argv` as closed
- `IO-worker RPC dispatch` as follow-up
- `Object-store live runtime` as improved

That framing is too optimistic if the user’s question is “are there no longer gaps?”

The correct conclusion is:

- pool management plumbing: largely implemented
- argv/config plumbing: largely implemented
- end-to-end IO-worker subsystem parity: **not closed**

Because the actual worker RPC execution path is still missing.

## Confirmed Closures

The following parts of the previous IO-worker gap do now appear genuinely improved or closed:

### IO-worker pool acquisition/release plumbing

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1037)
- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1054)

### `TryStartIOWorkers`-style spawning

- [worker_pool.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L1077)

### `--object-spilling-config` argv emission

- [worker_spawner.rs](/Users/istoica/src/ray/rust/ray-raylet/src/worker_spawner.rs#L223)

## Prescriptive Plan

### Priority 1: implement actual spill/restore/delete RPC dispatch

This is now the blocking parity item.

Implement all of the following:

1. For spill:
   build and send the Rust equivalent of C++ `SpillObjectsRequest`
2. For restore:
   build and send the Rust equivalent of the restore RPC
3. For delete:
   build and send the Rust equivalent of the delete RPC
4. On completion:
   update `LocalObjectManager` state
5. Always return the IO worker to the correct pool after RPC completion

Acceptance criteria:

1. Rust no longer logs “actual RPC is a follow-up”
2. `spill_completed` / `spill_failed` / `restore_completed` are driven by RPC callbacks
3. delete path actually issues worker RPCs instead of only acquiring workers

### Priority 2: align the Rust completion/error lifecycle with C++

After dispatch exists, verify parity for:

1. success path
2. partial spill success
3. failure path
4. worker return to pool
5. pending-byte accounting

Use [local_object_manager.cc](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L327) as the primary reference.

### Priority 3: downgrade report language until RPC dispatch is done

Until Priority 1 is complete, report this area as:

`IO-worker subsystem: partially implemented (pool and argv/config wired; RPC execution still open)`

Do not call it closed.

## Merge Recommendation

My recommendation is firm:

- Do **not** claim the IO-worker subsystem gap is closed
- Do **not** claim there are no remaining gaps

The accurate status is:

- IO-worker pool state and spawning: implemented
- IO-worker argv/config wiring: implemented
- IO-worker end-to-end execution: still open due to missing RPC dispatch

That is the technically defensible reading of the current Rust and C++ code.
