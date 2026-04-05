# Codex Re-Audit: Claude Session Report IO-Worker Implementation V10

**Date:** 2026-04-04
**Audited report:** `/Users/istoica/src/ray/rust/reports/2026-04-04_CODEX_REAUDIT_SESSION_REPORT_IO_WORKER_IMPLEMENTATION_V9.md`
**Current result:** V9 is directionally better than V8, but it is still incomplete. Several V8-class gaps are now closed. However, C++ parity is still **not** closed because the new Rust free/delete path is not fully wired through the real owner-free and spill-completion lifecycle.

## Executive Verdict

Do **not** declare IO-worker parity closed.

The current Rust tree has genuinely closed these previously identified gaps:

1. `free_objects_period_milliseconds` now exists in Rust config.
2. `NodeManager` now schedules periodic `flush_free_objects()`.
3. `NodeManager` now schedules threshold-driven spilling when `object_spilling_config` is non-empty.
4. `LocalObjectManager` now has an `on_objects_freed` callback and a full `flush_free_objects()` method.

Those closures are real.

But parity is still not closed for three concrete reasons:

1. Rust `release_freed_object()` still does **not** enqueue freed objects into `pending_deletion`, does **not** enqueue spilled objects into the spilled-delete queue, and does **not** perform the C++ immediate-flush behavior for `free_objects_period_ms == 0` or batch-size hits.
2. Rust spill completion still does **not** register spilled URLs into the new `spilled_objects_url` / `url_ref_count` tracking path that the delete queue depends on.
3. Rust `on_objects_freed` still deletes directly from plasma instead of going through the C++-equivalent `object_manager->FreeObjects(object_ids, /*local_only=*/false)` behavior.

These are not polish items. They are production semantic gaps.

## What V9 Correctly Identifies As Closed

### 1. Config surface gap is closed

Rust now exposes `free_objects_period_milliseconds` in `RayConfig`.

Evidence:

- Rust config field: `rust/ray-common/src/config.rs:96-97`
- Rust default: `rust/ray-common/src/config.rs:163`
- Rust JSON parsing: `rust/ray-common/src/config.rs:268`
- C++ definition: `src/ray/common/ray_config_def.h:148`

This part of V9 is correct.

### 2. Periodic flush scheduling is now present

Rust `NodeManager::run()` now spawns a periodic task that calls:

- `lom.lock().flush_free_objects()`

using:

- `free_objects_period_milliseconds`

Evidence:

- Rust timer wiring: `rust/ray-raylet/src/node_manager.rs:1325-1344`
- C++ reference: `src/ray/raylet/node_manager.cc:418-422`

This part of V9 is also correct.

### 3. Threshold-driven spill scheduling is now present

Rust `NodeManager::run()` now arms a second periodic task that:

1. checks `object_spilling_config` is non-empty
2. computes current usage
3. compares against `object_spilling_threshold`
4. calls `spill_objects_upto_max_throughput(&lom)`

Evidence:

- Rust timer wiring: `rust/ray-raylet/src/node_manager.rs:1346-1391`
- C++ reference: `src/ray/raylet/node_manager.cc:423-430`, `src/ray/raylet/node_manager.cc:2400-2413`

Again, this is real progress.

### 4. Full flush helper now exists

Rust now has a real `flush_free_objects()` method, distinct from the old legacy `flush_freed_objects()` helper.

Evidence:

- Rust legacy helper: `rust/ray-raylet/src/local_object_manager.rs:355-368`
- Rust full helper: `rust/ray-raylet/src/local_object_manager.rs:370-392`
- Rust delete-queue helper: `rust/ray-raylet/src/local_object_manager.rs:394-453`

This closes the specific V9 claim that Rust only had a batch-drain helper.

## Remaining Gaps

### Severity 1: `release_freed_object()` still does not match C++ `ReleaseFreedObject()`

This is now the highest-severity remaining gap.

C++ `ReleaseFreedObject()` does all of the following:

1. marks the object freed
2. unpins it if still pinned
3. if already spilled or still spilling, enqueues it into `spilled_object_pending_delete_`
4. if `free_objects_period_ms_ >= 0`, adds it to `objects_pending_deletion_`
5. if batch size is reached or `free_objects_period_ms_ == 0`, immediately calls `FlushFreeObjects()`

Evidence:

- C++ behavior: `src/ray/raylet/local_object_manager.cc:111-147`

Rust `release_freed_object()` only:

1. marks the object freed
2. unpins it if not pending spill

It does **not**:

- add the object to `pending_deletion`
- enqueue spilled objects into `spilled_object_pending_delete`
- trigger immediate flush on batch threshold
- trigger immediate flush when `free_objects_period_ms == 0`

Evidence:

- Rust implementation: `rust/ray-raylet/src/local_object_manager.rs:292-309`

Why this matters:

- In Rust, owner-free / eviction callback delivery does not currently push the object into the actual free/delete pipeline.
- But that callback is exactly how production `PinObjectIDs` releases retained pins.
- Therefore the new flush/delete machinery is still not connected to the most important production free path.

This is a merge-blocking parity gap.

### Severity 1: Spill completion does not register spilled URLs for later deletion

C++ spill completion (`OnObjectSpilled`) updates:

- `spilled_objects_url_`
- `url_ref_count_`

This bookkeeping is what later allows `ProcessSpilledObjectsDeleteQueue()` to delete the right spill files safely.

Evidence:

- C++ spill bookkeeping: `src/ray/raylet/local_object_manager.cc:400-430`
- C++ delete queue relies on it: `src/ray/raylet/local_object_manager.cc:538-560`

Rust added:

- `register_spilled_url(...)`
- `spilled_objects_url`
- `url_ref_count`
- `process_spilled_objects_delete_queue(...)`

But the production spill-completion path still only calls:

- `spill_completed(oid, url.clone())`

and does **not** call `register_spilled_url(...)`.

Evidence:

- Rust spill callback: `rust/ray-raylet/src/local_object_manager.rs:1017-1024`
- Rust `spill_completed(...)`: `rust/ray-raylet/src/local_object_manager.rs:604-614`
- Rust registration helper exists but is not wired: `rust/ray-raylet/src/local_object_manager.rs:465-469`

Why this matters:

- The new spilled-delete queue is largely dead code on the production spill path.
- Tests can pass because they call `register_spilled_url(...)` manually.
- The runtime path still does not populate the structures the delete queue needs.

This is another merge-blocking parity gap.

### Severity 1: Rust `on_objects_freed` callback is still not the C++ free path

C++ wires:

- `object_manager->FreeObjects(object_ids, /*local_only=*/false)`

into `LocalObjectManager`.

Evidence:

- C++ main wiring: `src/ray/raylet/main.cc:884-887`

Rust wires a callback that directly deletes objects from the plasma store:

```rust
plasma.delete_object(oid, plasma.allocator().as_ref())
```

Evidence:

- Rust callback wiring: `rust/ray-raylet/src/node_manager.rs:548-563`

Why this matters:

- This is not the same contract as object-manager-level freeing.
- The source comment itself admits the gap: remote-node fanout is not implemented.
- If the parity target is C++ behavior, this must not be waived away.

This is a real semantic difference, not a wording issue.

### Severity 2: Immediate flush semantics are still missing from the free path

In C++, the free path flushes immediately when:

1. `objects_pending_deletion_.size() == free_objects_batch_size_`, or
2. `free_objects_period_ms_ == 0`

Evidence:

- C++ logic: `src/ray/raylet/local_object_manager.cc:141-147`

Rust has the periodic timer and a config field, but `mark_for_deletion()` and `release_freed_object()` do not implement these immediate-flush edge semantics.

Evidence:

- Rust `mark_for_deletion()`: `rust/ray-raylet/src/local_object_manager.rs:348-353`
- Rust `release_freed_object()`: `rust/ray-raylet/src/local_object_manager.rs:292-309`

Why this matters:

- With `free_objects_period_ms == 0`, Rust still will not flush on every free the way C++ does.
- With a full batch, Rust still waits for the timer instead of flushing immediately.

This is smaller than the missing enqueue behavior, but it is still a parity gap.

## Secondary Gap

### Rust still does not expose the local-spill visibility hook used by C++ scheduling/reporting

C++ has:

- `HasLocallySpilledObjects()`

and uses it when reporting used object-store memory for scheduling / drain protection.

Evidence:

- C++ method: `src/ray/raylet/local_object_manager.cc:673-681`
- C++ use site: `src/ray/raylet/main.cc:912-920`

I did not find a Rust equivalent method on `LocalObjectManager`.

This is not the primary blocker for the current IO-worker audit, but it is still a source-level parity difference around spilled-object visibility.

## Assessment Of V9

V9 is no longer stale in the way V8 was. It correctly recognizes that the config/timer/flush-helper work has been added.

But V9 still stops one layer too early.

It checks that the new mechanisms exist. It does **not** fully verify that the production lifecycle uses them correctly:

- free-event path
- spill-completion path
- object-manager free path

That is why V9 overstates closure.

## Prescriptive Closeout Plan

### Priority 1: Fix `release_freed_object()` to match C++ exactly

This is mandatory and should be done first.

Required changes:

1. When a freed object is already spilled or still spilling, enqueue it into `spilled_object_pending_delete`.
2. When `free_objects_period_ms >= 0`, insert the object into `pending_deletion`.
3. If `pending_deletion.len() == free_objects_batch_size`, call `flush_free_objects()` immediately.
4. If `free_objects_period_ms == 0`, call `flush_free_objects()` immediately.
5. Preserve current unpin behavior for the pinned case.

Files:

- `rust/ray-raylet/src/local_object_manager.rs`

Recommendation:

- Do not patch this partially.
- Port the C++ control flow directly and test it line-for-line against the C++ semantics.

### Priority 2: Wire spill completion into spilled-URL bookkeeping

This is also mandatory.

Required changes:

1. On successful spill completion, call `register_spilled_url(object_id, url.clone())`.
2. Ensure the object transitions match the C++ `OnObjectSpilled()` ordering closely enough to avoid double-accounting.
3. Add tests that exercise the real spill-completion callback path, not just manual helper calls.

Files:

- `rust/ray-raylet/src/local_object_manager.rs`

Recommendation:

- The helper already exists. Use it on the real path.
- Do not rely on tests that manually seed `spilled_objects_url`.

### Priority 3: Replace direct plasma deletion with an object-manager-level free path

This is required for true parity.

Required changes:

1. Add or expose a Rust `ObjectManager` method equivalent to C++ `FreeObjects(object_ids, local_only=false)`.
2. Wire `on_objects_freed` to that method instead of raw `plasma.delete_object(...)`.
3. Preserve any local-only / remote fanout semantics as explicitly as possible.

Files:

- `rust/ray-object-manager/src/object_manager.rs`
- `rust/ray-raylet/src/node_manager.rs`

Recommendation:

- Do not hide this under comments like "close enough".
- If remote fanout is intentionally deferred, it must be called out as a remaining non-parity item, not silently folded into a parity claim.

### Priority 4: Add immediate-flush tests for the real free path

Current tests focus on helper existence more than full lifecycle semantics.

Add tests for:

1. `release_freed_object()` enqueues into `pending_deletion`
2. `release_freed_object()` enqueues spilled objects into `spilled_object_pending_delete`
3. `free_objects_period_ms == 0` triggers immediate flush
4. hitting `free_objects_batch_size` triggers immediate flush
5. owner-eviction callback path causes objects to enter the actual delete pipeline

Files:

- `rust/ray-raylet/src/local_object_manager.rs`
- `rust/ray-raylet/src/grpc_service.rs`
- `rust/ray-raylet/tests/integration_test.rs`

Recommendation:

- Treat these tests as mandatory before making any parity-closure statement.

## Bottom Line

The current Rust tree has made real progress since V8:

- config gap closed
- timer gap closed
- threshold scheduling gap closed
- flush helper gap closed

But parity is still **not** closed because the new machinery is not yet fully connected to the production lifecycle:

- free events do not enqueue correctly
- spill completion does not populate delete bookkeeping
- object freeing still bypasses the object-manager-level path

The strongest defensible statement today is:

**Rust now has the periodic scheduling and flush primitives that were missing before, but the production free-event and spill-completion lifecycle still does not match C++ end-to-end. IO-worker parity is therefore still not closed.**
