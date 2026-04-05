# Codex Re-Audit: Claude Session Report IO-Worker Implementation V8

**Date:** 2026-04-04
**Audited report:** `/Users/istoica/src/ray/rust/reports/2026-04-04_CODEX_REAUDIT_SESSION_REPORT_IO_WORKER_IMPLEMENTATION_V7.md`
**Current result:** V7 is materially stale. Its headline claim about missing production config wiring and missing production spillability wiring is no longer true. However, IO-worker parity is still not closed because the Rust `NodeManager` still does not drive the local-object-manager lifecycle the way C++ does.

## Executive Verdict

Do **not** declare IO-worker parity closed.

The current Rust tree has already fixed the two production-path issues that V7 called out:

1. Rust `NodeManager` now constructs `LocalObjectManager` from live `RayConfig` values, not `LocalObjectManagerConfig::default()`.
2. Rust `NodeManager` now wires a real spillability predicate backed by `PlasmaStore::is_object_spillable(...)`.

Those V7 findings are obsolete.

But parity is still not closed, because Rust is still missing the C++ runtime control loop around the local object manager:

1. No Rust equivalent of the periodic `FlushFreeObjects()` scheduling done by C++ `NodeManager`.
2. No Rust equivalent of the periodic threshold-based `SpillIfOverPrimaryObjectsThreshold()` scheduling done by C++ `NodeManager`.
3. Rust `RayConfig` does not expose `free_objects_period_milliseconds`, so Rust cannot currently match the C++ timer contract even if the scheduling code were added.
4. Rust `flush_freed_objects()` is only a batching helper; it does not perform the full C++ `FlushFreeObjects()` contract of invoking the object-store free callback and draining spilled-object deletion work.

That is the real remaining gap set.

## What V7 Gets Wrong

### 1. The "production config wiring is still missing" claim is wrong

Current Rust `NodeManager::new()` explicitly builds `LocalObjectManagerConfig` from `RayConfig`:

- `min_spilling_size`
- `max_fused_object_count`
- `max_spilling_file_size_bytes`
- `free_objects_batch_size`
- `object_spilling_threshold`
- `max_io_workers`

Evidence:

- Rust: `rust/ray-raylet/src/node_manager.rs:465-484`
- C++ reference: `src/ray/raylet/main.cc:871-883`

This is no longer a defaults-only construction path.

### 2. The "production spillability wiring is still missing" claim is wrong

Current Rust `NodeManager::new()` creates the real object manager and installs:

```rust
lom.set_is_plasma_object_spillable(Arc::new(move |object_id| {
    om_ref.plasma_store().is_object_spillable(object_id)
}));
```

Evidence:

- Rust object-manager creation: `rust/ray-raylet/src/node_manager.rs:532`
- Rust predicate wiring: `rust/ray-raylet/src/node_manager.rs:536-545`
- Rust spillability implementation: `rust/ray-object-manager/src/plasma/store.rs:308-315`
- C++ reference wiring: `src/ray/raylet/main.cc:884-891`

This is the production path. V7’s main conclusion is therefore outdated.

## Confirmed Closed Items

These are now genuinely present in the current Rust tree:

1. Spillability callback support in `LocalObjectManager`.
2. Spillability filtering in spill selection.
3. Construction-time validation that `max_spilling_file_size_bytes >= min_spilling_size` when enabled.
4. Production config wiring from `RayConfig`.
5. Production spillability callback wiring from the real object manager.

Evidence:

- Rust spill-selection hook: `rust/ray-raylet/src/local_object_manager.rs:361-368`
- Rust max-file-size cap: `rust/ray-raylet/src/local_object_manager.rs:373-380`
- Rust fused-object-count cap: `rust/ray-raylet/src/local_object_manager.rs:386-389`
- Rust defer-small-batch behavior: `rust/ray-raylet/src/local_object_manager.rs:399-424`
- Rust constructor validation: `rust/ray-raylet/src/local_object_manager.rs:170-177`

## Remaining Gaps

### Severity 1: Rust `NodeManager` does not schedule periodic free-object flushing

C++ `NodeManager` installs a periodic timer:

- `local_object_manager_.FlushFreeObjects()`

driven by:

- `RayConfig::instance().free_objects_period_milliseconds()`

Evidence:

- C++ scheduler wiring: `src/ray/raylet/node_manager.cc:418-422`

I did **not** find an equivalent Rust scheduling path in `NodeManager::run()`.

The current Rust `NodeManager::run()` handles agent startup, port resolution, worker spawning, metrics setup, and GCS registration, but it does not schedule any periodic local-object-manager flush task.

Evidence:

- Rust `run()` body inspected: `rust/ray-raylet/src/node_manager.rs:1006-1265`
- No `FlushFreeObjects` equivalent found in Rust `node_manager.rs`
- No periodic caller of `flush_freed_objects()` found in Rust `ray-raylet/src`

Why this matters:

- In C++, out-of-scope objects are eventually flushed even when the batch size is not reached.
- In Rust, `pending_deletion` can accumulate with no production timer to drain it.
- That is not a minor difference. It changes steady-state object lifecycle behavior.

### Severity 1: Rust `NodeManager` does not schedule threshold-driven spilling

C++ `NodeManager` also installs a second periodic timer:

- `SpillIfOverPrimaryObjectsThreshold()`

This checks object-store usage against `object_spilling_threshold()` and calls:

- `local_object_manager_.SpillObjectUptoMaxThroughput()`

Evidence:

- C++ scheduler wiring: `src/ray/raylet/node_manager.cc:423-430`
- C++ threshold handler: `src/ray/raylet/node_manager.cc:2400-2413`

I did **not** find any Rust equivalent in `NodeManager`.

Rust stores `object_spilling_threshold` in `LocalObjectManagerConfig`, and `LocalObjectManager` has `should_spill(...)`, but there is no production scheduling path that computes used fraction and triggers spilling.

Evidence:

- Rust threshold field wiring: `rust/ray-raylet/src/node_manager.rs:473-480`
- Rust threshold helper exists: `rust/ray-raylet/src/local_object_manager.rs:531`
- No Rust `SpillIfOverPrimaryObjectsThreshold` equivalent found
- No Rust caller of `spill_objects_upto_max_throughput(...)` found outside tests/helpers

Why this matters:

- The spill-selection logic can be correct and still be inactive in practice.
- C++ parity requires the runtime trigger path, not just helper methods.

### Severity 1: Rust does not expose `free_objects_period_milliseconds`

C++ constructor and timer behavior depend on:

- `free_objects_period_milliseconds`

Current Rust `RayConfig` exposes:

- `object_spilling_threshold`
- `object_spilling_config`
- `max_io_workers`
- `min_spilling_size`
- `max_spilling_file_size_bytes`
- `max_fused_object_count`
- `free_objects_batch_size`

But it does **not** expose `free_objects_period_milliseconds`.

Evidence:

- Rust config object-store section: `rust/ray-common/src/config.rs:74-94`
- C++ config definition: `src/ray/common/ray_config_def.h:148`

Why this matters:

- Rust cannot faithfully reproduce the C++ flush/spill timer cadence.
- This is a source-level parity gap, not just a missing test.

### Severity 2: Rust `flush_freed_objects()` is not the full C++ `FlushFreeObjects()` contract

C++ `FlushFreeObjects()` does three things:

1. Converts pending deletions to a batch.
2. Invokes `on_objects_freed_(objects_to_delete)` so the object manager actually frees them.
3. Calls `ProcessSpilledObjectsDeleteQueue(free_objects_batch_size_)`.

Evidence:

- C++ flush behavior: `src/ray/raylet/local_object_manager.cc:150-162`
- C++ constructor wiring of `on_objects_freed`: `src/ray/raylet/main.cc:884-887`

Rust `flush_freed_objects()` only returns a batch from `pending_deletion`.

Evidence:

- Rust flush helper: `rust/ray-raylet/src/local_object_manager.rs:323-331`

There is no equivalent callback field in Rust `LocalObjectManagerConfig`, no `on_objects_freed` installation in Rust `NodeManager`, and no production caller that takes the returned batch and frees it through the object manager.

Why this matters:

- Even if a timer were added tomorrow, the current flush helper still would not match the C++ side effect contract.
- This is a real semantic gap.

## Secondary Observations

### 1. V7’s core conclusion should not be used going forward

Do not continue to cite:

- "NodeManager still constructs `LocalObjectManager` with defaults"
- "NodeManager still does not wire `set_is_plasma_object_spillable(...)`"

Those are contradicted by the current source tree.

### 2. The remaining work is operational, not capability-level

The code now contains the right spill-selection primitives. The missing parity is in:

- runtime scheduling
- config surface completeness
- full flush-side effects

That is a better and more accurate framing than V7’s constructor-focused diagnosis.

## Prescriptive Closeout Plan

### Priority 1: Add `free_objects_period_milliseconds` to Rust config now

This is mandatory.

Required changes:

1. Add `free_objects_period_milliseconds: i64` to `ray_common::config::RayConfig`.
2. Set the C++-matching default of `1000`.
3. Parse it from JSON config input.
4. Add default/conformance tests for it.

Files:

- `rust/ray-common/src/config.rs`
- `rust/ray-conformance-tests/src/categories/config_conformance.rs`
- `rust/ray-conformance-tests/golden-data/config/default_values.json`

Recommendation:

- Do not hardcode the period inside `NodeManager`.
- Make the timer source explicit and reviewable through `RayConfig`.

### Priority 2: Implement the periodic flush timer in Rust `NodeManager`

This is also mandatory.

Required behavior:

1. If `free_objects_period_milliseconds > 0`, spawn a periodic task in `NodeManager::run()`.
2. On each tick, lock `local_object_manager`, perform the full flush path, and update last-run state if needed.
3. Preserve C++ semantics for the `0` and negative cases as closely as Rust currently can.

Files:

- `rust/ray-raylet/src/node_manager.rs`
- `rust/ray-raylet/src/local_object_manager.rs`

Recommendation:

- Do not stop at a timer that only drains `pending_deletion`.
- The timer must execute the full free-and-delete behavior, not a partial helper.

### Priority 3: Implement the full Rust equivalent of C++ `FlushFreeObjects()`

This is where the current Rust model is still materially incomplete.

Required behavior:

1. Extend `LocalObjectManager` so it can execute an `on_objects_freed` callback or equivalent object-manager action.
2. On flush, free pending local objects from the real object manager / plasma-backed path.
3. Also process spilled-object deletion work in the same flush path.
4. Ensure the flush path is idempotent and testable.

Files:

- `rust/ray-raylet/src/local_object_manager.rs`
- `rust/ray-raylet/src/node_manager.rs`
- potentially `rust/ray-object-manager/src/object_manager.rs`

Recommendation:

- Match the C++ structure directly instead of inventing another parallel helper path.
- The current "return a Vec and let someone else maybe use it" design is too weak for production parity.

### Priority 4: Add threshold-driven spill scheduling in Rust `NodeManager`

This is required to activate the already-implemented spill logic.

Required behavior:

1. Periodically compute object-store usage from the real object manager / plasma allocator.
2. Compare usage against `object_spilling_threshold`.
3. If above threshold and spilling config is non-empty, call the Rust equivalent of `SpillObjectUptoMaxThroughput()`.
4. Preserve the C++ guard that object spilling is disabled when `object_spilling_config` is empty.

Files:

- `rust/ray-raylet/src/node_manager.rs`
- `rust/ray-raylet/src/local_object_manager.rs`

Recommendation:

- Do not wire this only through tests or ad hoc calls.
- It must be part of the production `run()` lifecycle.

### Priority 5: Add production-path integration tests

Unit tests around `LocalObjectManager` are no longer enough.

Add tests that prove:

1. `NodeManager::run()` installs and executes the free-object flush timer.
2. `free_objects_period_milliseconds` from config changes runtime behavior.
3. Threshold-driven spilling is triggered only when object-store usage crosses the configured threshold.
4. An empty `object_spilling_config` disables spill triggering, matching C++.
5. Flushing actually frees real object-manager-backed objects, not just entries in `pending_deletion`.

Files:

- `rust/ray-raylet/tests/integration_test.rs`

Recommendation:

- These tests should be treated as merge-blocking for any claim of IO-worker parity closure.

## Bottom Line

The current state is better than V7 says.

Specifically:

- production config wiring is now present
- production spillability wiring is now present

But parity is still not closed because Rust still lacks the C++ runtime loop that:

- flushes freed objects periodically
- triggers spilling periodically when over threshold
- honors `free_objects_period_milliseconds`
- executes the full `FlushFreeObjects()` side effects

The correct statement today is:

**Rust has now closed the earlier production-constructor and spillability-callback gaps, but it still does not match the C++ `NodeManager` runtime scheduling and flush semantics for local object management. IO-worker parity is therefore still not closed.**
