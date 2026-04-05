# Codex Re-Audit: Claude Session Report IO-Worker Implementation V7

**Date:** 2026-04-04
**Audited report:** `/Users/istoica/src/ray/rust/reports/2026-04-04_SESSION_REPORT_IO_WORKER_IMPLEMENTATION_V6.md`
**Result:** Claude correctly narrowed the remaining gap, but parity is still not closed.

## Executive Verdict

Claude's V6 report is substantially accurate.

The two source-level issues from the prior audit are now addressed in isolation:

- Rust now has an `is_plasma_object_spillable` predicate hook.
- Rust now validates `max_spilling_file_size_bytes >= min_spilling_size` at construction time.

However, those changes are not enough to close parity, because the production Rust path still does not wire them in.

The main remaining gap is not the existence of the hook. It is the missing end-to-end integration:

1. `NodeManager` still constructs `LocalObjectManager` with `LocalObjectManagerConfig::default()`.
2. `NodeManager` still does not call `set_is_plasma_object_spillable(...)`.
3. As a result, the real Rust raylet path still does not use the C++-equivalent spillability decision.
4. The real Rust raylet path also still does not pass through the actual configured local-object-manager parameters that C++ supplies at construction.

My recommendation is firm: do not declare IO-worker parity closed yet.

## What Claude Fixed Correctly

### 1. Spillability hook now exists in Rust

Rust `LocalObjectManager` now contains:

- `is_plasma_object_spillable: Option<Arc<dyn Fn(&ObjectID) -> bool + Send + Sync>>`
- `set_is_plasma_object_spillable(...)`
- predicate-based filtering in `try_select_objects_to_spill()`

That closes the earlier "missing capability" gap at the type and method level.

Evidence:

- Rust field: `rust/ray-raylet/src/local_object_manager.rs:161`
- Rust selection hook: `rust/ray-raylet/src/local_object_manager.rs:361`
- Rust setter: `rust/ray-raylet/src/local_object_manager.rs:625`
- C++ reference: `src/ray/raylet/local_object_manager.h:61`, `src/ray/raylet/local_object_manager.h:365`, `src/ray/raylet/local_object_manager.cc:196`

### 2. Construction-time validation is now present

Rust now validates the same basic invariant that C++ enforces for spill-file size configuration.

Evidence:

- Rust validation: `rust/ray-raylet/src/local_object_manager.rs:170`
- C++ validation: `src/ray/raylet/local_object_manager.h:89`

## Remaining Gaps

### 1. Production spillability wiring is still missing

This is the highest-severity remaining issue.

C++ does not merely define the predicate. It wires the real object-manager-backed callback into `LocalObjectManager` construction:

- `object_manager->IsPlasmaObjectSpillable(object_id)`

Rust does not do this on the production path I audited.

The real Rust `NodeManager` path still does:

```rust
LocalObjectManager::new(LocalObjectManagerConfig::default())
```

and then only wires the IO worker pool. It does **not** set the spillability predicate.

Evidence:

- Rust production construction: `rust/ray-raylet/src/node_manager.rs:465`
- Rust only wires IO worker pool: `rust/ray-raylet/src/node_manager.rs:472`
- No production `set_is_plasma_object_spillable(...)` call found
- C++ production wiring: `src/ray/raylet/main.cc:872`
- C++ spillability callback wiring: `src/ray/raylet/main.cc:889`

Why this matters:

- The Rust hook exists but is inactive on the real path.
- The real Rust raylet still defaults to "all eligible objects are spillable."
- That means the previously identified behavioral parity gap still exists in production.

This should be treated as merge-blocking if the claim is C++ parity.

### 2. Production config wiring for `LocalObjectManager` is still not parity-complete

This is the second major gap.

C++ constructs `LocalObjectManager` with live configuration:

- `free_objects_batch_size`
- `free_objects_period_milliseconds`
- `max_io_workers`
- `max_fused_object_count`
- spillability callback
- and other runtime dependencies

Rust still constructs `LocalObjectManager` with `LocalObjectManagerConfig::default()` in the real path.

Evidence:

- Rust production construction with defaults: `rust/ray-raylet/src/node_manager.rs:465`
- C++ production construction with real values: `src/ray/raylet/main.cc:872`
- C++ `max_fused_object_count` wiring: `src/ray/raylet/main.cc:883`
- C++ `max_io_workers` wiring: `src/ray/raylet/main.cc:880`

Why this matters:

- Even though Rust added new config fields, the production path is still not using the real runtime values.
- So several newly added parity mechanisms may be correct in unit tests but still inactive or misconfigured in the real raylet.

This is broader than the spillability callback alone.

## Assessment Of Claude's Report

Claude's V6 report is more accurate than the previous versions because it explicitly admits the missing production wiring.

That said, the framing is still slightly too soft. The report lists production wiring as a remaining item, but source review shows it is not optional polish. It is the current blocker to claiming parity:

- the capability exists
- the production path does not use it
- the production path still uses default config instead of the C++-equivalent constructor inputs

So the correct engineering interpretation is:

"The local object manager parity hooks now exist in Rust, but production raylet integration is still incomplete."

## Prescriptive Closeout Plan

### Priority 1: Replace `LocalObjectManagerConfig::default()` in production

Build `LocalObjectManagerConfig` from the real Rust raylet configuration instead of defaults.

At minimum, wire through:

1. `max_io_workers`
2. `max_fused_object_count`
3. `max_spilling_file_size_bytes`
4. `min_spilling_size`
5. `free_objects_batch_size`
6. `object_spilling_threshold`

Files to update first:

- `rust/ray-raylet/src/node_manager.rs`
- any Rust config source that should own these values

Recommendation:

- Do not leave hidden defaults on the production path.
- Make the production constructor call explicit and reviewable.

### Priority 2: Wire the real spillability predicate

Thread a production predicate into `LocalObjectManager` that matches the C++ intent as closely as Rust currently can.

Required work:

1. Identify the Rust component that owns the equivalent of `IsPlasmaObjectSpillable(object_id)`.
2. Expose a closure or trait method from that component.
3. Call `set_is_plasma_object_spillable(...)` immediately after `LocalObjectManager` construction.
4. Add integration coverage verifying the predicate is actually invoked on the production path.

Files to update first:

- `rust/ray-raylet/src/node_manager.rs`
- the Rust object manager / store integration layer that can answer spillability

### Priority 3: Add end-to-end tests for production wiring

The current unit coverage is not enough.

Add tests that validate:

1. production `NodeManager` creates `LocalObjectManager` with non-default values when config differs from defaults
2. spillability predicate is installed on the production path
3. non-spillable objects are rejected during real spill selection
4. `max_fused_object_count` and `max_spilling_file_size_bytes` use production config values, not hardcoded defaults

Files to update:

- `rust/ray-raylet/src/node_manager.rs`
- integration tests around local object manager and object store interaction

## Merge Recommendation

Do not use any of the following language yet:

- "no remaining gaps"
- "IO-worker parity closed"
- "full C++ parity complete"

The strongest defensible statement today is:

"Rust now has the local-object-manager spillability and validation hooks, but the production raylet path still does not wire them in or construct `LocalObjectManager` from real runtime config. Full parity is therefore not yet closed."
