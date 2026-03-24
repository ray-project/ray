# Claude Remaining Plan — GCS-6 Round 10

Date: 2026-03-22
Branch: `cc-to-rust-experimental`
Author: Codex

## Bottom Line

Do not claim FULL parity yet.

The remaining issue is narrow but real:

- Rust now implements file export for actor lifecycle events.
- Rust still does not appear to implement the full live `enable_ray_event`
  aggregator path equivalent to C++.

## Required Objective

Finish the actor-event `enable_ray_event` path so that it is not just buffered
in memory, but actually wired to a live sink / flush path with observable output
behavior close enough to the C++ backend.

## Read First

### C++ sources

- `src/ray/gcs/actor/gcs_actor.cc`
- `src/ray/observability/ray_event_recorder.cc`
- `src/ray/common/ray_config_def.h`
- `src/ray/util/event.cc`

### Rust sources

- `rust/ray-gcs/src/actor_manager.rs`
- `rust/ray-observability/src/export.rs`

## What is still missing

### 1. Runtime sink wiring

I do not see live Rust `ray-gcs` code doing any of:

- `event_exporter.set_sink(...)`
- `event_exporter.start_periodic_flush(...)`

Without that, `enable_ray_event` still looks like an in-memory buffer only.

### 2. Runtime flush lifecycle

The current Rust tests prove buffering and file writes.
They do not prove that actor lifecycle events flow through a live runtime exporter
path equivalent to the C++ ray-event recorder behavior.

## Required workflow

### A. Re-derive the exact C++ aggregator-path contract

Write down:

- what `enable_ray_event` does in `gcs_actor.cc`
- what `RayEventRecorder::AddEvents(...)` feeds
- how events leave the recorder in the live C++ runtime
- what observable behavior must be preserved for parity

### B. Add failing tests first

Do not change implementation first.

Add targeted tests for:

1. `test_actor_ray_event_path_uses_real_sink_when_enabled`
2. `test_actor_ray_event_path_flushes_buffer_through_runtime_exporter`
3. `test_actor_ray_event_path_not_just_buffer_len`

If exact names differ, keep the semantics identical.

The tests must fail under the current implementation.

The tests must prove more than:

- `buffer_len() > 0`

They must prove:

- a real sink is attached
- a flush actually happens
- events become observable through that path

### C. Implement the missing runtime integration

You need to wire the live Rust actor-event path so that:

1. when `enable_ray_event=false`, no aggregator-path export runs
2. when `enable_ray_event=true`, actor lifecycle events go through a real runtime sink path
3. the runtime exporter is actually started / flushed
4. the behavior is not test-only

Do not stop at:

- adding a sink only in tests
- adding a helper method without runtime wiring
- leaving events in memory indefinitely

### D. Re-run validation

Run at least:

- targeted `ray-gcs` event tests
- targeted `ray-observability` exporter tests
- `cargo test -p ray-gcs --lib`
- `cargo test -p ray-observability --lib`

Use isolated `CARGO_TARGET_DIR` values if needed.

## Acceptance criteria

Only call this fixed if all of the following are true:

1. actor file-export path still works
2. actor `enable_ray_event` path is wired to a live sink/flush path
3. tests prove actual observable aggregator-style behavior, not just in-memory buffering
4. the implementation holds up under source-level re-audit

If any of those is missing, status must remain:

- `still open`
