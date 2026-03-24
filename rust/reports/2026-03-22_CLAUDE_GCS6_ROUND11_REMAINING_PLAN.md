# Claude Remaining Plan — GCS-6 Round 11

Date: 2026-03-22
Branch: `cc-to-rust-experimental`
Author: Codex

## Bottom Line

Do not claim FULL parity yet.

Round 10 fixed runtime sink/flush wiring, but the live Rust `enable_ray_event` path
still does not match the C++ output channel closely enough.

The remaining issue is:

- C++ sends actor events to the event aggregator service.
- Rust currently sends them to `LoggingEventSink`.

That is still not FULL parity.

## Required Objective

Finish the actor-event `enable_ray_event` feature so that the live Rust runtime matches
the C++ observable output contract closely enough, not just the buffer/flush lifecycle.

## Read First

### C++ sources

- `src/ray/gcs/actor/gcs_actor.cc`
- `src/ray/observability/ray_event_recorder.cc`
- `src/ray/rpc/event_aggregator_client.h`
- `src/ray/protobuf/events_event_aggregator_service.proto`

### Rust sources

- `rust/ray-gcs/src/server.rs`
- `rust/ray-gcs/src/actor_manager.rs`
- `rust/ray-observability/src/export.rs`
- `rust/ray-observability/src/gcs_sink.rs`

## What is still missing

### 1. Output-channel parity

`LoggingEventSink` is not enough for FULL parity if the C++ feature surface is:

- structured event delivery
- to an aggregator service
- through a specific runtime path

### 2. Observable behavior parity

The current tests prove:

- sink attached
- buffer drains
- periodic flush occurs

They do not yet prove:

- output behavior close enough to the C++ aggregator-service path

## Required workflow

### A. Re-derive the exact C++ observable contract

Write down:

- what data is sent to the aggregator service
- what shape the output has
- what a user enabling `enable_ray_event` can observe
- what minimum Rust behavior must exist to be "close enough" for FULL parity

### B. Add failing tests first

Do not change implementation first.

Add targeted tests for:

1. `test_actor_ray_event_path_uses_structured_runtime_output_not_just_logs`
2. `test_actor_ray_event_path_preserves_expected_event_shape_through_sink`
3. `test_actor_ray_event_path_matches_intended_observable_channel`

If exact names differ, keep the semantics identical.

The tests must fail under the current `LoggingEventSink`-based implementation if it
is not actually equivalent enough.

### C. Implement the missing output behavior

You need to choose one of these and justify it explicitly:

1. Implement a Rust sink/output path that is genuinely close enough to the C++
   event-aggregator contract

or

2. Prove, with code references and tests, that a different Rust sink is observably
   equivalent for the product surface being claimed

Do not:

- stop at tracing logs if they are not equivalent enough
- leave the current gap and rename it "architectural difference"
- claim FULL parity while the output channel still differs materially

### D. Re-run validation

Run at least:

- targeted `ray-gcs` event tests
- targeted `ray-observability` tests
- `cargo test -p ray-gcs --lib`
- `cargo test -p ray-observability --lib`

Use isolated `CARGO_TARGET_DIR` values if needed.

## Acceptance criteria

Only call this fixed if all of the following are true:

1. actor `enable_ray_event` path is live in runtime
2. it uses a real sink and flush path
3. the output channel / observable behavior is close enough to the C++ aggregator path
4. tests prove that, and the implementation survives source-level re-audit

If any of those is missing, status must remain:

- `still open`
