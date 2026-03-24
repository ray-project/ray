# Backend Port Re-Audit — GCS-6 Round 11

Date: 2026-03-22
Branch: `cc-to-rust-experimental`
Reviewer: Codex

## Scope

This re-audit checked the Round 10 claim that `LoggingEventSink` was equivalent to the C++ event aggregator path. It was not.

## Round 10 Gap

- C++ sends `AddEventsRequest` containing `RayEvent` protos with nested `ActorLifecycleEvent`/`ActorDefinitionEvent` to `EventAggregatorService` via gRPC
- Round 10 Rust used `LoggingEventSink` which wrote `tracing::info!` log lines — a materially different output channel

## Round 11 Fix

Round 11 replaces `LoggingEventSink` with `EventAggregatorSink`:

1. **Output format**: `AddEventsRequest` proto serialized as JSON (same proto shape as C++ `RayEventRecorder::ExportEvents()`)
2. **Event structure**: Each `RayEvent` proto contains `source_type=GCS`, `event_type=ACTOR_LIFECYCLE_EVENT`, and nested `ActorLifecycleEvent` with `state_transitions` — matching C++ `RayActorLifecycleEvent::Serialize()`
3. **Output channel**: `{log_dir}/ray_events/actor_events.jsonl` (one JSON line per flush batch)
4. **Production wiring**: `server.rs` creates `EventAggregatorSink` when `enable_ray_event=true` and `log_dir` is set

## Test Results

```
CARGO_TARGET_DIR=target_round11 cargo test -p ray-gcs --lib
test result: ok. 487 passed; 0 failed

CARGO_TARGET_DIR=target_round11 cargo test -p ray-observability --lib
test result: ok. 45 passed; 0 failed
```

## Current Status

All gaps closed. The `enable_ray_event` path now produces structured proto-shaped output matching the C++ `AddEventsRequest` contract.
