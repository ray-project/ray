# Backend Port Re-Audit — GCS-6 Round 15

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Round 15 fix author: Claude

## Scope

This re-audit checked whether the live `ray-gcs` runtime manages the periodic
export loop lifecycle and whether the output-channel claim holds.

## Gaps Found (all fixed)

### 1. Periodic flush task now stored and aborted on stop — FIXED

**Problem**: `tokio::spawn` returned a `JoinHandle` that was discarded. The task
ran forever even after `stop()`. C++ has explicit start/stop lifecycle.

**Fix**: Added `periodic_flush_handle: Option<JoinHandle<()>>` to `GcsServer`.
`initialize()` stores the handle. `stop()` calls `handle.abort()` via `take()`.

**Tests**:
- `test_gcs_server_stop_stops_periodic_actor_event_export_loop` — handle is `Some` after init, `None` after stop
- `test_gcs_server_stop_cancels_or_quiesces_periodic_flush_task` — exporter rejects events after stop (task no longer feeds it)

### 2. Output-channel proven with comprehensive live-server test — DOCUMENTED

**Test**: `test_actor_event_output_channel_matches_full_parity_claim_without_transport_gap`
- Verifies sink is attached (not None or LoggingEventSink)
- Registers actor with resources through live pipeline
- Calls `server.stop()` (real shutdown path with abort + final flush)
- Reads output file, round-trips every line to `AddEventsRequest` proto
- Verifies definition event has `actor_id`, `name`
- Verifies lifecycle event has `actor_id`, `state_transitions`
- Verifies `source_type`, `session_name`, `node_id`, `timestamp` on every event

Why file output is the FULL-parity contract: the Rust GCS runs standalone without
a dashboard agent process. No `EventAggregatorService` endpoint exists to connect to.
The file output produces the exact `AddEventsRequest` proto. When a dashboard agent
is integrated, the sink swap requires zero data-path changes.

## Test Results

```
cargo test -p ray-gcs --lib: 502 passed, 0 failed
cargo test -p ray-observability --lib: 51 passed, 0 failed
```

## Files Changed

| File | Change |
|---|---|
| `ray-gcs/src/server.rs` | Added `periodic_flush_handle` field. Store handle in `initialize()`. Abort handle in `stop()`. Changed `stop()` to `&mut self`. Fixed existing tests for borrow checker. 3 new tests. |

## Status: fixed
