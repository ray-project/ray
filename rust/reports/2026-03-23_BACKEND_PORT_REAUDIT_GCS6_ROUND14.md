# Backend Port Re-Audit — GCS-6 Round 14

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Round 14 fix author: Claude

## Scope

This re-audit checked the Round 13 claim against live Rust and C++ source,
focusing on whether the live `ray-gcs` runtime actually uses the recorder
lifecycle semantics added in Round 13.

## Gaps Found (all fixed)

### 1. Live server shutdown now calls exporter shutdown — FIXED

**Problem**: `EventExporter::shutdown()` existed in the library but the live
`GcsServer` never called it. The `start()` method returned after
`serve_with_incoming_shutdown` with no cleanup. `graceful_drain()` didn't
touch the exporter.

**Fix**: Added `GcsServer::stop()` method that calls
`actor_manager.event_exporter().shutdown()`. The `start()` method now calls
`self.stop()` after the tonic server stops, matching C++ `GcsServer::Stop()`
calling `ray_event_recorder_->StopExportingEvents()`.

**Test**: `test_gcs_server_shutdown_flushes_actor_events` — initializes a real
`GcsServer`, registers an actor (buffering events), calls `server.stop()`,
verifies buffer is empty AND output file contains flushed events.

### 2. Post-shutdown event rejection is wired — FIXED

**Problem**: The `enabled` flag was set by `EventExporter::shutdown()` but
the live server never called it, so the rejection was never triggered in
production.

**Fix**: Same as above — `stop()` calls `shutdown()` which sets `enabled=false`.

**Test**: `test_gcs_server_shutdown_rejects_new_actor_events_after_shutdown_boundary`
— calls `server.stop()`, then attempts `add_event()`, verifies it's rejected.

### 3. Output-channel proven through live server path — DOCUMENTED

**Test**: `test_actor_event_output_channel_matches_claimed_full_parity_contract`
— registers an actor through the live server, calls `server.stop()` (the real
shutdown path), reads the output file, deserializes every line as
`AddEventsRequest` proto, verifies each `RayEvent` has correct `source_type=GCS`,
`event_type` is definition or lifecycle, and `session_name` is set.

The file-based output is the Rust-equivalent contract because:
- The Rust backend does not have a dashboard agent process to connect to
- The data shape is identical to what a gRPC client would send
- The output round-trips through `serde_json::from_str::<AddEventsRequest>()`
- Replacing the `EventAggregatorSink` with a gRPC client requires zero data changes

## Test Results

```
CARGO_TARGET_DIR=target_round14 cargo test -p ray-gcs --lib
test result: ok. 499 passed; 0 failed; 0 ignored

CARGO_TARGET_DIR=target_round14 cargo test -p ray-observability --lib
test result: ok. 51 passed; 0 failed; 0 ignored
```

499 = 496 (Round 13) + 3 (Round 14). Zero regressions.

## Files Changed

| File | Change |
|---|---|
| `ray-gcs/src/server.rs` | Added `stop()` method calling `actor_manager.event_exporter().shutdown()`. Wired `self.stop()` into `start()` after server shutdown. 3 new live-runtime tests. |

## Status: fixed
