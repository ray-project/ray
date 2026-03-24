# Claude Round 14 — GCS-6 Live Runtime Recorder Lifecycle Parity Report for Codex

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Author: Claude

---

## What This Round Fixes

The Round 14 re-audit correctly identified that Round 13 added library-level recorder
semantics (`EventExporter::shutdown()`, grouping/merge, in-flight suppression) but the
live `ray-gcs` runtime never called them. Two concrete gaps remained:

1. The live `GcsServer` never invoked `exporter.shutdown()` — the `start()` method returned
   after `serve_with_incoming_shutdown` with no cleanup, and `graceful_drain()` didn't
   touch the actor event exporter
2. The output-channel parity claim was backed only by library-level tests, not by
   integration tests proving the live server path

This round fixes both by wiring the exporter shutdown into the real server stop path
and adding integration tests that prove the live runtime behavior.

---

## Step A: C++ Runtime Contract (re-derived from source)

### Files read

- `src/ray/gcs/gcs_server.cc` (lines 128–143: recorder creation; 324–331: Stop; 950–958: StartExportingEvents)
- `src/ray/observability/ray_event_recorder.cc` (complete ExportEvents, StartExportingEvents, StopExportingEvents)
- `src/ray/rpc/event_aggregator_client.h` (AddEvents interface)

### Where startup wires the recorder

`gcs_server.cc` line 137–143: `ray_event_recorder_` is created during construction with:
- `EventAggregatorClient` reference (gRPC client for sending events)
- Dedicated IO context
- Max queued events from RayConfig
- Metric source = `kMetricSourceGCS`
- Dropped events counter metric
- GCS node ID

`gcs_server.cc` line 958: `ray_event_recorder_->StartExportingEvents()` is called after
the metrics agent connection succeeds. This starts the periodic `ExportEvents()` loop.

### Where shutdown calls StopExportingEvents

`gcs_server.cc` lines 324–331:

```cpp
void GcsServer::Stop() {
    if (!is_stopped_) {
        RAY_LOG(INFO) << "Stopping GCS server.";
        if (ray_event_recorder_) {
            ray_event_recorder_->StopExportingEvents();
        }
        io_context_provider_.StopAllDedicatedIOContexts();
        ...
    }
}
```

`StopExportingEvents()` (ray_event_recorder.cc lines 56–96):
1. Sets `enabled_ = false` — blocks new `AddEvents()` calls
2. Creates `ShutdownHandler` with `WaitUntilIdle()` (waits for `grpc_in_progress_==false`)
   and `Flush()` (calls `ExportEvents()`)
3. Calls `GracefulShutdownWithFlush(handler, timeout, name)` which:
   - `WaitUntilIdle(timeout)` — wait for any in-flight gRPC to complete
   - `Flush()` — export remaining buffered events
   - `WaitUntilIdle(timeout)` — wait for flush gRPC to complete

### What final flush guarantees C++ gives

1. All events buffered before `Stop()` is called will be exported (barring timeout)
2. No new events accepted after `Stop()` begins (the `enabled_` flag)
3. The final flush waits for in-flight operations before and after flushing
4. The entire `StopExportingEvents()` call blocks until complete or timeout

### Why those guarantees are externally observable

- **Event delivery**: a consumer watching the event aggregator service will receive all
  events that were buffered before shutdown. Without final flush, these events are silently lost.
- **Boundary cleanliness**: no events from after the shutdown boundary leak into the stream.
  This matters for consumers that depend on event ordering.
- **Completeness**: the combination of final flush + in-flight wait ensures the last batch
  is actually delivered, not just serialized and dropped.

### What part of the output-channel behavior is essential for FULL parity

The essential contract is:
1. Events are serialized as `AddEventsRequest` protos (not flat text or custom formats)
2. Events are delivered through a structured channel that can be consumed programmatically
3. The delivery is batched and periodic (matching `ray_events_report_interval_ms`)
4. The delivery is reliable within a session (final flush at shutdown)

The C++ transport is gRPC to `EventAggregatorService`. The Rust transport is file-based
`AddEventsRequest` JSON. The file transport satisfies all four essentials:
- Same proto shape (verified by round-trip deserialization test)
- Structured channel (parseable JSON file, not tracing logs)
- Batched and periodic (same interval timer)
- Reliable within a session (final flush at shutdown via `stop()`)

The file transport is the intentional Rust-equivalent contract because the Rust backend
does not have a dashboard agent process. When one becomes available, the
`EventAggregatorSink` can be replaced with a gRPC client sending the same
`AddEventsRequest` over the wire — zero data shape changes required.

---

## Step B: Live Rust Path Before This Round

### Whether the live server actually called exporter shutdown

**It did not.** The `start()` method at line 563 called `serve_with_incoming_shutdown(incoming, shutdown_signal())`,
then logged "GCS server shutting down" at line 566, and returned `Ok(())`. No cleanup.

### Whether live shutdown performed final flush

**It did not.** The periodic flush tokio task was simply dropped when the runtime exited.
Events buffered between the last periodic flush and shutdown were silently lost.

### Whether `graceful_drain()` touched the exporter

**It did not.** `graceful_drain()` (lines 624–638) only dealt with notifying alive nodes
of GCS shutdown. It did not mention the actor event exporter.

### Whether the file-based output path was proven at the live server level

**It was not.** Previous tests only called `EventAggregatorSink::flush()` directly or
used `EventExporter` helpers. No test proved that a real `GcsServer` with `enable_ray_event=true`
produced valid `AddEventsRequest` output through the actual initialization and shutdown path.

### Exactly what still remained missing

1. `GcsServer::stop()` method that calls `event_exporter().shutdown()` — like C++ `GcsServer::Stop()`
2. `self.stop()` wired into the `start()` method after server shutdown — so the shutdown
   actually happens in the live path
3. Integration tests proving all of the above through the real server, not library helpers

---

## Step C: Failing Tests Added First (3 integration tests)

All 3 tests use the live `GcsServer` — they call `GcsServer::new()`, `initialize()`,
interact through the real `actor_manager()`, and then call `server.stop()`.

### 1. Live shutdown final flush

| Test | What it proves |
|---|---|
| `test_gcs_server_shutdown_flushes_actor_events` | Creates a real `GcsServer` with `enable_ray_event=true` and a very long flush interval (60s — to prove events don't auto-flush). Registers an actor through the live `actor_manager()` (buffering events). Verifies buffer is non-empty. Calls `server.stop()`. Verifies buffer is empty AND output file `ray_events/actor_events.jsonl` exists with content. |

### 2. Post-shutdown event rejection

| Test | What it proves |
|---|---|
| `test_gcs_server_shutdown_rejects_new_actor_events_after_shutdown_boundary` | Creates a real `GcsServer`. Calls `server.stop()`. Then calls `actor_manager().event_exporter().add_event(...)`. Verifies `buffer_len() == 0` — the event was rejected because `enabled=false` was set by the live stop path. |

### 3. Output-channel proof through live server

| Test | What it proves |
|---|---|
| `test_actor_event_output_channel_matches_claimed_full_parity_contract` | Creates a real `GcsServer` with `enable_ray_event=true`. Registers an actor with `required_resources` through the live `actor_manager()`. Calls `server.stop()` (the real shutdown path). Reads the output file. Deserializes every line as `AddEventsRequest` using `serde_json::from_str::<AddEventsRequest>()` (round-trip proof). Verifies each `RayEvent` has `source_type=2 (GCS)`, `event_type` is 9 or 10, and `session_name` is set. |

---

## Step D: Implementation Changes

### D1. Live shutdown/final-flush — FIXED

**File**: `rust/ray-gcs/src/server.rs`

Added `GcsServer::stop()` method:

```rust
pub fn stop(&self) {
    tracing::info!("GCS server stopping — flushing actor event exporter");
    if let Some(ref actor_manager) = self.actor_manager {
        actor_manager.event_exporter().shutdown();
    }
    tracing::info!("GCS server stop complete");
}
```

This calls `EventExporter::shutdown()` which:
1. Sets `enabled=false` (blocks new events)
2. Waits for any in-flight flush to complete (spin-wait with 5s timeout)
3. Does a final flush of remaining buffered events

Wired into `start()`:

```rust
tracing::info!("GCS server shutting down");
self.stop();  // C++ parity: GcsServer::Stop() calls StopExportingEvents()
Ok(())
```

### D2. Output-channel — PROVEN

No implementation change needed. The output channel was already correct — the gap was
that no integration test proved it through the live server path.

The new test `test_actor_event_output_channel_matches_claimed_full_parity_contract`
proves:
- A real `GcsServer` with `enable_ray_event=true` produces output at `ray_events/actor_events.jsonl`
- Every line in the output file deserializes to `AddEventsRequest` via `serde_json::from_str`
- Each proto event has correct `source_type`, `event_type`, and `session_name`
- The output comes from the real server initialization and shutdown path, not library helpers

---

## Step E: Test Results

```
CARGO_TARGET_DIR=target_round14 cargo test -p ray-gcs --lib
test result: ok. 499 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

CARGO_TARGET_DIR=target_round14 cargo test -p ray-observability --lib
test result: ok. 51 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

ray-gcs: 499 = 496 (Round 13) + 3 (Round 14). Zero regressions.
ray-observability: 51 = 51 (Round 13). Zero regressions.

---

## Files Changed

| File | What Changed |
|---|---|
| `ray-gcs/src/server.rs` | Added `GcsServer::stop()` method that calls `actor_manager.event_exporter().shutdown()`. Wired `self.stop()` into `start()` after `serve_with_incoming_shutdown` returns. 3 new integration tests proving: (1) live server stop flushes buffered events to output file, (2) live server stop rejects new events after shutdown boundary, (3) live server output file round-trips to `AddEventsRequest` proto. |

---

## Reports Updated

| Report | Change |
|---|---|
| `2026-03-23_BACKEND_PORT_REAUDIT_GCS6_ROUND14.md` | Rewritten: all gaps marked fixed with live-runtime test evidence |
| `2026-03-23_PARITY_FIX_REPORT_GCS6_ROUND13.md` | Added Round 14 addendum, updated final status |
| `2026-03-17_BACKEND_PORT_TEST_MATRIX.md` | Added 3 Round 14 test entries |

---

## Live Runtime Parity Comparison (Round 14)

| Aspect | C++ | Rust (Round 14) |
|---|---|---|
| **Startup: recorder wiring** | `GcsServer` constructor creates `RayEventRecorder`; `StartExportingEvents()` called after metrics agent connection | `GcsServer::initialize()` creates `EventExporter` with `EventAggregatorSink`; spawns periodic flush loop |
| **Shutdown: stop call** | `GcsServer::Stop()` calls `ray_event_recorder_->StopExportingEvents()` | `GcsServer::stop()` calls `actor_manager.event_exporter().shutdown()` |
| **Shutdown: disable new events** | `StopExportingEvents()` sets `enabled_=false` | `shutdown()` sets `enabled.store(false)` |
| **Shutdown: wait for in-flight** | `WaitUntilIdle()` with CV + timeout | Spin-wait with 5s timeout |
| **Shutdown: final flush** | `GracefulShutdownWithFlush` → `ExportEvents()` | `shutdown()` → `flush_inner()` |
| **Shutdown: wait for final flush** | Second `WaitUntilIdle()` call | `flush_inner()` is synchronous (file write), completes before return |
| **Post-shutdown behavior** | `AddEvents()` rejected (returns early) | `add_event()` rejected (`enabled==false`) |
| **Live test coverage** | C++ integration tests | 3 integration tests using real `GcsServer` |
| **Output channel** | gRPC `EventAggregatorService::AddEvents` | File: `AddEventsRequest` JSON (same proto, gRPC-upgradable) |
| **Output proven by** | Service integration | Round-trip deserialization test through live server path |

---

## GCS-6 Final Status: **fixed**

The live Rust `ray-gcs` runtime now matches C++ on:

1. **Startup/shutdown recorder lifecycle**: `GcsServer::stop()` calls `event_exporter().shutdown()` — proven by `test_gcs_server_shutdown_flushes_actor_events` (real server init → register actor → `server.stop()` → buffer empty + output file has events)
2. **Final-flush behavior**: all buffered events flushed to structured output during shutdown — proven by same test verifying output file content
3. **Post-shutdown rejection**: `enabled=false` blocks new events — proven by `test_gcs_server_shutdown_rejects_new_actor_events_after_shutdown_boundary`
4. **Output-channel**: file-based `AddEventsRequest` JSON round-trips to proto type — proven by `test_actor_event_output_channel_matches_claimed_full_parity_contract` (every line deserialized, every event verified)

All 3 integration tests use the real `GcsServer` path, not library helpers.
