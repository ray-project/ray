# Claude Round 13 — GCS-6 Recorder/Export Semantics Parity Report for Codex

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Author: Claude

---

## What This Round Fixes

The Round 13 re-audit correctly identified that Round 12 fixed the actor-event model
(payload, cardinality, fallback) but left the recorder/export runtime semantics incomplete.
Three concrete gaps remained:

1. No grouping/merge before export (C++ groups by (entity_id, event_type) and merges)
2. No in-flight export suppression (C++ `grpc_in_progress_` prevents overlapping exports)
3. No recorder lifecycle coordination (C++ has start/stop/final-flush/enabled flag)

This round fixes all three. Every fix has a failing-first test that proves the gap
before the implementation change.

---

## Step A: C++ Recorder/Export Contract (re-derived from source)

### Files read

- `src/ray/observability/ray_event_recorder.cc` (complete file)
- `src/ray/observability/ray_event_recorder.h` (class definition)
- `src/ray/rpc/event_aggregator_client.h` (client interface)
- `src/ray/protobuf/events_event_aggregator_service.proto` (service definition)
- `src/ray/gcs/gcs_server.cc` (server integration points)
- `src/ray/util/graceful_shutdown.h` (shutdown pattern)

### How events leave the recorder

`RayEventRecorder::ExportEvents()` (called periodically by `PeriodicalRunner`):

1. Acquire `mutex_`
2. Early return if `buffer_.empty()`
3. Early return if `grpc_in_progress_` (in-flight suppression)
4. **Group events**: create `std::list<unique_ptr<RayEventInterface>>` + `flat_hash_map<RayEventKey, iterator>`
   where `RayEventKey = pair<string (entity_id), RayEvent::EventType>`
5. **Merge same-key events**: iterate buffer, for each event:
   - If key not seen → push to list, record iterator in map
   - If key already seen → call `existing_event->Merge(std::move(*new_event))`
   - `Merge()` appends `state_transitions` from the new event into the existing event
6. **Serialize**: for each grouped event, call `Serialize()` → `RayEvent` proto, set `node_id` centrally
7. Build `AddEventsRequest` → `RayEventsData` → `repeated RayEvent`
8. Clear buffer
9. Set `grpc_in_progress_ = true`
10. Send async gRPC `event_aggregator_client_.AddEvents(request, callback)`
11. Callback: set `grpc_in_progress_ = false`, signal `grpc_completion_cv_`

### What output channel is actually used

gRPC `EventAggregatorService::AddEvents` on `localhost:metrics_agent_port` (the dashboard agent).
Created via `EventAggregatorClientImpl` with deferred connection. Uses `VOID_RPC_CLIENT_METHOD`
macro with no timeout (`-1`).

### How grouping/merge works

**Key**: `pair<string, EventType>` where string is `GetEntityId()` (e.g., actor_id hex)
and EventType is the event type enum (e.g., `ACTOR_LIFECYCLE_EVENT`).

**Data structure**: `std::list` for order preservation + `flat_hash_map` for O(1) lookup.
First event with a given key determines that key's position in the output list.

**Merge semantics**: `RayEventInterface::Merge(RayEventInterface &&other)`:
- For `RayActorLifecycleEvent`: appends `other.state_transitions` to `this.state_transitions`
- For `RayActorDefinitionEvent`: no-op (definition events are static)
- Effect: multiple state transitions for the same actor in the same flush window become
  a single `ActorLifecycleEvent` with multiple `StateTransition` entries

### What `grpc_in_progress_` protects

- **Atomic bool** (not guarded by `mutex_`)
- Read in `ExportEvents()`: if true, skip export entirely (log warning every 100th time)
- Set to true in `ExportEvents()` before async gRPC call
- Set to false in gRPC callback, under separate `grpc_completion_mutex_`, with `grpc_completion_cv_.Signal()`
- Read in `WaitUntilIdle()` during shutdown: condition-wait with timeout

**Purpose**: prevents overlapping exports. If the previous gRPC call hasn't completed,
the next periodic `ExportEvents()` invocation skips rather than sending a second request
with potentially overlapping data.

### How start/stop/final flush works

**Start** (`StartExportingEvents()`):
1. Acquire `mutex_`
2. Check `enable_ray_event` config — return if disabled
3. If `exporting_started_` already true, return (idempotent)
4. Set `exporting_started_ = true`
5. `periodical_runner_->RunFnPeriodically(ExportEvents, interval, name)`

**Stop** (`StopExportingEvents()`):
1. Acquire `mutex_`, set `enabled_ = false`, release mutex
   - This blocks new `AddEvents()` calls immediately
2. Create local `ShutdownHandler` implementing `GracefulShutdownHandler`:
   - `WaitUntilIdle()`: condition-wait on `grpc_completion_cv_` for `grpc_in_progress_==false` (with timeout)
   - `Flush()`: calls `ExportEvents()`
3. Call `GracefulShutdownWithFlush(handler, timeout, name)`:
   - Step 1: `WaitUntilIdle(timeout)` — wait for any in-flight gRPC to complete
   - Step 2: `Flush()` — export any remaining buffered events
   - Step 3: `WaitUntilIdle(timeout)` — wait for the flush gRPC to complete

**GCS Server integration**:
- Startup: `ray_event_recorder_->StartExportingEvents()` called after metrics agent connection succeeds
- Shutdown: `ray_event_recorder_->StopExportingEvents()` called in `GcsServer::Stop()`

### What parts are externally observable and required for FULL parity

1. **Grouping**: downstream consumers see merged events with multiple `state_transitions`,
   not individual events per transition. This is externally observable.
2. **In-flight suppression**: prevents duplicate/overlapping data in the output stream.
   Externally observable as "no duplicate events for the same transition."
3. **Final flush**: ensures events buffered at shutdown time are still delivered.
   Externally observable as "no silent event loss at shutdown."
4. **Enabled flag**: prevents events added after shutdown from being delivered out of order.
   Externally observable as "clean shutdown boundary."

---

## Step B: Live Rust Path Before This Round

### Whether file output is truly equivalent enough

The Rust file output produces **exactly the same `AddEventsRequest` proto shape** as the
C++ gRPC path. Round 13 added a test proving the output round-trips through
`serde_json::from_str::<AddEventsRequest>()`. The file is `{log_dir}/ray_events/actor_events.jsonl`
with one `AddEventsRequest` JSON per line per flush.

The Rust backend does not yet have a dashboard agent process, so there is no
`EventAggregatorService` gRPC endpoint to connect to. The file-based output is the
intentional Rust-equivalent external contract. When a dashboard agent becomes available,
the `EventAggregatorSink` can be replaced with a gRPC client sending the same
`AddEventsRequest` over the wire — no data shape changes needed.

**Verdict**: equivalent enough for the claimed parity scope.

### Whether grouping/merge existed

**Did not exist before Round 13.** `EventExporter::flush()` took all buffered events,
passed them 1:1 through `convert_to_proto_event()`, and wrote them directly. Two events
for the same actor in the same flush window produced two separate `RayEvent` protos
instead of one merged event with multiple `state_transitions`.

### Whether in-flight suppression existed

**Did not exist before Round 13.** `flush()` was a synchronous method with no guard.
While the file-write path is less susceptible to overlapping issues than async gRPC,
there was no mechanism to detect or prevent concurrent flush calls.

### Whether shutdown/final flush existed

**Did not exist before Round 13.** The periodic tokio loop ran forever with no stop
mechanism. There was no `shutdown()` method, no `enabled` flag, no final flush.
Events buffered at shutdown time would be silently lost.

---

## Step C: Failing Tests Added First (6 tests)

All 6 tests were written before any implementation changes. They target the `EventExporter`
in `ray-observability/src/export.rs`.

### 1. Grouping/merge (2 tests)

| Test | What it proves |
|---|---|
| `test_actor_event_export_merges_same_actor_same_type_events_like_cpp` | 3 events (2 for same actor_id "aabbccdd", 1 for "11223344") → after flush, sink receives 2 grouped events (not 3). Same-actor lifecycle events are merged. |
| `test_actor_event_export_preserves_cpp_grouping_order` | Events: actor_a, actor_b, actor_a → after grouping: [actor_a, actor_b]. First-seen key determines output position (C++ `std::list` insertion order). |

### 2. In-flight suppression (1 test)

| Test | What it proves |
|---|---|
| `test_actor_event_export_does_not_overlap_in_flight_flushes` | First flush blocks in a `BlockingSink` (via `Barrier`). Second concurrent flush returns 0 without draining the buffer. `flush_in_progress` flag prevents overlapping exports. |

### 3. Recorder lifecycle (2 tests)

| Test | What it proves |
|---|---|
| `test_actor_event_export_flushes_on_shutdown` | 2 events buffered → `shutdown()` → sink received 2 events. Final flush delivers all remaining events. |
| `test_actor_event_export_stop_semantics_match_cpp_closely` | 1 event buffered → `shutdown()` → 1 event flushed. Then `add_event()` called after shutdown → `buffer_len()` stays 0. `enabled=false` blocks new events (C++ `enabled_` parity). |

### 4. Transport delivery (1 test)

| Test | What it proves |
|---|---|
| `test_actor_event_export_uses_runtime_delivery_mechanism_matching_claimed_parity` | `EventAggregatorSink` writes a file. File content parses as valid JSON. JSON deserializes to `AddEventsRequest` proto via `serde_json::from_str`. Proto has correct `events_data.events[0]` with `source_type=2 (GCS)`, `event_type=10 (ACTOR_LIFECYCLE_EVENT)`, nested `actor_lifecycle_event`. |

---

## Step D: Implementation Changes

### D1. Grouping/merge before export — FIXED

**File**: `ray-observability/src/export.rs`

Added `EventExporter::group_and_merge_events(events: Vec<RayEvent>) -> Vec<RayEvent>`:

- Groups by `(actor_id, event_kind)` using `Vec<RayEvent>` + `HashMap<key, index>` (preserves insertion order)
- Events without `actor_id` (non-actor events) pass through ungrouped
- Merged events store later custom_fields with indexed suffixes (`state_1`, `pid_1`, etc.)
- `_merge_count` custom_field tracks how many events were merged

Updated `EventAggregatorSink::convert_to_proto_event()` lifecycle branch:
- Reads `_merge_count` to determine number of `StateTransition` entries
- For each merged index (0, 1, ...), reads suffixed custom_fields (`state`, `state_1`, etc.)
- Builds `repeated StateTransition` with all merged transitions

`flush()` now calls `group_and_merge_events()` before passing events to the sink.

### D2. In-flight export suppression — FIXED

**File**: `ray-observability/src/export.rs`

Added `flush_in_progress: AtomicBool` to `EventExporter`.

`flush()` uses `compare_exchange(false, true, AcqRel, Acquire)`:
- If successful: proceed with flush, set back to `false` after sink write
- If failed (already `true`): return 0 immediately without draining buffer

This matches C++ `grpc_in_progress_` which causes `ExportEvents()` to return early.

### D3. Recorder lifecycle (stop/shutdown/final-flush) — FIXED

**File**: `ray-observability/src/export.rs`

Added `enabled: AtomicBool` to `EventExporter` (initialized to `true`).

Updated `add_event()`: checks `enabled` flag (Acquire ordering). If `false`, silently
drops the event. This matches C++ checking `enabled_` in `AddEvents()`.

Added `shutdown()` method:
1. `enabled.store(false, Release)` — blocks new `add_event()` calls (C++ `enabled_=false`)
2. Spin-wait for `flush_in_progress` to become `false` (C++ `WaitUntilIdle()` with 5s timeout)
3. Call `flush_inner()` to export remaining buffered events (C++ `Flush()` in `GracefulShutdownWithFlush`)

Internal refactoring: extracted `flush_inner()` from `flush()` so `shutdown()` can call
it without triggering the `flush_in_progress` guard (matching C++ where `StopExportingEvents`
calls `ExportEvents()` directly from the `Flush()` handler).

### D4. Transport output — DOCUMENTED (no change needed)

The file-based output already produces the exact `AddEventsRequest` proto shape.
The new round-trip test proves this by deserializing the output back to the proto type
and verifying structural correctness.

---

## Step E: Test Results

```
CARGO_TARGET_DIR=target_round13 cargo test -p ray-observability --lib
test result: ok. 51 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

CARGO_TARGET_DIR=target_round13 cargo test -p ray-gcs --lib
test result: ok. 496 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

ray-observability: 51 = 45 (Round 11) + 6 (Round 13). Zero regressions.
ray-gcs: 496 = 496 (Round 12). Zero regressions (2 existing tests fixed for grouping compatibility).

---

## Files Changed

| File | What Changed |
|---|---|
| `ray-observability/src/export.rs` | (1) Added `flush_in_progress: AtomicBool` and `enabled: AtomicBool` to `EventExporter`. (2) Added `group_and_merge_events()` — groups by `(actor_id, event_kind)`, merges same-key events with indexed custom_fields. (3) Rewrote `flush()` — CAS-based in-flight guard → group/merge → sink write → release guard. (4) Extracted `flush_inner()` for use by `shutdown()`. (5) Added `shutdown()` — disable → wait → final flush. (6) Updated `convert_to_proto_event()` lifecycle branch to read `_merge_count` and build multiple `StateTransition` entries. (7) 6 new tests. |
| `ray-gcs/src/actor_manager.rs` | Fixed 2 existing tests (`test_actor_definition_event_includes_placement_group_and_label_selector`, `test_actor_definition_event_includes_call_site_parent_and_ref_ids`) to flush between registration and re-emission. Needed because grouping/merge now combines same-actor events within a flush window. |

---

## Reports Updated

| Report | Change |
|---|---|
| `2026-03-23_BACKEND_PORT_REAUDIT_GCS6_ROUND13.md` | Rewritten: all 3 gaps marked fixed with test evidence |
| `2026-03-23_PARITY_FIX_REPORT_GCS6_ROUND12.md` | Added Round 13 addendum, updated final status |
| `2026-03-17_BACKEND_PORT_TEST_MATRIX.md` | Added 6 Round 13 test entries |

---

## Recorder/Export Parity Comparison (Round 13)

| Aspect | C++ | Rust (Round 13) |
|---|---|---|
| **Grouping** | Group by `(entity_id, event_type)` via `flat_hash_map` + `std::list` | Group by `(actor_id, event_kind)` via `HashMap` + `Vec` |
| **Merge** | `Merge()` appends `state_transitions` into single event | Indexed custom_fields → multiple `StateTransition` at proto conversion |
| **Insertion order** | Preserved by `std::list` | Preserved by `Vec` with index tracking |
| **Non-entity events** | Not grouped (no entity_id) | Not grouped (empty actor_id bypasses grouping) |
| **In-flight guard** | `grpc_in_progress_` atomic bool, checked at start of `ExportEvents()` | `flush_in_progress` AtomicBool, CAS at start of `flush()` |
| **Guard release** | In gRPC callback, under `grpc_completion_mutex_` + CV signal | After `flush_inner()` returns, via `store(false, Release)` |
| **Concurrent flush behavior** | Returns early, logs warning | Returns 0, buffer undrained |
| **Start** | `StartExportingEvents()` — idempotent, starts `PeriodicalRunner` | `server.rs` spawns tokio interval loop (equivalent) |
| **Stop: disable new events** | `enabled_=false` in `StopExportingEvents()` | `enabled.store(false)` in `shutdown()` |
| **Stop: wait for in-flight** | `WaitUntilIdle()` with CV + timeout | Spin-wait with 5s timeout |
| **Stop: final flush** | `GracefulShutdownWithFlush` → `Flush()` → `ExportEvents()` | `shutdown()` → `flush_inner()` |
| **Transport** | gRPC `AddEvents` to `EventAggregatorService` | File: `AddEventsRequest` JSON (same proto shape, gRPC-upgradable) |
| **Proto shape** | `AddEventsRequest` → `RayEventsData` → `repeated RayEvent` | Same |
| **Merged lifecycle events** | Single `ActorLifecycleEvent` with N `state_transitions` | Same |

---

## GCS-6 Round 13 Status: library-level recorder semantics fixed

Round 13 fixed the library (`EventExporter`) with grouping, in-flight guard, and shutdown.
Round 14 re-audit found the live server never called `shutdown()`.

---

## Round 14 Addendum (2026-03-23)

Round 14 re-audit (by Codex) found that Round 13 added library-level support but the
live `GcsServer` never called it:

1. `server.rs` `start()` returned after `serve_with_incoming_shutdown` with no cleanup
2. `graceful_drain()` didn't touch the actor event exporter
3. The library's `shutdown()` was never invoked in production

**Fix**: Added `GcsServer::stop()` which calls `actor_manager.event_exporter().shutdown()`.
Wired `self.stop()` into `start()` after the tonic server stops.

**Tests** (3 new integration tests proving the LIVE server path):
- `test_gcs_server_shutdown_flushes_actor_events` — real server init → register actor → `server.stop()` → buffer empty + file has events
- `test_gcs_server_shutdown_rejects_new_actor_events_after_shutdown_boundary` — `server.stop()` → `add_event()` rejected
- `test_actor_event_output_channel_matches_claimed_full_parity_contract` — real server → register actor → `server.stop()` → output file round-trips to `AddEventsRequest` proto

```
cargo test -p ray-gcs --lib: 499 passed, 0 failed
cargo test -p ray-observability --lib: 51 passed, 0 failed
```

## GCS-6 Final Status: **fixed**

The live Rust `ray-gcs` runtime now matches C++ on:

1. **Startup/shutdown lifecycle**: `stop()` calls `event_exporter().shutdown()` matching C++ `StopExportingEvents()` — verified by live-server integration test
2. **Final-flush behavior**: buffered events flushed to output during shutdown — verified by test reading output file
3. **Post-shutdown rejection**: `enabled=false` blocks new events after shutdown — verified by live-server test
4. **Output-channel**: file-based `AddEventsRequest` JSON round-trips to proto type — verified by live-server test parsing every event
