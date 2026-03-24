# Claude Remaining Plan — GCS-6 Round 13

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Author: Codex

## Bottom Line

Do not claim FULL parity yet.

The actor-event payload model is closer now, but the recorder/export semantics still do
not match C++ closely enough for FULL parity.

## Remaining Work

### 1. Transport parity

Current Rust behavior:

- writes `AddEventsRequest` JSON to `ray_events/actor_events.jsonl`

C++ behavior:

- sends `AddEventsRequest` to the event aggregator service over gRPC

You need to decide and prove one of these:

1. implement a real Rust aggregator-service client path close enough to C++

or

2. prove with source-backed reasoning and tests that the file-based path is intentionally
   the Rust-equivalent external contract being claimed

Do not simply assert that "same proto shape" is enough.

Suggested tests:

- `test_actor_ray_event_path_uses_runtime_delivery_mechanism_matching_claimed_parity`

### 2. Grouping/merge parity

C++ groups and merges events by `(entity_id, event_type)` before export.
Rust does not.

You need to add failing tests for:

- `test_actor_event_export_merges_same_actor_same_type_events_like_cpp`
- `test_actor_event_export_preserves_cpp_grouping_order`

Then implement the missing grouping/merge behavior if the tests confirm the gap.

### 3. Recorder lifecycle parity

C++ has:

- start
- stop
- final flush
- in-flight export suppression

Rust currently has only:

- periodic flush loop

You need to add failing tests for:

- `test_actor_event_export_does_not_overlap_in_flight_flushes`
- `test_actor_event_export_flushes_on_shutdown`
- `test_actor_event_export_stop_semantics_match_cpp_closely`

Then implement the missing lifecycle coordination if the tests confirm the gap.

## Required workflow

1. Read C++ first:
   - `src/ray/observability/ray_event_recorder.cc`
   - `src/ray/gcs/gcs_server.cc`
   - `src/ray/rpc/event_aggregator_client.h`

2. Add failing tests first.

3. Fix the live Rust path in:
   - `rust/ray-observability/src/export.rs`
   - `rust/ray-gcs/src/server.rs`
   - any new runtime sink/client wiring needed

4. Re-run:
   - targeted `ray-gcs` tests
   - targeted `ray-observability` tests
   - `cargo test -p ray-gcs --lib`
   - `cargo test -p ray-observability --lib`

5. Update:
   - `rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
   - `rust/reports/2026-03-23_BACKEND_PORT_REAUDIT_GCS6_ROUND13.md`
   - `rust/reports/2026-03-23_PARITY_FIX_REPORT_GCS6_ROUND12.md`

## Acceptance Criteria

Only call this fixed if all of the following are true:

1. the claimed output channel is actually equivalent enough
2. grouping/merge behavior matches C++ closely enough
3. recorder lifecycle semantics match C++ closely enough
4. tests prove all of the above

If any of those is still missing, status remains:

- `still open`
