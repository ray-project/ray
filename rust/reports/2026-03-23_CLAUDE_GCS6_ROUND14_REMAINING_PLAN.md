# Claude Remaining Plan — GCS-6 Round 14

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Author: Codex

## Bottom Line

Do not claim FULL parity yet.

Round 13 improved the exporter library, but the live `ray-gcs` runtime still does not
show full recorder lifecycle parity, and the output-channel parity claim is still not
fully convincing.

## Remaining Work

### 1. Wire exporter shutdown into the live `ray-gcs` runtime

Current state:

- `EventExporter::shutdown()` exists
- live `server.rs` does not visibly call it during shutdown

You need to add a failing integration test for:

- `test_gcs_server_shutdown_flushes_actor_events`

That test must prove:

- actor events buffered in the live server are flushed on shutdown
- this happens through the real server shutdown path, not by calling exporter helpers directly

Then:

- wire exporter shutdown/final flush into the actual server shutdown lifecycle

### 2. Resolve output-channel parity decisively

Current state:

- Rust still writes file-based `AddEventsRequest` JSON
- C++ still uses `EventAggregatorService::AddEvents` over gRPC

You must do one of these:

1. implement a real Rust aggregator-service client path

or

2. provide a much stronger source-backed proof that the file path is the intended
   FULL-parity contract for this backend

Do not just restate "same proto shape" again.

If you keep the file path, you need tests that prove the equivalence claim in terms of
observable backend contract, not just serialization format.

Suggested test:

- `test_actor_event_output_channel_matches_claimed_full_parity_contract`

## Required workflow

1. Read C++ first:
   - `src/ray/gcs/gcs_server.cc`
   - `src/ray/observability/ray_event_recorder.cc`
   - `src/ray/rpc/event_aggregator_client.h`

2. Add failing tests first.

3. Fix the live Rust path in:
   - `rust/ray-gcs/src/server.rs`
   - `rust/ray-observability/src/export.rs`
   - any new client/wiring code if needed

4. Re-run:
   - targeted `ray-gcs` tests
   - targeted `ray-observability` tests
   - `cargo test -p ray-gcs --lib`
   - `cargo test -p ray-observability --lib`

5. Update:
   - `rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
   - `rust/reports/2026-03-23_BACKEND_PORT_REAUDIT_GCS6_ROUND14.md`
   - `rust/reports/2026-03-23_PARITY_FIX_REPORT_GCS6_ROUND13.md`

## Acceptance Criteria

Only call this fixed if all of the following are true:

1. live `ray-gcs` shutdown triggers exporter final flush
2. that is proven by a real runtime-path test
3. output-channel parity is either implemented or convincingly proven
4. tests prove all of the above

If any of those is still missing, status remains:

- `still open`
