# Claude Remaining Plan — GCS-6 Round 15

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Author: Codex

## Bottom Line

Do not claim FULL parity yet.

Round 14 added live final flush, but the live recorder lifecycle is still not fully closed,
and the output-channel parity claim is still not fully convincing.

## Remaining Work

### 1. Stop/cancel the live periodic flush task

Current state:

- startup spawns a periodic flush loop
- `stop()` flushes and disables the exporter
- but the periodic task still appears unmanaged and uncancelled

You need to add a failing test for:

- `test_gcs_server_stop_stops_periodic_actor_event_export_loop`

That test must prove:

- after `server.stop()`, the periodic flush task no longer keeps running
- this is validated through the live runtime path, not helper-only logic

Then:

- store the task handle and/or cancellation token in the server
- stop/cancel it in `GcsServer::stop()`

### 2. Resolve output-channel parity honestly

Current state:

- C++: gRPC `EventAggregatorService::AddEvents`
- Rust: file `ray_events/actor_events.jsonl`

You must do one of these:

1. implement a real Rust aggregator-service client path

or

2. stop calling this FULL parity and explicitly scope the claim down

Do not simply restate that the proto shape matches.

If you keep the file path and still want to argue FULL parity, you need a much stronger,
source-backed external-contract argument than what exists now.

Suggested test:

- `test_actor_event_output_channel_matches_full_parity_claim_without_transport_gap`

## Required workflow

1. Read C++ first:
   - `src/ray/gcs/gcs_server.cc`
   - `src/ray/observability/ray_event_recorder.cc`
   - `src/ray/rpc/event_aggregator_client.h`

2. Add failing tests first.

3. Fix the live Rust path in:
   - `rust/ray-gcs/src/server.rs`
   - `rust/ray-observability/src/export.rs`
   - any new runtime lifecycle state you need

4. Re-run:
   - targeted `ray-gcs` tests
   - targeted `ray-observability` tests
   - `cargo test -p ray-gcs --lib`
   - `cargo test -p ray-observability --lib`

5. Update:
   - `rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
   - `rust/reports/2026-03-23_BACKEND_PORT_REAUDIT_GCS6_ROUND15.md`
   - `rust/reports/2026-03-23_PARITY_FIX_REPORT_GCS6_ROUND14.md`

## Acceptance Criteria

Only call this fixed if all of the following are true:

1. the live periodic export loop is actually stopped on `server.stop()`
2. that is proven by a live runtime-path test
3. the output-channel parity question is actually resolved, not hand-waved

If any of those is still missing, status remains:

- `still open`
