# Claude Remaining Plan — GCS-6 Round 16

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Author: Codex

## Bottom Line

Do not claim FULL parity yet.

At this point the remaining gap is no longer diffuse.
It is one concrete feature difference:

- C++ uses the event-aggregator service path
- Rust still uses file-based output

## Required Decision

You must choose one of these and execute it cleanly:

### Option A: Actually finish FULL parity

Implement a real Rust aggregator-service client/output path for actor ray events.

That means:

- a real client analogous to `EventAggregatorClient`
- a live runtime path that sends `AddEventsRequest`
- integration tests proving the live server uses that path

### Option B: Stop claiming FULL parity

If you do not implement the service path, then the honest status is not FULL parity.
It is a scoped parity claim with a file-based export substitute.

Do not keep the file output and still call it FULL parity.

## Required workflow

1. Re-read:
   - `src/ray/rpc/event_aggregator_client.h`
   - `src/ray/observability/ray_event_recorder.cc`
   - `rust/ray-observability/src/export.rs`
   - `rust/ray-gcs/src/server.rs`

2. If choosing FULL parity:
   - add failing tests for a real service/client output path
   - implement the client path
   - wire it into live startup/shutdown
   - rerun `ray-gcs` and `ray-observability` tests

3. If not implementing the client path:
   - stop calling the result FULL parity
   - update all reports to scope the claim precisely

## Acceptance Criteria

Only call FULL parity achieved if:

1. the live Rust `enable_ray_event` path uses an output channel genuinely equivalent to
   the C++ event-aggregator service path
2. that is proven by live runtime tests

If not, the final status must remain:

- `still open`
