# Backend Port Re-Audit — GCS-6 Round 15

Date: 2026-03-23
Branch: `cc-to-rust-experimental`
Reviewer: Codex

## Scope

This re-audit checks the latest claim in:

- `rust/reports/2026-03-23_PARITY_FIX_REPORT_GCS6_ROUND14.md`

against the live Rust and C++ source.

## Bottom Line

I still do not think FULL parity is complete.

Round 14 fixed a real live-runtime gap:

- the live `GcsServer` shutdown path now calls `actor_manager.event_exporter().shutdown()`

But the latest report still overstates closure.

## Strongest Remaining Gaps

### 1. The live periodic flush task still is not stopped/cancelled

Relevant files:

- `src/ray/observability/ray_event_recorder.cc`
- `src/ray/gcs/gcs_server.cc`
- `rust/ray-gcs/src/server.rs`

C++ behavior:

- the recorder has explicit start/stop lifecycle
- `StopExportingEvents()` is part of the recorder lifecycle
- the recorder’s periodic export loop is part of that controlled lifecycle

Rust behavior:

- `server.rs` spawns a tokio interval loop for periodic `exporter.flush()`
- I do not see the task handle stored anywhere
- I do not see the task cancelled/stopped in `GcsServer::stop()`

So Rust now disables/flushed the exporter, but it still leaves the background flush
task running until the process/runtime itself dies.

That is still a recorder lifecycle difference.

### 2. Output-channel parity is still not the same feature

Relevant files:

- `src/ray/rpc/event_aggregator_client.h`
- `rust/ray-observability/src/export.rs`

C++ behavior:

- sends `AddEventsRequest` to the event aggregator service over gRPC

Rust behavior:

- writes `AddEventsRequest` JSON batches to `ray_events/actor_events.jsonl`

The current report argues this is equivalent enough, but for FULL parity the feature
surface is still different:

- C++ exposes service delivery
- Rust exposes file delivery

That is still an implementation and product-surface difference unless explicitly scoped out.

## Source-Level Evidence

### Rust start path still spawns an unmanaged background loop

In `rust/ray-gcs/src/server.rs`:

- startup does:
  - `tokio::spawn(async move { loop { ticker.tick().await; exporter.flush(); } })`

But the server struct does not appear to store that join handle or a cancellation token.

So even after `stop()` calls `event_exporter().shutdown()`, the task itself still exists and
keeps waking up.

### C++ recorder lifecycle is tighter

In `src/ray/observability/ray_event_recorder.cc` and `src/ray/gcs/gcs_server.cc`:

- startup explicitly starts the recorder exporting lifecycle
- shutdown explicitly stops it

That lifecycle is not just "flush once"; it is also "stop the exporter loop."

## Accurate Current Status

- Live shutdown final flush: improved
- Full recorder lifecycle parity: still open
- Output-channel parity: still open

## What Claude should do next

1. Store and manage the periodic flush task in the live server/runtime.
2. Add a failing test proving the periodic actor-event export loop stops after `server.stop()`.
3. Revisit the output-channel claim honestly:
   - either implement the real aggregator-service client path
   - or explicitly narrow the claim instead of calling it FULL parity
4. Only then claim FULL parity.
