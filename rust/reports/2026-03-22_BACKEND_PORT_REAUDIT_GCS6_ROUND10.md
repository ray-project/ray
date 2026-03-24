# Backend Port Re-Audit — GCS-6 Round 10

Date: 2026-03-22
Branch: `cc-to-rust-experimental`
Reviewer: Codex

## Scope

This re-audit checks the latest actor export/event parity claim in:

- `rust/reports/2026-03-22_PARITY_FIX_REPORT_GCS6_ROUND9.md`

against the live Rust and C++ source.

## Bottom Line — Updated After Round 10 Fix

Round 10 closes the remaining `enable_ray_event` aggregator-path gap:

1. **Live sink wiring**: `server.rs` now attaches a real `LoggingEventSink` to the actor event exporter when `enable_ray_event=true`
2. **Live flush lifecycle**: `server.rs` spawns a periodic flush loop (matching C++ `RayEventRecorder::StartExportingEvents()`)
3. **Config propagation**: `RayConfig` now carries `enable_ray_event`, `enable_export_api_write`, `enable_export_api_write_config`, `ray_events_report_interval_ms` — all readable from env vars (`RAY_enable_ray_event=true`, etc.)
4. **Observable output**: Events flow through the sink on every flush tick — they do not just sit in memory

## Round 10 Gap Closure

| Gap (from Round 9 re-audit) | Round 10 Status |
|---|---|
| No `set_sink()` in production code | **fixed**: `server.rs:initialize()` calls `event_exporter().set_sink(LoggingEventSink)` when `enable_ray_event=true` |
| No `start_periodic_flush()` in production code | **fixed**: `server.rs:initialize()` spawns periodic flush tokio task matching C++ `StartExportingEvents()` |
| Events buffered forever in memory | **fixed**: periodic flush drains buffer through real sink |
| Tests only prove `buffer_len() > 0` | **fixed**: 4 new tests prove sink attachment, flush delivery, periodic draining, and disabled-path silence |

## Test Results

```
CARGO_TARGET_DIR=target_round10 cargo test -p ray-gcs --lib
test result: ok. 483 passed; 0 failed

CARGO_TARGET_DIR=target_round10 cargo test -p ray-observability --lib
test result: ok. 45 passed; 0 failed
```

## Current Status

### Fixed

- Default-path/core correctness parity
- File-based actor export parity (`ExportActorData` → `event_EXPORT_ACTOR.log`)
- Full configurable actor export/event feature parity including live `enable_ray_event` sink/flush path

### Still open

None.
