# Backend Port Re-Audit — GCS-6 Round 17

Date: 2026-03-24
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Round 17 fix author: Claude

## Scope

This re-audit checked whether the Rust `enable_ray_event` path has the same
failure/fallback semantics as C++ (no file fallback).

## Gap Found: FIXED

### File fallback removed from `enable_ray_event` path

**Problem**: When gRPC connection failed or `metrics_agent_port` was absent, Rust
fell back to file-based `EventAggregatorSink`. C++ does not — it logs an error and
events are not exported.

**Fix**: Removed all file fallback logic from the `enable_ray_event` path in
`server.rs`. When gRPC is unavailable:
- `metrics_agent_port` absent → log warning, no sink, no export
- gRPC connection fails → log error, no sink, no export

This matches C++ exactly: `StartExportingEvents()` is only called after
`WaitForServerReady` succeeds; on failure, events are not exported.

**Tests**:
- `test_enable_ray_event_with_unavailable_aggregator_does_not_fallback_to_file` — port with no server → no file output
- `test_enable_ray_event_without_metrics_agent_port_does_not_claim_full_parity_path` — no port → no sink, no export, no file
- Updated 3 existing tests to use mock gRPC servers instead of file output

## Test Results

```
cargo test -p ray-gcs --lib: 507 passed, 0 failed
cargo test -p ray-observability --lib: 51 passed, 0 failed
```

## Files Changed

| File | Change |
|---|---|
| `ray-gcs/src/server.rs` | Removed file fallback from `enable_ray_event` path. Updated 3 existing tests to use mock gRPC servers. Added `start_mock_aggregator()` helper. 2 new fallback-semantics tests. |

## Status: fixed
