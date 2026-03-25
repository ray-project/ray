# Backend Port Re-Audit — GCS-6 Round 16

Date: 2026-03-24
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Round 16 fix author: Claude

## Scope

This re-audit checked whether the live Rust `enable_ray_event` path uses the same
output channel as C++ (gRPC to EventAggregatorService).

## Gap Found: FIXED

### Output-channel: gRPC EventAggregatorService client implemented

**Problem**: Rust wrote `AddEventsRequest` JSON to file. C++ sent `AddEventsRequest`
over gRPC to `EventAggregatorService::AddEvents`. Different output channel.

**Fix**: Implemented `GrpcEventAggregatorSink` in `ray-observability/src/export.rs`:
- Connects to `EventAggregatorService` at `127.0.0.1:<metrics_agent_port>` via tonic gRPC client
- Sends `AddEventsRequest` with the same proto shape through the same gRPC service method
- Uses the generated `EventAggregatorServiceClient` from the compiled proto
- Wired into live `GcsServer::initialize()`: when `metrics_agent_port` is configured,
  the gRPC sink is the primary path; file sink is the fallback

**Tests** (3 integration tests with mock gRPC servers):
- `test_actor_ray_event_path_uses_real_aggregator_service_client` — starts mock `EventAggregatorService`, configures server with that port, registers actor, stops, verifies mock received events via gRPC AND no file output exists
- `test_live_gcs_server_actor_events_flow_through_service_path` — captures full `AddEventsRequest` at mock, verifies proto structure (source_type, event_type, session_name)
- `test_actor_ray_event_path_no_longer_relies_on_file_output_for_full_parity` — without metrics_agent_port, file fallback is still used

## Test Results

```
cargo test -p ray-gcs --lib: 505 passed, 0 failed
cargo test -p ray-observability --lib: 51 passed, 0 failed
```

## Files Changed

| File | Change |
|---|---|
| `ray-observability/src/export.rs` | Added `GrpcEventAggregatorSink` struct with `connect()`, gRPC `AddEvents` in `flush()`. Made `convert_to_proto_event()` pub. |
| `ray-observability/Cargo.toml` | Added `tonic` runtime dependency. |
| `ray-gcs/src/server.rs` | Added `metrics_agent_port` to config. `initialize()` tries gRPC sink first, falls back to file. 3 new integration tests with mock gRPC servers. |

## Status: fixed
