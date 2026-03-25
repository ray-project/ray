# Claude Round 16 — GCS-6 gRPC Output-Channel Parity Report for Codex

Date: 2026-03-24
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Author: Claude

---

## What This Round Fixes

The Round 16 re-audit correctly identified the last remaining gap: the Rust
`enable_ray_event` path still wrote `AddEventsRequest` JSON to a file instead of
sending it over gRPC to `EventAggregatorService::AddEvents` like C++ does.

Previous rounds had argued "same proto shape" was close enough. It was not.
C++ uses gRPC; Rust must use gRPC. This round implements that.

---

## Step A: C++ Output-Channel Contract (re-derived from source)

### Files read

- `src/ray/rpc/event_aggregator_client.h` (full client interface and impl)
- `src/ray/observability/ray_event_recorder.cc` (ExportEvents sends via client)
- `src/ray/protobuf/events_event_aggregator_service.proto` (service definition)
- `src/ray/gcs/gcs_server.cc` (creation, connection, startup, shutdown)
- `python/ray/dashboard/modules/aggregator/aggregator_agent.py` (server-side handler)

### What client is used

`EventAggregatorClientImpl` — wraps `GrpcClient<EventAggregatorService>`.

Two constructors:
- **Deferred**: takes only `ClientCallManager`; connection happens later via `Connect(port)`
- **Immediate**: takes port + `ClientCallManager`; connects immediately

`Connect(port)` creates the gRPC client to `127.0.0.1:<port>`.

### What service method is called

```protobuf
service EventAggregatorService {
  rpc AddEvents(AddEventsRequest) returns (AddEventsReply);
}
```

- Package: `ray.rpc.events`
- No timeout (`-1`) — communication is always local
- No retry — event sending is best effort
- Request: `AddEventsRequest` containing `RayEventsData` with `repeated RayEvent`
- Reply: `AddEventsReply` containing `AddEventsStatus` (code + message)

### What the live runtime does when ray events are enabled

1. **Construction** (gcs_server.cc:128–143): Creates `EventAggregatorClientImpl` with
   deferred connection and `RayEventRecorder` referencing it
2. **Connection** (gcs_server.cc:940–959): When head node registers,
   `InitMetricsExporter(metrics_agent_port)` calls `event_aggregator_client_->Connect(port)`
3. **Export** (ray_event_recorder.cc:98–154): `ExportEvents()` groups/merges events,
   serializes to `AddEventsRequest`, calls `event_aggregator_client_.AddEvents(request, callback)`
4. **Server side** (aggregator_agent.py:178–213): Python `AggregatorAgent.AddEvents()` receives
   events, buffers them, optionally publishes to external HTTP/GCS, records metrics

### Why this is still different from file output

- C++ sends structured proto over gRPC to a running service that buffers, publishes, and
  tracks metrics
- File output writes JSON to disk where nothing reads it in real-time
- A downstream consumer of the event-aggregator service sees live events; a downstream
  consumer of a file does not
- The transport IS the externally observable feature, not just the data shape

---

## Step B: Live Rust Path Before This Round

### Where Rust still wrote to file

`ray-observability/src/export.rs` `EventAggregatorSink::flush()`:
- Serialized `AddEventsRequest` to JSON via `serde_json::to_string`
- Appended to `{log_dir}/ray_events/actor_events.jsonl`

`ray-gcs/src/server.rs` `initialize()`:
- Created `EventAggregatorSink::new(log_dir, node_id, session_name)`
- Set it as the exporter's sink

### What client/service path was still missing

- No `GrpcEventAggregatorSink` existed
- No tonic `EventAggregatorServiceClient` was used anywhere in the Rust codebase
- No `metrics_agent_port` configuration existed on `GcsServerConfig`
- The generated gRPC client stub existed in `ray-proto` (compiled from
  `events_event_aggregator_service.proto` with `build_client(true)`) but was never used

### Exactly what must be implemented to close the gap

1. A `GrpcEventAggregatorSink` struct implementing `EventSink` that:
   - Holds a tonic `EventAggregatorServiceClient<Channel>`
   - Connects to `127.0.0.1:<port>` (matching C++ `EventAggregatorClientImpl::Connect`)
   - Calls `client.add_events(request)` in `flush()` (matching C++ `AddEvents`)
2. `metrics_agent_port` on `GcsServerConfig`
3. Live server `initialize()` wiring: try gRPC first, fall back to file

---

## Step C: Tests Added (3 integration tests)

All 3 tests start a real mock `EventAggregatorService` gRPC server, configure the
`GcsServer` with that port, register actors through the live pipeline, call `server.stop()`,
and verify events were delivered via gRPC.

### 1. gRPC client path

| Test | What it proves |
|---|---|
| `test_actor_ray_event_path_uses_real_aggregator_service_client` | Starts mock `EventAggregatorService` on random port. Configures `GcsServer` with `metrics_agent_port=<port>`. Registers actor. Calls `server.stop()`. Verifies: (1) mock server received events (count > 0), (2) file output `ray_events/actor_events.jsonl` does NOT exist — events went through gRPC, not file. |

### 2. End-to-end proto verification

| Test | What it proves |
|---|---|
| `test_live_gcs_server_actor_events_flow_through_service_path` | Starts mock that captures full `AddEventsRequest`. Registers actor through live server. After `stop()`, verifies captured request has: `events_data` with non-empty events array, each event has `source_type=2 (GCS)`, `event_type` is 9 or 10, `session_name` is set. |

### 3. Fallback behavior

| Test | What it proves |
|---|---|
| `test_actor_ray_event_path_no_longer_relies_on_file_output_for_full_parity` | Configures server with `metrics_agent_port=None` (no gRPC). Verifies file-based sink is attached and file output is created after stop. Proves file path is retained as fallback, not primary. |

---

## Step D: Implementation

### D1. `GrpcEventAggregatorSink` — NEW

**File**: `rust/ray-observability/src/export.rs`

```rust
pub struct GrpcEventAggregatorSink {
    client: Mutex<EventAggregatorServiceClient<Channel>>,
    node_id: Vec<u8>,
    session_name: String,
    runtime_handle: tokio::runtime::Handle,
}
```

**`connect(port, node_id, session_name)`**: Creates tonic client to `http://127.0.0.1:<port>`.
Matches C++ `EventAggregatorClientImpl::Connect(port)`.

**`flush(events)`**:
1. Reuses `EventAggregatorSink::convert_to_proto_event()` to build proto events
   (made `pub` for this purpose)
2. Builds `AddEventsRequest` with `RayEventsData` containing the proto events
3. Clones the client, spawns the async `client.add_events(request)` on a dedicated
   thread via `std::thread::spawn` + `runtime_handle.block_on()` to avoid deadlocking
   the tokio runtime
4. Waits for result with 5s timeout via `mpsc::recv_timeout`
5. On success: returns event count. On error: logs and returns 0 (best effort, matching C++)

### D2. Live server wiring

**File**: `rust/ray-gcs/src/server.rs`

Added `metrics_agent_port: Option<u16>` to `GcsServerConfig`.

Updated `initialize()`:
```
if enable_ray_event:
  1. If metrics_agent_port is set:
     → Try GrpcEventAggregatorSink::connect(port, node_id, session_name)
     → On success: use gRPC sink (FULL parity with C++)
     → On failure: log warning, fall through to file
  2. If no gRPC sink set and log_dir available:
     → Use EventAggregatorSink (file fallback)
  3. Otherwise:
     → No sink, events buffered but not delivered
```

### D3. `convert_to_proto_event` made public

Changed from `fn convert_to_proto_event` to `pub fn convert_to_proto_event` on
`EventAggregatorSink` so `GrpcEventAggregatorSink` can reuse the same proto
conversion logic without duplication.

---

## Step E: Test Results

```
CARGO_TARGET_DIR=target_round16 cargo test -p ray-gcs --lib
test result: ok. 505 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

CARGO_TARGET_DIR=target_round16 cargo test -p ray-observability --lib
test result: ok. 51 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

ray-gcs: 505 = 502 (Round 15) + 3 (Round 16). Zero regressions.
ray-observability: 51 = 51 (Round 13). Zero regressions.

---

## Files Changed

| File | What Changed |
|---|---|
| `ray-observability/src/export.rs` | (1) Added `GrpcEventAggregatorSink` struct with `connect()`, `from_channel()`, and `EventSink::flush()` implementation that sends `AddEventsRequest` via tonic gRPC client. (2) Changed `EventAggregatorSink::convert_to_proto_event()` from private to `pub` for reuse. |
| `ray-observability/Cargo.toml` | Added `tonic = { workspace = true }` to runtime dependencies. |
| `ray-gcs/src/server.rs` | (1) Added `metrics_agent_port: Option<u16>` to `GcsServerConfig` and `Default`. (2) Rewrote sink wiring in `initialize()`: try gRPC first when port set, fall back to file. (3) 3 new integration tests with mock gRPC `EventAggregatorService` servers. |

---

## Reports Updated

| Report | Change |
|---|---|
| `2026-03-23_BACKEND_PORT_REAUDIT_GCS6_ROUND16.md` | Rewritten: gap fixed with gRPC client implementation and test evidence |
| `2026-03-23_PARITY_FIX_REPORT_GCS6_ROUND15.md` | Added Round 16 addendum, updated final status |
| `2026-03-17_BACKEND_PORT_TEST_MATRIX.md` | Added 3 Round 16 test entries |

---

## Output-Channel Parity Comparison (Round 16)

| Aspect | C++ | Rust (Round 16) |
|---|---|---|
| **Client type** | `EventAggregatorClientImpl` wrapping `GrpcClient<EventAggregatorService>` | `GrpcEventAggregatorSink` wrapping tonic `EventAggregatorServiceClient<Channel>` |
| **Connection** | `Connect(port)` → `127.0.0.1:<port>` | `connect(port)` → `http://127.0.0.1:<port>` |
| **Service** | `EventAggregatorService::AddEvents` | Same — `EventAggregatorService::AddEvents` |
| **Request type** | `AddEventsRequest` proto | Same — `AddEventsRequest` proto |
| **Reply type** | `AddEventsReply` (status ignored for best effort) | Same — `AddEventsReply` (errors logged, best effort) |
| **Timeout** | -1 (no timeout, local communication) | 5s recv timeout (defensive, local communication) |
| **Error handling** | Log error, no retry | Log error, return 0, no retry |
| **Async model** | Async gRPC with callback, `grpc_in_progress_` guard | Dedicated thread + `runtime_handle.block_on()`, `flush_in_progress` guard |
| **Deferred connection** | Connects when metrics agent port known | Connects during `initialize()` when `metrics_agent_port` set |
| **Fallback** | None (recorder not started if no connection) | File-based `EventAggregatorSink` when no port available |
| **Proto path** | `events_event_aggregator_service.proto` | Same — generated by tonic from same proto |

---

## GCS-6 Round 16 Status: gRPC output channel implemented

Round 17 re-audit found that the file fallback under `enable_ray_event` still
differed from C++ (C++ does not fall back to file on failure).

---

## Round 17 Addendum (2026-03-24)

**Problem**: When gRPC connection failed or `metrics_agent_port` was absent, Rust
fell back to file-based `EventAggregatorSink`. C++ logs an error and does not export.

**Fix**: Removed all file fallback from the `enable_ray_event` path. On failure:
no sink, no export, clear error log. Matches C++ exactly.

**Tests** (2 new + 3 updated):
- `test_enable_ray_event_with_unavailable_aggregator_does_not_fallback_to_file`
- `test_enable_ray_event_without_metrics_agent_port_does_not_claim_full_parity_path`
- Updated 3 existing tests to use mock gRPC servers

```
cargo test -p ray-gcs --lib: 507 passed, 0 failed
cargo test -p ray-observability --lib: 51 passed, 0 failed
```

## GCS-6 Final Status: **fixed**

The live Rust `enable_ray_event` path now matches C++ on:

1. **gRPC transport**: `GrpcEventAggregatorSink` sends `AddEventsRequest` to
   `EventAggregatorService::AddEvents` — same service, method, proto
2. **Failure behavior**: no file fallback — on gRPC failure or missing port,
   events are not exported (matching C++ `WaitForServerReady` failure path)
3. **Proven by mock + failure tests**: gRPC delivery verified by mock service;
   no-fallback verified by failure and missing-port tests
