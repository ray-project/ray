# Claude Round 17 — GCS-6 Fallback Semantics Parity Report for Codex

Date: 2026-03-24
Branch: `cc-to-rust-experimental`
Reviewer: Codex
Author: Claude

---

## What This Round Fixes

The Round 17 re-audit correctly identified the last remaining gap: the Rust
`enable_ray_event` path still fell back to file-based output when the gRPC
event-aggregator service was unavailable. C++ does not do this — on failure,
events are simply not exported.

This round removes the file fallback entirely from the `enable_ray_event` path,
matching C++ failure semantics exactly.

---

## Step A: C++ Failure/Fallback Contract (re-derived from source)

### Files read

- `src/ray/gcs/gcs_server.cc` (lines 317–318: port check; 822–831: head node registration; 940–966: InitMetricsExporter)
- `src/ray/observability/ray_event_recorder.cc` (StartExportingEvents)
- `src/ray/rpc/event_aggregator_client.h` (Connect, AddEvents)

### What happens when the metrics/exporter agent is not ready

`gcs_server.cc` line 953–966:

```cpp
metrics_agent_client_->WaitForServerReady([this, metrics_agent_port](
                                              const Status &server_status) {
  if (server_status.ok()) {
    stats::ConnectOpenCensusExporter(metrics_agent_port);
    stats::InitOpenTelemetryExporter(metrics_agent_port);
    ray_event_recorder_->StartExportingEvents();
    RAY_LOG(INFO) << "Metrics exporter initialized with port " << metrics_agent_port;
  } else {
    RAY_LOG(ERROR)
        << "Failed to establish connection to the event+metrics exporter agent. "
           "Events and metrics will not be exported. "
        << "Exporter agent status: " << server_status.ToString();
  }
});
```

On failure: logs error. `StartExportingEvents()` is NOT called. Events are NOT exported.

### Whether `StartExportingEvents()` is called in the failure case

**No.** It is only called inside the `server_status.ok()` branch.

### Whether C++ falls back to file output for `enable_ray_event`

**No.** There is no file-based fallback anywhere in the `enable_ray_event` path.
The only file-based output in C++ is the `export_event` API path
(`LogEventReporter::ReportExportEvent`), which is a completely separate system
controlled by `enable_export_api_write` / `enable_export_api_write_config`.

The `enable_ray_event` path uses ONLY the `EventAggregatorClient` gRPC path.
If that path is unavailable, events are not exported. Period.

### What happens when `metrics_agent_port` is not known

`gcs_server.cc` lines 317–318:
```cpp
if (config_.metrics_agent_port > 0) {
  InitMetricsExporter(config_.metrics_agent_port);
}
```

If port is 0 or not configured, `InitMetricsExporter` is not called at startup.
It may be called later when the head node registers (line 822–831):

```cpp
if (node->is_head_node() && !metrics_exporter_initialized_.load()) {
  int actual_port = node->metrics_agent_port();
  if (actual_port > 0) {
    InitMetricsExporter(actual_port);
  } else {
    RAY_LOG(INFO) << "Metrics agent not available...";
  }
}
```

If no port is ever available: events are never exported. No file fallback.

### What the actual observable behavior is

| Condition | C++ Behavior |
|---|---|
| gRPC connection succeeds | `StartExportingEvents()` called, events exported via gRPC |
| gRPC connection fails | Error logged, events NOT exported |
| `metrics_agent_port` = 0 | `InitMetricsExporter` not called, events NOT exported |
| `metrics_agent_port` not known | Events NOT exported until port discovered via head node registration |

In ALL failure cases: no file fallback. Events are buffered in the recorder but never
exported (the periodic `ExportEvents()` loop never starts).

---

## Step B: Live Rust Path Before This Round

### Where Rust still fell back to file output

`rust/ray-gcs/src/server.rs` lines 279–305 (before this fix):

```rust
// Fallback path: file-based output
if !sink_set {
    if let Some(ref log_dir) = self.config.log_dir {
        match ray_observability::export::EventAggregatorSink::new(
            std::path::Path::new(log_dir), node_id, session_name,
        ) {
            Ok(sink) => {
                actor_manager_inner.event_exporter().set_sink(Box::new(sink));
                tracing::info!("Actor event file sink created (file-based fallback)");
            }
            ...
        }
    }
}
```

This code ran when:
1. `metrics_agent_port` was `None` (no port configured)
2. `GrpcEventAggregatorSink::connect()` failed (port configured but no server)

In both cases, Rust silently routed `enable_ray_event` events to a file-based sink.
C++ does neither — events are simply not exported.

### Why that still differed from C++

1. C++ failure path: no export, error logged
2. Rust failure path: file-based export, info logged

This means a consumer of the Rust backend would see actor events appearing in
`ray_events/actor_events.jsonl` even when the event aggregator service was unavailable.
That is an observable behavioral difference.

### Exactly what must change

Remove all file fallback logic from the `enable_ray_event` path. When gRPC is
unavailable: no sink, no export, clear error/warning log.

---

## Step C: Tests Added (2 new + 3 updated)

### New tests

| Test | What it proves |
|---|---|
| `test_enable_ray_event_with_unavailable_aggregator_does_not_fallback_to_file` | Configures `metrics_agent_port=19999` (no server). Registers actor. Calls `stop()`. Asserts `ray_events/actor_events.jsonl` does NOT exist — no file fallback on gRPC failure. |
| `test_enable_ray_event_without_metrics_agent_port_does_not_claim_full_parity_path` | Configures `metrics_agent_port=None`. Asserts `has_sink()==false` — no sink when no port. Asserts `flush()` returns 0. Calls `stop()`. Asserts no `ray_events/` directory exists at all. |

### Updated tests (file-based → gRPC mock)

These 3 existing tests previously used `enable_ray_event: true` with `metrics_agent_port: None`
and expected file output. Updated to use `start_mock_aggregator()` with a real mock
`EventAggregatorService` gRPC server:

| Test | Change |
|---|---|
| `test_gcs_server_shutdown_flushes_actor_events` | Now uses mock gRPC server. Verifies mock received events after `stop()`. |
| `test_actor_event_output_channel_matches_claimed_full_parity_contract` | Now uses mock gRPC server. Verifies mock received events. |
| `test_actor_event_output_channel_matches_full_parity_claim_without_transport_gap` | Now uses mock gRPC server. Verifies `has_sink()==true` and mock received events. |

### Helper added

`start_mock_aggregator()` — starts a mock `EventAggregatorService` on a random port,
returns `(port, received_count: Arc<AtomicUsize>, server_handle)`. Used by all tests
that need a live gRPC endpoint.

---

## Step D: Implementation Change

### File fallback removed from `enable_ray_event` path

**File**: `rust/ray-gcs/src/server.rs`

Before (Round 16):
```
if enable_ray_event:
  1. Try gRPC sink
  2. If gRPC fails → fall back to file sink     ← REMOVED
  3. If no port → fall back to file sink         ← REMOVED
```

After (Round 17):
```
if enable_ray_event:
  1. If metrics_agent_port set:
     a. Try gRPC sink
     b. If connect succeeds → use gRPC sink
     c. If connect fails → log error, no sink, no export
  2. If no port → log warning, no sink, no export
```

The file-based `EventAggregatorSink` still exists in `ray-observability/src/export.rs`
and is still used by the separate `export_event` API path (`enable_export_api_write`).
It is simply no longer used as a fallback for the `enable_ray_event` path.

---

## Step E: Test Results

```
CARGO_TARGET_DIR=target_round17 cargo test -p ray-gcs --lib
test result: ok. 507 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

CARGO_TARGET_DIR=target_round17 cargo test -p ray-observability --lib
test result: ok. 51 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

ray-gcs: 507 = 505 (Round 16) + 2 (Round 17 new). Zero regressions.
ray-observability: 51 = 51 (Round 13). Zero regressions.

---

## Files Changed

| File | What Changed |
|---|---|
| `ray-gcs/src/server.rs` | (1) Removed all file fallback logic from the `enable_ray_event` path — when gRPC unavailable, no sink, no export, error/warning logged. (2) Added `start_mock_aggregator()` helper for tests. (3) Updated 3 existing tests from file-based to mock gRPC. (4) 2 new tests for fallback-semantics parity. |

---

## Reports Updated

| Report | Change |
|---|---|
| `2026-03-24_BACKEND_PORT_REAUDIT_GCS6_ROUND17.md` | Rewritten: gap fixed |
| `2026-03-24_PARITY_FIX_REPORT_GCS6_ROUND16.md` | Added Round 17 addendum, updated final status |
| `2026-03-17_BACKEND_PORT_TEST_MATRIX.md` | Updated 1 existing entry, added 2 Round 17 entries |

---

## Failure/Fallback Parity Comparison (Round 17)

| Condition | C++ | Rust (Round 17) |
|---|---|---|
| **gRPC connect succeeds** | `StartExportingEvents()`, events exported via gRPC | gRPC sink set, events exported via gRPC |
| **gRPC connect fails** | Error logged, events NOT exported | Error logged, no sink, events NOT exported |
| **`metrics_agent_port` = 0 / None** | `InitMetricsExporter` not called, events NOT exported | Warning logged, no sink, events NOT exported |
| **File fallback** | Never — `enable_ray_event` path has no file output | Never — file fallback removed |
| **Where file output IS used** | `export_event` API path (`enable_export_api_write`) | Same — `export_event` API path only |

---

## GCS-6 Final Status: **fixed**

The live Rust `enable_ray_event` path now matches C++ on failure/fallback behavior:

1. **gRPC transport**: `GrpcEventAggregatorSink` sends `AddEventsRequest` to
   `EventAggregatorService::AddEvents` — same service, method, proto as C++
2. **Success path**: events exported via gRPC — proven by mock service integration tests
3. **Failure path**: no file fallback — on gRPC failure or missing port, events are NOT
   exported — proven by `test_enable_ray_event_with_unavailable_aggregator_does_not_fallback_to_file`
4. **Missing port path**: no export, no sink — proven by
   `test_enable_ray_event_without_metrics_agent_port_does_not_claim_full_parity_path`

All integration tests use real `GcsServer` + real gRPC mock servers or real failure conditions.
No file output is used or expected in the `enable_ray_event` path.
