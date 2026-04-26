# Blocker 3 Recheck: Usage Stats and Metrics Exporter

Date: 2026-04-20
Repository: `/Users/istoica/src/cc-to-rust/ray`
Previous report: `ray/src/ray/rust/reports/2026-04-20-gcs-parity-review-recheck.md`
Scope: verify whether Finding 3 from the parity recheck report is now fully closed.

## Conclusion

Blocker 3 is substantially improved, but it is not fully closed.

The current Rust tree now implements the broad shape of the C++ usage-stats and metrics-exporter initialization path:

1. `GcsServerConfig` now carries `metrics_agent_port`.
2. `main.rs` passes the CLI `--metrics_agent_port` into `GcsServerConfig`.
3. Rust now constructs a shared `UsageStatsClient`.
4. Rust wires that client into worker, actor, placement-group, and task managers.
5. Rust now has eager metrics-exporter initialization when the startup `metrics_agent_port` is positive.
6. Rust now has a deferred head-node registration hook.

However, I found a remaining C++ parity bug in the deferred metrics-exporter path:

- C++ reads `GcsNodeInfo.metrics_agent_port`.
- Rust currently reads `GcsNodeInfo.metrics_export_port`.

That means the dynamic head-node path can fail to initialize the exporter in the common case where the head node reports only `metrics_agent_port`, which is exactly the field C++ uses.

I would not mark Blocker 3 closed yet.

## What Is Fixed

### Usage-stats client exists and is wired to the four C++ managers

C++ behavior:

- `GcsServer::InitUsageStatsClient()` creates a `UsageStatsClient`.
- It installs that client into:
  - `GcsWorkerManager`
  - `GcsActorManager`
  - `GcsPlacementGroupManager`
  - `GcsTaskManager`

Relevant C++:

- `ray/src/ray/gcs/gcs_server.cc:629-636`

Rust now mirrors this shape:

- server fields:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:239-249`
- construction and manager wiring:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:393-408`
- `UsageStatsClient` implementation:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/usage_stats.rs:1-130`

The manager setters and emit points are present:

- worker manager:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs`
- actor manager:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs`
- placement-group manager:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs`
- task manager:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:626-662`

This addresses the usage-stats portion of the original blocker.

### Startup `metrics_agent_port` now reaches the server

C++ behavior:

- `GcsServerConfig::metrics_agent_port` is passed into `GcsServer`.
- If it is positive at startup, C++ calls `InitMetricsExporter(config_.metrics_agent_port)`.

Relevant C++:

- `ray/src/ray/gcs/gcs_server.cc:324-329`
- `ray/src/ray/gcs/gcs_server.h:59`

Rust now has the same config surface:

- config field:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:135-143`
- default:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:169-172`
- CLI parse and propagation:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:133-136`
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:221-235`
- eager initialization:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:985-993`

This addresses the eager metrics-exporter part of the original blocker.

## What Is Still Wrong

### Deferred head-node initialization reads the wrong proto field

C++ deferred behavior:

- when a node is added,
- if it is the head node,
- and the exporter has not already been initialized,
- C++ reads `node->metrics_agent_port()`,
- and calls `InitMetricsExporter(actual_port)` when that value is positive.

Relevant C++:

- `ray/src/ray/gcs/gcs_server.cc:831-842`

The proto has two distinct fields:

- `metrics_export_port = 9`
- `metrics_agent_port = 30`

Relevant proto:

- `ray/src/ray/protobuf/gcs.proto:318-325`

Rust currently does this in the deferred path:

- `let port = node.metrics_export_port;`
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:1005-1017`

That is not what C++ does. C++ uses `metrics_agent_port`, not `metrics_export_port`.

Why this matters:

- raylet populates both fields separately:
  - `ray/src/ray/raylet/main.cc:1115-1116`
- the metrics agent is the dashboard-agent gRPC port, field `metrics_agent_port`
- `metrics_export_port` is a different port: the port at which the node exposes metrics
- using the wrong field can leave the Rust GCS exporter uninitialized when the C++ GCS would initialize it

The existing Rust deferred-init test also uses the wrong field:

- test comment says `metrics_export_port > 0`
- test data sets `metrics_export_port: 1`
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:3161-3214`

So that test passes while still testing behavior that differs from C++.

### The Rust exporter is still not a complete C++ `InitMetricsExporter` equivalent

C++ `InitMetricsExporter` does more than flip a flag:

- connects the event-aggregator client
- creates a `MetricsAgentClientImpl`
- waits for server readiness
- connects OpenCensus exporter
- initializes OpenTelemetry exporter
- starts exporting Ray events

Relevant C++:

- `ray/src/ray/gcs/gcs_server.cc:950-975`

Rust `MetricsExporter` currently dials `127.0.0.1:<port>` and logs success/failure:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/metrics_exporter.rs:88-147`

The implementation explicitly states that Rust does not yet wire OpenCensus or OpenTelemetry:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/metrics_exporter.rs:17-33`

This may be an accepted temporary scope choice, but under the original “drop-in replacement” parity bar it is still not fully identical to the C++ side effects.

## Test Evidence

I ran the focused Rust tests that exist for this blocker. All passed:

- `usage_stats_client_is_wired_to_all_four_managers`
- `metrics_exporter_initializes_eagerly_when_port_is_set`
- `metrics_exporter_initializes_on_head_node_register`

These passing tests confirm that:

- usage-stats wiring exists
- eager initialization flips the one-shot exporter state
- the current Rust deferred hook fires when `metrics_export_port` is set

They do not prove C++ parity for deferred initialization, because C++ uses `metrics_agent_port`.

## Required Work To Fully Close Blocker 3

To close this blocker, Rust should at minimum:

1. Change the deferred head-node path to read `node.metrics_agent_port`, not `node.metrics_export_port`.
2. Update the deferred-init test to set `metrics_agent_port`, not `metrics_export_port`.
3. Add a regression test where `metrics_agent_port > 0` and `metrics_export_port == 0`; Rust should initialize.
4. Add a regression test where `metrics_export_port > 0` and `metrics_agent_port == 0`; Rust should not initialize through the C++ deferred path.
5. Decide whether the Rust `MetricsExporter` is intended to fully implement the C++ exporter side effects. If yes, it still needs real event-aggregator / metrics-agent client wiring rather than only a raw `Channel` dial.

Until at least the field mismatch is corrected, Blocker 3 should remain open.
