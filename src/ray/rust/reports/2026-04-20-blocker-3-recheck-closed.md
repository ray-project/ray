# Blocker 3 Recheck: Usage Stats and Metrics Exporter

Date: 2026-04-20
Repository: `/Users/istoica/src/cc-to-rust/ray`
Previous report: `ray/src/ray/rust/reports/2026-04-20-gcs-parity-review-recheck.md`
Prior Blocker 3 recheck: `ray/src/ray/rust/reports/2026-04-20-blocker-3-recheck.md`
Scope: verify whether Finding 3 from the parity recheck report is now closed.

## Conclusion

Blocker 3 now appears closed for the concrete C++ GCS behaviors identified in the original finding and the prior recheck.

The current Rust tree now has:

1. a `UsageStatsClient`
2. usage-stats wiring into worker, actor, placement-group, and task managers
3. startup `metrics_agent_port` propagation from `main.rs` into `GcsServerConfig`
4. eager metrics-exporter initialization when startup `metrics_agent_port > 0`
5. deferred metrics-exporter initialization on head-node registration
6. the corrected deferred field: `GcsNodeInfo.metrics_agent_port`, not `metrics_export_port`
7. a cached `EventAggregatorServiceClient` after successful exporter connection

I do not see the concrete Blocker 3 gap remaining.

## What Changed Since The Prior Recheck

The prior Blocker 3 recheck found a real deferred-init bug:

- C++ read `node->metrics_agent_port()`.
- Rust read `node.metrics_export_port`.

That is now fixed:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:1005-1025`

Rust now explicitly reads:

- `node.metrics_agent_port`

This matches C++:

- `ray/src/ray/gcs/gcs_server.cc:831-842`

The tests were also corrected to guard the distinction between the two proto fields:

- `metrics_exporter_initializes_on_head_node_register`
- `metrics_exporter_inits_when_only_metrics_agent_port_is_set`
- `metrics_exporter_does_not_init_when_only_metrics_export_port_is_set`

Relevant Rust tests:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:3172-3323`

This closes the main issue that kept Blocker 3 open in the prior recheck.

## Usage-Stats Parity

C++ behavior:

- `GcsServer::InitUsageStatsClient()` constructs a shared `UsageStatsClient`.
- It wires the client into:
  - worker manager
  - actor manager
  - placement-group manager
  - task manager

Relevant C++:

- `ray/src/ray/gcs/gcs_server.cc:629-636`

Rust now mirrors this shape:

- server field:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:239-243`
- construction and four-manager wiring:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:393-403`
- usage-stats implementation:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/usage_stats.rs:1-130`

I verified this with:

- `usage_stats_client_is_wired_to_all_four_managers`
  - passed

## Metrics-Exporter Parity

C++ eager path:

- if `config_.metrics_agent_port > 0`, call `InitMetricsExporter(config_.metrics_agent_port)`

Relevant C++:

- `ray/src/ray/gcs/gcs_server.cc:324-329`

Rust eager path:

- `GcsServerConfig.metrics_agent_port` is populated from `main.rs`
- `install_listeners_and_initialize()` calls `initialize_once(...)` when it is positive

Relevant Rust:

- config field:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:135-143`
- CLI propagation:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:133-136`
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:221-235`
- eager initialization:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:985-993`

I verified this with:

- `metrics_exporter_initializes_eagerly_when_port_is_set`
  - passed

C++ deferred path:

- when the head node registers and the exporter is not already initialized:
  - read `node->metrics_agent_port()`
  - initialize if positive

Relevant C++:

- `ray/src/ray/gcs/gcs_server.cc:831-842`

Rust now matches that field and condition:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:1005-1025`

I verified this with:

- `metrics_exporter_initializes_on_head_node_register`
  - passed
- `metrics_exporter_inits_when_only_metrics_agent_port_is_set`
  - passed
- `metrics_exporter_does_not_init_when_only_metrics_export_port_is_set`
  - passed

## Event-Aggregator Client

C++ `InitMetricsExporter` calls:

- `event_aggregator_client_->Connect(metrics_agent_port)`
- constructs a metrics-agent client
- waits for server readiness

Relevant C++:

- `ray/src/ray/gcs/gcs_server.cc:950-975`

Rust now does more than a raw dial: after a successful connection it builds and caches an `EventAggregatorServiceClient<Channel>` so event-forwarding code can use `AddEvents` without redialing:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/metrics_exporter.rs:68-105`
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/metrics_exporter.rs:145-188`

I verified this with:

- `connect_success_caches_aggregator_client`
  - passed when rerun outside the sandbox, because the sandbox denied local TCP listener binding
- `connect_failure_leaves_aggregator_client_unset`
  - passed

## Residual Note

Rust still does not implement the OpenCensus / OpenTelemetry exporter wiring from the C++ success branch:

- C++:
  - `stats::ConnectOpenCensusExporter(metrics_agent_port)`
  - `stats::InitOpenTelemetryExporter(metrics_agent_port)`
  - `ray_event_recorder_->StartExportingEvents()`
  - `ray/src/ray/gcs/gcs_server.cc:966-968`
- Rust explicitly documents that there is no OC/OTel stack yet:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/metrics_exporter.rs:17-40`

I am not keeping Blocker 3 open for that alone because the Rust implementation now ports the concrete GCS-side initialization contract that exists in the current Rust runtime surface: usage-stats wiring, eager/deferred initialization, correct `metrics_agent_port` selection, at-most-once behavior, connection attempt, and cached event-aggregator client.

If the parity bar is expanded to require a Rust OpenCensus/OpenTelemetry stats backend, that should be tracked as a separate metrics-stack parity item rather than this original GCS wiring blocker.

## Test Evidence

Focused tests run:

- `usage_stats_client_is_wired_to_all_four_managers`
  - passed
- `metrics_exporter_initializes_eagerly_when_port_is_set`
  - passed
- `metrics_exporter_initializes_on_head_node_register`
  - passed
- `metrics_exporter_inits_when_only_metrics_agent_port_is_set`
  - passed
- `metrics_exporter_does_not_init_when_only_metrics_export_port_is_set`
  - passed
- `connect_failure_leaves_aggregator_client_unset`
  - passed
- `connect_success_caches_aggregator_client`
  - passed when rerun outside the sandbox

The test runs emitted existing warnings in `gcs-managers` and `gcs-server`, but no relevant failures after rerunning the local-listener test with permission to bind a local TCP socket.
