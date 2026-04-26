# Rust GCS vs C++ GCS Parity Review Update

Date: 2026-04-18
Repository: `/Users/istoica/src/cc-to-rust/ray`
Scope: Re-review after the latest round of fixes, with the bar set to functional equivalence and drop-in replacement behavior for the existing C++ GCS.

## Executive Summary

The Rust GCS has improved materially, and several earlier gaps are genuinely closed:

- the event-export RPC now uses the correct `RayEventExportGcsService`
- placement groups now go through a real scheduler path rather than immediate success
- actor creation now uses a scheduler path rather than an immediate local transition
- worker state is persisted through `WorkerTable`
- the Rust `gcs-server` unit tests pass when run outside the sandbox

However, the Rust GCS is still **not functionally identical** to the C++ GCS, and it is still **not yet a drop-in replacement**.

The strongest remaining blockers are not cosmetic:

1. the real Rust binary startup path skips lifecycle listener installation that `start()` has
2. even where lifecycle listeners exist, they are still materially weaker than the C++ listener wiring
3. the Rust binary does not initialize and consume `config_list` the way C++ initializes `RayConfig`
4. the Rust server still omits the C++ `RaySyncerService` and the associated resource-broadcast command path
5. autoscaler/resource state still lacks the C++ raylet-driven resource load ingestion loop
6. `DrainNode` semantics are still weaker than C++, because Rust does not forward the drain request to the raylet and does not mark actors as preempted

These are all externally visible or operationally significant differences.

## Verification Performed

I compared the current Rust and C++ implementations directly in source, focusing on:

- binary startup path
- gRPC services actually registered
- manager wiring and event listeners
- autoscaler/resource state update flow
- worker/job/node lifecycle side effects
- drain-node semantics

I also ran the Rust test suite:

- `cargo test --manifest-path ray/src/ray/rust/gcs/Cargo.toml`
  - this initially failed in the sandbox for `gcs-server` socket-binding tests with `Operation not permitted`
- `cargo test --manifest-path ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib`
  - rerun outside the sandbox, and all `gcs-server` tests passed

That test result is useful, but it does not change the parity verdict below because the main remaining issues are design and wiring mismatches, not unit-test breakages.

## Findings

### 1. Critical: the real Rust binary path skips lifecycle listener installation

The Rust production binary in [`main.rs`](../gcs/crates/gcs-server/src/main.rs) constructs a listener and always starts the server via `start_with_listener()`:

- Rust: `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:226-258`

The problem is that `start_with_listener()` does **not** call `start_lifecycle_listeners()`, while `start()` does:

- Rust `start()` installs lifecycle listeners: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:645-656`
- Rust `start_with_listener()` omits them: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:705-710`

So the actual launched Rust binary misses the listener wiring that is supposed to drive:

- `worker_dead -> task_manager.on_worker_dead`
- `job_finished -> task_manager.on_job_finished`
- `node_removed -> job_manager.on_node_dead`

The C++ binary does not have this split-brain behavior. `gcs_server_main.cc` calls `gcs_server.Start()`, and `GcsServer::DoStart()` always calls `InstallEventListeners()` before the server runs:

- C++ main path: `ray/src/ray/gcs/gcs_server_main.cc:254-263`
- C++ installs listeners unconditionally: `ray/src/ray/gcs/gcs_server.cc:286-307`, `819-908`

This is a hard drop-in failure because the Rust binary that Ray launches is not equivalent to the Rust `start()` code path used by some tests.

### 2. Critical: Rust lifecycle listener behavior is still materially weaker than C++

Even if `start_lifecycle_listeners()` were installed on the production path, it still does less than the C++ listener set.

Rust lifecycle listener coverage:

- `worker_dead -> task_manager.on_worker_dead`: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:511-536`
- `job_finished -> task_manager.on_job_finished`: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:538-553`
- `node_removed -> job_manager.on_node_dead`: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:555-563`

By contrast, the C++ worker/job/node listeners also do the following:

- `worker_dead -> gcs_actor_manager_->OnWorkerDead(...)`
- `worker_dead -> gcs_placement_group_scheduler_->HandleWaitingRemovedBundles()`
- `worker_dead -> pubsub_handler_->AsyncRemoveSubscriberFrom(worker_id.Binary())`
- `job_finished -> gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id)`
- `node_removed -> pubsub_handler_->AsyncRemoveSubscriberFrom(node_id.Binary())`

Evidence:

- C++ worker-dead handling: `ray/src/ray/gcs/gcs_server.cc:879-900`
- C++ job-finished handling: `ray/src/ray/gcs/gcs_server.cc:902-907`
- C++ node-removed handling: `ray/src/ray/gcs/gcs_server.cc:856-870`

The Rust source itself acknowledges one of these gaps:

- Rust TODO for PG cleanup on job death: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:506-508`

The missing `OnWorkerDead` actor hook is especially important. In C++, worker death drives actor reconstruction. In the current Rust server wiring, that side effect is absent from the listener path entirely.

### 3. Critical: the Rust binary does not initialize or honor `config_list` like C++

C++ decodes `config_list`, validates it strictly, and initializes the global `RayConfig` from it:

- strict base64 decode: `ray/src/ray/gcs/gcs_server_main.cc:108-110`
- `RayConfig::instance().initialize(config_list)`: `ray/src/ray/gcs/gcs_server_main.cc:120`

Rust does not do the equivalent. Instead it:

- decodes base64 opportunistically and silently falls back to raw bytes if decode fails
- parses only `event_log_reporter_enabled`
- passes the decoded JSON string onward as `raylet_config_list`

Evidence:

- Rust decode/fallback: `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:107-118`
- Rust only parses one bool from config: `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:54-77`, `188-202`

This is a broad parity problem because many C++ runtime behaviors are controlled through `RayConfig`, including:

- health check timings
- publish batching / subscriber timeouts
- storage mode behavior
- RPC thread counts
- metrics/reporting periods
- resource-load pull intervals
- autoscaler feature flags

The Rust code does not have an equivalent configuration initialization step, so current behavior depends on hard-coded defaults and a few ad hoc environment variables instead of the same config contract as the C++ binary.

This issue also shows up in concrete startup differences:

- C++ default `gcs_server_port` is `0`: `ray/src/ray/gcs/gcs_server_main.cc:48`
- Rust defaults `gcs_server_port` to `6379`: `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:87-90`
- Rust `GcsServerConfig::default()` also defaults to `6379`: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:122-137`

That is not a drop-in CLI default.

### 4. Critical: Rust still omits the C++ `RaySyncerService`

The Rust server claims it is a drop-in replacement implementing the same service surface:

- Rust claim: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:1-5`

But the registered services do not match C++.

Rust registers:

- health
- node info
- job info
- worker info
- task info
- node resource info
- internal KV
- actor info
- placement group info
- internal pubsub
- runtime env
- autoscaler state
- ray event export

Evidence:

- Rust registration in `start()`: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:676-690`
- Rust registration in `start_with_listener()`: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:726-740`

C++ additionally registers `RaySyncerService`:

- C++ `InitRaySyncer()` registers service: `ray/src/ray/gcs/gcs_server.cc:620-621`

`RaySyncerService` is not an internal implementation detail; it is part of the server surface used for resource-view and command propagation. Its absence means the Rust server does not expose the same runtime interface as the C++ server.

### 5. Critical: autoscaler/resource state still lacks the C++ raylet-driven load ingestion loop

C++ periodically pulls `GetResourceLoad()` from every alive raylet and feeds both:

- `gcs_resource_manager_->UpdateResourceLoads(...)`
- `gcs_autoscaler_state_manager_->UpdateResourceLoadAndUsage(...)`

Evidence:

- C++ periodic polling loop: `ray/src/ray/gcs/gcs_server.cc:415-446`

Rust has the `update_resource_load_and_usage()` method, but the code explicitly states it has no callers:

- Rust method and comment: `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:213-229`

And there is no corresponding periodic raylet load pull in the Rust server.

Impact:

- autoscaler `pending_resource_requests` can diverge from C++
- node running/idle state can diverge from C++
- node resource usage reporting can diverge from C++
- any logic depending on current raylet-reported resource pressure will be weaker or stale

This is not a theoretical gap. The current Rust code only seeds autoscaler resource state from node registration totals and drain-state changes, not from the live per-node load/usage feed that C++ relies on.

### 6. Critical: `DrainNode` semantics are still weaker than C++

C++ `HandleDrainNode` does more than validate and mark local state:

- it calls `gcs_actor_manager_.SetPreemptedAndPublish(node_id)`
- it forwards the drain to the target raylet via `DrainRaylet(...)`
- it only records node draining when the raylet accepts the request
- it propagates raylet rejection back to the caller

Evidence:

- C++ drain-node flow: `ray/src/ray/gcs/gcs_autoscaler_state_manager.cc:463-523`

Rust `drain_node`:

- validates the request
- treats alive nodes as accepted immediately
- updates `node_manager` and local autoscaler cache directly
- never forwards to the raylet
- never marks actors on that node as preempted and republishes them

Evidence:

- Rust drain-node flow: `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:681-752`
- Rust autoscaler manager does not even hold an actor-manager reference, unlike C++: `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:127-170`
- C++ autoscaler manager does hold `GcsActorManager &gcs_actor_manager_`: `ray/src/ray/gcs/gcs_autoscaler_state_manager.h:38-46`, `189-190`
- Rust server constructs autoscaler manager without actor manager or publisher: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:282-288`

This changes client-visible behavior in at least two ways:

- drain requests cannot be rejected by the raylet in Rust, but can in C++
- actor preemption state publication on drain is missing in Rust

That is not drop-in equivalent behavior.

### 7. Medium: health-check service semantics still differ

C++ registers a custom `grpc.health.v1.Health` implementation that is deliberately tied to the GCS event loop, so a stuck event loop causes health checks to time out:

- C++ custom health registration: `ray/src/ray/gcs/gcs_server.cc:304-305`

Rust uses the default tonic health reporter and marks only `NodeInfoGcsService` as serving:

- Rust health registration in `start()`: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:661-662`
- Rust health registration in `start_with_listener()`: `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:723-724`

This may still satisfy basic liveness probing, but it is not the same failure-detection contract as the C++ GCS.

## What Improved Since The Last Review

The following earlier issues do appear to have been fixed or materially narrowed:

- event export now uses `RayEventExportGcsService` rather than the wrong service surface
- placement groups now go through `GcsPlacementGroupScheduler` with bundle prepare/commit RPCs
- actors now go through `GcsActorScheduler` instead of an immediate local state jump
- worker state persists through `WorkerTable`
- export events for node/actor/job are now wired

These improvements are real. The remaining verdict stays negative because the unresolved differences are still on production-path behavior, service surface, or control-plane semantics.

## Overall Verdict

The Rust GCS is closer than in the prior review, but it is still **not yet functionally identical** to the C++ GCS and still **should not be treated as a drop-in replacement**.

If the goal is true replacement parity, the next fixes should focus on:

1. making the production `start_with_listener()` path install the same listeners as `start()`
2. completing listener parity for worker/job/node side effects
3. introducing a real `RayConfig` initialization/consumption model for Rust GCS
4. implementing `RaySyncerService` parity
5. wiring periodic raylet resource load ingestion
6. making `DrainNode` actually forward to raylets and publish actor preemption state
