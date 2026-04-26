# Rust GCS vs C++ GCS Parity Review

Date: 2026-04-15
Repository: `/Users/istoica/src/cc-to-rust/ray`
Scope: Re-review after the latest Rust GCS fixes to determine whether the Rust implementation is functionally identical to the C++ GCS and can serve as a drop-in replacement.

## Executive Summary

The Rust GCS is materially closer to the C++ GCS than in the previous review. The most important improvement is that the previously weak node/resource/autoscaler path is now substantially more complete:

- `GetAllNodeInfo` filtering and `num_filtered` behavior are now implemented in Rust.
- `CheckAlive` now returns a Ray version string.
- node drain and node address/liveness handling are much closer to C++ semantics.
- the server now wires node lifecycle events into the Rust resource manager and autoscaler manager before persisted state is initialized.
- autoscaler state is no longer an empty placeholder; it now builds cluster resource state from node and resource data, tracks constraints, persists cluster config, and reflects draining nodes.

Those are significant parity improvements.

However, the Rust GCS is still **not functionally identical** to the C++ GCS, and it is still **not yet a drop-in replacement**.

The remaining blockers are no longer dominated by node/autoscaler basics. They are now concentrated in:

- event-export service mismatch
- incomplete task-event parity
- worker persistence mismatch
- still-simplified actor and placement-group scheduling semantics
- incomplete startup/main-process parity
- remaining integration gaps in pubsub and runtime-env behavior

Bottom line:

- major progress has been made
- several previously reported gaps are now closed or much smaller
- but the Rust GCS still cannot be described as a drop-in replacement for the C++ GCS

## What Improved Since The Last Review

## 1. Node manager parity improved substantially

The Rust `GcsNodeManager` now implements several behaviors that were previously missing:

- `GetAllNodeInfo` supports:
  - `state_filter`
  - `node_selectors`
  - `limit`
  - `num_filtered`
- `CheckAlive` returns a Ray version string
- `DrainNode` now echoes the requested node IDs like C++
- `GetAllNodeAddressAndLiveness` now returns both alive and dead nodes and includes `death_info`

Relevant Rust code:

- `src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:426`
- `src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:524`
- `src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:552`
- `src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:594`

Relevant C++ comparison:

- `src/ray/gcs/gcs_node_manager.cc:199-214`
- `src/ray/gcs/gcs_node_manager.cc:237-378`
- `src/ray/gcs/gcs_node_manager.cc:395-485`

This is a real improvement. One of the biggest prior parity gaps was in this area, and that gap has been reduced significantly.

## 2. Autoscaler state handling improved substantially

The Rust autoscaler service now has real behavior instead of just returning empty placeholders. It now:

- builds `ClusterResourceState`
- includes `cluster_session_name`
- tracks node state for alive and dead nodes
- reflects draining state
- aggregates pending resource requests from resource demand
- persists cluster config in KV
- validates drain deadlines
- records drain state for alive nodes

Relevant Rust code:

- `src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:138`
- `src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:166`
- `src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:232`
- `src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:323`
- `src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:419`

Relevant C++ comparison:

- `src/ray/gcs/gcs_autoscaler_state_manager.cc:148-176`
- `src/ray/gcs/gcs_autoscaler_state_manager.cc:262-346`
- `src/ray/gcs/gcs_autoscaler_state_manager.cc:348-455`
- `src/ray/gcs/gcs_autoscaler_state_manager.cc:457-564`

This is a major step forward. The autoscaler path is no longer obviously empty.

## 3. Server wiring improved

The Rust server now installs node lifecycle listeners so that:

- node add events populate the resource manager
- node remove events clean up resource state
- node draining updates propagate into the resource manager
- node add/remove/draining events also propagate into the autoscaler manager

Relevant Rust code:

- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:296-384`

This is directionally consistent with the C++ `InstallEventListeners` path:

- `src/ray/gcs/gcs_server.cc:817-877`

This closes a meaningful integration gap from the previous review.

## 4. Test coverage increased

The Rust manager tests are now much broader than in the previous review.

From `cargo test` in `src/ray/rust/gcs`:

- `gcs-kv`: 20 passed
- `gcs-managers`: 123 passed
- `gcs-pubsub`: 13 passed

This is useful evidence that parity work is being encoded into tests rather than only into comments.

## Current Verdict

Despite the progress above, the Rust GCS is still **not identical** to the C++ GCS and still **not a drop-in replacement**.

The remaining blockers below are externally visible or recovery-visible differences, not internal implementation preferences.

## Detailed Findings

## A. Event export still does not match the C++ service surface

This is now one of the clearest remaining blockers.

### Rust status

The Rust server registers `EventAggregatorServiceServer` backed by an empty stub:

- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:33`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:440`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:487`
- `src/ray/rust/gcs/crates/gcs-managers/src/event_export_stub.rs:1-19`

That stub just returns OK and does not process events.

### C++ comparison

The C++ GCS registers `RayEventExportGrpcService` against `GcsTaskManager`:

- `src/ray/gcs/gcs_server.cc:808-814`
- `src/ray/gcs/grpc_service_interfaces.h:337-343`

The generated Rust proto also includes `RayEventExportGcsServiceServer`, but the Rust server does not register it:

- generated output under `src/ray/rust/gcs/target/debug/build/gcs-proto-*/out/ray.rpc.rs`

### Why this matters

This is not just an internal omission. It means the Rust server is exposing a different event-facing gRPC surface and is not routing event export into task-event handling the way C++ does.

### Severity

Critical.

## B. Task manager is still much simpler than C++

The Rust task manager still stores events in a flat in-memory map with simple arbitrary eviction and hard-coded statistics:

- `src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:19-112`

Observed differences:

- no multi-priority GC policy like C++
- no job summary GC
- no worker/job failure marking integration
- `num_profile_task_events_dropped = 0`
- `num_status_task_events_dropped = 0`
- `num_filtered_on_gcs = 0`
- `num_truncated = 0`

### C++ comparison

The C++ task manager has richer storage, GC, indexing, failure marking, and stats behavior:

- `src/ray/gcs/gcs_task_manager.cc:36-220`

### Why this matters

Task-event APIs are externally visible. Returning structurally correct but behaviorally simplified results is not the same as drop-in compatibility.

### Severity

High.

## C. Worker manager still does not match C++ persistence semantics

The Rust worker manager still has a `table_storage` field that is not actually used:

- `src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs:21-25`

The implementation keeps worker state only in an in-memory map:

- `src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs:49-167`

The build warnings confirm that `table_storage` is still unused.

### C++ comparison

The C++ worker manager persists to and reads from GCS table storage:

- `src/ray/gcs/gcs_worker_manager.cc:27-131`
- `src/ray/gcs/gcs_worker_manager.cc:143-183`
- `src/ray/gcs/gcs_worker_manager.cc:194-219`

### Why this matters

This affects restart/recovery behavior and state consistency. A drop-in replacement must preserve worker state with compatible persistence semantics.

### Severity

High.

## D. Actor manager is improved but still explicitly simplifies scheduling

The Rust actor manager file now describes itself as a full implementation, and it clearly improved since the previous review. But it still explicitly states that actor creation transitions to `ALIVE` with scheduling stubbed:

- `src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:9-19`

Specifically:

- Rust comment says `CreateActor` transitions to `ALIVE`
- C++ goes through `PENDING_CREATION` and real scheduler placement

### C++ comparison

The C++ actor manager includes actual scheduling, pending-creation transitions, reconstruction handling, and callback-driven lifecycle progression:

- `src/ray/gcs/actor/gcs_actor_manager.cc:308-333`
- `src/ray/gcs/actor/gcs_actor_manager.cc:335-425`
- `src/ray/gcs/actor/gcs_actor_manager.cc:427-459`

### Why this matters

Actor lifecycle semantics are part of the observable contract. If Rust compresses the lifecycle and skips real scheduling behavior, it is not equivalent.

### Severity

Critical.

## E. Placement group manager is improved but still explicitly simplifies scheduling

The Rust placement-group manager has also improved, but it still explicitly documents the key divergence:

- `src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs:9-17`

It says placement groups transition directly to `CREATED` because there is no bundle scheduler yet.

### C++ comparison

The C++ placement-group manager has real scheduling, retries, and lifecycle handling:

- `src/ray/gcs/gcs_placement_group_manager.cc:353-476`
- `src/ray/gcs/gcs_placement_group_manager.cc:574-640`

### Why this matters

This is still an explicit semantic mismatch. Immediate success in Rust is not equivalent to the C++ scheduler-driven lifecycle.

### Severity

Critical for placement-group workloads.

## F. Autoscaler parity is much better, but still incomplete

The Rust autoscaler service is no longer empty, but it still diverges from C++ in important ways.

### Remaining gaps

1. `pending_gang_resource_requests` is still always empty:

- `src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:159`

That comment itself notes this requires placement-group load.

2. `resize_raylet_resource_instances` still returns `Unimplemented` for alive nodes:

- `src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:497-530`

### C++ comparison

C++ forwards resize requests to the raylet client pool:

- `src/ray/gcs/gcs_autoscaler_state_manager.cc:525-564`

Also, C++ `HandleDrainNode` interacts with actor preemption and raylet drain RPCs:

- `src/ray/gcs/gcs_autoscaler_state_manager.cc:457-523`

Rust records drain state locally, but it does not have equivalent raylet-client integration.

### Why this matters

Autoscaler parity improved a lot, but the remaining missing control-plane integrations are still meaningful.

### Severity

High.

## G. Resource manager is still simplified relative to C++

The Rust resource manager now receives node add/remove/draining events from the server, which is an improvement. But the manager itself is still a lightweight cache:

- `src/ray/rust/gcs/crates/gcs-managers/src/resource_manager.rs:17-117`

What is still missing relative to C++:

- broader cluster resource scheduler integration
- Ray Syncer integration for `RESOURCE_VIEW` and `COMMANDS`
- richer update semantics from raylet resource reports

### C++ comparison

C++ wires resource updates through `GcsResourceManager`, cluster scheduler state, and Ray Syncer:

- `src/ray/gcs/gcs_server.cc:607-621`
- `src/ray/gcs/gcs_server.cc:386-444`

### Why this matters

The Rust server now updates resource state better than before, but it still does not expose the same integrated behavior as the C++ control plane.

### Severity

High.

## H. PubSub implementation is better, but publisher coverage still differs from C++

The Rust internal pubsub service is now a real long-poll mailbox implementation:

- `src/ray/rust/gcs/crates/gcs-managers/src/pubsub_stub.rs:1-91`

That is a clear improvement.

However, the Rust `GcsPublisher` still only exposes helpers for:

- actor
- job
- node info
- worker failure

Relevant Rust code:

- `src/ray/rust/gcs/crates/gcs-pubsub/src/lib.rs:28-78`

### C++ comparison

C++ also publishes:

- node address and liveness
- error info

Relevant C++ code:

- `src/ray/pubsub/gcs_publisher.cc:21-69`

There is also Python test coverage for resource-usage pubsub expectations:

- `python/ray/tests/test_gcs_pubsub.py:62-71`

### Why this matters

The internal pubsub transport exists, but the set of messages emitted by the Rust GCS still appears narrower than the C++ publisher surface.

### Severity

Medium to high.

## I. Runtime env handling improved, but still does not match C++ end-to-end integration

The Rust runtime env service now performs URI pinning with timed expiry and reference counting:

- `src/ray/rust/gcs/crates/gcs-managers/src/runtime_env_stub.rs:1-174`

That is materially better than the earlier empty placeholder.

But the C++ runtime-env path includes a runtime env manager plus deletion behavior tied to KV-backed GCS storage:

- `src/ray/gcs/gcs_server.cc:690-737`
- `src/ray/gcs/runtime_env_handler.cc:22-42`

The Rust implementation remains a local in-memory reference counter and does not show equivalent external deletion/integration behavior.

### Severity

Medium to high.

## J. Server main / process behavior still does not match C++ closely enough

The Rust `main.rs` improved, especially around Redis URL handling, config-list decoding, and port-file writing:

- `src/ray/rust/gcs/crates/gcs-server/src/main.rs:1-119`

But it still differs materially from the C++ process entrypoint:

- no metrics initialization path comparable to C++
- no event framework initialization
- no signal/failure handler parity
- much narrower CLI/config surface
- no C++-equivalent metrics exporter setup
- no C++-equivalent logging/rotation behavior

Relevant C++ reference:

- `src/ray/gcs/gcs_server_main.cc:42-240`

### Why this matters

For a true drop-in replacement, process-level behavior matters too, not only gRPC method signatures.

### Severity

High.

## K. Some earlier gaps are now clearly reduced or closed

It is important to state explicitly what no longer appears to be a top blocker:

1. Redis-backed persistent storage support now exists.
2. cluster ID persistence/recovery is implemented.
3. persisted init-data loading is implemented.
4. node/resource/autoscaler listener wiring is present.
5. node info and liveness APIs are much closer to C++ than before.

These were major concerns in the previous review, and the latest fixes made real progress here.

## Test Results

I reran `cargo test` in:

- `/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs`

Observed results:

- `gcs-kv`: 20 passed
- `gcs-managers`: 123 passed
- `gcs-pubsub`: 13 passed
- `gcs-server`: 6 passed, 6 failed

The failing `gcs-server` tests were:

- `tests::test_start`
- `tests::test_start_with_listener`
- `tests::test_register_node_populates_resources`
- `tests::test_unregister_node_cleans_up_resources`
- `tests::test_recovery_populates_resources`
- `tests::test_drain_node_propagates_to_resource_manager`

All six failed in this environment with socket bind `PermissionDenied`, which appears to be a sandbox/environment limitation rather than a functional parity regression.

The build warnings are also useful evidence:

- `worker_manager.rs` still has unused `table_storage`
- `runtime_env_stub.rs` still has an unused internal removal method
- `gcs-server/src/lib.rs` still has an unused `publisher` field

Those warnings do not prove incorrectness on their own, but they do support the conclusion that some parity-related integration remains unfinished.

## Conclusion

The latest fixes substantially improved the Rust GCS. The node/resource/autoscaler control-plane surface is much closer to the C++ GCS than in the previous review, and that is real progress.

Even so, the Rust GCS is still **not functionally identical** to the C++ GCS and still **cannot yet be considered a drop-in replacement**.

The clearest remaining blockers are:

- wrong/incomplete event-export service wiring
- simplified task-event handling
- worker persistence still in-memory only
- actor scheduling semantics still simplified
- placement-group scheduling semantics still simplified
- autoscaler still missing some real raylet/placement-group integrations
- process-level startup/main behavior still materially narrower than C++

## Updated Bottom-Line Verdict

Compared with the 2026-04-14 review, the Rust GCS is noticeably closer to parity, and one of the biggest previously identified gaps, node/resource/autoscaler behavior, has improved substantially.

But the correct statement is still:

The Rust GCS is not yet functionally identical to the C++ GCS and is not yet a drop-in replacement.
