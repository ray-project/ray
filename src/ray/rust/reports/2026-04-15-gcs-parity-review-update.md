# Rust GCS vs C++ GCS Parity Review Update

Date: 2026-04-15
Repository: `/Users/istoica/src/cc-to-rust/ray`
Scope: Re-review after the latest round of Rust GCS fixes to determine whether the Rust implementation is functionally identical to the C++ GCS and can be treated as a drop-in replacement.

## Executive Summary

The Rust GCS is materially closer to the C++ GCS than in the prior review from earlier on 2026-04-15.

This latest round of fixes closed several meaningful gaps:

- event ingestion is no longer an empty stub; Rust now converts incoming Ray events into task events and forwards them into the task manager
- task-event storage is much richer than before and now supports merging, priority-based eviction, and drop accounting
- worker state now persists through `WorkerTable` instead of living only in memory
- pubsub coverage is broader, including error, log, resource-usage, and node-address/liveness channel helpers
- runtime-env pin expiry now integrates with KV deletion for `gcs://` URIs
- actor lifecycle now explicitly includes `PENDING_CREATION`
- placement-group lifecycle now explicitly includes `PENDING`

These are real improvements.

However, the Rust GCS is still **not functionally identical** to the C++ GCS, and it is still **not yet a drop-in replacement**.

The remaining gaps are narrower than before, but several are still important and externally visible:

- the event-export gRPC service surface still does not match C++
- actor creation and placement-group creation still skip the real scheduler phase and transition immediately
- autoscaler integration still lacks raylet-backed resize and placement-group-backed gang requests
- Rust server wiring still misses some C++ task-manager listener integrations
- process-level `main()` behavior is still narrower than the C++ binary
- some pubsub publications that exist in C++ are still not emitted from the corresponding Rust manager paths

## What Improved Since The Prior Review

## 1. Event export is no longer a no-op

Rust now has a real event-export handler that:

- accepts `AddEvents`
- converts supported Ray event variants into `TaskEvents`
- groups them by job ID
- forwards them into the task manager via `add_task_event_data`

Relevant Rust code:

- `src/ray/rust/gcs/crates/gcs-managers/src/event_export_stub.rs:1-290`

This is a substantial improvement over the earlier state where event export simply returned OK without doing anything.

## 2. Task manager is much closer to C++

The Rust task manager now supports:

- event merging by `(task_id, attempt_number)`
- priority-based eviction
- tracking dropped attempts
- tracking dropped profile events
- job-based filtering
- more meaningful reply statistics

Relevant Rust code:

- `src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:1-360`

This meaningfully narrows the prior parity gap.

## 3. Worker manager now persists state

The Rust worker manager now persists worker updates through `WorkerTable` and reads `get_all_worker_info` from storage:

- `src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs:1-340`

This closes one of the major prior concerns, where the Rust worker manager had storage fields but did not actually use them.

## 4. PubSub coverage improved

Rust `GcsPublisher` now includes helpers for:

- worker failure
- job
- node info
- actor
- error info
- logs
- node resource usage
- node address and liveness

Relevant Rust code:

- `src/ray/rust/gcs/crates/gcs-pubsub/src/lib.rs:28-119`

The server also now bridges publisher traffic into the long-poll pubsub manager:

- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:396-438`

This is a real improvement from the previous review.

## 5. Runtime env cleanup is closer to C++

Rust runtime-env pinning now accepts a KV interface and deletes released `gcs://` URIs from KV on final expiry:

- `src/ray/rust/gcs/crates/gcs-managers/src/runtime_env_stub.rs:1-228`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:214-217`

This narrows the previous end-to-end lifecycle gap.

## 6. Actor and placement-group state machines are closer

Rust actor creation now explicitly transitions through `PENDING_CREATION`, and placement-group creation now explicitly transitions through `PENDING` before `CREATED`:

- `src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:395-496`
- `src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs:156-236`

This is better than the earlier direct jump to final states.

## Overall Verdict

The Rust GCS is now closer than before in storage semantics, event ingestion, and several externally visible APIs.

But it still is **not yet identical** to the C++ GCS, and it still should **not** be described as a drop-in replacement.

The remaining blockers are fewer, but they are still meaningful enough to prevent a drop-in equivalence claim.

## Detailed Findings

## A. Event export still uses the wrong gRPC service surface

This is now one of the clearest remaining blockers.

### Rust status

The Rust server still registers:

- `EventAggregatorServiceServer`

via:

- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:33`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:493`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:541`

Although the implementation now converts and stores events, the service identity exposed by the server is still `ray.rpc.events.EventAggregatorService`.

### C++ comparison

The C++ GCS registers:

- `rpc::events::RayEventExportGrpcService`

against the task manager:

- `src/ray/gcs/gcs_server.cc:808-814`
- `src/ray/gcs/grpc_services.h:312-331`
- `src/ray/gcs/grpc_service_interfaces.h:337-343`

That means the C++ server exposes `ray.rpc.RayEventExportGcsService/AddEvents`, not the event-aggregator service endpoint the Rust server currently registers.

### Why this matters

This is not an implementation-detail difference. It is an externally visible RPC surface mismatch. Even though Rust now has real event conversion logic, the server is still presenting a different gRPC service than C++.

### Severity

Critical.

## B. Actor creation is closer, but still not equivalent to C++ scheduling semantics

Rust actor lifecycle now includes `PENDING_CREATION`, which is an improvement. But the implementation still transitions immediately from `PENDING_CREATION` to `ALIVE` because there is no real actor scheduler phase:

- `src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:395-496`

The file itself still states:

- C++ does actual scheduler placement between these states
- Rust transitions immediately but publishes both states

### C++ comparison

C++ actor management includes actual scheduling, placement callbacks, node/worker failure reactions, and reconstruction flow:

- `src/ray/gcs/actor/gcs_actor_manager.cc:308-333`
- `src/ray/gcs/actor/gcs_actor_manager.cc:335-425`
- `src/ray/gcs/actor/gcs_actor_manager.cc:799-877`

### Why this matters

Publishing the intermediate state is better than before, but immediate transition is still not the same as real scheduling behavior. That can affect ordering, callback timing, failure handling, and externally observed state transitions.

### Severity

Critical.

## C. Placement-group creation is closer, but still not equivalent to C++ scheduling semantics

Rust placement-group creation now explicitly transitions through `PENDING` and supports real wait callbacks:

- `src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs:1-236`

That is a meaningful improvement.

However, Rust still immediately transitions `PENDING -> CREATED` because no real bundle scheduler exists:

- `src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs:166-230`

### C++ comparison

C++ placement-group creation uses the real scheduling path, retry behavior, and lifecycle management:

- `src/ray/gcs/gcs_placement_group_manager.cc:353-476`
- `src/ray/gcs/gcs_placement_group_manager.cc:574-640`

### Why this matters

Placement-group readiness and scheduling are externally observable. Immediate success is still not equivalent to scheduler-driven semantics.

### Severity

Critical for placement-group users.

## D. Autoscaler parity improved, but still has important missing integrations

Rust autoscaler state remains much better than before, but several key gaps still remain.

### 1. `pending_gang_resource_requests` is still always empty

Rust still returns:

- `pending_gang_resource_requests: vec![]`

with an inline comment pointing to missing placement-group load integration:

- `src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:151-159`

This matters because C++ feeds placement-group load into autoscaler state.

### 2. `resize_raylet_resource_instances` is still `Unimplemented`

Rust still validates the request and then returns `Unimplemented` for alive nodes:

- `src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:492-523`

### C++ comparison

C++ forwards resize requests to the raylet client pool:

- `src/ray/gcs/gcs_autoscaler_state_manager.cc:525-564`

### Why this matters

This is still an externally visible behavior difference in autoscaler RPC semantics.

### Severity

High.

## E. Task manager is stronger, but still misses C++ integration hooks

The task manager itself improved a lot. But the server still does not appear to wire the same listener-based integrations that C++ installs.

### C++ comparison

C++ `InstallEventListeners()` connects:

- worker dead events -> `gcs_task_manager_->OnWorkerDead(...)`
- job finished events -> `gcs_task_manager_->OnJobFinished(...)`

Relevant C++ code:

- `src/ray/gcs/gcs_server.cc:878-905`

### Rust status

Rust has:

- `GcsWorkerManager::add_worker_dead_listener`
- `GcsJobManager::add_job_finished_listener`

but current Rust server wiring does not hook those listeners into the task manager:

- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs`

No equivalent `OnWorkerDead` / `OnJobFinished` integration path is present in the Rust server.

### Why this matters

Even with better storage semantics, the Rust task manager still does not participate in all the same lifecycle-triggered state transitions as C++.

### Severity

High.

## F. PubSub coverage improved, but not all C++ publication triggers are present

Rust now has publisher helpers for more channels, which is good:

- `src/ray/rust/gcs/crates/gcs-pubsub/src/lib.rs:80-119`

However, I still do not see all corresponding manager-side publication points.

### Examples

1. C++ node manager publishes both:

- node info
- node address and liveness

from the same publication path:

- `src/ray/gcs/gcs_node_manager.cc:766-774`

In Rust node manager I still only see `publish_node_info(...)` in register/remove paths:

- `src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:212`
- `src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:239`

I do not see the corresponding `publish_node_address_and_liveness(...)` call in those paths.

2. C++ publishes error messages from several places, including:

- node manager
- autoscaler state manager
- actor manager
- job manager

Relevant C++ references:

- `src/ray/gcs/gcs_node_manager.cc:671`
- `src/ray/gcs/gcs_autoscaler_state_manager.cc:96`
- `src/ray/gcs/actor/gcs_actor_manager.cc:718`
- `src/ray/gcs/gcs_job_manager.cc:467`

In Rust, job-manager error publication exists:

- `src/ray/rust/gcs/crates/gcs-managers/src/job_manager.rs:150-162`

but I do not see equivalent error publications from the other manager paths above.

### Why this matters

The pubsub transport exists and the channel helpers exist, but not all C++ publication triggers appear to be wired yet.

### Severity

Medium to high.

## G. Process-level `main()` parity is better, but still not equivalent to C++

Rust `main.rs` now parses more C++-style flags, logs version/commit, handles more Redis options, and installs a simple SIGTERM handler:

- `src/ray/rust/gcs/crates/gcs-server/src/main.rs:1-210`

This is better than before.

But it still does not match the C++ server process behavior in several important respects:

- no C++-equivalent metrics initialization
- no event framework initialization
- no equivalent failure-signal handling
- no equivalent log redirection and rotation behavior
- no equivalent metrics exporter initialization flow
- no equivalent `RayConfig` / stats initialization path

Relevant C++ reference:

- `src/ray/gcs/gcs_server_main.cc:42-240`

### Why this matters

For a true drop-in replacement, process-level startup and runtime behavior matter in addition to service handlers.

### Severity

High.

## H. Some previously important gaps now appear substantially reduced

The following gaps from earlier reviews are no longer primary blockers:

1. Redis-backed persistence support exists.
2. cluster ID persistence/recovery exists.
3. startup state loading exists.
4. worker persistence is now materially closer to C++.
5. event ingestion is now real instead of a no-op.
6. task manager semantics are significantly richer.
7. runtime-env cleanup is more aligned with C++.

These changes are meaningful and should be credited as real progress.

## Test Results

I reran `cargo test` in:

- `/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs`

Observed results:

- `gcs-kv`: 20 passed
- `gcs-managers`: 147 passed
- `gcs-pubsub`: 13 passed
- `gcs-server`: 6 passed, 6 failed

The failing `gcs-server` tests were:

- `tests::test_start`
- `tests::test_start_with_listener`
- `tests::test_register_node_populates_resources`
- `tests::test_unregister_node_cleans_up_resources`
- `tests::test_recovery_populates_resources`
- `tests::test_drain_node_propagates_to_resource_manager`

All six failed in this environment with socket bind `PermissionDenied`, which appears to be an environment/sandbox limitation rather than a semantic regression in the code under review.

The build also emitted some warnings that reinforce the “still not fully integrated” conclusion:

- unused imports in several managers
- runtime-env internal cleanup helper methods still partially unused
- server bridge still has unused imports

These warnings are secondary evidence, not primary proof, but they are consistent with the remaining unfinished integration work.

## Conclusion

Compared with the previous review from earlier on 2026-04-15, the Rust GCS is noticeably closer to C++ parity.

The most important newly closed or reduced gaps are:

- worker persistence
- event ingestion
- task-event storage semantics
- pubsub channel coverage
- runtime-env KV cleanup
- richer actor and placement-group lifecycle state exposure

That said, the Rust GCS is still **not functionally identical** to the C++ GCS and still **cannot yet be treated as a drop-in replacement**.

The strongest remaining blockers are:

- event-export service name/surface mismatch
- actor scheduling still collapsed to immediate success
- placement-group scheduling still collapsed to immediate success
- autoscaler still missing raylet-backed resize and gang-request parity
- task manager still missing C++ listener integrations
- process-level `main()` behavior still materially narrower than C++
- some pubsub publication triggers still not wired from the same manager paths as C++

## Updated Bottom-Line Verdict

The Rust GCS is substantially closer than before, and this round of fixes resolved several previously important parity concerns.

But the correct statement is still:

The Rust GCS is not yet functionally identical to the C++ GCS and is not yet a drop-in replacement.
