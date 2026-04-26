# Rust GCS vs C++ GCS Parity Review Update

Date: 2026-04-14
Repository: `/Users/istoica/src/cc-to-rust/ray`
Scope: Re-review after recent fixes to determine whether the Rust GCS implementation is functionally identical to the C++ GCS and can act as a drop-in replacement.

## Executive Summary

The recent fixes materially improved the Rust implementation. In particular, the Rust GCS now has Redis-backed storage support, persisted initialization data loading on startup, cluster ID initialization from KV storage, and a more substantial runtime environment manager than before. Those are meaningful steps toward parity.

However, the Rust GCS is still **not functionally identical** to the C++ GCS, and it is **not yet a drop-in replacement**.

The main reason is no longer just server bootstrap. The remaining gaps are now concentrated in manager behavior and integration semantics. Several major subsystems are still implemented as stubs or simplified approximations, and some externally visible RPC behavior still differs from the C++ implementation. That means a deployment that assumes C++ GCS semantics can still observe behavior differences under normal operation, not only at the edges.

Bottom line:

- Bootstrap and persistence parity improved significantly.
- Full behavioral parity has **not** been reached.
- Rust GCS is **not yet safe to claim as a drop-in replacement** for the C++ GCS.

## What Improved Since The Prior Review

### 1. Redis-backed storage support now exists

The Rust tree now includes a Redis store client:

- `src/ray/rust/gcs/crates/gcs-store/src/redis_store_client.rs`
- `src/ray/rust/gcs/crates/gcs-store/src/lib.rs`

This closes one of the most important gaps from the previous review, where Rust GCS had no production-equivalent persistence backend at all.

### 2. Server startup now supports Redis and persisted cluster metadata

Rust `GcsServer` now supports:

- Redis-backed construction
- cluster ID initialization through KV storage
- loading persisted initialization data at startup
- Redis health checks

Relevant Rust code:

- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:66`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:91`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:147`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:237`

This is a substantial improvement relative to the previous state and moves Rust much closer to C++ server lifecycle behavior.

### 3. Persisted table loading on startup exists now

Rust startup now loads persisted table data:

- `src/ray/rust/gcs/crates/gcs-server/src/init_data.rs`

This is important because the C++ server restores state from persistent storage and expects managers to start from that recovered view.

### 4. Runtime env handling is no longer an empty placeholder

The Rust runtime environment manager now keeps references, tracks expirations, and performs periodic cleanup in memory:

- `src/ray/rust/gcs/crates/gcs-managers/src/runtime_env_stub.rs`

This is a real improvement over the earlier empty stub.

## Overall Verdict

Despite the improvements above, the implementation is still short of drop-in replacement parity because the remaining mismatches affect externally observable behavior:

1. Some services are still explicit stubs.
2. Some RPC responses still omit fields or ignore request filters.
3. Several managers still use local in-memory approximations instead of persisted or integrated behavior.
4. Scheduling-related semantics for actors and placement groups are still simplified versus the C++ GCS.
5. Autoscaler, pubsub, and event export functionality still diverge in meaningful ways.

These are not cosmetic differences. They are behavioral differences that can affect clients, control-plane recovery, observability, autoscaling, or scheduling workflows.

## Detailed Findings

## A. Server Bootstrap And Persistence: Much Better, But Not Sufficient For Parity

### Rust status

Rust server wiring now includes Redis config, health checks, KV-backed cluster ID generation, and init-data loading:

- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:66-89`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:91-104`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:147-235`
- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:237-251`
- `src/ray/rust/gcs/crates/gcs-server/src/init_data.rs`

### C++ comparison

Equivalent C++ bootstrap and persistence behavior is wired in:

- `src/ray/gcs/gcs_server.cc:156-193`
- `src/ray/gcs/gcs_server.cc:217-230`
- `src/ray/gcs/gcs_server.cc:639-675`
- `src/ray/gcs/gcs_server.cc:696-737`

### Assessment

This area improved the most. On its own, it removes a major prior blocker. But parity still depends on the behavior of the individual managers after startup, and that is where significant mismatches remain.

## B. Explicit Stub Services Still Exist In The Rust Server

The Rust server still imports multiple stubbed managers:

- `src/ray/rust/gcs/crates/gcs-server/src/lib.rs:20-28`

Those include:

- `actor_stub`
- `autoscaler_stub`
- `event_export_stub`
- `placement_group_stub`
- `pubsub_stub`
- `runtime_env_stub`

The mere presence of a `*_stub` module is not automatically disqualifying if it fully matches C++ behavior. But in this tree, several of these modules still implement reduced semantics and therefore remain parity blockers.

## C. Event Export Is Still Missing

Rust event export remains effectively unimplemented:

- `src/ray/rust/gcs/crates/gcs-managers/src/event_export_stub.rs:1-19`

This is a direct functionality gap. The C++ GCS has real event framework integration, including startup wiring and export handling:

- `src/ray/gcs/gcs_server_main.cc:108-180`

### Why this matters

If production systems depend on event export behavior, observability hooks, or downstream event consumers, the Rust GCS will not behave as a drop-in replacement.

### Severity

High.

## D. Autoscaler Service Is Still Materially Incomplete

Rust autoscaler behavior is still a stubbed approximation:

- `src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:42-163`

Observed behavior from the Rust implementation:

- `get_cluster_resource_state` returns empty `node_states`
- `get_cluster_resource_state` returns empty `pending_resource_requests`
- `get_cluster_resource_state` returns empty `pending_gang_resource_requests`
- `cluster_session_name` is blank
- `drain_node` accepts requests without matching full C++ semantics
- `resize_raylet_resource_instances` is a no-op

### C++ comparison

The C++ implementation contains real resource state and update handling:

- `src/ray/gcs/gcs_resource_manager.cc:37-59`
- `src/ray/gcs/gcs_resource_manager.cc:129-145`
- `src/ray/gcs/gcs_resource_manager.cc:163-205`

### Why this matters

Autoscaler APIs are externally visible and used by higher-level control-plane logic. Returning empty cluster state or accepting operations without the same semantics as C++ is not drop-in compatible.

### Severity

Critical for parity.

## E. Node Manager Behavior Still Differs From C++

Rust node manager still diverges in several externally visible ways:

- `src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:259-340`

Specific mismatches:

### 1. `GetAllNodeInfo` filtering semantics are not implemented

Rust ignores request-side filters and always reports `num_filtered = 0`.

C++ has richer filtering and accounting:

- `src/ray/gcs/gcs_node_manager.cc:237-378`

### 2. `CheckAlive` does not provide equivalent metadata

Rust returns empty `ray_version`.

C++ includes fuller version and liveness context:

- `src/ray/gcs/gcs_node_manager.cc:199-235`

### 3. `DrainNode` response semantics differ

Rust returns a placeholder empty node ID in the response path noted during inspection.

C++ has real drain handling:

- `src/ray/gcs/gcs_node_manager.cc:380-485`

### 4. Address/liveness results are reduced

Rust `get_all_node_address_and_liveness` only includes alive nodes and sets `death_info: None`.

That loses information compared with C++ node state reporting.

### Why this matters

Node manager behavior is central to cluster orchestration, health reporting, and client/server compatibility. Missing filters and incomplete drain/liveness metadata are user-visible differences.

### Severity

Critical for drop-in parity.

## F. Worker Manager Still Does Not Match C++ Persistence Semantics

Rust `WorkerManager` has a `table_storage` field, but it remains unused:

- `src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs:21-25`

The implementation currently stores worker state only in local in-memory maps:

- `src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs:53-178`

This is reinforced by build warnings indicating the field is unused.

### C++ comparison

C++ worker manager persists and restores worker-related state via GCS storage:

- `src/ray/gcs/gcs_worker_manager.cc:32-131`
- `src/ray/gcs/gcs_worker_manager.cc:152-219`

### Why this matters

If worker state is not persisted and restored with equivalent semantics, behavior after restart or during coordination can diverge. That is incompatible with a drop-in replacement claim.

### Severity

High.

## G. Task Manager Remains A Simplified Approximation

Rust task manager is still much simpler than C++:

- `src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:19-112`

Observed limitations:

- flat in-memory map
- simplified retention and eviction behavior
- statistics such as dropped or truncated counts remain hard-coded to zero

### C++ comparison

The C++ task manager is richer and tracks additional state and stats:

- `src/ray/gcs/gcs_task_manager.cc:36-120`

### Why this matters

Task introspection and task-event behavior are part of the externally visible control-plane surface. Simplified accounting changes behavior and observability.

### Severity

Medium to high, depending on which task APIs are relied upon.

## H. Resource Manager Is Not Fully Integrated With Real Update Sources

Rust resource manager contains internal helper methods but does not appear to be wired to the same update sources as the C++ GCS:

- `src/ray/rust/gcs/crates/gcs-managers/src/resource_manager.rs:18-127`

Relevant methods include:

- `update_node_resources`
- `set_node_draining`
- `on_node_dead`

But the server-side integration path that feeds these updates in C++ is broader and includes syncer-based resource propagation:

- `src/ray/gcs/gcs_server.cc:607-621`
- `src/ray/gcs/gcs_resource_manager.cc:37-59`

### Why this matters

Even if the manager can hold resource data, parity depends on how that data is produced, refreshed, and synchronized. If the Rust server does not wire the same update flow, the externally observed cluster resource state can still differ materially.

### Severity

High.

## I. Placement Group Semantics Are Still Not Equivalent

The Rust implementation explicitly acknowledges a scheduling simplification:

- `src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs:9-22`

The key comment states that without a bundle scheduler, placement groups transition directly to `CREATED` on creation.

Other observed differences:

- `ray_namespace` is set to `String::new()`
- no real scheduler-backed wait/create lifecycle

Relevant Rust sections:

- `src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs:123-126`
- `src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs:147-159`

### C++ comparison

C++ placement group handling includes actual scheduling, retry, lifecycle, and callback behavior:

- `src/ray/gcs/gcs_placement_group_manager.cc:353-476`
- `src/ray/gcs/gcs_placement_group_manager.cc:574-640`

### Why this matters

Placement group lifecycle semantics are observable to clients and schedulers. Immediate success in Rust where C++ schedules asynchronously is not equivalent behavior.

### Severity

Critical for workloads that use placement groups.

## J. Actor Lifecycle Is Improved, But Scheduling Semantics Still Diverge

Rust actor logic has improved and includes real lifecycle bookkeeping, but scheduling is still explicitly stubbed:

- `src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:9-19`

The file-level commentary and creation path still indicate that actor creation transitions are simplified because scheduling is stubbed.

### C++ comparison

C++ actor management includes richer scheduling, reconstruction, and callback semantics:

- `src/ray/gcs/actor/gcs_actor_manager.cc:308-333`
- `src/ray/gcs/actor/gcs_actor_manager.cc:335-425`
- `src/ray/gcs/actor/gcs_actor_manager.cc:427-459`

### Why this matters

Actor creation, restart, and scheduling semantics are a core part of Ray behavior. If Rust promotes actors through lifecycle states differently from C++, that is not drop-in compatible.

### Severity

Critical.

## K. PubSub Coverage Still Appears Incomplete

Rust pubsub support exists, but the publisher surface still appears narrower than the C++ system:

- `src/ray/rust/gcs/crates/gcs-pubsub/src/lib.rs:28-85`

Visible helper coverage includes:

- worker failure
- job info
- node info
- actor info

What is not evident from the Rust implementation is equivalent support for all channels expected in the broader Ray ecosystem, including error/log/resource usage paths.

### Test evidence from Python tree

The Python tests expect GCS pubsub behavior for additional message types:

- `python/ray/tests/test_gcs_pubsub.py:14-30`
- `python/ray/tests/test_gcs_pubsub.py:35-58`
- `python/ray/tests/test_gcs_pubsub.py:61-76`

### Why this matters

If clients rely on those channels and Rust GCS does not provide them with equivalent semantics, then it is not a drop-in replacement.

### Severity

High.

## L. Runtime Env Handling Improved, But Still Does Not Match C++ End-To-End

Rust runtime env handling is no longer empty, but it is still local and simplified:

- `src/ray/rust/gcs/crates/gcs-managers/src/runtime_env_stub.rs:1-190`

The current implementation performs in-memory URI reference counting and timed expiration. That is better than before, but it still does not show equivalent integration with the C++ runtime env manager and deleter path:

- `src/ray/gcs/runtime_env_handler.cc:22-42`
- `src/ray/gcs/gcs_server.cc:696-737`

### Why this matters

Runtime env lifecycle correctness depends on more than just reference counting. It also depends on integration with deletion, storage, and cluster coordination behavior.

### Severity

Medium to high.

## Test And Build Evidence

I reran the Rust GCS cargo tests from:

- `/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs`

Observed results:

- `gcs-kv`: passed
- `gcs-managers`: passed
- `gcs-pubsub`: passed
- `gcs-server`: mostly passed, but two startup tests failed in this environment because socket bind returned `PermissionDenied`

Failing tests:

- `tests::test_start`
- `tests::test_start_with_listener`

The failures appear environmental rather than parity-related because they are bind-permission errors during local test startup. They do not change the functional parity conclusions above.

The build also emitted warnings that are relevant to parity analysis, including:

- unused `table_storage` in `worker_manager.rs`
- unused paths in runtime env handling
- unused `publisher` in server wiring

Those warnings reinforce that some parity-related integration paths are still incomplete.

## Conclusion

The recent fixes closed several major bootstrap-level gaps and significantly improved the credibility of the Rust GCS implementation. In particular, Redis storage support, persisted startup state loading, cluster ID initialization, and better runtime env handling are all real progress.

That said, the Rust GCS still does **not** provide identical functionality to the C++ GCS, and it should **not** yet be treated as a drop-in replacement.

The strongest remaining blockers are:

- event export is still stubbed
- autoscaler behavior is still incomplete
- node manager RPC behavior still differs
- worker persistence semantics still differ
- resource manager integration is incomplete
- placement group lifecycle is simplified
- actor scheduling semantics are still stubbed
- pubsub coverage still appears incomplete

## Recommended Acceptance Standard Before Declaring Drop-In Replacement

I would not consider the Rust GCS a drop-in replacement until all of the following are demonstrated:

1. No remaining externally visible stub behavior in actor, autoscaler, placement group, event export, or pubsub services.
2. RPC-level response parity for node, worker, task, and resource APIs, including filters, metadata fields, and recovery semantics.
3. Persistent storage and restart behavior that matches C++ for workers, nodes, jobs, placement groups, actors, and runtime env state where applicable.
4. Equivalent control-plane integration for resource updates, runtime env deletion, event export, and autoscaler state publication.
5. Validation against the relevant Python and integration test suites that currently encode expected C++ GCS behavior.

Until then, the correct statement is:

The Rust GCS is improving rapidly and is materially closer to the C++ GCS than in the prior review, but it is still not functionally identical and not yet a drop-in replacement.
