# Rust GCS vs C++ GCS Parity Review

Date: 2026-04-14

## Executive Summary

The Rust GCS implementation is **not functionally identical** to the C++ GCS implementation today, and it is **not yet a safe drop-in replacement** for the C++ GCS binary.

The main issue is not the RPC surface alone. The Rust server does expose the same top-level gRPC service types, but several services are stubs or materially simplified, and multiple system-level behaviors that Ray depends on in production are either absent or not wired up:

1. **No persistent storage / failover durability**
2. **Actor lifecycle and placement group scheduling semantics are substantially simplified**
3. **Runtime env, event export, pubsub channels, and autoscaler behavior are incomplete**
4. **Resource tracking is not connected to any real update source**
5. **Node, worker, and task semantics are materially weaker than C++**
6. **Process startup/configuration/observability behavior is much narrower than the C++ binary**

Because Ray relies on GCS for correctness across node membership, actor lifecycle, placement groups, autoscaling state, runtime env pinning, worker failure propagation, and recovery after restart, these gaps are blockers for drop-in replacement.

## Review Scope and Method

I reviewed:

- Rust implementation under `ray/src/ray/rust/gcs`
- C++ implementation under `ray/src/ray/gcs`
- Selected Ray Python tests that exercise expected GCS behavior

The review focused on whether Rust GCS provides the **same externally observable behavior** as C++ GCS, not just whether equivalent file/module names exist.

## High-Confidence Verdict

**Verdict: Not equivalent. Not drop-in compatible.**

The Rust implementation is best described as:

- a partial GCS reimplementation,
- with matching protobuf/gRPC service shells,
- but with several intentionally simplified behaviors,
- and without the persistence, recovery, scheduling, resource propagation, and observability guarantees that the C++ implementation provides.

## Findings

### 1. Blocker: Rust GCS has no Redis-backed persistent storage and therefore cannot match C++ failover/restart behavior

This is the largest parity gap.

#### Rust evidence

- The Rust storage crate explicitly exports only `InMemoryStoreClient`, with Redis called out as future work:
  - `ray/src/ray/rust/gcs/crates/gcs-store/src/lib.rs:1-10`
- `GcsServer::new` always constructs `InMemoryStoreClient`:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:99-101`
- Rust server config has no Redis address/port/auth/storage-type fields:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:43-50`

#### C++ evidence

- C++ supports both in-memory and Redis persistence, and uses Redis health checks when configured:
  - `ray/src/ray/gcs/gcs_server.cc:156-193`
- C++ also separately initializes KV on the configured backend:
  - `ray/src/ray/gcs/gcs_server.cc:639-675`

#### Why this breaks drop-in behavior

Ray’s production/fault-tolerant GCS behavior depends on persistence across GCS restarts. Without Redis-backed storage:

- cluster metadata disappears on restart,
- actor / placement group / worker / node metadata is lost,
- cluster ID persistence semantics differ,
- tests and production flows relying on external Redis-backed restart behavior cannot pass.

This is directly contradicted by Ray’s existing fault-tolerance tests, for example:

- `ray/python/ray/tests/test_gcs_fault_tolerance.py:61-80`
- `ray/python/ray/tests/test_gcs_fault_tolerance.py:122-130`

Those tests assume GCS restart preserves enough durable state for the cluster to continue functioning. The Rust implementation cannot currently do that.

### 2. Blocker: Cluster ID generation/persistence does not match C++ semantics

#### Rust evidence

- Rust `GcsServerConfig::default()` generates a pseudo-random 28-byte cluster ID in process memory:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:52-71`
- `GcsServer::new` passes that config value directly to the node manager and pubsub manager:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:104-110`
- There is no KV-backed get-or-generate flow in Rust.

#### C++ evidence

- C++ loads persisted tables and initializes KV before obtaining the cluster ID:
  - `ray/src/ray/gcs/gcs_server.cc:217-230`
- C++ persists the cluster ID in KV if missing, otherwise reuses the stored one:
  - `ray/src/ray/gcs/gcs_server.cc:233-260` (via `GetOrGenerateClusterId`; see surrounding logic started at `217`)

#### Impact

On GCS restart, Rust will generate a fresh cluster ID unless an external launcher injects the same value. That is not the same behavior as the C++ server and breaks identity/failover expectations.

### 3. Blocker: Actor management is materially simplified and does not preserve C++ actor lifecycle semantics

#### Rust evidence

- `create_actor` explicitly says it skips scheduling and marks actors `ALIVE` immediately:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:177-180`
- The implementation directly transitions the actor to `ALIVE` and returns:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:196-239`

#### C++ evidence

- C++ actor registration and creation are separate, stateful operations:
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:308-333`
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:427-459`
- C++ `CreateActor` goes through actual scheduling / push-task flow and can return cancellation or failure information:
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:435-452`
- C++ restart-for-lineage logic has strict idempotency, stale-message handling, restartability checks, and asynchronous restart orchestration:
  - `ray/src/ray/gcs/actor/gcs_actor_manager.cc:335-425`

#### Impact

The Rust behavior is not equivalent in at least these ways:

- actor creation success is declared too early,
- placement/scheduling failure paths are not represented,
- restart semantics are weaker,
- state transitions are compressed,
- caller-visible timing and failure behavior diverge from C++.

This is a functional mismatch, not just an implementation detail.

### 4. Blocker: Placement group management skips actual scheduling and returns readiness immediately

#### Rust evidence

- File-level comment states placement groups are immediately set to `CREATED` and actual scheduling is not performed:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs:3-6`
- `create_placement_group` sets state to `CREATED` immediately:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs:69-83`
- `wait_placement_group_until_ready` returns OK unconditionally:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/placement_group_stub.rs:206-213`

#### C++ evidence

- C++ placement groups go through registration, lifecycle tracking, resource destruction, queue management, and state persistence:
  - `ray/src/ray/gcs/gcs_placement_group_manager.cc:353-476`
- `WaitPlacementGroupUntilReady` genuinely waits for creation or returns meaningful failure/not-found outcomes:
  - `ray/src/ray/gcs/gcs_placement_group_manager.cc:574-640`

#### Impact

Any code depending on real placement group scheduling, infeasibility, delayed readiness, rescheduling, removal-before-created, or persistent load/state is not getting C++-equivalent behavior from Rust.

### 5. Blocker: Runtime env service is a stub and does not implement pin/reference expiry semantics

#### Rust evidence

- Runtime env service is explicitly a stub returning default replies:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/runtime_env_stub.rs:1-19`

#### C++ evidence

- C++ `HandlePinRuntimeEnvURI` adds a URI reference, schedules expiry, and only replies after the reference is established:
  - `ray/src/ray/gcs/runtime_env_handler.cc:22-42`
- C++ also wires an actual runtime env manager into the server:
  - `ray/src/ray/gcs/gcs_server.cc:696-737`

#### Impact

Runtime environment artifact lifetime management is not equivalent. Rust currently acknowledges the RPC without enforcing the retention semantics that the C++ implementation provides.

### 6. Blocker: Event export service is a stub

#### Rust evidence

- Event aggregator/export is explicitly stubbed and returns an empty success reply:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/event_export_stub.rs:1-19`

#### C++ evidence

- C++ initializes event export plumbing and event framework in the process startup path:
  - `ray/src/ray/gcs/gcs_server_main.cc:144-160`
- The service is a first-class registered RPC service in C++:
  - `ray/src/ray/gcs/grpc_services.cc:208-216`

#### Impact

Any downstream component relying on event export side effects or persisted/exported event streams will not observe equivalent behavior.

### 7. Blocker: Autoscaler state behavior is much weaker than C++

#### Rust evidence

- Rust returns empty `node_states`, empty pending requests, empty gang requests, and blank session name:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:46-68`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:122-135`
- `drain_node` simply accepts and returns success:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:138-153`
- `report_cluster_config` stores config only in memory:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/autoscaler_stub.rs:86-94`

#### C++ evidence

- C++ builds cluster resource state from real node, actor, placement-group, raylet, and KV-backed inputs:
  - `ray/src/ray/gcs/gcs_autoscaler_state_manager.cc:48-75`
  - `ray/src/ray/gcs/gcs_autoscaler_state_manager.cc:170-186`
- C++ persists cluster config in KV for failover:
  - `ray/src/ray/gcs/gcs_autoscaler_state_manager.cc:144-156`
- C++ drain-node handling is a real operation, not unconditional acceptance:
  - `ray/src/ray/gcs/gcs_autoscaler_state_manager.cc:457` and surrounding implementation

#### Impact

The autoscaler view exposed by Rust does not describe the real cluster state in the way C++ does. This is a major behavior mismatch for autoscaler integration and for any clients reading cluster state.

### 8. Blocker: Resource manager is not wired to any real update source, so resource RPCs will remain empty in real use

#### Rust evidence

- Rust resource manager has only local mutation helpers (`update_node_resources`, `set_node_draining`, `on_node_dead`):
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/resource_manager.rs:26-48`
- The RPC getters simply return the current in-memory maps:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/resource_manager.rs:51-127`
- `GcsServer::new` constructs `GcsResourceManager`, but the server does not wire any raylet syncer or other source of resource updates into it:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:123-125`

#### C++ evidence

- C++ initializes `RaySyncer` and registers the resource manager as a consumer of resource view updates:
  - `ray/src/ray/gcs/gcs_server.cc:607-621`
- C++ resource manager consumes sync messages and updates resource state:
  - `ray/src/ray/gcs/gcs_resource_manager.cc:37-59`
  - `ray/src/ray/gcs/gcs_resource_manager.cc:129-145`
  - `ray/src/ray/gcs/gcs_resource_manager.cc:163-205`

#### Impact

Even if the RPC endpoints exist, they do not represent live cluster resources the way the C++ GCS does. In a real cluster, Rust resource queries would be incomplete or empty.

### 9. Major: PubSub coverage is incomplete relative to C++ channels and Ray expectations

#### Rust evidence

- Rust `GcsPublisher` exposes helper publishers only for worker failure, job, node info, and actor:
  - `ray/src/ray/rust/gcs/crates/gcs-pubsub/src/lib.rs:51-85`
- Rust server registers `InternalPubSubGcsService`, but no Rust-side logic publishes C++’s error/log/resource-usage channels.

#### C++ evidence

- C++ publisher is initialized with these channels:
  - `GCS_ACTOR_CHANNEL`
  - `GCS_JOB_CHANNEL`
  - `GCS_NODE_INFO_CHANNEL`
  - `GCS_WORKER_DELTA_CHANNEL`
  - `RAY_ERROR_INFO_CHANNEL`
  - `RAY_LOG_CHANNEL`
  - `RAY_NODE_RESOURCE_USAGE_CHANNEL`
  - `GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL`
  - Source: `ray/src/ray/gcs/gcs_server.cc:195-210`

#### Ray test evidence

- Ray Python tests expect publish/subscribe for error info, logs, and node resource usage:
  - `ray/python/ray/tests/test_gcs_pubsub.py:14-30`
  - `ray/python/ray/tests/test_gcs_pubsub.py:35-58`
  - `ray/python/ray/tests/test_gcs_pubsub.py:61-76`

#### Impact

PubSub is not behaviorally equivalent from a Ray client’s point of view. Matching the long-poll service alone is not sufficient if the expected channels/messages are not produced.

### 10. Major: Worker manager does not persist or reload worker state the way C++ does

#### Rust evidence

- Rust worker manager stores workers only in `DashMap` and never writes to `table_storage` in any RPC path:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs:21-25`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs:55-64`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs:116-154`
- The unused `table_storage` field is a strong signal this path is incomplete:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/worker_manager.rs:23`

#### C++ evidence

- C++ persists worker info and worker failures to `WorkerTable`:
  - `ray/src/ray/gcs/gcs_worker_manager.cc:106-107`
  - `ray/src/ray/gcs/gcs_worker_manager.cc:219`
- C++ `GetAllWorkerInfo` reads from table storage rather than only a process-local cache:
  - `ray/src/ray/gcs/gcs_worker_manager.cc:152-199`

#### Impact

Rust worker state disappears on restart and may diverge from persistent expectations even during runtime. This is not equivalent to C++ behavior.

### 11. Major: Node manager behavior is materially weaker than C++

#### Rust evidence

- `get_all_node_info` ignores filters and always reports `num_filtered = 0`:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:259-279`
- `check_alive` returns empty `ray_version`:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:282-299`
- `drain_node` returns a placeholder empty node ID rather than the requested drained nodes:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:302-316`
- `get_all_node_address_and_liveness` only returns alive nodes and always sets `death_info = None`:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/node_manager.rs:318-340`

#### C++ evidence

- C++ supports selectors, state filters, limits, totals, and filtered counts for `GetAllNodeInfo`:
  - `ray/src/ray/gcs/gcs_node_manager.cc:237-378`
- C++ `GetAllNodeAddressAndLiveness` includes alive and dead nodes and copies `death_info`:
  - `ray/src/ray/gcs/gcs_node_manager.cc:380-485`
- C++ `HandleDrainNode` drains specific nodes through raylet interaction:
  - `ray/src/ray/gcs/gcs_node_manager.cc:199-235`

#### Impact

Clients relying on filtering, accurate node liveness snapshots, death info, or drain semantics will observe different behavior.

### 12. Major: Task manager semantics are much simpler than C++

#### Rust evidence

- Rust stores task events in a flat hash map with arbitrary eviction of the first key encountered:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:18-31`
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:48-66`
- Returned stats are mostly hard-coded zeros:
  - `ray/src/ray/rust/gcs/crates/gcs-managers/src/task_manager.rs:95-102`

#### C++ evidence

- C++ task manager has GC policy, priority lists, per-job summaries, dropped-event accounting, worker/job failure marking, and periodic cleanup:
  - `ray/src/ray/gcs/gcs_task_manager.cc:36-53`
  - `ray/src/ray/gcs/gcs_task_manager.cc:55-120`

#### Impact

The Rust task event store does not preserve C++ semantics for prioritization, truncation accounting, or failure propagation. It may be acceptable for basic tests, but it is not behaviorally identical.

### 13. Major: Startup/configuration/observability behavior is much narrower than the C++ binary

#### Rust evidence

- Rust main parses only a small subset of flags and ignores many C++ startup concerns:
  - `ray/src/ray/rust/gcs/crates/gcs-server/src/main.rs:15-90`

#### C++ evidence

- C++ main accepts Redis settings, retry flags, metrics agent port, node IP, session name, logging config, etc.:
  - `ray/src/ray/gcs/gcs_server_main.cc:42-58`
- C++ initializes `RayConfig`, metrics, event framework, IO context behavior, and structured startup:
  - `ray/src/ray/gcs/gcs_server_main.cc:108-180`

#### Impact

Even if the Rust binary starts, it does not yet behave like the C++ production binary from a deployment and observability perspective.

## Items That Are Closer to Parity

The following areas look materially closer, though still not enough to make the whole server drop-in compatible:

- Internal KV basic CRUD and namespace prefix behavior are reasonably aligned at the RPC level.
  - Rust: `ray/src/ray/rust/gcs/crates/gcs-kv/src/lib.rs:165-280`
  - C++: `ray/src/ray/gcs/gcs_kv_manager.cc:24-156`
- Basic job add / mark-finished / list / next-id flows are implemented and tested.
- Internal pubsub long-poll mechanics exist and are more than a placeholder.

These are positive signs, but they do not offset the blocker-level gaps above.

## Verification Performed

### Static comparison

I compared the Rust implementation against the corresponding C++ managers and server wiring, plus selected Ray Python tests that define expected GCS behavior.

### Rust test execution

Command run:

```bash
cd ray/src/ray/rust/gcs
cargo test
```

Observed result:

- `gcs_kv`: passed
- `gcs_managers`: passed
- `gcs_pubsub`: passed
- `gcs_server`: 2 tests failed in this sandbox due to socket bind permission errors

Failure details:

- `crates/gcs-server/src/lib.rs:330`
- `crates/gcs-server/src/lib.rs:396`

Error:

```text
PermissionDenied / Operation not permitted
```

I do **not** treat those two bind failures as parity findings; they are sandbox/environment limitations. They do mean full server-level verification was incomplete in this environment.

## Bottom Line

The Rust GCS implementation is **not ready to replace** the C++ GCS implementation in Ray if the requirement is “identical functionality” or “drop-in replacement.”

The decisive blockers are:

1. No durable Redis-backed storage
2. No C++-equivalent restart/failover behavior
3. Simplified actor lifecycle
4. Simplified placement group lifecycle
5. Stub runtime env and event export services
6. Incomplete autoscaler/resource/pubsub behavior
7. Weaker node/worker/task semantics

## Recommended Next Steps Before Claiming Drop-In Compatibility

1. Implement Redis-backed store support and restart-state recovery.
2. Persist and reload cluster ID exactly as C++ does.
3. Replace actor and placement group stubs/simplifications with full lifecycle logic.
4. Implement real runtime env pinning/reference expiry and event export.
5. Wire resource manager to real raylet/sync updates.
6. Expand pubsub to cover error, log, and node-resource-usage channels.
7. Align node/worker/task filtering, counts, status, and persistence behavior.
8. Validate against existing C++ and Python GCS integration/fault-tolerance tests, not only Rust unit tests.

