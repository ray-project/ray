# Ray C++ to Rust Backend Port Review

**Date:** 2026-03-17
**Branch:** `cc-to-rust-experimental`
**Reviewer:** Codex
**Status:** Completed

## Executive Summary

This review finds that the Rust branch is not functionally equivalent to Ray’s
original C++ backend.

The strongest conclusions are:

- The Rust port still contains many behavior-reducing rewrites and explicit
  stubs, not just implementation-language changes.
- The highest-risk mismatches are in GCS control-plane behavior, Raylet lease
  and worker lifecycle, Core Worker ownership/reference semantics, Object
  Manager transfer/spill behavior, and the Python/native `_raylet` contract.
- Several differences are likely user-visible even without rare failure
  injection, especially around job/worker/actor RPCs, `ray.wait`, object fetch
  and spill/restore, actor task behavior, and Python `ObjectRef` / `CoreWorker`
  behavior.

Highest-priority likely defects:

1. GCS functionality is incomplete in multiple manager/service paths, including
   internal KV persistence, job/worker/actor operations, placement groups, and
   autoscaler/drain behavior.
2. Raylet semantics diverge in worker lease return, dependency tracking,
   `wait`, worker-pool matching, cleanup RPCs, and shutdown/drain control.
3. Core Worker semantics diverge in reference counting, distributed ownership,
   task and actor submission, dependency resolution, and object recovery.
4. Object Manager semantics diverge in distributed location tracking, pull
   prioritization/failure behavior, push serving, chunk validation/sealing, and
   spill/restore integration.
5. The Python boundary is not a drop-in replacement for Cython `_raylet`; the
   exposed API and behavior are materially smaller and different.

Recommended next actions:

1. Treat this branch as experimental, not behaviorally interchangeable with the
   C++ backend.
2. Start with the Tier 1 and Tier 2 tests in the prioritized test plan at the
   end of this report.
3. Use those tests to separate intentional redesigns from actual bugs before
   making broader equivalence claims.

## Goal

Review the Rust backend port against the original C++ backend and determine:

1. Whether the Rust implementation is functionally equivalent to the C++ implementation.
2. Where the Rust port differs structurally or behaviorally from the C++ implementation.
3. Which differences look intentional, benign, risky, or incorrect.
4. Which additional tests would best expose likely defects or semantic mismatches.

## Review Method

The review is being executed in the following order:

1. Build a subsystem mapping from C++ modules to Rust crates/modules.
2. Derive behavioral contracts from the C++ implementation, protobuf surfaces, and C++ tests.
3. Compare Rust behavior at the RPC/protobuf boundary before comparing internals.
4. Audit each major subsystem:
   - GCS
   - Raylet
   - Core Worker
   - Object Manager
   - Python/PyO3/RDT boundary
5. Classify each observed difference as:
   - Equivalent redesign
   - Intentional behavior change
   - Likely bug in Rust
   - Inherited C++ bug or quirk
   - Missing or unclear
6. Propose tests that would expose each risky mismatch.

## Working Notes

This document is intentionally incremental. It records the actual review path,
the code inspected, and intermediate findings as the audit proceeds.

## Step Trace

### Step 1: Establish scope and subsystem map

Inspected:

- `rust/Cargo.toml`
- `rust/CONVERSION_PLAN.md`
- `rust/TEST_COVERAGE_REPORT.md`
- `src/ray/gcs/`
- `src/ray/raylet/`
- `src/ray/core_worker/`
- `src/ray/object_manager/`
- `rust/ray-gcs/`
- `rust/ray-raylet/`
- `rust/ray-core-worker/`
- `rust/ray-object-manager/`
- `rust/ray-core-worker-pylib/`

Initial observations:

- The Rust port is a full multi-crate backend workspace, not a narrow module translation.
- The Rust implementation is not a strict file-for-file port. Several concepts are merged, split, or redesigned.
- Because of that, functional equivalence must be established at behavioral boundaries:
  RPCs, protobuf payloads, storage semantics, scheduling decisions, ownership/refcount state, and Python-visible behavior.
- The branch includes internal porting and test-coverage reports, but those are treated as claims to verify, not as proof.

Subsystem map:

- GCS:
  `src/ray/gcs/*` -> `rust/ray-gcs/src/*`
- Raylet:
  `src/ray/raylet/*` and `src/ray/raylet/scheduling/*` -> `rust/ray-raylet/src/*`
- Core worker:
  `src/ray/core_worker/*` -> `rust/ray-core-worker/src/*`
- Object manager:
  `src/ray/object_manager/*` -> `rust/ray-object-manager/src/*`
- Python/native boundary:
  C++ native bindings and Python-facing backend hooks -> `rust/ray-core-worker-pylib/`, `rust/ray/_raylet.py`, `rust/ray/rdt.py`

## Current Execution Plan

1. Build a more exact per-module mapping table.
2. Start with GCS and compare:
   - service surface
   - storage behavior
   - state transitions
   - idempotency and ordering
   - test coverage parity
3. Expand to Raylet and Core Worker after documenting GCS findings.

## Findings

### GCS: first-pass verified differences

This section covers the first detailed audit pass over the GCS server and its
main service handlers. The focus was on externally visible behavior, not
implementation style.

#### Finding GCS-1: Rust GCS internal KV is in-memory even when server storage is Redis

Severity: High

Evidence:

- Rust creates a Redis `StoreClient` for table storage, but still constructs the
  KV manager with `InMemoryInternalKV`:
  [`rust/ray-gcs/src/server.rs:166`](/Users/istoica/src/ray/rust/ray-gcs/src/server.rs#L166)
- C++ wires `GcsInternalKVManager` to `StoreClientInternalKV`, so internal KV
  uses the configured backing store:
  [`src/ray/gcs/gcs_server.cc:647`](/Users/istoica/src/ray/src/ray/gcs/gcs_server.cc#L647)

Impact:

- Internal KV data will not survive GCS restart in the Rust implementation.
- Any behavior that depends on KV persistence or HA semantics can diverge from C++.
- This also likely breaks parity for any component that expects internal KV to
  share the GCS storage backend.

Assessment:

- This is a real behavioral mismatch, not just a refactor.
- It is especially risky because the server still appears to support Redis mode.

Suggested exposing tests:

- Start Rust GCS with Redis configured, write KV entries, restart GCS, verify
  the entries still exist. The same test should pass against C++ and Rust.
- Add a conformance test that writes both job-table data and internal KV data,
  restarts the server, and compares post-restart visibility.

#### Finding GCS-2: `ReportJobError` is stubbed in Rust

Severity: High

Evidence:

- Rust returns a default reply without processing the request:
  [`rust/ray-gcs/src/grpc_services.rs:103`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L103)
- C++ publishes the job error to GCS pubsub:
  [`src/ray/gcs/gcs_job_manager.cc:463`](/Users/istoica/src/ray/src/ray/gcs/gcs_job_manager.cc#L463)

Impact:

- Job error propagation can silently disappear in the Rust backend.
- Python or dashboard consumers subscribed to job errors may observe missing
  error events despite successful RPC replies.

Assessment:

- This looks like a likely bug or incomplete port, not an equivalent redesign.

Suggested exposing tests:

- Submit a `ReportJobError` request and verify that a subscriber on the
  appropriate error channel receives the published error payload.
- Add a Python end-to-end test that causes a driver/job error and verifies the
  expected error event appears through the normal reporting path.

#### Finding GCS-3: `on_node_dead` in the Rust job manager is explicitly a no-op

Severity: High

Evidence:

- Rust leaves node-death job cleanup unimplemented:
  [`rust/ray-gcs/src/job_manager.rs:191`](/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs#L191)
- C++ marks jobs finished when the driver node dies:
  [`src/ray/gcs/gcs_job_manager.cc:482`](/Users/istoica/src/ray/src/ray/gcs/gcs_job_manager.cc#L482)

Impact:

- Jobs whose drivers die on a node failure may remain live in Rust when they
  should transition to finished/dead.
- This can leak running-job state, skew metrics, and cause stale job listings.

Assessment:

- This is a confirmed semantic gap.
- The code comment in Rust acknowledges missing state needed for parity.

Suggested exposing tests:

- Register a job whose driver address points to a node, simulate node death, and
  verify the job is marked dead in both storage and `GetAllJobInfo`.
- Add a restart test to ensure the finished state remains persisted after
  node-triggered cleanup.

#### Finding GCS-4: Rust `GetAllJobInfo` omits C++ enrichment behavior

Severity: High

Evidence:

- Rust returns the cached job table entries directly:
  [`rust/ray-gcs/src/job_manager.rs:165`](/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs#L165)
- C++ additionally:
  - filters jobs,
  - optionally queries workers for `is_running_tasks`,
  - loads Jobs API metadata from internal KV,
  - populates `job_info`:
  [`src/ray/gcs/gcs_job_manager.cc:300`](/Users/istoica/src/ray/src/ray/gcs/gcs_job_manager.cc#L300)

Impact:

- Rust `GetAllJobInfo` can return structurally valid but semantically incomplete
  replies.
- The most likely observable differences are missing `is_running_tasks`,
  missing submission metadata in `job_info`, and differing filter behavior.

Assessment:

- This is broader than a single missing field. It changes the effective contract
  of the RPC.

Suggested exposing tests:

- Add a job submitted through the Ray Job API, store the corresponding internal
  KV metadata, and verify `GetAllJobInfo` returns the populated `job_info`.
- Add a worker-backed test that verifies `is_running_tasks` is set or cleared in
  the same cases as C++.
- Add a request-filter conformance test for the `limit` and skip-field options.

#### Finding GCS-5: Rust `UnregisterNode` drops `node_death_info`

Severity: Medium

Evidence:

- Rust passes only `request.node_id` into the node manager:
  [`rust/ray-gcs/src/grpc_services.rs:143`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L143)
- C++ copies `request.node_death_info()` into the dead-node delta and publishes it:
  [`src/ray/gcs/gcs_node_manager.cc:169`](/Users/istoica/src/ray/src/ray/gcs/gcs_node_manager.cc#L169)

Impact:

- Cause-of-death and associated metadata may be lost on graceful unregister in Rust.
- Clients querying dead nodes or listening on node pubsub can observe less
  information than with C++.

Assessment:

- This is a concrete data-loss difference at the API boundary.

Suggested exposing tests:

- Send `UnregisterNode` with a populated `node_death_info` and assert that:
  - the persisted dead node contains it,
  - `GetAllNodeInfo` returns it,
  - the pubsub update contains it.

#### Finding GCS-6: Rust `DrainNode` does not perform the C++ side effect

Severity: Medium to High

Evidence:

- Rust records the node as draining with deadline `0` in the RPC layer and
  returns success:
  [`rust/ray-gcs/src/grpc_services.rs:189`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L189)
- Rust node manager only updates an in-memory `draining_nodes` map:
  [`rust/ray-gcs/src/node_manager.rs:183`](/Users/istoica/src/ray/rust/ray-gcs/src/node_manager.rs#L183)
- C++ `DrainNode` triggers a `ShutdownRaylet` RPC on the target raylet:
  [`src/ray/gcs/gcs_node_manager.cc:197`](/Users/istoica/src/ray/src/ray/gcs/gcs_node_manager.cc#L197)

Impact:

- A caller can receive a successful drain reply from Rust without the node
  actually being drained or shut down.
- This can cause operational divergence in autoscaler or maintenance workflows.

Assessment:

- The semantics are weaker in Rust than in C++.
- Whether this is acceptable depends on whether another Rust path performs the
  actual draining. In this RPC path, parity is not present.

Suggested exposing tests:

- Integration test with a fake or instrumented raylet client verifying that
  `DrainNode` results in a shutdown/drain RPC to the target node.
- End-to-end test that drains a node and asserts its worker capacity actually
  disappears within a bounded time.

#### Finding GCS-7: Rust `GetAllNodeAddressAndLiveness` ignores request filters

Severity: Medium

Evidence:

- Rust ignores the request payload and always returns all nodes:
  [`rust/ray-gcs/src/grpc_services.rs:215`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L215)
- C++ honors `node_ids`, `state_filter`, and `limit`:
  [`src/ray/gcs/gcs_node_manager.cc:455`](/Users/istoica/src/ray/src/ray/gcs/gcs_node_manager.cc#L455)

Impact:

- Rust callers cannot rely on the filtering contract of this RPC.
- This can increase payload size and produce different results for autoscaler or
  control-plane callers that expect filtered responses.

Assessment:

- Clear API contract mismatch.

Suggested exposing tests:

- Conformance test with mixed alive/dead nodes and a request that filters by:
  specific node IDs, state, and limit. Compare exact reply contents between C++
  and Rust.

#### Finding GCS-8: Rust pubsub semantics differ from C++ in long-polling and command handling

Severity: Medium

Evidence:

- Rust long-polling ignores `request.publisher_id`, imposes a fixed 1-second
  timeout at the gRPC layer, and returns a server-side `publisher_id` field:
  [`rust/ray-gcs/src/grpc_services.rs:920`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L920)
- C++ forwards both `subscriber_id` and `publisher_id` to the underlying
  publisher connection logic:
  [`src/ray/gcs/pubsub_handler.cc:40`](/Users/istoica/src/ray/src/ray/gcs/pubsub_handler.cc#L40)
- Rust unsubscribe handling removes the whole subscriber registration:
  [`rust/ray-gcs/src/pubsub_handler.rs:239`](/Users/istoica/src/ray/rust/ray-gcs/src/pubsub_handler.rs#L239)
- C++ supports channel/key-specific unregister and tracks sender-to-subscriber
  mappings via `sender_id`:
  [`src/ray/gcs/pubsub_handler.cc:57`](/Users/istoica/src/ray/src/ray/gcs/pubsub_handler.cc#L57)

Impact:

- Reconnect and publisher identity semantics may diverge.
- Unsubscribing one channel/key in Rust appears to remove the entire subscriber,
  which is stronger than the C++ behavior.
- Callers depending on `sender_id`-based cleanup may not be handled correctly.

Assessment:

- This needs deeper review, but the current code already shows visible contract
  differences.

Suggested exposing tests:

- Subscribe one subscriber to multiple channels/keys, unsubscribe only one, and
  verify the others remain active.
- Exercise reconnect/poll flows with non-empty `publisher_id` and compare the
  next poll semantics with C++.
- Add a sender-disconnect cleanup test using `sender_id`.

#### Finding GCS-9: Rust actor lineage-reconstruction restart RPC is stubbed

Severity: High

Evidence:

- Rust returns a default reply without executing restart logic:
  [`rust/ray-gcs/src/grpc_services.rs:356`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L356)
- C++ has non-trivial logic for:
  - permanently dead actors,
  - stale restart requests,
  - duplicate in-flight requests,
  - actual actor restart:
  [`src/ray/gcs/actor/gcs_actor_manager.cc:335`](/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc#L335)

Impact:

- Lineage reconstruction can falsely appear to succeed in Rust while doing nothing.
- This risks broken recovery after owner failures or replay scenarios.

Assessment:

- This is a major semantic hole in a failure-handling path.

Suggested exposing tests:

- Build a conformance test where an actor requires lineage reconstruction and
  verify that the restart count, actor state, and reply semantics match C++.
- Add an idempotency test for duplicate restart requests.

#### Finding GCS-10: Rust `ReportActorOutOfScope` is stubbed

Severity: High

Evidence:

- Rust returns success without acting on the request:
  [`rust/ray-gcs/src/grpc_services.rs:416`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L416)
- C++ checks stale reports and may destroy the actor with graceful shutdown:
  [`src/ray/gcs/actor/gcs_actor_manager.cc:276`](/Users/istoica/src/ray/src/ray/gcs/actor/gcs_actor_manager.cc#L276)

Impact:

- Out-of-scope actors may remain alive in Rust when C++ would destroy them.
- This can leak actor state and alter lifecycle guarantees observed by clients.

Assessment:

- Another confirmed lifecycle mismatch in GCS actor management.

Suggested exposing tests:

- Report an actor out of scope and verify:
  - the actor transitions to dead,
  - pubsub reflects the state change,
  - stale reports after restart are ignored consistently with C++.

#### Finding GCS-11: Rust `GetWorkerInfo` and worker update RPCs are stubbed or reduced

Severity: Medium to High

Evidence:

- Rust explicitly returns empty for `GetWorkerInfo`:
  [`rust/ray-gcs/src/grpc_services.rs:619`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L619)
- Rust returns default replies for `UpdateWorkerDebuggerPort` and
  `UpdateWorkerNumPausedThreads` without performing storage updates:
  [`rust/ray-gcs/src/grpc_services.rs:656`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L656)
- C++ implements all three behaviors against persisted worker state:
  [`src/ray/gcs/gcs_worker_manager.cc:134`](/Users/istoica/src/ray/src/ray/gcs/gcs_worker_manager.cc#L134)

Impact:

- Single-worker lookup can silently fail in Rust even when the worker exists.
- Debugger port and paused-thread counters can diverge from C++ and from actual
  worker state.
- Any dashboard or control-plane code depending on these fields may observe
  missing or stale data.

Assessment:

- These are confirmed API-surface gaps.
- The effect is not necessarily fatal for all workloads, but it is not
  functionally equivalent.

Suggested exposing tests:

- Add roundtrip tests that:
  - add worker info,
  - update debugger port,
  - update paused-thread count,
  - fetch the worker again,
  and verify the stored fields changed as in C++.
- Add a filter test for `GetAllWorkerInfo` once worker filters are reviewed,
  since the C++ implementation also supports filtering beyond just `limit`.

#### Finding GCS-12: Rust placement-group creation collapses scheduling into immediate `Created`

Severity: High

Evidence:

- Rust converts the spec directly into `PlacementGroupTableData` and forces
  `state = Created` with the comment "single-node: no 2PC needed":
  [`rust/ray-gcs/src/grpc_services.rs:689`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L689)
- C++ registers the placement group and drives it through a scheduling pipeline:
  [`src/ray/gcs/gcs_placement_group_manager.cc:353`](/Users/istoica/src/ray/src/ray/gcs/gcs_placement_group_manager.cc#L353)

Impact:

- Rust skips the pending/rescheduling lifecycle that C++ exposes.
- Any caller waiting on actual bundle placement versus mere registration can see
  different behavior.
- Infeasible, delayed, or partially placed placement groups are likely to be
  misrepresented as ready in Rust.

Assessment:

- This is a major semantic simplification, not equivalence.

Suggested exposing tests:

- Create placement groups whose bundles cannot be placed immediately and verify
  that C++ reports `PENDING`/waits while Rust does not.
- Add tests for all strategies (`PACK`, `SPREAD`, `STRICT_PACK`,
  `STRICT_SPREAD`) under insufficient resources.

#### Finding GCS-13: Rust placement-group removal deletes state instead of persisting `REMOVED`

Severity: High

Evidence:

- Rust removes the placement group from memory and deletes it from storage:
  [`rust/ray-gcs/src/placement_group_manager.rs:138`](/Users/istoica/src/ray/rust/ray-gcs/src/placement_group_manager.rs#L138)
- C++ persists the placement group with state `REMOVED`, updates stats, and
  notifies waiters:
  [`src/ray/gcs/gcs_placement_group_manager.cc:400`](/Users/istoica/src/ray/src/ray/gcs/gcs_placement_group_manager.cc#L400)

Impact:

- After removal, Rust may behave as if the placement group never existed, while
  C++ preserves removed-state history.
- Waiters and introspection APIs can observe different results.
- Restart behavior can diverge because removed placement groups are not
  recoverable from persisted state in Rust.

Assessment:

- Clear persistence and lifecycle mismatch.

Suggested exposing tests:

- Create then remove a placement group, restart GCS, and compare:
  `GetPlacementGroup`, `GetAllPlacementGroup`, and waiter behavior in C++ vs Rust.
- Add a test where a client is waiting for readiness when removal occurs and
  verify the returned error/status.

#### Finding GCS-14: Rust `WaitPlacementGroupUntilReady` is reduced to a synchronous state check

Severity: Medium to High

Evidence:

- Rust checks whether the current state is `Created` and returns a synthetic
  `GcsStatus`:
  [`rust/ray-gcs/src/grpc_services.rs:765`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L765)
- C++ performs an actual wait flow with callbacks and distinguishes removed or
  missing placement groups:
  [`src/ray/gcs/gcs_placement_group_manager.cc:574`](/Users/istoica/src/ray/src/ray/gcs/gcs_placement_group_manager.cc#L574)

Impact:

- Rust loses the asynchronous wait contract.
- It can return "not ready" immediately where C++ would block until ready or
  return a specific removal/not-found status.

Assessment:

- This is another API contract mismatch caused by flattening the lifecycle.

Suggested exposing tests:

- Start waiting before placement completes, then complete placement and compare
  response timing/shape.
- Remove the placement group while waiting and verify both systems return the
  same error semantics.

#### Finding GCS-15: Rust resource-manager RPCs expose a much weaker model than C++

Severity: High

Evidence:

- Rust resource manager stores only basic total/available/load maps in a local
  hash map:
  [`rust/ray-gcs/src/resource_manager.rs:29`](/Users/istoica/src/ray/rust/ray-gcs/src/resource_manager.rs#L29)
- C++ resource manager is driven from the cluster resource view and ray syncer,
  excludes the local GCS node, tracks draining deadlines, aggregate load, and
  placement-group load:
  [`src/ray/gcs/gcs_resource_manager.cc:61`](/Users/istoica/src/ray/src/ray/gcs/gcs_resource_manager.cc#L61)
  [`src/ray/gcs/gcs_resource_manager.cc:163`](/Users/istoica/src/ray/src/ray/gcs/gcs_resource_manager.cc#L163)

Impact:

- Rust replies for:
  `GetAllAvailableResources`,
  `GetAllTotalResources`,
  `GetAllResourceUsage`,
  `GetDrainingNodes`
  may be structurally correct but semantically incomplete.
- Aggregate demand, label selectors, placement-group load, and real draining
  deadlines are not represented.

Assessment:

- This is not a narrow field mismatch; it is a reduced model of cluster
  resource state.

Suggested exposing tests:

- Compare `GetAllResourceUsage` between C++ and Rust on a multi-node cluster
  with pending tasks, infeasible shapes, and placement-group load.
- Verify `GetDrainingNodes` returns the actual drain deadline and excludes the
  local GCS node the same way as C++.

#### Finding GCS-16: Rust autoscaler state handling drops C++ versioning and side effects

Severity: High

Evidence:

- Rust stores raw autoscaling state bytes and last-seen version without stale
  request rejection or infeasible-request side effects:
  [`rust/ray-gcs/src/autoscaler_state_manager.rs:136`](/Users/istoica/src/ray/rust/ray-gcs/src/autoscaler_state_manager.rs#L136)
- C++ rejects outdated autoscaler-state updates and may publish infeasible-task
  errors or cancel infeasible requests:
  [`src/ray/gcs/gcs_autoscaler_state_manager.cc:63`](/Users/istoica/src/ray/src/ray/gcs/gcs_autoscaler_state_manager.cc#L63)

Impact:

- Rust can accept stale autoscaler state that C++ would discard.
- Infeasible-resource reporting behavior can diverge significantly.

Assessment:

- This is a control-plane correctness issue, not just observability.

Suggested exposing tests:

- Send autoscaler state updates out of order and verify C++ rejects stale ones
  while Rust currently overwrites them.
- Submit infeasible resource requests and verify the expected error publication
  path is exercised.

#### Finding GCS-17: Rust `GetClusterStatus` / cluster-resource-state is much less complete than C++

Severity: High

Evidence:

- Rust `handle_get_cluster_resource_state()` builds node states mostly from
  alive nodes with status hard-coded to `RUNNING`:
  [`rust/ray-gcs/src/autoscaler_state_manager.rs:99`](/Users/istoica/src/ray/rust/ray-gcs/src/autoscaler_state_manager.rs#L99)
- C++ `MakeClusterResourceStateInternal()` populates:
  - session name,
  - last-seen autoscaler version,
  - dead/alive/draining/idle status,
  - pending resource requests,
  - pending gang resource requests,
  - cluster resource constraints:
  [`src/ray/gcs/gcs_autoscaler_state_manager.cc:163`](/Users/istoica/src/ray/src/ray/gcs/gcs_autoscaler_state_manager.cc#L163)

Impact:

- Autoscaler consumers can receive a materially less informative cluster view
  from Rust.
- Idle/draining/dead classification and pending-demand reporting are likely to diverge.

Assessment:

- This is a major semantic gap in an autoscaler-facing API.

Suggested exposing tests:

- Multi-node test with:
  idle nodes,
  draining nodes,
  dead nodes,
  pending tasks,
  pending placement groups,
  cluster resource constraints.
  Compare `GetClusterStatus` payloads between C++ and Rust.

#### Finding GCS-18: Rust autoscaler `DrainNode` always accepts and bypasses raylet feedback

Severity: High

Evidence:

- Rust marks the node draining and always returns `is_accepted = true`:
  [`rust/ray-gcs/src/grpc_services.rs:1114`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs#L1114)
- C++ validates the deadline, handles dead/unknown nodes distinctly, calls
  `DrainRaylet`, and propagates rejection reason/message from raylet:
  [`src/ray/gcs/gcs_autoscaler_state_manager.cc:456`](/Users/istoica/src/ray/src/ray/gcs/gcs_autoscaler_state_manager.cc#L456)

Impact:

- Rust can report successful drain acceptance even when C++ would reject or
  defer based on raylet feedback.
- Rejection-reason semantics are absent.

Assessment:

- Another control-plane mismatch with operational consequences.

Suggested exposing tests:

- Negative-deadline request test.
- Fake-raylet test where drain is rejected; verify rejection is surfaced.
- Unknown-node and already-dead-node tests comparing exact reply semantics.

## Interim Conclusion

The first GCS audit pass already shows multiple confirmed behavioral gaps.
These are not primarily type-safety or memory-safety issues; they are semantic
differences at the storage, RPC, and control-plane behavior layers.

The strongest currently verified risks are:

1. Internal KV persistence mismatch in Redis mode.
2. Missing `ReportJobError` behavior.
3. Missing job cleanup on node death.
4. Incomplete `GetAllJobInfo` semantics.
5. Missing actor recovery/lifecycle RPC behavior.
6. Stubbed worker inspection/update RPCs.
7. Placement-group lifecycle collapsed into immediate success.
8. Resource and autoscaler RPCs expose a materially weaker control-plane model.

These alone are enough to conclude that the current Rust GCS implementation is
not yet functionally equivalent to the C++ implementation.

### Raylet: first-pass verified differences

This section records the first direct comparison between the Rust raylet gRPC
service and the original C++ `NodeManager`.

#### Finding RAYLET-1: Rust `ReturnWorkerLease` does not actually release worker or resources

Severity: High

Evidence:

- Rust explicitly notes that it does not release allocated resources or return
  the worker to the pool, and instead only triggers re-scheduling:
  [`rust/ray-raylet/src/grpc_service.rs:174`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L174)
- C++ releases the lease, handles disconnect/exiting cases, unblocks workers if
  needed, releases worker resources, and returns reusable workers to the pool:
  [`src/ray/raylet/node_manager.cc:2080`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L2080)

Impact:

- Rust can leak leased capacity or worker lifecycle state after a return.
- Worker reuse semantics can diverge substantially from C++.

Suggested exposing tests:

- Lease a worker, return it, then verify:
  - resources become available again,
  - the worker can be reused,
  - disconnect and worker-exiting flags have the same effect as in C++.

#### Finding RAYLET-2: Rust `RequestWorkerLease` lacks important C++ retry/cancellation semantics

Severity: High

Evidence:

- Rust returns results from a reduced lease manager and does not model the
  richer C++ retry/dead-caller path:
  [`rust/ray-raylet/src/grpc_service.rs:96`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L96)
- C++ handles:
  - retrying an already granted lease,
  - caller dead checks,
  - queued lease reply callbacks,
  - worker prestart integration,
  - actor-creation-specific reply shaping:
  [`src/ray/raylet/node_manager.cc:1781`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L1781)

Impact:

- Lease retries, cancellation, and dead-caller behavior can diverge.
- Actor creation and queueing behavior is likely weaker in Rust.

Suggested exposing tests:

- Retry the same lease request and verify idempotent worker assignment.
- Submit a lease whose caller dies and verify the cancellation semantics and
  failure type match C++.
- Add actor-creation lease tests that compare rejection and spillback fields.

#### Finding RAYLET-3: Rust `ReportWorkerBacklog` is acknowledged but not integrated

Severity: Medium

Evidence:

- Rust says backlog reporting is only acknowledged for now:
  [`rust/ray-raylet/src/grpc_service.rs:254`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L254)
- C++ updates local lease-manager backlog state per scheduling class:
  [`src/ray/raylet/node_manager.cc:1760`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L1760)

Impact:

- Backlog-aware scheduling and autoscaling signals can diverge.

Suggested exposing tests:

- Report backlog from multiple workers and verify it affects scheduling or
  demand reporting equivalently to C++.

#### Finding RAYLET-4: Rust object pinning is a blanket success stub

Severity: High

Evidence:

- Rust returns `successes = true` for all object IDs and notes real pinning is
  not implemented:
  [`rust/ray-raylet/src/grpc_service.rs:278`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L278)
- C++ performs actual pinning/failure handling in the object-management path:
  [`src/ray/raylet/node_manager.cc:2625`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L2625)

Impact:

- Clients can be told objects were pinned when they were not.
- This can break object lifetime assumptions and failure diagnosis.

Suggested exposing tests:

- Request pinning for existing and missing objects and compare per-object success
  vectors between C++ and Rust.

#### Finding RAYLET-5: Rust `ResizeLocalResourceInstances` is stubbed

Severity: Medium to High

Evidence:

- Rust ignores the request and returns a default reply:
  [`rust/ray-raylet/src/grpc_service.rs:392`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L392)
- C++ validates unit-instance resources, applies clamped deltas, updates local
  capacity, and triggers rescheduling:
  [`src/ray/raylet/node_manager.cc:1983`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L1983)

Impact:

- Dynamic resource resizing will not match C++ behavior.

Suggested exposing tests:

- Resize custom resources and verify totals update.
- Attempt to resize a unit-instance resource like GPU and verify Rust currently
  fails to enforce the C++ restriction.

#### Finding RAYLET-6: Rust `GetNodeStats` is drastically weaker than C++

Severity: Medium

Evidence:

- Rust returns only `num_workers`:
  [`rust/ray-raylet/src/grpc_service.rs:431`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L431)
- C++ fills object store stats and gathers worker/core-worker stats before
  replying:
  [`src/ray/raylet/node_manager.cc:2643`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L2643)

Impact:

- Dashboard, diagnostics, and monitoring clients will observe far less data from Rust.

Suggested exposing tests:

- Compare `GetNodeStats` payload richness on a live node with active workers and
  objects in store.

#### Finding RAYLET-7: Rust `DrainRaylet` always accepts and skips C++ rejection/requeue semantics

Severity: High

Evidence:

- Rust simply sets local draining state and always returns `is_accepted = true`:
  [`rust/ray-raylet/src/grpc_service.rs:499`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L499)
- C++:
  - rejects idle termination if the node is no longer idle,
  - tracks rejection reason,
  - cancels local leases and requeues them to the cluster lease manager:
  [`src/ray/raylet/node_manager.cc:2129`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L2129)

Impact:

- Rust drain acceptance semantics are too permissive.
- Pending work is not obviously transitioned the same way as in C++.

Suggested exposing tests:

- Idle-termination drain request on a busy node should be rejected in C++; verify
  Rust behavior.
- Add a test that pending leases are requeued after accepted drain.

#### Finding RAYLET-8: Rust `ShutdownRaylet` does not match C++ shutdown semantics

Severity: High

Evidence:

- Rust converts shutdown into a local drain request and returns:
  [`rust/ray-raylet/src/grpc_service.rs:479`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L479)
- C++ exits immediately for non-graceful shutdown and, for graceful shutdown,
  drains the server-call executor and invokes explicit graceful shutdown logic
  with `NodeDeathInfo`:
  [`src/ray/raylet/node_manager.cc:2181`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L2181)

Impact:

- Rust can continue running in cases where C++ would terminate.
- Shutdown observability and node-death propagation can diverge.

Suggested exposing tests:

- Compare process liveness after graceful and non-graceful shutdown RPCs.
- Verify the node unregister/death path seen by GCS matches C++.

#### Finding RAYLET-9: Rust actor/task/local cleanup RPCs are largely no-ops

Severity: High

Evidence:

- `ReleaseUnusedActorWorkers` is a no-op:
  [`rust/ray-raylet/src/grpc_service.rs:523`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L523)
- `KillLocalActor` is acknowledged but no-op:
  [`rust/ray-raylet/src/grpc_service.rs:600`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L600)
- `CancelLocalTask` is acknowledged but no-op:
  [`rust/ray-raylet/src/grpc_service.rs:614`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs#L614)
- C++ actively destroys unused actor workers, kills actors, and cancels work:
  [`src/ray/raylet/node_manager.cc:2208`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc#L2208)

Impact:

- Actor and task lifecycle control from GCS/core-worker can silently stop working.

Suggested exposing tests:

- Request release of unused actor workers and verify processes exit.
- Kill a local actor and verify worker/process teardown.
- Cancel a local task and verify execution stops or reports cancellation.

#### Finding RAYLET-10: Rust dependency management omits object pulling, owner tracking, and request-type semantics

Severity: High

Evidence:

- Rust `DependencyManager` is a local wait-set tracker over `waiting_tasks`,
  `object_waiters`, `local_objects`, and `lost_objects`; it has no object-owner
  addresses, no object-manager pull integration, and no distinct handling for
  `wait`, `get`, and queued lease dependencies:
  [`rust/ray-raylet/src/dependency_manager.rs:56`](/Users/istoica/src/ray/rust/ray-raylet/src/dependency_manager.rs#L56)
  [`rust/ray-raylet/src/dependency_manager.rs:108`](/Users/istoica/src/ray/rust/ray-raylet/src/dependency_manager.rs#L108)
  [`rust/ray-raylet/src/dependency_manager.rs:166`](/Users/istoica/src/ray/rust/ray-raylet/src/dependency_manager.rs#L166)
- C++ `LeaseDependencyManager`:
  - stores owner addresses for required objects,
  - starts and cancels pull requests for `ray.wait`,
  - starts and cancels pull requests for `ray.get`,
  - separately manages queued lease dependencies and task-arg pulls:
  [`src/ray/raylet/lease_dependency_manager.cc:33`](/Users/istoica/src/ray/src/ray/raylet/lease_dependency_manager.cc#L33)
  [`src/ray/raylet/lease_dependency_manager.cc:69`](/Users/istoica/src/ray/src/ray/raylet/lease_dependency_manager.cc#L69)
  [`src/ray/raylet/lease_dependency_manager.cc:118`](/Users/istoica/src/ray/src/ray/raylet/lease_dependency_manager.cc#L118)
  [`src/ray/raylet/lease_dependency_manager.cc:212`](/Users/istoica/src/ray/src/ray/raylet/lease_dependency_manager.cc#L212)

Impact:

- Non-local dependencies will not be fetched with C++-equivalent behavior.
- Cancellation semantics for `wait`, `get`, and queued-lease requests are weaker.
- Any code that expects the raylet to know dependency owners can diverge.

Suggested exposing tests:

- Submit a task whose arguments are remote-only and verify the Rust raylet
  triggers dependency fetches before lease execution.
- Start and cancel `ray.get` / `ray.wait` requests and verify pending pulls are
  canceled, not leaked.
- Kill an owner while a dependency is outstanding and compare C++ vs Rust
  failure propagation.

#### Finding RAYLET-11: Rust `WaitManager` does not implement real timeout execution and drops some C++ validation/ordering behavior

Severity: High

Evidence:

- Rust:
  - accepts duplicate object IDs and arbitrary `num_required`,
  - stores timeout cancel handles but explicitly does not schedule the timeout,
  - returns ready objects from a `HashSet`, so ready ordering is not tied to
    input order:
  [`rust/ray-raylet/src/wait_manager.rs:56`](/Users/istoica/src/ray/rust/ray-raylet/src/wait_manager.rs#L56)
  [`rust/ray-raylet/src/wait_manager.rs:96`](/Users/istoica/src/ray/rust/ray-raylet/src/wait_manager.rs#L96)
  [`rust/ray-raylet/src/wait_manager.rs:162`](/Users/istoica/src/ray/rust/ray-raylet/src/wait_manager.rs#L162)
- C++:
  - rejects duplicate object IDs,
  - validates timeout bounds and `num_required <= object_ids.size()`,
  - schedules an actual timeout callback,
  - returns ready/remaining objects in the original input order:
  [`src/ray/raylet/wait_manager.cc:25`](/Users/istoica/src/ray/src/ray/raylet/wait_manager.cc#L25)
  [`src/ray/raylet/wait_manager.cc:55`](/Users/istoica/src/ray/src/ray/raylet/wait_manager.cc#L55)
  [`src/ray/raylet/wait_manager.cc:83`](/Users/istoica/src/ray/src/ray/raylet/wait_manager.cc#L83)

Impact:

- `ray.wait` can block forever in Rust where C++ would time out.
- Invalid requests that C++ rejects can be accepted in Rust.
- Result ordering can diverge even when the same objects become ready.

Suggested exposing tests:

- Issue a wait with a short timeout and no object arrival; verify C++ returns on
  timeout while Rust currently does not.
- Pass duplicate object IDs and oversized `num_required`; verify C++ rejects and
  Rust behavior diverges.
- Assert that ready and remaining object order matches input order.

#### Finding RAYLET-12: Rust worker-pool matching and lifecycle semantics are much weaker than C++

Severity: High

Evidence:

- Rust worker selection only matches on language and exact job ID, then either
  returns an idle worker or starts a new one via a callback:
  [`rust/ray-raylet/src/worker_pool.rs:154`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L154)
  [`rust/ray-raylet/src/worker_pool.rs:195`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L195)
- Rust job-finish and idle-kill logic only disconnects/pops workers locally:
  [`rust/ray-raylet/src/worker_pool.rs:261`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L261)
  [`rust/ray-raylet/src/worker_pool.rs:301`](/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs#L301)
- C++ worker startup and reuse depend on many more lease properties and lifecycle
  rules:
  - startup includes worker type, dynamic options, runtime env, startup keepalive,
    and process management:
    [`src/ray/raylet/worker_pool.cc:462`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L462)
  - idle-worker killing respects keepalive windows and sends explicit exit RPCs:
    [`src/ray/raylet/worker_pool.cc:1163`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1163)
  - worker reuse filters on worker type, detached-actor root, job assignment,
    GPU/actor-worker flags, runtime env, and dynamic options:
    [`src/ray/raylet/worker_pool.cc:1276`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1276)
  - `PopWorker` carries full lease metadata and can return `JobFinished` when a
    just-matched worker becomes invalid for a finished job:
    [`src/ray/raylet/worker_pool.cc:1400`](/Users/istoica/src/ray/src/ray/raylet/worker_pool.cc#L1400)

Impact:

- Rust can reuse the wrong cached worker relative to the C++ contract.
- Runtime-env and dynamic-option isolation appear to be missing.
- Idle-worker cleanup and job-finish behavior can leak or mis-handle workers.

Suggested exposing tests:

- Start two leases with the same language/job but different runtime envs or
  dynamic worker options and verify Rust does not incorrectly reuse the same
  cached worker.
- Exercise actor-worker vs normal-worker reuse and detached-actor-root reuse.
- Finish a job while workers are being allocated and compare `JobFinished`
  behavior and process cleanup.

#### Finding RAYLET-13: Rust local object management drops owner-subscription, spill/restore, and spilled-object cleanup semantics

Severity: High

Evidence:

- Rust local object management is an in-process bookkeeping layer over pinned
  objects, pending spill state, and pending deletions; it has no owner
  subscription, no object-directory reporting, no restore path, and no external
  spilled-object deletion protocol:
  [`rust/ray-raylet/src/local_object_manager.rs:102`](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L102)
  [`rust/ray-raylet/src/local_object_manager.rs:148`](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L148)
  [`rust/ray-raylet/src/local_object_manager.rs:199`](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L199)
  [`rust/ray-raylet/src/local_object_manager.rs:242`](/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs#L242)
- C++:
  - pins objects together with owner metadata and subscribes to owner-driven
    eviction / owner-death events:
    [`src/ray/raylet/local_object_manager.cc:31`](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L31)
  - frees out-of-scope objects via cluster/object-store callbacks:
    [`src/ray/raylet/local_object_manager.cc:111`](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L111)
  - spills through IO workers and reports spilled URLs:
    [`src/ray/raylet/local_object_manager.cc:285`](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L285)
    [`src/ray/raylet/local_object_manager.cc:401`](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L401)
  - restores spilled objects and reference-counts/deletes spilled files:
    [`src/ray/raylet/local_object_manager.cc:464`](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L464)
    [`src/ray/raylet/local_object_manager.cc:523`](/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc#L523)

Impact:

- Object lifetime after owner death or eviction publication is not C++-equivalent.
- Spilling may not update cluster-visible metadata the same way.
- Restoring and deleting spilled objects are materially under-implemented.

Suggested exposing tests:

- Pin an object, kill its owner, and verify whether the object is eventually
  released as it is in C++.
- Spill an object and verify the spilled URL is reported and later used for
  restore on demand.
- Free multiple objects that share the same spilled file and verify deletion is
  reference-counted, not premature.

## Core Worker Findings

#### Finding CORE-1: Rust `CoreWorker` is centered on a local memory-store model and omits major C++ integration points

Severity: High

Evidence:

- Rust `CoreWorker::new` wires together local-only components, with no visible
  GCS client, raylet RPC client, plasma store provider, shutdown coordinator, or
  callback-rich process integration comparable to C++:
  [`rust/ray-core-worker/src/core_worker.rs:65`](/Users/istoica/src/ray/rust/ray-core-worker/src/core_worker.rs#L65)
- Rust object APIs operate directly on the in-process memory store:
  [`rust/ray-core-worker/src/core_worker.rs:140`](/Users/istoica/src/ray/rust/ray-core-worker/src/core_worker.rs#L140)
  [`rust/ray-core-worker/src/core_worker.rs:159`](/Users/istoica/src/ray/rust/ray-core-worker/src/core_worker.rs#L159)
  [`rust/ray-core-worker/src/core_worker.rs:177`](/Users/istoica/src/ray/rust/ray-core-worker/src/core_worker.rs#L177)
- C++ `CoreWorker` is integrated with GCS subscription/re-registration, memory
  store async ownership checks, full cancellation plumbing, shutdown
  coordination, and richer worker stats/object-store handling:
  [`src/ray/core_worker/core_worker.cc:3436`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L3436)
  [`src/ray/core_worker/core_worker.cc:3516`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L3516)
  [`src/ray/core_worker/core_worker.cc:3878`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L3878)
  [`src/ray/core_worker/core_worker.cc:4105`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L4105)

Impact:

- Even where Rust has modules with the same names, the central worker runtime is
  not yet acting like the same distributed component as the C++ core worker.
- Functional equivalence claims need to be treated skeptically until the missing
  cross-process integrations are shown elsewhere.

Suggested exposing tests:

- End-to-end tests that exercise object ownership, owner callbacks, cancellation,
  and GCS restart from a live cluster, not just in-process unit tests.
- Compare worker stats, object ownership behavior, and shutdown behavior between
  C++ and Rust under the same workload.

#### Finding CORE-2: Rust reference counting is materially weaker than C++ ownership/borrowing semantics

Severity: High

Evidence:

- Rust tracks only a reduced set of fields per object and deletes entries when
  local plus submitted-task refs reach zero, with limited lineage handling:
  [`rust/ray-core-worker/src/reference_counter.rs:24`](/Users/istoica/src/ray/rust/ray-core-worker/src/reference_counter.rs#L24)
  [`rust/ray-core-worker/src/reference_counter.rs:97`](/Users/istoica/src/ray/rust/ray-core-worker/src/reference_counter.rs#L97)
  [`rust/ray-core-worker/src/reference_counter.rs:123`](/Users/istoica/src/ray/rust/ray-core-worker/src/reference_counter.rs#L123)
  [`rust/ray-core-worker/src/reference_counter.rs:201`](/Users/istoica/src/ray/rust/ray-core-worker/src/reference_counter.rs#L201)
- C++ reference counting additionally implements:
  - shutdown draining on outstanding refs:
    [`src/ray/core_worker/reference_counter.cc:47`](/Users/istoica/src/ray/src/ray/core_worker/reference_counter.cc#L47)
  - borrowed-object nesting and foreign-owner monitoring:
    [`src/ray/core_worker/reference_counter.cc:115`](/Users/istoica/src/ray/src/ray/core_worker/reference_counter.cc#L115)
  - richer owned-object registration including call site, lineage eligibility,
    pinning location, and tensor transport:
    [`src/ray/core_worker/reference_counter.cc:220`](/Users/istoica/src/ray/src/ray/core_worker/reference_counter.cc#L220)
  - dynamic returns / streaming generator return ownership:
    [`src/ray/core_worker/reference_counter.cc:243`](/Users/istoica/src/ray/src/ray/core_worker/reference_counter.cc#L243)
    [`src/ray/core_worker/reference_counter.cc:273`](/Users/istoica/src/ray/src/ray/core_worker/reference_counter.cc#L273)
  - merge of borrower refs and release of submitted refs on task completion:
    [`src/ray/core_worker/reference_counter.cc:543`](/Users/istoica/src/ray/src/ray/core_worker/reference_counter.cc#L543)
  - out-of-scope/freed callbacks and object recovery bookkeeping:
    [`src/ray/core_worker/reference_counter.cc:872`](/Users/istoica/src/ray/src/ray/core_worker/reference_counter.cc#L872)
  - full ref-count inspection and borrower cleanup helpers:
    [`src/ray/core_worker/reference_counter.cc:1017`](/Users/istoica/src/ray/src/ray/core_worker/reference_counter.cc#L1017)

Impact:

- Borrowed-ref propagation, nested object ownership, generator returns, and
  out-of-scope callbacks are not C++-equivalent in Rust.
- Shutdown and recovery semantics that depend on reference draining can diverge.

Suggested exposing tests:

- Nested borrowed-ref test: pass an object containing object refs through
  multiple tasks and verify owner notifications and out-of-scope behavior.
- Streaming generator test: emit partial returns, drop the generator, and verify
  dynamic return refs are cleaned up identically.
- Shutdown test: hold outstanding refs during worker shutdown and compare whether
  C++ drains while Rust exits or leaks state.

#### Finding CORE-3: Rust normal-task submission is still a reduced lease path and explicitly supports a local stub mode

Severity: High

Evidence:

- Rust `NormalTaskSubmitter`:
  - has an explicit “stub mode” when no raylet client is present,
  - requests a single lease directly,
  - does not maintain per-scheduling-key queues, worker reuse, backlog
    reporting, or full spillback resubmission:
  [`rust/ray-core-worker/src/normal_task_submitter.rs:93`](/Users/istoica/src/ray/rust/ray-core-worker/src/normal_task_submitter.rs#L93)
  [`rust/ray-core-worker/src/normal_task_submitter.rs:155`](/Users/istoica/src/ray/rust/ray-core-worker/src/normal_task_submitter.rs#L155)
  [`rust/ray-core-worker/src/normal_task_submitter.rs:193`](/Users/istoica/src/ray/rust/ray-core-worker/src/normal_task_submitter.rs#L193)
  [`rust/ray-core-worker/src/normal_task_submitter.rs:256`](/Users/istoica/src/ray/rust/ray-core-worker/src/normal_task_submitter.rs#L256)
- C++ normal-task submission:
  - resolves dependencies before scheduling,
  - maintains scheduling-key queues,
  - manages leased workers and lease returns,
  - cancels excess lease requests,
  - reports backlog,
  - rate-limits and spillback-resubmits lease requests:
  [`src/ray/core_worker/task_submission/normal_task_submitter.cc:35`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/normal_task_submitter.cc#L35)
  [`src/ray/core_worker/task_submission/normal_task_submitter.cc:115`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/normal_task_submitter.cc#L115)
  [`src/ray/core_worker/task_submission/normal_task_submitter.cc:195`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/normal_task_submitter.cc#L195)
  [`src/ray/core_worker/task_submission/normal_task_submitter.cc:265`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/normal_task_submitter.cc#L265)
  [`src/ray/core_worker/task_submission/normal_task_submitter.cc:275`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/normal_task_submitter.cc#L275)

Impact:

- Rust normal-task scheduling is not equivalent to the C++ worker-lease
  protocol.
- Spillback, backlog, and multi-task scheduling behavior are likely to diverge
  under load or multi-node placement.

Suggested exposing tests:

- Multi-node spillback test: force a lease to spill back and verify the task is
  eventually executed remotely rather than just marked spilled.
- Backlog test: submit many same-class tasks and compare backlog reporting and
  number of pending leases.
- Cancellation test: cancel before and after dependency resolution and compare
  C++ vs Rust task state transitions.

#### Finding CORE-4: Rust actor-task submission drops important C++ semantics around dependency resolution, restart, and cancellation

Severity: High

Evidence:

- Rust actor submission:
  - increments a local sequence counter,
  - drains the whole queue once connected,
  - requeues failed sends locally,
  - cancellation simply pops the oldest pending task for the actor:
  [`rust/ray-core-worker/src/actor_task_submitter.rs:104`](/Users/istoica/src/ray/rust/ray-core-worker/src/actor_task_submitter.rs#L104)
  [`rust/ray-core-worker/src/actor_task_submitter.rs:198`](/Users/istoica/src/ray/rust/ray-core-worker/src/actor_task_submitter.rs#L198)
  [`rust/ray-core-worker/src/actor_task_submitter.rs:241`](/Users/istoica/src/ray/rust/ray-core-worker/src/actor_task_submitter.rs#L241)
  [`rust/ray-core-worker/src/actor_task_submitter.rs:258`](/Users/istoica/src/ray/rust/ray-core-worker/src/actor_task_submitter.rs#L258)
- C++ actor submission additionally:
  - reports actor out-of-scope to GCS:
    [`src/ray/core_worker/task_submission/actor_task_submitter.cc:30`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/actor_task_submitter.cc#L30)
  - handles actor creation through GCS with task-manager integration:
    [`src/ray/core_worker/task_submission/actor_task_submitter.cc:93`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/actor_task_submitter.cc#L93)
  - resolves dependencies before dispatch and tracks per-task queue position /
    concurrency group ordering:
    [`src/ray/core_worker/task_submission/actor_task_submitter.cc:168`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/actor_task_submitter.cc#L168)
  - handles reconnect/restart versions and fails inflight tasks on restart:
    [`src/ray/core_worker/task_submission/actor_task_submitter.cc:300`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/actor_task_submitter.cc#L300)
  - retries actor-task cancellation against the executor/raylet until a final
    answer is obtained:
    [`src/ray/core_worker/task_submission/actor_task_submitter.cc:941`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/actor_task_submitter.cc#L941)

Impact:

- Actor task ordering and cancellation are not guaranteed to match C++.
- Actor restart/reconnect behavior is materially weaker in Rust.
- Owned-actor lifecycle reporting to GCS appears incomplete.

Suggested exposing tests:

- Actor restart test: kill and restart an actor with queued and inflight tasks,
  then compare task replay/failure behavior.
- Concurrency-group ordering test: interleave actor calls across groups and
  verify execution order.
- Actor cancellation test: cancel a sent actor task and verify repeated cancel
  attempts eventually stop execution as in C++.

#### Finding CORE-5: Rust task execution is a generic semaphore-based executor, not the C++ task-receiver protocol

Severity: High

Evidence:

- Rust `TaskReceiver`:
  - runs tasks through one semaphore,
  - stores a cancel flag locally,
  - does not use request sequence / `client_processed_up_to`,
  - does not build ordered or unordered actor execution queues,
  - does not propagate borrowed refs / dynamic returns / actor creation queue
    setup in the same way:
  [`rust/ray-core-worker/src/task_receiver.rs:135`](/Users/istoica/src/ray/rust/ray-core-worker/src/task_receiver.rs#L135)
  [`rust/ray-core-worker/src/task_receiver.rs:175`](/Users/istoica/src/ray/rust/ray-core-worker/src/task_receiver.rs#L175)
  [`rust/ray-core-worker/src/task_receiver.rs:233`](/Users/istoica/src/ray/rust/ray-core-worker/src/task_receiver.rs#L233)
  [`rust/ray-core-worker/src/task_receiver.rs:275`](/Users/istoica/src/ray/rust/ray-core-worker/src/task_receiver.rs#L275)
- C++ `TaskReceiver`:
  - combines execution result shaping with borrowed refs, streaming/dynamic
    returns, actor creation metadata, and worker-exiting semantics:
    [`src/ray/core_worker/task_execution/task_receiver.cc:30`](/Users/istoica/src/ray/src/ray/core_worker/task_execution/task_receiver.cc#L30)
  - queues tasks differently for normal, actor creation, and actor calls:
    [`src/ray/core_worker/task_execution/task_receiver.cc:144`](/Users/istoica/src/ray/src/ray/core_worker/task_execution/task_receiver.cc#L144)
  - uses ordered vs unordered actor execution queues keyed by caller worker and
    RPC sequencing data:
    [`src/ray/core_worker/task_execution/task_receiver.cc:214`](/Users/istoica/src/ray/src/ray/core_worker/task_execution/task_receiver.cc#L214)
  - supports cancellation of queued actor and normal tasks separately:
    [`src/ray/core_worker/task_execution/task_receiver.cc:255`](/Users/istoica/src/ray/src/ray/core_worker/task_execution/task_receiver.cc#L255)

Impact:

- Actor task reordering, cancellation, and readiness semantics can diverge.
- Generator / dynamic-return behavior is not obviously preserved by the Rust
  execution path.
- Rust cancellation appears cooperative-only once execution has started.

Suggested exposing tests:

- Ordered actor test: send actor tasks with deliberate network reorder and
  verify C++ serializes execution while Rust behavior matches or diverges.
- Cancellation test for queued vs already-running tasks.
- Generator task test: return dynamic and streaming results and compare the
  `PushTaskReply` payloads and cleanup behavior.

#### Finding CORE-6: Rust task-management and several CoreWorker RPC handlers are reduced or stubbed relative to C++

Severity: High

Evidence:

- Rust `TaskManager` tracks a simpler lifecycle and retry model; it does not
  appear to manage dynamic returns, reconstructable return IDs, detailed error
  typing, lineage footprint eviction, or borrower-ref merging on completion:
  [`rust/ray-core-worker/src/task_manager.rs:296`](/Users/istoica/src/ray/rust/ray-core-worker/src/task_manager.rs#L296)
  [`rust/ray-core-worker/src/task_manager.rs:353`](/Users/istoica/src/ray/rust/ray-core-worker/src/task_manager.rs#L353)
  [`rust/ray-core-worker/src/task_manager.rs:411`](/Users/istoica/src/ray/rust/ray-core-worker/src/task_manager.rs#L411)
- C++ `TaskManager` handles:
  - richer pending-task registration and generator stream setup:
    [`src/ray/core_worker/task_manager.cc:220`](/Users/istoica/src/ray/src/ray/core_worker/task_manager.cc#L220)
  - dynamic returns, reconstructable returns, streaming-generator finalization,
    and lineage pinning on completion:
    [`src/ray/core_worker/task_manager.cc:909`](/Users/istoica/src/ray/src/ray/core_worker/task_manager.cc#L909)
  - retry classification, backoff, and detailed failure handling:
    [`src/ray/core_worker/task_manager.cc:1141`](/Users/istoica/src/ray/src/ray/core_worker/task_manager.cc#L1141)
    [`src/ray/core_worker/task_manager.cc:1261`](/Users/istoica/src/ray/src/ray/core_worker/task_manager.cc#L1261)
- Rust gRPC service explicitly says remaining RPCs are stubs and includes no-op
  handlers for:
  - `RayletNotifyGcsRestart`:
    [`rust/ray-core-worker/src/grpc_service.rs:214`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs#L214)
  - `ActorCallArgWaitComplete`:
    [`rust/ray-core-worker/src/grpc_service.rs:230`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs#L230)
  - `WaitForActorRefDeleted`:
    [`rust/ray-core-worker/src/grpc_service.rs:237`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs#L237)
  - `RegisterMutableObjectReader`:
    [`rust/ray-core-worker/src/grpc_service.rs:555`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs#L555)
- The corresponding C++ handlers perform real work:
  [`src/ray/core_worker/core_worker.cc:3415`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L3415)
  [`src/ray/core_worker/core_worker.cc:3436`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L3436)
  [`src/ray/core_worker/core_worker.cc:3516`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L3516)
  [`src/ray/core_worker/core_worker.cc:4085`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L4085)
  [`src/ray/core_worker/core_worker.cc:4105`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L4105)
  [`src/ray/core_worker/core_worker.cc:4206`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L4206)
  [`src/ray/core_worker/core_worker.cc:4324`](/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc#L4324)

Impact:

- Task retry, failure, and generator semantics are not functionally equivalent.
- Several control-plane RPCs that other components rely on are currently
  placeholders in Rust.
- Even where Rust has implementations for spill/restore/assign-owner RPCs, they
  are simplified and not obviously equivalent to the C++ callback-driven paths.

Suggested exposing tests:

- Generator retry test: run a generator task that partially succeeds, then fails,
  and compare dynamic-return / end-of-stream behavior.
- GCS restart test: restart GCS and verify subscriptions and ownership state are
  actually re-established.
- Mutable-object reader and actor-ref-deletion tests using the existing C++
  test cases as the baseline.
- Compare `GetCoreWorkerStats` payload richness and per-task/object fields.

#### Finding CORE-7: Rust dependency resolution is only a local waiter, not the C++ inlining/actor-aware resolver

Severity: High

Evidence:

- Rust dependency resolution only waits for object IDs to appear in the local
  memory store and exposes per-object cancellation:
  [`rust/ray-core-worker/src/dependency_resolver.rs:29`](/Users/istoica/src/ray/rust/ray-core-worker/src/dependency_resolver.rs#L29)
  [`rust/ray-core-worker/src/dependency_resolver.rs:62`](/Users/istoica/src/ray/rust/ray-core-worker/src/dependency_resolver.rs#L62)
  [`rust/ray-core-worker/src/dependency_resolver.rs:122`](/Users/istoica/src/ray/rust/ray-core-worker/src/dependency_resolver.rs#L122)
- C++ dependency resolution:
  - distinguishes local object dependencies and actor-registration dependencies,
  - asynchronously fetches dependencies from the in-memory store,
  - inlines small/direct objects into task args,
  - tracks nested contained refs,
  - informs the task manager when dependencies were inlined:
  [`src/ray/core_worker/task_submission/dependency_resolver.cc:26`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/dependency_resolver.cc#L26)
  [`src/ray/core_worker/task_submission/dependency_resolver.cc:97`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/dependency_resolver.cc#L97)
  [`src/ray/core_worker/task_submission/dependency_resolver.cc:152`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/dependency_resolver.cc#L152)
  [`src/ray/core_worker/task_submission/dependency_resolver.cc:174`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/dependency_resolver.cc#L174)

Impact:

- Rust does not appear to preserve the C++ contract around argument inlining,
  nested contained refs, or actor-registration dependencies.
- Task payload shape and reference-count timing can diverge before the task is
  even submitted.

Suggested exposing tests:

- Submit a task with a mix of by-ref, small inlineable, and nested-ref
  arguments; compare the final transmitted `TaskSpec`.
- Submit tasks that depend on an actor still registering and verify Rust waits
  equivalently.
- Check that inlining a dependency does not prematurely release nested refs.

#### Finding CORE-8: Rust future resolution is not a full owner-RPC resolver yet

Severity: High

Evidence:

- Rust `FutureResolver` accumulates waiters and requires an external caller to
  feed `ResolutionReply`; it is not itself issuing owner RPCs:
  [`rust/ray-core-worker/src/future_resolver.rs:84`](/Users/istoica/src/ray/rust/ray-core-worker/src/future_resolver.rs#L84)
  [`rust/ray-core-worker/src/future_resolver.rs:127`](/Users/istoica/src/ray/rust/ray-core-worker/src/future_resolver.rs#L127)
  [`rust/ray-core-worker/src/future_resolver.rs:157`](/Users/istoica/src/ray/rust/ray-core-worker/src/future_resolver.rs#L157)
- C++ `FutureResolver` actively connects to the owner and sends
  `GetObjectStatus`, then processes the reply into memory-store state and
  borrower refs:
  [`src/ray/core_worker/future_resolver.cc:23`](/Users/istoica/src/ray/src/ray/core_worker/future_resolver.cc#L23)
  [`src/ray/core_worker/future_resolver.cc:43`](/Users/istoica/src/ray/src/ray/core_worker/future_resolver.cc#L43)
- Rust also records locations as hex strings and only adds borrowed nested refs
  if it can already discover an owner locally:
  [`rust/ray-core-worker/src/future_resolver.rs:178`](/Users/istoica/src/ray/rust/ray-core-worker/src/future_resolver.rs#L178)
  [`rust/ray-core-worker/src/future_resolver.rs:193`](/Users/istoica/src/ray/rust/ray-core-worker/src/future_resolver.rs#L193)

Impact:

- Borrowed-object resolution may not happen automatically in Rust.
- Nested borrower propagation and locality updates are weaker than in C++.
- Error cases such as unreachable owner vs out-of-scope object may not map the
  same way end to end.

Suggested exposing tests:

- Borrow a remote object and verify that a Rust worker actually issues owner
  lookups and resolves it without extra orchestration.
- Resolve an object with nested refs and compare borrower registration on the
  nested IDs.
- Kill the owner during resolution and compare the stored error object and
  subsequent `get` behavior.

#### Finding CORE-9: Rust object recovery omits the main C++ lineage/pinning recovery logic

Severity: High

Evidence:

- Rust object recovery only considers:
  - spill URL,
  - first known alternate location,
  - otherwise unrecoverable.
  It does not actually invoke lineage reconstruction based on task lineage:
  [`rust/ray-core-worker/src/object_recovery_manager.rs:72`](/Users/istoica/src/ray/rust/ray-core-worker/src/object_recovery_manager.rs#L72)
  [`rust/ray-core-worker/src/object_recovery_manager.rs:128`](/Users/istoica/src/ray/rust/ray-core-worker/src/object_recovery_manager.rs#L128)
  [`rust/ray-core-worker/src/object_recovery_manager.rs:196`](/Users/istoica/src/ray/rust/ray-core-worker/src/object_recovery_manager.rs#L196)
- C++ recovery:
  - rejects unreconstructable borrowed refs,
  - checks pinned/spilled status,
  - pins secondary copies via raylet,
  - reconstructs via task-manager lineage when needed,
  - recursively recovers task dependencies:
  [`src/ray/core_worker/object_recovery_manager.cc:24`](/Users/istoica/src/ray/src/ray/core_worker/object_recovery_manager.cc#L24)
  [`src/ray/core_worker/object_recovery_manager.cc:93`](/Users/istoica/src/ray/src/ray/core_worker/object_recovery_manager.cc#L93)
  [`src/ray/core_worker/object_recovery_manager.cc:109`](/Users/istoica/src/ray/src/ray/core_worker/object_recovery_manager.cc#L109)
  [`src/ray/core_worker/object_recovery_manager.cc:140`](/Users/istoica/src/ray/src/ray/core_worker/object_recovery_manager.cc#L140)

Impact:

- Rust object recovery is not equivalent to the C++ reconstruction path.
- Lost objects that C++ can recover via lineage or secondary-copy promotion may
  be marked unrecoverable in Rust.

Suggested exposing tests:

- Lose the primary copy of an object that still has a secondary copy and verify
  Rust promotes/pins it like C++.
- Lose an object recoverable through task lineage and compare whether both
  implementations resubmit the producing task.
- Borrowed-object recovery test: verify Rust rejects or mishandles cases that
  C++ classifies precisely.

#### Finding CORE-10: Rust ownership tracking is only a local map, not the C++ ownership/location-update protocol

Severity: High

Evidence:

- Rust `OwnershipDirectory` is a local table of owned and borrowed objects with
  simple borrower sets and pinned/spilled flags:
  [`rust/ray-core-worker/src/ownership_directory.rs:52`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs#L52)
  [`rust/ray-core-worker/src/ownership_directory.rs:83`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs#L83)
  [`rust/ray-core-worker/src/ownership_directory.rs:193`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs#L193)
  [`rust/ray-core-worker/src/ownership_directory.rs:265`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs#L265)
- The C++ ownership/location protocol spans more than a local table. In
  particular, `OwnershipBasedObjectDirectory`:
  - updates node/spill/pending-creation state from owner publications,
  - reports add/remove/spill updates back to owners in batches,
  - subscribes to owner-published object-location streams,
  - converts owner death or lookup failure into object failure notifications:
  [`src/ray/object_manager/ownership_object_directory.cc:51`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L51)
  [`src/ray/object_manager/ownership_object_directory.cc:121`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L121)
  [`src/ray/object_manager/ownership_object_directory.cc:167`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L167)
  [`src/ray/object_manager/ownership_object_directory.cc:202`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L202)
  [`src/ray/object_manager/ownership_object_directory.cc:260`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L260)
  [`src/ray/object_manager/ownership_object_directory.cc:320`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L320)

Impact:

- Rust ownership tracking does not currently look like a drop-in replacement for
  the distributed ownership/location-update protocol used by C++.
- Spill reporting, owner-driven location updates, and owner-death propagation
  are likely incomplete end to end.

Suggested exposing tests:

- Object-location subscription test: move/spill/remove an object and compare the
  owner-side updates observed by subscribers.
- Owner-death test: kill the owner and verify subscribers and borrowers observe
  the same failure transitions.
- Batch location update test: stress add/remove/spill updates and compare final
  owner state across C++ and Rust.

#### Finding CORE-11: Rust direct actor transport is a lightweight queue, not the C++ actor transport/state machine

Severity: High

Evidence:

- Rust `DirectActorTransport` tracks only:
  - actor address,
  - simple connection state,
  - FIFO pending queue,
  - in-flight count,
  - basic retry bookkeeping:
  [`rust/ray-core-worker/src/direct_transport.rs:52`](/Users/istoica/src/ray/rust/ray-core-worker/src/direct_transport.rs#L52)
  [`rust/ray-core-worker/src/direct_transport.rs:96`](/Users/istoica/src/ray/rust/ray-core-worker/src/direct_transport.rs#L96)
  [`rust/ray-core-worker/src/direct_transport.rs:163`](/Users/istoica/src/ray/rust/ray-core-worker/src/direct_transport.rs#L163)
  [`rust/ray-core-worker/src/direct_transport.rs:197`](/Users/istoica/src/ray/rust/ray-core-worker/src/direct_transport.rs#L197)
- The C++ actor transport path, as represented by `ActorTaskSubmitter`, carries
  a much richer transport/state model:
  - out-of-order vs sequential submit queues,
  - actor liveness/restart generations,
  - pending tasks waiting for death info,
  - inflight RPC callback tracking,
  - pending-call backpressure,
  - recursive cancellation protocol:
  [`src/ray/core_worker/task_submission/actor_task_submitter.h:65`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/actor_task_submitter.h#L65)
  [`src/ray/core_worker/task_submission/actor_task_submitter.h:184`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/actor_task_submitter.h#L184)
  [`src/ray/core_worker/task_submission/actor_task_submitter.h:259`](/Users/istoica/src/ray/src/ray/core_worker/task_submission/actor_task_submitter.h#L259)

Impact:

- Rust direct actor transport is not obviously equivalent to the C++ direct
  actor RPC protocol under restart, backpressure, cancellation, or out-of-order
  execution scenarios.
- Actor-call delivery guarantees under failure are weaker than the C++ design.

Suggested exposing tests:

- Actor transport fault-injection test: disconnect/reconnect the actor worker
  during inflight RPCs and compare replay/drop behavior.
- Backpressure test: exceed actor pending-call thresholds and compare rejection
  and warning behavior.
- Out-of-order execution test for actors configured to allow it.

#### Finding CORE-12: Rust `ActorManager` is mostly a handle registry, not the C++ actor-lifecycle manager

Severity: High

Evidence:

- Rust `ActorManager` maintains a local map of handles and named actors:
  [`rust/ray-core-worker/src/actor_manager.rs:11`](/Users/istoica/src/ray/rust/ray-core-worker/src/actor_manager.rs#L11)
  [`rust/ray-core-worker/src/actor_manager.rs:22`](/Users/istoica/src/ray/rust/ray-core-worker/src/actor_manager.rs#L22)
  [`rust/ray-core-worker/src/actor_manager.rs:32`](/Users/istoica/src/ray/rust/ray-core-worker/src/actor_manager.rs#L32)
- C++ `ActorManager` additionally:
  - registers borrowed actor handles through the reference counter:
    [`src/ray/core_worker/actor_management/actor_manager.cc:27`](/Users/istoica/src/ray/src/ray/core_worker/actor_management/actor_manager.cc#L27)
  - performs named-actor resolution through GCS:
    [`src/ray/core_worker/actor_management/actor_manager.cc:52`](/Users/istoica/src/ray/src/ray/core_worker/actor_management/actor_manager.cc#L52)
  - connects actor handles to actor-task submitter queues and ownership refs:
    [`src/ray/core_worker/actor_management/actor_manager.cc:123`](/Users/istoica/src/ray/src/ray/core_worker/actor_management/actor_manager.cc#L123)
  - waits for actor-ref deletion through reference-count callbacks:
    [`src/ray/core_worker/actor_management/actor_manager.cc:197`](/Users/istoica/src/ray/src/ray/core_worker/actor_management/actor_manager.cc#L197)
  - consumes GCS actor-state notifications and updates the submitter on alive /
    restarting / dead transitions:
    [`src/ray/core_worker/actor_management/actor_manager.cc:214`](/Users/istoica/src/ray/src/ray/core_worker/actor_management/actor_manager.cc#L214)
  - subscribes actor state from GCS and caches named actors:
    [`src/ray/core_worker/actor_management/actor_manager.cc:281`](/Users/istoica/src/ray/src/ray/core_worker/actor_management/actor_manager.cc#L281)

Impact:

- Rust actor-handle management is not yet a functional replacement for the C++
  actor lifecycle manager.
- Named actor lookup, actor state updates, and ref-deletion driven cleanup are
  likely to diverge.

Suggested exposing tests:

- Named actor test against a live GCS: create, look up, restart, and kill named
  actors, comparing handle validity and cache behavior.
- Actor ref deletion test using the existing C++ `WaitForActorRefDeleted`
  scenarios.
- Actor state notification test: compare submitter behavior as actors move
  through `ALIVE`, `RESTARTING`, and `DEAD`.

### Object Manager Findings

#### Finding OBJECT-1: Rust object-directory is a local cache, not the C++ owner-driven distributed directory

Severity: High

Evidence:

- Rust `ObjectDirectory` is just an in-memory map plus local callback lists. Its
  subscription callback only receives a slice of node IDs; there is no owner
  address, no owner RPC client pool, no pubsub subscription, no failed-lookup
  path, and no owner update batching:
  [`rust/ray-object-manager/src/object_directory.rs:20`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_directory.rs#L20)
  [`rust/ray-object-manager/src/object_directory.rs:43`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_directory.rs#L43)
  [`rust/ray-object-manager/src/object_directory.rs:61`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_directory.rs#L61)
  [`rust/ray-object-manager/src/object_directory.rs:114`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_directory.rs#L114)
  [`rust/ray-object-manager/src/object_directory.rs:156`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_directory.rs#L156)
- The C++ ownership-based directory is materially richer. It:
  - filters dead nodes from owner-published locations,
  - tracks spilled URL, spilled node, pending creation, and object size,
  - batches add/remove/spill updates back to the owning worker,
  - subscribes to owner-published location streams,
  - propagates lookup failure / owner failure to listeners:
  [`src/ray/object_manager/ownership_object_directory.cc:39`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L39)
  [`src/ray/object_manager/ownership_object_directory.cc:51`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L51)
  [`src/ray/object_manager/ownership_object_directory.cc:121`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L121)
  [`src/ray/object_manager/ownership_object_directory.cc:167`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L167)
  [`src/ray/object_manager/ownership_object_directory.cc:202`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L202)
  [`src/ray/object_manager/ownership_object_directory.cc:260`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc#L260)

Impact:

- Rust object-location tracking is not a drop-in replacement for the C++
  distributed ownership directory.
- Owner-driven updates, lookup-failure propagation, dead-node filtering, and
  spill metadata propagation are likely incomplete end to end.

Suggested exposing tests:

- Cross-node ownership-directory test: create an object on node A, move/spill it,
  remove locations, and compare subscriber callbacks and final location state.
- Owner-death test: kill the owning worker while another node is subscribed and
  verify failure propagation matches C++.
- Batched add/remove/spill stress test using the existing C++
  `ownership_object_directory_test` scenarios as the baseline.

#### Finding OBJECT-2: Rust `PullManager` drops the C++ activation, pinning, timeout-failure, and restoration semantics

Severity: High

Evidence:

- Rust `PullManager` stores bundles in plain vectors and selects the first known
  location. It does not deduplicate bundle contents, does not maintain active vs
  inactive bundle sets, does not deactivate lower-priority bundles to satisfy
  higher-priority ones, does not pin pulled objects, and does not fail missing
  objects after timeout:
  [`rust/ray-object-manager/src/pull_manager.rs:94`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs#L94)
  [`rust/ray-object-manager/src/pull_manager.rs:127`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs#L127)
  [`rust/ray-object-manager/src/pull_manager.rs:170`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs#L170)
  [`rust/ray-object-manager/src/pull_manager.rs:224`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs#L224)
  [`rust/ray-object-manager/src/pull_manager.rs:263`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs#L263)
  [`rust/ray-object-manager/src/pull_manager.rs:274`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs#L274)
  [`rust/ray-object-manager/src/pull_manager.rs:299`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs#L299)
- The C++ `PullManager` does all of those things:
  - deduplicates bundle object refs up front,
  - moves bundles between pullable / active / inactive states,
  - deactivates lower-priority bundles to preserve quota,
  - pins newly available objects,
  - restores spilled objects with abort-before-restore behavior,
  - fails pulls after fetch timeout when the object has no locations and is not
    pending reconstruction:
  [`src/ray/object_manager/pull_manager.cc:52`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L52)
  [`src/ray/object_manager/pull_manager.cc:109`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L109)
  [`src/ray/object_manager/pull_manager.cc:180`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L180)
  [`src/ray/object_manager/pull_manager.cc:232`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L232)
  [`src/ray/object_manager/pull_manager.cc:317`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L317)
  [`src/ray/object_manager/pull_manager.cc:362`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L362)
  [`src/ray/object_manager/pull_manager.cc:446`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L446)
  [`src/ray/object_manager/pull_manager.cc:511`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L511)
  [`src/ray/object_manager/pull_manager.cc:547`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L547)
  [`src/ray/object_manager/pull_manager.cc:586`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L586)

Impact:

- Rust pull scheduling is much weaker than C++ under mixed `get` / `wait` /
  task-arg pressure.
- Objects that should be pinned, canceled, restored, or failed can instead stay
  in an ambiguous local state.
- Duplicate object IDs inside a bundle may be overcounted or retried
  differently from C++.

Suggested exposing tests:

- Port the C++ `pull_manager_test.cc` priority/quota cases directly and run them
  against Rust.
- Duplicate-object bundle test: submit the same object twice in one pull bundle
  and compare quota accounting, retry counts, and completion behavior.
- Missing-object timeout test: no locations, no pending creation, no spill URL;
  C++ should surface `OBJECT_FETCH_TIMED_OUT`, and Rust currently has no
  equivalent failure path.
- Spill-restore test: compare local spill, external spill, and remote-spilled
  node cases against the existing C++ tests.

#### Finding OBJECT-3: Rust push/object-manager flow omits C++ deferred pushes, resend behavior, and remote-spill serving

Severity: High

Evidence:

- Rust `ObjectManager::push` only starts a push if the object is already present
  in `local_objects`; otherwise it returns `false`. The `pull` RPC handler also
  just calls `push()` and returns success regardless of whether any transfer was
  actually initiated:
  [`rust/ray-object-manager/src/object_manager.rs:100`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_manager.rs#L100)
  [`rust/ray-object-manager/src/grpc_service.rs:67`](/Users/istoica/src/ray/rust/ray-object-manager/src/grpc_service.rs#L67)
- Rust `PushManager` deduplicates an existing `(node, object)` push by rejecting
  it. C++ instead treats a duplicate push request as a resend request and
  reschedules all chunks:
  [`rust/ray-object-manager/src/push_manager.rs:64`](/Users/istoica/src/ray/rust/ray-object-manager/src/push_manager.rs#L64)
  [`src/ray/object_manager/push_manager.cc:19`](/Users/istoica/src/ray/src/ray/object_manager/push_manager.cc#L19)
- C++ `ObjectManager::Push` has additional behavior Rust does not:
  - if the object is not local but spilled on local filesystem, push from spill,
  - if the object is not ready yet, keep an unfulfilled push request and time it
    out later,
  - restart pending pushes when the object is eventually added:
  [`src/ray/object_manager/object_manager.cc:171`](/Users/istoica/src/ray/src/ray/object_manager/object_manager.cc#L171)
  [`src/ray/object_manager/object_manager.cc:200`](/Users/istoica/src/ray/src/ray/object_manager/object_manager.cc#L200)
  [`src/ray/object_manager/object_manager.cc:283`](/Users/istoica/src/ray/src/ray/object_manager/object_manager.cc#L283)
  [`src/ray/object_manager/object_manager.cc:321`](/Users/istoica/src/ray/src/ray/object_manager/object_manager.cc#L321)

Impact:

- A Rust node can acknowledge a `Pull` RPC without actually scheduling a usable
  transfer if the object is not already local.
- Duplicate or retrying remote pull requests can behave differently from C++,
  especially when the receiver expects chunk resend behavior.
- Objects spilled locally or becoming available shortly after the request may be
  served by C++ but silently missed by Rust.

Suggested exposing tests:

- Deferred-push test: request an object before it is sealed locally; C++ should
  queue and later serve it, Rust should currently fail to do so.
- Duplicate-pull/resend test: send repeated `Pull` requests for the same object
  and compare chunk resend behavior.
- Local-spill serving test: spill an object locally, issue a remote `Pull`, and
  verify whether the object is served without first restoring it to memory.

#### Finding OBJECT-4: Rust inbound chunk handling does not preserve C++ buffer-allocation, validation, and sealing semantics

Severity: High

Evidence:

- Rust `ObjectBufferPool::write_chunk` ignores the incoming data payload and
  only increments a received-chunk counter. It does not validate chunk length,
  does not allocate/write plasma buffers, does not protect against duplicate or
  out-of-order chunk writes, and does not seal/release the object in the store:
  [`rust/ray-object-manager/src/object_buffer_pool.rs:66`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_buffer_pool.rs#L66)
  [`rust/ray-object-manager/src/object_buffer_pool.rs:85`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_buffer_pool.rs#L85)
- Rust `ObjectManager::handle_push` marks the object as local once the buffer
  pool reports that enough chunks were counted, but it never writes those bytes
  into `PlasmaStore`:
  [`rust/ray-object-manager/src/object_manager.rs:141`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_manager.rs#L141)
- The C++ buffer pool performs the real receive path:
  - ensures the buffer exists with the correct size,
  - rejects duplicate or conflicting chunk states,
  - copies bytes into the allocated buffer,
  - seals and releases the object only after all chunks arrive,
  - aborts partially created objects safely:
  [`src/ray/object_manager/object_buffer_pool.cc:100`](/Users/istoica/src/ray/src/ray/object_manager/object_buffer_pool.cc#L100)
  [`src/ray/object_manager/object_buffer_pool.cc:120`](/Users/istoica/src/ray/src/ray/object_manager/object_buffer_pool.cc#L120)
  [`src/ray/object_manager/object_buffer_pool.cc:180`](/Users/istoica/src/ray/src/ray/object_manager/object_buffer_pool.cc#L180)
  [`src/ray/object_manager/object_buffer_pool.cc:224`](/Users/istoica/src/ray/src/ray/object_manager/object_buffer_pool.cc#L224)

Impact:

- Rust can mark an object as locally available even when the received bytes were
  never written to storage.
- Out-of-order, duplicate, truncated, or inconsistent chunks are not validated
  the way C++ validates them.
- Any consumer that reads the object after a remote push may observe missing
  data, stale data, or an object that exists only in bookkeeping.

Suggested exposing tests:

- End-to-end remote push test with non-zero payload bytes and metadata, followed
  by a local readback; compare exact bytes between C++ and Rust.
- Out-of-order chunk test: deliver chunk 1 before chunk 0 and compare whether
  the object is rejected, buffered, or falsely accepted.
- Duplicate-chunk test: send the same chunk twice and compare final object size
  and contents.
- Conflicting-size test: send later chunks with mismatched `data_size` or
  `metadata_size` and verify C++ rejects them while Rust currently does not.

#### Finding OBJECT-5: Rust spill support is mostly standalone and is not integrated like the C++ spill/restore path

Severity: Medium-High

Evidence:

- Rust has a `SpillManager` that can write/read spill files locally:
  [`rust/ray-object-manager/src/spill_manager.rs:67`](/Users/istoica/src/ray/rust/ray-object-manager/src/spill_manager.rs#L67)
  [`rust/ray-object-manager/src/spill_manager.rs:97`](/Users/istoica/src/ray/rust/ray-object-manager/src/spill_manager.rs#L97)
  [`rust/ray-object-manager/src/spill_manager.rs:165`](/Users/istoica/src/ray/rust/ray-object-manager/src/spill_manager.rs#L165)
- But the `TransportLoop` only stores an optional `spill_manager` and never
  actually uses it in `process_restores`; restore behavior depends entirely on
  an external callback:
  [`rust/ray-object-manager/src/transport.rs:77`](/Users/istoica/src/ray/rust/ray-object-manager/src/transport.rs#L77)
  [`rust/ray-object-manager/src/transport.rs:105`](/Users/istoica/src/ray/rust/ray-object-manager/src/transport.rs#L105)
  [`rust/ray-object-manager/src/transport.rs:177`](/Users/istoica/src/ray/rust/ray-object-manager/src/transport.rs#L177)
- The Rust object manager itself does not consult `SpillManager` when serving a
  remote `Pull`, and the pull manager has no logic equivalent to the C++
  direct-restore / filesystem-serving flow:
  [`rust/ray-object-manager/src/object_manager.rs:100`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_manager.rs#L100)
  [`rust/ray-object-manager/src/pull_manager.rs:255`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs#L255)
  [`rust/ray-object-manager/src/pull_manager.rs:263`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs#L263)
- C++ wires spill handling directly into object-manager behavior:
  - `PullManager::TryToMakeObjectLocal` chooses between remote pull and direct
    restore:
    [`src/ray/object_manager/pull_manager.cc:463`](/Users/istoica/src/ray/src/ray/object_manager/pull_manager.cc#L463)
  - `ObjectManager::Push` can serve directly from locally spilled filesystem
    data:
    [`src/ray/object_manager/object_manager.cc:328`](/Users/istoica/src/ray/src/ray/object_manager/object_manager.cc#L328)
  - `ObjectManager::PushFromFilesystem` uses `SpilledObjectReader` to stream the
    spilled object to the requester:
    [`src/ray/object_manager/object_manager.cc:409`](/Users/istoica/src/ray/src/ray/object_manager/object_manager.cc#L409)

Impact:

- Rust spill tests may pass in isolation while the real object-manager
  spill/restore behavior still diverges from C++.
- A spilled object may be restorable in a unit test but not servable through the
  actual remote pull path.

Suggested exposing tests:

- End-to-end spilled-object fetch test: spill an object, evict it from memory,
  then fetch it from another node through the standard object-manager path.
- Restore-path integration test: configure the Rust `TransportLoop` with a real
  `SpillManager` and verify a spilled object is restored into the store and then
  read back correctly.
- Filesystem-spill serving test: compare whether C++ can stream directly from a
  local spill file while Rust requires out-of-band restoration logic.

### Python / PyO3 Boundary Findings

#### Finding PY-1: Rust `_raylet` surface is far narrower than the C++/Cython Python contract

Severity: High

Evidence:

- The Rust compatibility shim [`rust/ray/_raylet.py`](/Users/istoica/src/ray/rust/ray/_raylet.py)
  only re-exports `ObjectRef`:
  [`rust/ray/_raylet.py:1`](/Users/istoica/src/ray/rust/ray/_raylet.py#L1)
- The real Cython `_raylet` module exposes a very large Python-facing surface,
  including `CoreWorker`, `ObjectRef`, streaming APIs, channel APIs, async get
  callbacks, actor/task control, ownership helpers, and many other internals:
  [`python/ray/_raylet.pyx:2779`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L2779)
  [`python/ray/_raylet.pyx:3028`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L3028)
  [`python/ray/_raylet.pyx:3138`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L3138)
  [`python/ray/_raylet.pyx:3903`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L3903)
  [`python/ray/_raylet.pyx:4117`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L4117)
  [`python/ray/_raylet.pyx:4639`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L4639)
  [`python/ray/_raylet.pyx:4768`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L4768)
- The Rust PyO3 module does export some types and helpers, but it is still a
  much smaller surface:
  [`rust/ray-core-worker-pylib/src/lib.rs:65`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/lib.rs#L65)

Impact:

- Python code that imports `_raylet`-level internals from the Cython backend is
  very likely to break or silently lose behavior under the Rust port.
- This is a contract-level incompatibility, not just an internal rewrite.

Suggested exposing tests:

- Python API parity smoke test: import the Rust `_raylet` replacement and verify
  the expected subset of Cython symbols exists or is intentionally absent.
- Run representative internal Python tests that touch `_raylet` directly
  instead of only the public `ray.*` surface.

#### Finding PY-2: Rust `PyCoreWorker` constructor and lifecycle are not equivalent to the Cython `CoreWorker`

Severity: High

Evidence:

- The Cython `CoreWorker.__cinit__` takes many initialization inputs and installs
  a large callback/control surface: sockets, GCS options, runtime env hash,
  signal handlers, task execution callback, spill/restore/delete callbacks,
  actor shutdown callback, metrics, cluster/session metadata, and more:
  [`python/ray/_raylet.pyx:2781`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L2781)
- Rust `PyCoreWorker.__new__` accepts only:
  `worker_type`, `node_ip_address`, `gcs_address`, `job_id_int`, optional
  `worker_id`, optional `node_id`, and `max_concurrency`, then fills the rest
  from `CoreWorkerOptions::default()`:
  [`rust/ray-core-worker-pylib/src/core_worker.rs:156`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/core_worker.rs#L156)
  [`rust/ray-core-worker-pylib/src/core_worker.rs:176`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/core_worker.rs#L176)
- The Cython `CoreWorker` also exposes lifecycle methods such as
  `shutdown_driver`, `run_task_loop`, and `drain_and_exit_worker`:
  [`python/ray/_raylet.pyx:2841`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L2841)
  [`python/ray/_raylet.pyx:2851`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L2851)
  [`python/ray/_raylet.pyx:2855`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L2855)
- No equivalent Python-facing lifecycle/control methods are visible in the Rust
  `PyCoreWorker` implementation that was inspected:
  [`rust/ray-core-worker-pylib/src/core_worker.rs:144`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/core_worker.rs#L144)

Impact:

- The Rust `PyCoreWorker` is not a drop-in constructor or lifecycle replacement
  for the Cython `CoreWorker`.
- Initialization-time behaviors around spill workers, restore workers, task
  callbacks, signal handling, and shutdown are likely to diverge sharply.

Suggested exposing tests:

- Driver/worker initialization parity test: instantiate the Cython and Rust
  workers with production-like arguments and compare accepted parameters and
  post-init state.
- Worker shutdown/drain test: compare Python-visible behavior for normal worker
  exit, intentional drain, and driver shutdown.
- Callback integration test: verify task execution, spill, restore, and actor
  shutdown callbacks are actually wired through the Python boundary.

#### Finding PY-3: Rust `ObjectRef` Python semantics are materially weaker than the Cython `ObjectRef`

Severity: High

Evidence:

- The Cython `ObjectRef` constructor accepts owner address, call-site data,
  local-ref skipping, and tensor-transport metadata. It also adds/removes local
  references through `global_worker.core_worker`, supports `future()`,
  `as_future()`, `_on_completed()`, `owner_address()`, and transport metadata:
  [`python/ray/includes/object_ref.pxi:50`](/Users/istoica/src/ray/python/ray/includes/object_ref.pxi#L50)
  [`python/ray/includes/object_ref.pxi:63`](/Users/istoica/src/ray/python/ray/includes/object_ref.pxi#L63)
  [`python/ray/includes/object_ref.pxi:86`](/Users/istoica/src/ray/python/ray/includes/object_ref.pxi#L86)
  [`python/ray/includes/object_ref.pxi:123`](/Users/istoica/src/ray/python/ray/includes/object_ref.pxi#L123)
  [`python/ray/includes/object_ref.pxi:153`](/Users/istoica/src/ray/python/ray/includes/object_ref.pxi#L153)
- Rust `PyObjectRef` only wraps an `ObjectID` with optional owner info and call
  site. The exposed Python methods are basically `binary`, `hex`, `is_nil`,
  `call_site`, owner IP/port, and standard repr/hash/bool:
  [`rust/ray-core-worker-pylib/src/object_ref.rs:16`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/object_ref.rs#L16)
  [`rust/ray-core-worker-pylib/src/object_ref.rs:78`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/object_ref.rs#L78)
- The Rust shim `rust/ray/_raylet.py` also assumes `ObjectRef` comes from
  `ray.__init__`, not from the PyO3 `_raylet` module itself:
  [`rust/ray/_raylet.py:3`](/Users/istoica/src/ray/rust/ray/_raylet.py#L3)

Impact:

- Python `ObjectRef` lifecycle, local reference counting, async wait/future
  integration, and transport metadata behavior are not obviously preserved.
- Code that relies on `ObjectRef.future()`, `ObjectRef.as_future()`,
  `_on_completed()`, or ownership metadata can regress even if basic ID
  equality still works.

Suggested exposing tests:

- Python `ObjectRef` lifecycle test: create refs, drop refs, force GC, and
  compare local-reference side effects against the Cython backend.
- Async integration test: compare `await ref`, `ref.future()`, and callback
  completion semantics.
- Ownership metadata test: serialize/deserialize refs and compare owner address
  and call-site preservation.

#### Finding PY-4: Rust `PyCoreWorker.get` and error mapping do not match the Cython Python behavior

Severity: Medium-High

Evidence:

- The Rust `PyCoreWorker.get` takes raw binary object IDs, not `ObjectRef`
  objects, and returns `(data_bytes, metadata_bytes)` tuples or `None`. It also
  converts objects whose metadata equals `b"ERROR"` into a generic
  `PyRuntimeError("RayTaskError: ...")` string:
  [`rust/ray-core-worker-pylib/src/core_worker.rs:214`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/core_worker.rs#L214)
  [`rust/ray-core-worker-pylib/src/core_worker.rs:231`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/core_worker.rs#L231)
- The Cython `CoreWorker.get_objects` accepts Python `ObjectRef`s and returns
  serialized Ray objects via `RayObjectsToSerializedRayObjects`, not just raw
  `(data, metadata)` tuples:
  [`python/ray/_raylet.pyx:2995`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L2995)
- Rust general error conversion also collapses many errors into broad Python
  exception classes:
  [`rust/ray-core-worker-pylib/src/common.rs:94`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/common.rs#L94)
  [`rust/ray-core-worker-pylib/src/common.rs:112`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/common.rs#L112)

Impact:

- Python callers can observe different argument conventions, return types, and
  exception shapes from `get`.
- Task/actor failure behavior visible to Python is likely weaker and less typed
  than the Cython backend.

Suggested exposing tests:

- Python conformance test for `get`: call through the same helper code against
  Cython and Rust and compare accepted arguments, return shapes, and timeout
  behavior.
- Error round-trip test: return task errors, actor-death errors, and object-miss
  cases and compare exact Python exception types and messages.

#### Finding PY-5: Rust `PyGcsClient` is reduced and includes explicit Python-visible stubs

Severity: Medium-High

Evidence:

- The Cython `GcsClient` wraps a richer `InnerGcsClient` surface, including:
  - sync and async internal KV APIs,
  - `internal_kv_multi_get`,
  - typed `check_alive`,
  - real `drain_nodes`,
  - richer `get_all_node_info` filtering:
  [`python/ray/_raylet.pyx:2754`](/Users/istoica/src/ray/python/ray/_raylet.pyx#L2754)
  [`python/ray/includes/gcs_client.pxi:95`](/Users/istoica/src/ray/python/ray/includes/gcs_client.pxi#L95)
  [`python/ray/includes/gcs_client.pxi:183`](/Users/istoica/src/ray/python/ray/includes/gcs_client.pxi#L183)
  [`python/ray/includes/gcs_client.pxi:281`](/Users/istoica/src/ray/python/ray/includes/gcs_client.pxi#L281)
  [`python/ray/includes/gcs_client.pxi:316`](/Users/istoica/src/ray/python/ray/includes/gcs_client.pxi#L316)
- Rust `PyGcsClient` only exposes a smaller sync subset, converts keys to
  `String` in `internal_kv_keys`, implements `internal_kv_exists` via `get`,
  and contains an explicit `drain_nodes` stub that returns an empty vector:
  [`rust/ray-core-worker-pylib/src/gcs_client.rs:72`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/gcs_client.rs#L72)
  [`rust/ray-core-worker-pylib/src/gcs_client.rs:132`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/gcs_client.rs#L132)
  [`rust/ray-core-worker-pylib/src/gcs_client.rs:151`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/gcs_client.rs#L151)
  [`rust/ray-core-worker-pylib/src/gcs_client.rs:210`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/gcs_client.rs#L210)
  [`rust/ray-core-worker-pylib/src/gcs_client.rs:294`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/gcs_client.rs#L294)

Impact:

- Python code that expects the Cython GCS client contract can observe different
  types, missing async APIs, and stubbed drain behavior.
- This compounds the earlier GCS backend mismatches with Python-visible client
  mismatches.

Suggested exposing tests:

- Python GCS client parity test: compare sync/async KV, `multi_get`,
  `check_alive`, and `drain_nodes` behavior against the Cython backend.
- Type-shape test: verify key/value APIs preserve bytes-vs-string behavior and
  exception behavior under missing keys and RPC failures.

#### Finding PY-6: The RDT path remains a hybrid Python/Rust system, not a clean drop-in backend replacement

Severity: Medium

Evidence:

- The Python RDT store explicitly tries to import `PyRDTStore` from `_raylet`
  and otherwise falls back to a pure-Python implementation:
  [`rust/ray/experimental/rdt/rdt_store.py:25`](/Users/istoica/src/ray/rust/ray/experimental/rdt/rdt_store.py#L25)
  [`rust/ray/experimental/rdt/rdt_store.py:75`](/Users/istoica/src/ray/rust/ray/experimental/rdt/rdt_store.py#L75)
- Even when Rust `PyRDTStore` is present, major transport behavior remains in
  Python helpers and Python transport-manager classes:
  [`rust/ray/experimental/rdt/rdt_store.py:176`](/Users/istoica/src/ray/rust/ray/experimental/rdt/rdt_store.py#L176)
  [`rust/ray/experimental/rdt/tensor_transport_manager.py:28`](/Users/istoica/src/ray/rust/ray/experimental/rdt/tensor_transport_manager.py#L28)
- The Rust transport registry itself still imports Python classes dynamically
  and instantiates Python transport managers, including default-registration
  behavior gated on importing `torch` and Python transport modules:
  [`rust/ray-core-worker-pylib/src/rdt/registry.rs:55`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/rdt/registry.rs#L55)
  [`rust/ray-core-worker-pylib/src/rdt/registry.rs:107`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/rdt/registry.rs#L107)
  [`rust/ray-core-worker-pylib/src/rdt/registry.rs:217`](/Users/istoica/src/ray/rust/ray-core-worker-pylib/src/rdt/registry.rs#L217)

Impact:

- The RDT path should be treated as a mixed Python/Rust subsystem, not evidence
  that the Python/native backend boundary is fully ported.
- Behavior can differ depending on whether `_raylet` and optional Python
  transport dependencies are present.

Suggested exposing tests:

- Two-mode RDT test: run the same RDT unit/integration cases with Rust
  `PyRDTStore` available and with forced fallback to the Python implementation,
  then compare behavior.
- Import-time dependency test: validate default transport registration behavior
  with and without `torch` and transport modules present.
- End-to-end GPU/RDT actor test: exercise `__ray_send__`, `__ray_recv__`,
  metadata extraction, and cleanup through the real worker path.

## Overall Assessment

The Rust branch is not functionally equivalent to Ray’s original C++ backend.
The strongest evidence is not just implementation style drift; it includes
confirmed behavior differences, missing distributed-control logic, stubbed RPCs,
weaker failure handling, and a materially smaller Python/native contract.

The highest-confidence non-equivalences are:

- GCS control-plane behavior: internal KV persistence, job/worker/actor RPCs,
  placement groups, autoscaler/drain behavior, and pubsub semantics.
- Raylet control/data-plane behavior: worker lease lifecycle, dependency
  management, `wait`, worker-pool matching, local-object management, and drain
  semantics.
- Core Worker semantics: reference counting, ownership tracking, task/actor
- submission, task execution ordering/cancellation, dependency/future
  resolution, and object recovery.
- Object Manager semantics: distributed object-location tracking, pull
  prioritization and failure behavior, push serving, chunk validation/sealing,
  and spill/restore integration.
- Python boundary behavior: `_raylet` symbol coverage, `CoreWorker`
  initialization/lifecycle, `ObjectRef` semantics, `get`/error behavior, and
  GCS client API parity.

## Prioritized Test Plan

### Tier 1: Cross-language conformance tests

- Run the same scenario against the C++ backend and the Rust backend and compare
  externally visible behavior, not internal metrics.
- Start with:
  - object fetch / wait / timeout / spill-restore,
  - actor task ordering / restart / cancellation,
  - worker lease acquire / return / drain,
  - placement-group lifecycle,
  - Python `ObjectRef` / `CoreWorker.get` / `_raylet` compatibility.

### Tier 2: Failure-revealing distributed integration tests

- Owner death while objects are borrowed remotely.
- Node death during pull / push / spill restore.
- Duplicate or out-of-order object chunks.
- Drain/shutdown during active leases and actor tasks.
- Missing-object timeout and reconstruction paths.

### Tier 3: API parity tests at the Python boundary

- `_raylet` import/symbol parity smoke tests.
- `CoreWorker` constructor / lifecycle / shutdown behavior.
- `ObjectRef.future()`, `await ref`, callback completion, and ref-GC behavior.
- `GcsClient` KV / async / drain/alive parity.

### Tier 4: Regression tests ported directly from C++ coverage

- Port the strongest existing C++ tests first, especially for:
  - `pull_manager_test.cc`,
  - `ownership_object_directory_test.cc`,
  - object-manager transfer/spill tests,
  - reference-counter / actor-submitter / task-manager tests,
  - raylet wait/dependency/worker-pool tests.

## Next Steps In This Review

1. Convert the highest-severity findings into an executable checklist of tests to
   add or port.
2. If needed, validate a subset of the most important mismatches at runtime with
   focused integration tests.
3. Separate intentional redesigns from likely bugs only after those tests pass.

## Must-Fix First

This is the short list of issues that should be treated as blockers before the
Rust backend is described as a drop-in replacement for the C++ backend.

1. `GCS-1`: internal KV persistence mismatch. Rust uses in-memory internal KV
   where C++ uses the configured GCS storage backend.
2. `GCS-2`, `GCS-9`, `GCS-10`, `GCS-11`: stubbed or reduced GCS RPCs for job
   errors, actor lineage/out-of-scope handling, and worker info/update paths.
3. `GCS-12`, `GCS-13`, `GCS-14`, `GCS-16`, `GCS-18`: placement-group and
   autoscaler/drain semantics diverge materially from C++.
4. `RAYLET-1`, `RAYLET-2`, `RAYLET-7`, `RAYLET-8`, `RAYLET-9`: worker lease
   return, lease request lifecycle, drain, shutdown, and cleanup RPC behavior
   are not equivalent.
5. `RAYLET-10`, `RAYLET-11`, `RAYLET-12`, `RAYLET-13`: dependency management,
   `wait`, worker-pool lifecycle, and local object management are all weaker
   than the C++ semantics.
6. `CORE-2`, `CORE-10`: distributed ownership and reference-counting semantics
   are not preserved.
7. `CORE-3`, `CORE-4`, `CORE-5`, `CORE-6`: task submission, actor submission,
   task execution, and key CoreWorker RPC handlers are reduced or stubbed.
8. `CORE-7`, `CORE-8`, `CORE-9`, `CORE-11`, `CORE-12`: dependency resolution,
   future resolution, object recovery, direct actor transport, and actor
   lifecycle management diverge significantly from C++.
9. `OBJECT-1`, `OBJECT-2`, `OBJECT-3`: the Object Manager does not preserve the
   C++ distributed object-location, pull scheduling/failure, or push-serving
   behavior.
10. `OBJECT-4`: inbound chunk handling is not equivalent to the C++ buffer
    allocation, validation, write, seal, and abort flow.
11. `OBJECT-5`: spill support exists in isolation but is not integrated into
    object-manager behavior the way C++ integrates spill/restore.
12. `PY-1`, `PY-2`, `PY-3`, `PY-4`, `PY-5`: the Python/native boundary is not a
    drop-in replacement for Cython `_raylet`, especially for `_raylet`
    coverage, `CoreWorker`, `ObjectRef`, `get`, and `GcsClient`.

Recommended fix order:

1. Restore correctness of distributed ownership, object fetch/transfer, and
   worker lease semantics.
2. Eliminate explicit control-plane stubs in GCS, Raylet, and Core Worker.
3. Bring the Python/native boundary back to the minimum contract needed by the
   existing Python stack.
4. Only then optimize or refactor further.

## Open Questions

- Which Rust crates are used as production drop-in replacements today versus experimental sidecars or partial implementations?
- Which Python test suites are currently exercised against the Rust backend, and under what feature flags?
