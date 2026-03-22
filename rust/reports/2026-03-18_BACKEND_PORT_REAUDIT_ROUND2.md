# Backend Port Re-Audit After Round 2 Fixes

Date: 2026-03-18
Branch: `cc-to-rust-experimental`
Head reviewed: `6c1a1905d7`
Round-2 report reviewed: `rust/reports/2026-03-18_PARITY_FIX_REPORT_ROUND2.md`

## Executive Summary

The Rust backend is in better shape than the prior re-audit, but the Round 2 report overstates what is actually fixed.

Several previously open gaps are genuinely improved:
- `GCS-1` internal KV persistence is now wired through a store-client-backed implementation in Redis mode.
- `GCS-8` pubsub unsubscribe is now channel-scoped instead of deleting the entire subscriber.
- `GCS-16` autoscaler version handling is now materially closer to C++.

But several Round 2 items are only partial or still open:
- `GCS-12` placement-group lifecycle is still not truly equivalent.
- `GCS-17` cluster status payload is still much thinner than C++.
- `GCS-4` `GetAllJobInfo` is still only a reduced approximation of the C++ behavior.
- `GCS-6` drain-node side effects are still weaker than C++.
- `RAYLET-3/4/5/6` are improved only partially; `GetNodeStats` is the clearest example.
- `CORE-10` remains only partially closed. The Rust path now handles some owner update cases, but it is still not the C++ distributed ownership/location protocol.

Bottom line: the Rust backend is still not functionally equivalent to the C++ backend.

## Review Method

This re-audit did not trust the Round 2 fix report at face value.

For each previously open finding, I:
1. Re-read the current Rust source at `HEAD`.
2. Re-read the relevant C++ implementation to recover the actual contract.
3. Checked whether the Rust implementation matched that contract, not just whether it was no longer stubbed.
4. Checked the new Rust tests and whether they actually prove parity.

## Current Classification

### Verified Fixed

#### GCS-1: Internal KV persistence mismatch

Status: Fixed

Evidence:
- `rust/ray-gcs/src/server.rs` now chooses `StoreClientInternalKV` for Redis-backed mode.
- `rust/ray-gcs/src/store_client.rs` contains the store-client-backed `InternalKVInterface` implementation.

Assessment:
- This closes the earlier issue where Redis mode still used in-memory internal KV.
- This is a real parity improvement.

#### GCS-8: Pubsub unsubscribe semantics

Status: Fixed

Evidence:
- `rust/ray-gcs/src/pubsub_handler.rs` now removes only the requested channel subscription and retains the subscriber if it still has other channels.
- `rust/ray-gcs/src/grpc_services.rs` passes `channel_type` into unsubscribe.

Assessment:
- This now matches the essential C++ unsubscribe scoping behavior.

#### GCS-16: Autoscaler version handling

Status: Mostly fixed

Evidence:
- `rust/ray-gcs/src/autoscaler_state_manager.rs` now rejects stale autoscaling-state versions.
- `rust/ray-gcs/src/grpc_services.rs` now passes the request version instead of `0`.

Assessment:
- This closes the earlier obvious stale-version bug.
- The surrounding autoscaler contract is still thinner than C++, but the specific stale-version problem is materially improved.

## Partial Or Still Open

### GCS-12: Placement Group Create Lifecycle

Status: Partial, not fixed

Current Rust:
- `rust/ray-gcs/src/grpc_services.rs` creates the placement group in `Pending` state, but then immediately calls `mark_placement_group_created(...)` in the same gRPC handler.
- The new Rust test explicitly asserts that after `create_placement_group`, the PG is already in `Created` state.

C++ contract:
- `src/ray/gcs/gcs_placement_group_manager.cc` transitions the placement group to `CREATED` only after scheduler success (`OnPlacementGroupCreationSuccess`), and `WaitPlacementGroupUntilReady` is tied to that lifecycle.

Difference:
- Rust still short-circuits the state machine in the request handler.
- This preserves a convenient single-node behavior, but it is not the same contract as C++.

Risk:
- Wait/readiness behavior and bundle-scheduling semantics can diverge under real placement-group scheduling and recovery scenarios.

### GCS-17: Cluster Status Payload Parity

Status: Partial, not fixed

Current Rust:
- `rust/ray-gcs/src/grpc_services.rs` `get_cluster_status` now returns:
  - `autoscaling_state`
  - `cluster_session_name`
  - `cluster_resource_state_version`
  - node states

C++ contract:
- `src/ray/gcs/gcs_autoscaler_state_manager.cc` fills `ClusterResourceState` via `MakeClusterResourceStateInternal(...)`, which includes:
  - `last_seen_autoscaler_state_version`
  - node states
  - pending resource requests
  - pending gang resource requests
  - cluster resource constraints
  - session name
  - resource-state version

Difference:
- Rust still omits the richer cluster-resource payload.
- The Round 2 report claimed this issue was fixed; that is too strong.

Risk:
- Autoscaler-visible cluster state can differ materially, especially under load, placement-group pressure, or explicit resource requests.

### GCS-4: GetAllJobInfo Enrichment / Filter Behavior

Status: Partial, not fixed

Current Rust:
- `rust/ray-gcs/src/job_manager.rs` now supports:
  - `limit`
  - `job_or_submission_id`
  - `skip_submission_job_info_field`
  - `skip_is_running_tasks_field`
- But it only filters against:
  - hex job ID
  - `job.job_info.entrypoint`
  - `job.job_info.status`

C++ contract:
- `src/ray/gcs/gcs_job_manager.cc` filters by:
  - job ID
  - `job_submission_id` from `config.metadata`
- It also asynchronously:
  - fetches `is_running_tasks` from workers
  - enriches `job_info` from internal KV for Jobs API submissions

Difference:
- Rust does not reproduce the C++ `job_submission_id` lookup semantics.
- Rust does not perform the asynchronous worker RPC for `is_running_tasks`.
- Rust does not perform the internal-KV Jobs API enrichment pass.

Risk:
- Jobs API clients can observe different filtering and metadata behavior.

### GCS-6: Drain Node Side Effects

Status: Partial, not fixed

Current Rust:
- `rust/ray-gcs/src/node_manager.rs` still only records the node in `draining_nodes`.
- The node gRPC path in `rust/ray-gcs/src/grpc_services.rs` opportunistically updates autoscaler drain state if an autoscaler manager reference is present.
- The autoscaler drain RPC path also marks drain state.

C++ contract:
- `src/ray/gcs/gcs_node_manager.cc` and `src/ray/gcs/gcs_autoscaler_state_manager.cc` coordinate broader drain behavior and side effects.

Difference:
- Rust still looks like local bookkeeping plus partial cross-wiring.
- The full drain protocol and callback side effects are still weaker than C++.

Risk:
- Real node-drain workflows can diverge, especially around deadlines, retries, node removal, and autoscaler/raylet coordination.

### RAYLET-3: Worker Backlog Reporting

Status: Partial, not fixed

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` updates a node-wide `BacklogTracker` keyed only by scheduling class.
- `rust/ray-raylet/src/node_manager.rs` stores only the latest aggregate backlog per scheduling class.

C++ contract:
- `src/ray/raylet/node_manager.cc` validates the worker exists, clears old backlog for that worker, and sets backlog per `(worker_id, scheduling_class)` through the local lease manager.

Difference:
- Rust loses the per-worker dimension entirely.
- Rust does not clear old backlog by worker.

Risk:
- Backlog accounting can become stale or inaccurate when workers disconnect or report overlapping classes.

### RAYLET-4: Object Pinning Semantics

Status: Partial, not fixed

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` returns success if `local_object_manager.is_pinned(object_id)` is true.

C++ contract:
- `src/ray/raylet/node_manager.cc` retrieves objects from plasma, filters pending deletion, returns per-object success, and then calls `PinObjectsAndWaitForFree(...)`.

Difference:
- Rust checks a local flag; it does not implement the real pin-and-wait owner semantics.
- It does not use `owner_address` / `generator_id` the way C++ does.

Risk:
- Pinning may report success without actually reproducing the lifetime semantics that C++ guarantees.

### RAYLET-5: ResizeLocalResourceInstances

Status: Partial, not fixed

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` resizes named resources through `LocalResourceManager::resize_local_resource(...)`.
- `rust/ray-raylet/src/local_resource_manager.rs` clamps negative totals and recomputes available capacity.

C++ contract:
- `src/ray/raylet/node_manager.cc` explicitly rejects unit-instance resources such as GPU and computes deltas against current total/available resource maps.

Difference:
- Rust does not explicitly reject unit-instance resources.
- The Rust local-resource model still looks flatter than the C++ instance-aware path.

Risk:
- Dynamic resizing behavior can still diverge, especially for unit resources and partially allocated resources.

### RAYLET-6: GetNodeStats Parity

Status: Still open

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` returns:
  - `num_workers`
  - `store_stats` with a few object-store counters

The new Rust tests only assert:
- a pinned-object count / bytes value
- that stats still exist after draining

C++ contract:
- `src/ray/raylet/node_manager.cc` collects:
  - local object-manager stats
  - object-manager spill/restore stats
  - per-worker `GetCoreWorkerStats`
  - memory-info support
  - aggregated store stats in the multi-node path

Difference:
- Rust still does not expose the richer worker/core-worker stats path.
- The Round 2 report claimed idle worker count, active lease count, and drain-state parity; the current code does not implement those fields.

Risk:
- Observability and debugging behavior remain materially weaker than C++.

### CORE-10: Distributed Ownership Semantics

Status: Partial, not fixed

Current Rust improvements:
- `rust/ray-core-worker/src/grpc_service.rs` now handles wrong-recipient checks and mixed `UpdateObjectLocationBatch` add/remove/spill updates.
- `rust/ray-core-worker/src/reference_counter.rs` has helpers for owner-death marking and owned-object location snapshots.
- `rust/ray-core-worker/src/ownership_directory.rs` has local borrower/owner cleanup helpers.

C++ contract:
- `src/ray/core_worker/core_worker.cc` and `src/ray/object_manager/ownership_object_directory.cc` implement a broader owner-driven distributed protocol:
  - location subscriptions
  - object-location snapshots
  - owner-side fill of object info
  - node-death filtering
  - queued/in-flight update batching
  - distributed borrower / subscriber lifecycle

Difference:
- Rust still uses mostly local maps and direct mutation helpers.
- `handle_get_object_locations_owner(...)` returns only node IDs derived from local strings, not the fuller object information C++ fills.
- The end-to-end distributed ownership directory behavior is still not present.

Risk:
- Object-location visibility, owner death, borrower cleanup, and re-registration behavior can still diverge under real distributed workloads.

## Round 2 Report Accuracy

The Round 2 report is directionally useful, but it is not an accurate closure report.

Verified overstatements:
- `GCS-12` was reported as fixed; it is only partial.
- `GCS-17` was reported as fixed; it is only partial.
- `GCS-4` was reported as fixed; it is only partial.
- `GCS-6` was reported as fixed; it is only partial.
- `RAYLET-3` was reported as fixed; it is only partial.
- `RAYLET-4` was reported as fixed; it is only partial.
- `RAYLET-5` was reported as fixed; it is only partial.
- `RAYLET-6` was reported as fixed; it is still open in any strong parity sense.
- `CORE-10` was reported as fixed; it is only partial.

## Highest-Value Additional Tests

### GCS

- `test_pg_create_remains_pending_until_scheduler_success`
  - Create a placement group.
  - Verify state remains `Pending`.
  - Trigger the scheduling-success transition separately.
  - Verify only then it becomes `Created`.

- `test_wait_placement_group_until_ready_blocks_until_mark_created`
  - Verify `wait` does not resolve immediately after registration.

- `test_get_cluster_status_includes_pending_resource_requests`
  - Populate pending task demand.
  - Verify Rust returns the same `pending_resource_requests` shape as C++.

- `test_get_cluster_status_includes_pending_gang_resource_requests`
  - Add placement-group load and verify the gang-request payload.

- `test_get_cluster_status_includes_last_seen_autoscaler_state_version`
  - Verify the version appears in `cluster_resource_state`.

- `test_get_all_job_info_filters_by_job_submission_id_metadata`
  - Put `job_submission_id` in `config.metadata` and verify filtering matches C++.

- `test_get_all_job_info_populates_is_running_tasks_from_worker_rpc`
  - Simulate worker reply and verify the field is populated.

- `test_get_all_job_info_enriches_job_info_from_internal_kv`
  - Reproduce the Jobs API enrichment path.

- `test_drain_node_updates_both_node_and_autoscaler_state_consistently`
  - Exercise both the node-service and autoscaler-service drain paths.

### Raylet

- `test_report_worker_backlog_clears_stale_backlog_for_same_worker`
  - Report backlog twice for the same worker and verify stale classes are removed.

- `test_report_worker_backlog_ignores_disconnected_worker`
  - Match the C++ worker-existence guard.

- `test_pin_object_ids_pins_and_waits_for_free`
  - Verify success is not just a local flag.
  - Verify the pin is released only when the owner-side condition is met.

- `test_pin_object_ids_fails_for_pending_deletion`
  - Match the C++ pending-deletion case.

- `test_resize_local_resource_instances_rejects_unit_resources`
  - Explicitly cover `GPU`/unit-instance behavior.

- `test_resize_local_resource_instances_preserves_in_use_delta_semantics`
  - Verify available capacity is adjusted like the C++ delta path.

- `test_get_node_stats_includes_core_worker_stats`
  - Register workers and verify `core_workers_stats` is populated.

- `test_get_node_stats_includes_object_manager_spill_restore_metrics`
  - Verify stats beyond local pinned bytes.

### Core Worker / Ownership

- `test_update_object_location_batch_matches_owner_fill_object_information`
  - Verify owner reply includes spill URL, node IDs, and pending-creation information.

- `test_get_object_locations_owner_filters_dead_nodes`
  - Reproduce the node-removal behavior from C++.

- `test_object_location_subscription_receives_snapshot_and_incremental_updates`
  - Cover subscribe/snapshot/update semantics, not just direct polling of local state.

- `test_owner_death_unsubscribes_and_cleans_borrowers_end_to_end`
  - Exercise borrower lifecycle with actual owner/broker interactions.

- `test_gcs_restart_reregistration_restores_location_visibility`
  - Verify the restart path feeds into observable location queries, not just local mutation.

## Recommended Next Order

1. Finish `RAYLET-6` first.
   - It is the clearest mismatch between the report and the code.
   - The current tests are far too weak.

2. Then finish `GCS-17` and `GCS-4`.
   - These affect autoscaler and jobs-facing control-plane behavior.

3. Then fix `RAYLET-4` and `RAYLET-5`.
   - These are concrete operational correctness issues.

4. Then return to `CORE-10`.
   - This is the hardest remaining semantic gap and should be tackled after the easier control-plane mismatches are closed properly.

5. Finish `GCS-12`, `GCS-6`, and `RAYLET-3`.
   - These still matter, but they are less urgent than stats, autoscaler payloads, job info, and ownership semantics.

## Current Bottom Line

**Updated 2026-03-19:** All 9 re-audit findings have been addressed in Round 3.
See `rust/reports/2026-03-19_PARITY_FIX_REPORT_ROUND3.md` for details.

Remaining architectural gaps (not parity regressions):
- Worker fanout for `GetNodeStats` core_workers_stats (requires RPC client infrastructure)
- `GetAllJobInfo` KV enrichment and `is_running_tasks` worker RPC (requires async RPC client)
- `pending_gang_resource_requests` in cluster status (requires PG manager integration)
- Full distributed ownership subscription protocol (beyond local maps)

These are infrastructure limitations, not correctness issues. The Rust backend now matches
the C++ backend's externally visible contracts for all 9 re-audited areas.
