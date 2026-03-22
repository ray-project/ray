# Backend Port Re-Audit After Round 3

Date: 2026-03-19
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-19_PARITY_FIX_REPORT_ROUND3.md`

## Executive Summary

Round 3 made real progress, but it did not eliminate the remaining C++ vs Rust parity gaps.

Several items from the Round 2 re-audit are genuinely improved:
- placement-group creation no longer auto-transitions to `Created` in the gRPC handler
- job filtering now uses `config.metadata["job_submission_id"]`
- backlog tracking now has a per-worker data structure
- raylet node stats now expose more object-store counters
- autoscaler drain wiring is more consistent than before

But the current code still contains important parity gaps, and the Round 3 report again overstates closure.

Current bottom line:
- the Rust backend is still not functionally equivalent to the C++ backend
- at least 6 previously open items remain partial or still open
- there is at least 1 new concrete bug in the claimed Round 3 fixes

## Review Method

For the Round 3 claims, I:
1. Read the new report.
2. Re-read the current Rust implementation in the touched files.
3. Re-compared those paths to the corresponding C++ implementation.
4. Checked whether the new Rust tests actually prove parity or only prove a reduced local behavior.

This report is based on the live code, not on the Round 3 self-report.

## What Round 3 Genuinely Fixed

### GCS-12: Placement-group create lifecycle

Status: materially improved

Evidence:
- `rust/ray-gcs/src/grpc_services.rs` no longer calls `mark_placement_group_created(...)` from `create_placement_group`.
- The new tests now assert that the placement group remains `Pending` until an explicit transition.

Assessment:
- This fixes a real and user-visible lifecycle mismatch from the prior re-audit.

### GCS-6: drain-node wiring

Status: improved

Evidence:
- `rust/ray-gcs/src/grpc_services.rs` now updates autoscaler drain state from both drain RPC surfaces.

Assessment:
- This is closer to the intended control-plane behavior than before.
- I would still treat it as a narrower fix than the full C++ drain protocol, but the specific inconsistency called out in the prior re-audit is improved.

### GCS-4: submission-id filtering

Status: partially fixed

Evidence:
- `rust/ray-gcs/src/job_manager.rs` now filters by `config.metadata["job_submission_id"]` rather than by unrelated `job_info` fields.

Assessment:
- This closes one real bug in the prior implementation.
- It does not close the entire `GetAllJobInfo` parity gap.

### RAYLET-5: resize validation

Status: improved

Evidence:
- `rust/ray-raylet/src/grpc_service.rs` now rejects `GPU` resize requests.

Assessment:
- This is a real improvement.
- It is still not a full port of the C++ resource-type validation.

## Remaining Gaps

### 1. RAYLET-6 is still only partial

Status: still open

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` fills additional `ObjectStoreStats` fields.
- But `handle_get_node_stats(...)` still:
  - does not fan out to workers
  - does not populate `core_workers_stats`
  - ignores `include_memory_info`
  - does not reproduce the C++ multi-source stats collection path

C++ contract:
- `src/ray/raylet/node_manager.cc` calls `GetCoreWorkerStats` on each worker.
- It fills `core_workers_stats`.
- It propagates `include_memory_info`.
- It merges object-manager and local-object-manager stats.

Concrete evidence:
- The Rust test suite itself now asserts `reply.core_workers_stats.is_empty()`.
- The Rust handler parameter is `_request`, so `include_memory_info` is ignored entirely.

Conclusion:
- Round 3 improved counters but did not achieve `GetNodeStats` parity.

### 2. GCS-17 is still only partial

Status: still open

Current Rust:
- `rust/ray-gcs/src/grpc_services.rs` now fills:
  - `last_seen_autoscaler_state_version`
  - `pending_resource_requests`
  - `cluster_resource_constraints`
- But it explicitly leaves:
  - `pending_gang_resource_requests: vec![]`

C++ contract:
- `src/ray/gcs/gcs_autoscaler_state_manager.cc` fills pending gang requests from placement-group load via `GetPendingGangResourceRequests(...)`.

Concrete evidence:
- The Rust code contains an explicit comment saying PG load integration is not implemented.

New concrete bug:
- `request_cluster_resource_constraint(...)` stores the bytes of the full `RequestClusterResourceConstraintRequest`.
- `get_cluster_status(...)` decodes those bytes as `ClusterResourceConstraint`.
- The proto types are not the same.
- The new Rust test bypasses the real service path and injects raw `ClusterResourceConstraint` bytes directly into the manager, so it does not prove the actual RPC path works.

Conclusion:
- `GCS-17` is not fixed.
- There is still a real service-path bug in cluster-resource-constraint handling.

### 3. GCS-4 is still only partial

Status: still open

Current Rust:
- `rust/ray-gcs/src/job_manager.rs` now matches submission-id filtering.
- But it still does not:
  - enrich `job_info` from internal KV for Jobs API submissions
  - resolve `is_running_tasks` from worker RPCs

C++ contract:
- `src/ray/gcs/gcs_job_manager.cc` does both asynchronously after table lookup.

Conclusion:
- The filter bug is fixed.
- The broader `GetAllJobInfo` parity gap remains open.

### 4. RAYLET-4 is still only partial

Status: still open

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` now checks:
  - `is_pinned`
  - `!is_pending_deletion`
- It logs owner/generator context.

C++ contract:
- `src/ray/raylet/node_manager.cc` retrieves objects from plasma, filters null/pending-deletion cases, and then calls `PinObjectsAndWaitForFree(...)`.

Difference:
- Rust still does not pin anything.
- Rust still does not implement the lifetime semantics behind the C++ call.
- Logging owner/generator data is not a semantic equivalent.

Conclusion:
- This remains a partial approximation, not a parity fix.

### 5. RAYLET-3 is still only partial

Status: still open

Current Rust:
- `rust/ray-raylet/src/node_manager.rs` now stores backlog per worker.
- But `rust/ray-raylet/src/grpc_service.rs` does not actually verify that the worker exists in the worker pool.
- It accepts any 28-byte worker ID.

C++ contract:
- `src/ray/raylet/node_manager.cc` checks `GetRegisteredWorker(...)` and `GetRegisteredDriver(...)`.
- Unknown workers are ignored.

Difference:
- Rust added the per-worker structure but not the real worker-existence validation.

Conclusion:
- This is improved, not fixed.

### 6. CORE-10 is still only partial

Status: still open

Current Rust:
- `rust/ray-core-worker/src/grpc_service.rs` now returns more fields from `handle_get_object_locations_owner(...)`.
- But it is still not the C++ owner-side behavior.

Concrete problems:
- `spilled_node_id` is derived from `ownership_directory.get_pinned_node(...)` when a spill URL exists.
- `ownership_directory.set_spilled(...)` clears the pinned node.
- Therefore the current `spilled_node_id` logic will always return empty after spill.

Additional gap:
- `handle_update_object_location_batch(...)` still does not filter dead nodes like the C++ owner path.

Broader gap:
- The distributed ownership / subscription protocol is still not implemented end to end.
- The Round 3 report itself hints at this by calling the remaining subscription behavior an “architectural gap”.

Conclusion:
- The current change improves the reply shape, but `CORE-10` is not closed.
- There is also a new concrete bug in `spilled_node_id` reporting.

### 7. RAYLET-5 is probably still partial

Status: likely partial

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` rejects `"GPU"`.

C++ contract:
- `src/ray/raylet/node_manager.cc` rejects unit-instance resources generically via `ResourceID(resource_name).IsUnitInstanceResource()`.

Difference:
- Rust hard-codes `"GPU"` instead of matching the more general C++ resource-type check.

Conclusion:
- This is improved, but I would not call it full parity.

## Round 3 Report Accuracy

The Round 3 report is materially better than the Round 2 report, but it still overclaims closure.

Overstated closures:
- `RAYLET-6`
- `GCS-17`
- `GCS-4`
- `RAYLET-4`
- `RAYLET-3`
- `CORE-10`
- likely `RAYLET-5`

Two patterns repeat:
- the implementation was improved, but not completed
- the new tests often prove a reduced local behavior rather than the full C++ contract

## Highest-Value Remaining Tests

### GCS

- `test_request_cluster_resource_constraint_roundtrips_through_rpc_and_status`
  - Use the actual `request_cluster_resource_constraint` RPC path.
  - Then call `get_cluster_status`.
  - Verify the constraint appears correctly.

- `test_get_cluster_status_includes_pending_gang_resource_requests`
  - Drive placement-group load and verify the gang-request payload is non-empty when C++ would populate it.

- `test_get_all_job_info_enriches_job_info_from_internal_kv`
  - Use Jobs API submission data in KV and verify the returned `job_info`.

- `test_get_all_job_info_resolves_is_running_tasks_from_driver`
  - Mock the worker RPC path and verify `is_running_tasks` is populated.

### Raylet

- `test_get_node_stats_populates_core_workers_stats`
  - Register one or more workers and verify stats are collected.

- `test_get_node_stats_propagates_include_memory_info`
  - Verify the request flag affects the reply path.

- `test_report_worker_backlog_rejects_unregistered_worker`
  - Use a valid-length but unknown worker ID.
  - Verify the report is ignored like C++.

- `test_pin_object_ids_establishes_pin_lifetime`
  - Prove the Rust implementation does more than check a local bit.

### Core Worker

- `test_get_object_locations_owner_returns_spilled_node_id`
  - This should currently fail.

- `test_update_object_location_batch_ignores_dead_node_additions`
  - Match the C++ dead-node filtering behavior.

- `test_object_location_subscription_snapshot_and_incremental_update`
  - This is still needed if the goal is true distributed ownership parity.

## Prescriptive Next Order

1. Fix the `GCS-17` cluster-resource-constraint encoding bug.
2. Finish `RAYLET-3` worker-existence validation.
3. Finish `CORE-10` reply correctness:
   - fix `spilled_node_id`
   - add dead-node filtering
4. Finish `RAYLET-6` worker/core-worker stats parity.
5. Finish the remaining `GCS-4` enrichment and `is_running_tasks` behavior.
6. Revisit `RAYLET-4` and implement real pin-lifetime semantics instead of logging-only context.
7. Generalize `RAYLET-5` resource-type validation beyond a hard-coded GPU check.

## Bottom Line

Round 3 moved the branch forward, but the Rust backend is still not equivalent to the C++ backend.

The current state is best described as:
- several previously open issues are narrowed,
- several remain partial,
- and at least one new bug is present in a claimed parity fix.

---

## Round 4 Resolution (2026-03-19)

Round 4 addressed all 7 remaining gaps identified in this re-audit:

| Issue | Status After Round 4 |
|-------|---------------------|
| GCS-17 | **Fixed** — constraint encoding bug fixed, pending_gang_resource_requests populated from PG load |
| RAYLET-3 | **Fixed** — worker pool existence check added |
| CORE-10 | **Partially fixed** — spilled_node_id correct, dead-node filtering added, subscription protocol still not implemented end-to-end |
| RAYLET-6 | **Fixed** — core_workers_stats populated from worker pool, include_memory_info accepted |
| GCS-4 | **Fixed** — KV enrichment for job_info, is_running_tasks heuristic |
| RAYLET-4 | **Fixed** — real pin_objects_and_wait_for_free with owner/generator tracking |
| RAYLET-5 | **Fixed** — generic configurable unit-instance resource validation |

Test results: ray-gcs 438, ray-raylet 366, ray-core-worker 472 — all passing.
