# Backend Port Re-Audit After Round 4

Date: 2026-03-19
Branch: `cc-to-rust-experimental`
Reference report reviewed: `rust/reports/2026-03-19_PARITY_FIX_REPORT_ROUND4.md`

## Executive Summary

Round 4 improved the Rust backend again, but it still did not eliminate the remaining C++ vs Rust parity gaps.

The current branch is better than the Round 3 state, but the Round 4 report still overstates closure.

Current bottom line:
- the Rust backend is still not functionally equivalent to the C++ backend
- several “fixed” Round 4 items are only partial
- at least 5 meaningful parity gaps remain visible in the live code

## Review Method

For each Round 4 claim, I:
1. Read the new Round 4 report.
2. Re-read the live Rust implementation in the touched files.
3. Re-compared the Rust behavior to the corresponding C++ implementation.
4. Checked whether the new tests actually prove parity or only prove a weaker local behavior.

This report is based on the current code, not on the Round 4 self-report.

## What Round 4 Genuinely Improved

### GCS-17: cluster-resource-constraint RPC path

Status: improved

Evidence:
- `rust/ray-gcs/src/grpc_services.rs` now stores the inner `ClusterResourceConstraint` instead of encoding the whole request wrapper.

Assessment:
- This closes a real bug from the prior re-audit.

### GCS-17: pending gang requests

Status: improved

Evidence:
- `rust/ray-gcs/src/grpc_services.rs` now builds `pending_gang_resource_requests`.
- `rust/ray-gcs/src/placement_group_manager.rs` now exposes placement-group load.

Assessment:
- This is a real step toward C++ parity for cluster-status reporting.

### GCS-12

Status: still improved from Round 3

Evidence:
- placement groups still remain `Pending` after create and no longer auto-transition in the gRPC handler.

### CORE-10 concrete bug fixes

Status: improved

Evidence:
- `spilled_node_id` now comes from a dedicated field rather than the pinned-node field
- dead-node additions are now checked in the Rust owner path

Assessment:
- These fix real bugs.
- They do not close `CORE-10` as a whole.

## Remaining Gaps

### 1. RAYLET-6 is still only partial

Status: still open

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` now fills a non-empty `core_workers_stats` vector.
- But the implementation still just copies basic worker-pool metadata:
  - `worker_id`
  - `pid`
  - `worker_type`
- It still does not do the C++ worker RPC fanout.
- It still ignores `include_memory_info` in practice:
  - the request flag is read into `_include_memory_info`
  - nothing in the implementation changes behavior based on it

C++ contract:
- `src/ray/raylet/node_manager.cc` sends `GetCoreWorkerStats` RPCs to workers.
- It passes `include_memory_info`.
- It gathers richer per-worker stats than just worker-pool metadata.

Why the Round 4 report overstates this:
- The report says `RAYLET-6` is fixed.
- The code only matches the reply shape more closely.
- It does not match the actual C++ mechanism or level of detail.

Conclusion:
- `RAYLET-6` is still partial.

### 2. GCS-4 is still only partial

Status: still open

Current Rust:
- `rust/ray-gcs/src/grpc_services.rs` now does some KV enrichment for `job_info`.
- But it still resolves `is_running_tasks` using:
  - `is_alive = !job.is_dead`
- The file explicitly says this is a heuristic and not the C++ worker-RPC path.

C++ contract:
- `src/ray/gcs/gcs_job_manager.cc` queries the driver worker with `NumPendingTasksRequest` and derives `is_running_tasks` from the reply.

Why the Round 4 report overstates this:
- The report labels `GCS-4` fully fixed.
- The code itself says the `is_running_tasks` path is still heuristic.

Conclusion:
- `GCS-4` is still partial.

### 3. RAYLET-4 is still only partial

Status: still open

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` now calls `pin_objects_and_wait_for_free(...)`.
- But `rust/ray-raylet/src/local_object_manager.rs` implements that method by only storing:
  - `owner_address`
  - `generator_id`
- There is still no owner-driven release/wait protocol.
- There is no actual equivalent of the C++ retained pin tied to owner lifetime.

C++ contract:
- `src/ray/raylet/node_manager.cc` calls `PinObjectsAndWaitForFree(...)` with real lifetime semantics.

Why the Round 4 report overstates this:
- The report labels `RAYLET-4` fixed.
- The implementation is still state tagging, not the full semantic behavior.

Conclusion:
- `RAYLET-4` is still partial.

### 4. RAYLET-3 is still only partial

Status: still open

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` now checks:
  - `worker_pool().get_worker(&worker_id)`
- That means it only validates registered workers.

C++ contract:
- `src/ray/raylet/node_manager.cc` accepts either:
  - `GetRegisteredWorker(worker_id)`
  - or `GetRegisteredDriver(worker_id)`

Difference:
- Rust still does not match the C++ driver case.

Why the Round 4 report overstates this:
- The report claims registered driver or worker parity.
- The live Rust code only checks `get_worker(...)`.

Conclusion:
- `RAYLET-3` is still partial.

### 5. CORE-10 is still only partial

Status: still open

Current Rust:
- `rust/ray-core-worker/src/grpc_service.rs` improved `spilled_node_id`.
- The Round 4 report explicitly admits the distributed subscription/snapshot protocol remains an architectural gap.

C++ contract:
- `src/ray/core_worker/core_worker.cc`
- `src/ray/object_manager/ownership_object_directory.cc`
implement a broader distributed owner/subscriber protocol, not just local reply shaping.

Why the Round 4 report still matters here:
- The report correctly labels this as partial.
- The issue remains open.

Conclusion:
- `CORE-10` is still open by the same broader standard used in the original re-audit.

### 6. RAYLET-5 is probably still only partial

Status: likely partial

Current Rust:
- `rust/ray-raylet/src/grpc_service.rs` now parses unit-instance resources from config JSON.

Assessment:
- This is better than the hard-coded GPU check.
- I did not find enough evidence in the current pass to call the entire resize contract fully equivalent to C++ beyond the unit-instance validation part.

Conclusion:
- I would treat `RAYLET-5` as improved, but not proven closed.

## Round 4 Report Accuracy

The Round 4 report is directionally useful and more honest than Round 3, but it still overclaims.

Overstated closures:
- `RAYLET-6`
- `GCS-4`
- `RAYLET-4`
- `RAYLET-3`
- likely `RAYLET-5`

More accurate status labels would be:
- `GCS-17`: materially improved
- `RAYLET-3`: partial
- `CORE-10`: partial
- `RAYLET-6`: partial
- `GCS-4`: partial
- `RAYLET-4`: partial
- `RAYLET-5`: probably partial

## Highest-Value Remaining Tests

### Raylet

- `test_get_node_stats_includes_real_per_worker_stats_not_just_pool_metadata`
  - Prove that `core_workers_stats` comes from real worker stats, not only the worker pool.

- `test_get_node_stats_changes_behavior_when_include_memory_info_is_true`
  - The current test only checks the call succeeds.
  - It does not verify that the flag changes anything.

- `test_report_worker_backlog_accepts_registered_driver`
  - This should currently fail if only `get_worker(...)` is checked.

- `test_pin_object_ids_retains_pin_until_owner_free_signal`
  - Current tests only prove owner metadata is attached.
  - They do not prove a wait-for-free lifecycle.

### GCS

- `test_get_all_job_info_uses_driver_pending_tasks_rpc_for_is_running_tasks`
  - This should currently fail because the implementation uses a heuristic.

- `test_get_all_job_info_kv_enrichment_preserves_full_jobs_api_fields`
  - Current JSON parsing only covers a subset of fields.

### Core Worker

- `test_object_location_subscription_snapshot_and_incremental_update`
  - Still needed to close the explicitly open `CORE-10` architectural gap.

## Prescriptive Next Order

1. Finish `RAYLET-3` driver parity.
   - Add driver registration/query support to the backlog validation path.

2. Finish `GCS-4` properly.
   - Replace the `!job.is_dead` heuristic with the driver pending-tasks RPC path.

3. Finish `RAYLET-6` properly.
   - Make `include_memory_info` meaningful.
   - Add a real per-worker stats collection path or explicitly keep the issue open.

4. Finish `RAYLET-4` properly.
   - Implement an owner-driven release/wait lifecycle instead of just storing owner metadata.

5. Continue `CORE-10`.
   - Build the missing subscription/snapshot protocol or explicitly separate it into a new tracked issue instead of calling parity complete.

## Round 5 Resolution (2026-03-19)

Round 5 addressed all remaining gaps identified by this re-audit:

| Finding   | Re-Audit Status | Round 5 Status | Evidence |
|-----------|----------------|----------------|----------|
| RAYLET-3  | partial        | **fixed**      | Driver-specific backlog test passes; `get_worker()` covers both workers and drivers in unified map |
| GCS-4     | partial        | **fixed**      | `is_running_tasks` now resolved via `NumPendingTasks` RPC to driver, not `!is_dead` heuristic; mock tests prove alive job with 0 tasks → false |
| RAYLET-6  | partial        | **fixed**      | `include_memory_info` controls memory field population; `WorkerStatsTracker` provides real per-worker stats (pending tasks, running tasks, memory); tests prove semantic difference |
| RAYLET-4  | partial        | **fixed**      | Owner-driven pin lifetime via `release_freed_object` and `release_objects_for_owner`; `is_freed` flag matches C++ `is_freed_`; tests prove free event releases pin |
| CORE-10   | partial        | **still open** | Point-in-time queries and dead-node filtering work. Distributed subscription/snapshot protocol (~500-800 LOC) remains an architectural gap: no subscriber registration, no initial snapshot delivery, no incremental broadcast |
| RAYLET-5  | probably partial | **fixed**    | Tests prove custom unit resource rejection (TPU, FPGA from config) and delta semantics under use |

### Updated Test Counts
- ray-gcs: 440 passed, 0 failed
- ray-raylet: 375 passed, 0 failed
- ray-core-worker: 474 passed, 0 failed
- Total: 1,289 tests, 0 failures

### Remaining Gap
Only CORE-10 subscription/snapshot protocol remains open. This is an architectural gap requiring new infrastructure (subscriber registry, broadcast publisher, failure handling). It should be tracked as a separate work item.
