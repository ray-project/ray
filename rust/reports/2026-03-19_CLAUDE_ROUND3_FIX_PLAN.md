# Claude Fix Plan After Round 3 Re-Audit

Date: 2026-03-19
Scope: only the gaps still visible after the Round 3 pass

## Rules

For each item below:
1. Read the C++ implementation first.
2. Read the C++ tests first.
3. Add a failing Rust test first.
4. Run the test and confirm the current failure.
5. Fix the Rust implementation.
6. Re-run the targeted tests.
7. Update:
   - `rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
   - `rust/reports/2026-03-19_BACKEND_PORT_REAUDIT_ROUND3.md`
   - `rust/reports/2026-03-19_PARITY_FIX_REPORT_ROUND3.md`

Do not mark an item fixed if:
- the reply shape only looks closer
- the new test bypasses the real RPC path
- the implementation only logs context instead of enforcing semantics

## Priority Order

1. GCS-17 remaining gaps
2. RAYLET-3 remaining gap
3. CORE-10 remaining bugs
4. RAYLET-6 remaining gaps
5. GCS-4 remaining gaps
6. RAYLET-4 remaining gaps
7. RAYLET-5 remaining gap

## 1. GCS-17 Remaining Gaps

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/gcs/gcs_autoscaler_state_manager.cc`
    - `HandleGetClusterStatus`
    - `MakeClusterResourceStateInternal`
    - `GetPendingGangResourceRequests`
    - `GetClusterResourceConstraints`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs`
  - `/Users/istoica/src/ray/rust/ray-gcs/src/autoscaler_state_manager.rs`

### Remaining Problems

1. `pending_gang_resource_requests` is still hard-coded empty.
2. `request_cluster_resource_constraint(...)` stores encoded request bytes, but `get_cluster_status(...)` decodes them as `ClusterResourceConstraint`.

### Tests To Add First

- `test_request_cluster_resource_constraint_roundtrips_through_real_rpc`
- `test_get_cluster_status_includes_pending_gang_resource_requests_from_pg_load`

### Fix Guidance

- Fix the encoding mismatch first.
  - Either store only the inner `cluster_resource_constraint` bytes
  - or decode back as `RequestClusterResourceConstraintRequest` before extracting the field
- Then wire placement-group load into the autoscaler cluster-status path so gang requests are populated.

### Acceptance Criteria

- The real RPC path for cluster-resource constraints works end to end.
- `pending_gang_resource_requests` becomes non-empty in the same scenarios C++ reports it.

## 2. RAYLET-3 Remaining Gap

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`
    - `HandleReportWorkerBacklog`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
  - `/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs`
  - `/Users/istoica/src/ray/rust/ray-raylet/src/worker_pool.rs`

### Remaining Problem

Rust now stores backlog per worker, but it still accepts any 28-byte `worker_id` without verifying the worker is registered.

### Tests To Add First

- `test_report_worker_backlog_rejects_unregistered_worker_even_with_valid_length`
- `test_report_worker_backlog_accepts_registered_driver_or_worker`

### Fix Guidance

- Match the C++ validation:
  - check `GetRegisteredWorker`
  - check `GetRegisteredDriver`
  - ignore the report if neither exists

### Acceptance Criteria

- A valid-looking but unknown worker ID does not update backlog state.

## 3. CORE-10 Remaining Bugs

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/core_worker/core_worker.cc`
    - `HandleUpdateObjectLocationBatch`
    - `HandleGetObjectLocationsOwner`
  - `/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs`
  - `/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs`
  - `/Users/istoica/src/ray/rust/ray-core-worker/src/reference_counter.rs`

### Remaining Problems

1. `spilled_node_id` is currently derived from `get_pinned_node()` after spilling.
   - `set_spilled()` clears the pinned node, so this returns empty.
2. `handle_update_object_location_batch(...)` still does not filter dead-node additions like the C++ owner path.
3. The distributed subscription/snapshot protocol is still not implemented end to end.

### Tests To Add First

- `test_get_object_locations_owner_returns_spilled_node_id`
- `test_update_object_location_batch_ignores_dead_node_additions`
- `test_object_location_subscription_snapshot_and_incremental_updates`

### Fix Guidance

- Extend the ownership/reference state so spilled-node identity is preserved explicitly.
- Do not derive spilled-node state from the pinned-node field.
- Add dead-node filtering in the update batch path, matching the C++ owner logic.
- If the subscription protocol cannot be completed in one pass, clearly separate:
  - reply correctness
  - update filtering
  - subscription protocol

### Acceptance Criteria

- `spilled_node_id` is correct after spill.
- Dead-node additions are ignored.
- Subscription behavior is covered by a real end-to-end test or remains explicitly open.

## 4. RAYLET-6 Remaining Gaps

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`
    - `HandleGetNodeStats`
  - `/Users/istoica/src/ray/src/ray/raylet/local_object_manager.cc`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
  - `/Users/istoica/src/ray/rust/ray-raylet/src/node_manager.rs`

### Remaining Problems

1. `core_workers_stats` is still empty.
2. `include_memory_info` is still ignored.
3. The Rust path still does not reproduce the worker fanout behavior from C++.

### Tests To Add First

- `test_get_node_stats_populates_core_workers_stats`
- `test_get_node_stats_propagates_include_memory_info`
- `test_get_node_stats_collects_worker_stats_without_regression`

### Fix Guidance

- Implement the minimum worker/core-worker stats collection path needed to match the C++ observable contract.
- Do not keep tests that assert `core_workers_stats.is_empty()` once the contract is fixed.

### Acceptance Criteria

- `core_workers_stats` is non-empty when workers are present.
- `include_memory_info` is no longer ignored.

## 5. GCS-4 Remaining Gaps

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/gcs/gcs_job_manager.cc`
    - `HandleGetAllJobInfo`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs`
  - `/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs`

### Remaining Problems

1. No Jobs API KV enrichment for `job_info`
2. No worker RPC resolution for `is_running_tasks`

### Tests To Add First

- `test_get_all_job_info_enriches_job_info_from_internal_kv`
- `test_get_all_job_info_resolves_is_running_tasks_from_driver`

### Fix Guidance

- Add the missing enrichment path.
- Add the missing worker/driver RPC or mocked equivalent to populate `is_running_tasks`.

### Acceptance Criteria

- The Rust `GetAllJobInfo` behavior matches the major C++ enrichment steps, not just filtering.

## 6. RAYLET-4 Remaining Gaps

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`
    - `HandlePinObjectIDs`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`
  - `/Users/istoica/src/ray/rust/ray-raylet/src/local_object_manager.rs`

### Remaining Problem

Rust still does not implement real pin-lifetime semantics. It only checks local state and logs owner/generator data.

### Tests To Add First

- `test_pin_object_ids_establishes_pin_lifetime`
- `test_pin_object_ids_releases_pin_only_when_owner_condition_is_met`

### Fix Guidance

- Implement behavior closer to `PinObjectsAndWaitForFree(...)`.
- Do not treat logging of owner/generator context as parity.

### Acceptance Criteria

- Success corresponds to an actual retained pin, not a local boolean lookup.

## 7. RAYLET-5 Remaining Gap

### Read First

- C++:
  - `/Users/istoica/src/ray/src/ray/raylet/node_manager.cc`
    - `HandleResizeLocalResourceInstances`
- Rust:
  - `/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs`

### Remaining Problem

Rust only hard-codes `"GPU"` instead of matching the generic C++ unit-instance-resource check.

### Tests To Add First

- `test_resize_local_resource_instances_rejects_all_unit_instance_resources`

### Fix Guidance

- Replace the hard-coded GPU check with a generic unit-instance-resource classification that matches C++ behavior.

### Acceptance Criteria

- Validation matches the C++ resource-type rule, not just one example resource name.

## Final Validation

After finishing the items above:

1. Run targeted crate tests for each fix.
2. Re-run:
   - `cargo test -p ray-gcs`
   - `cargo test -p ray-raylet`
   - `cargo test -p ray-core-worker`
3. Re-run the Python parity suite and Tier 1 cluster scaffold if any of the changes affect observable runtime behavior.

If cargo target locking is a problem, continue using isolated `CARGO_TARGET_DIR` values.
