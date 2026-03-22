# Claude Fix Plan After Round 2 Re-Audit

Date: 2026-03-18
Scope: still-open or only-partially-fixed parity gaps after the Round 2 fix pass

## Rules

For each issue below:
1. Read the C++ implementation first.
2. Read the C++ tests first.
3. Write the smallest Rust test that would fail if Rust still differs.
4. Run that test and confirm the current failure mode.
5. Fix the Rust implementation.
6. Re-run the targeted tests.
7. Update:
   - `rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md`
   - `rust/reports/2026-03-18_BACKEND_PORT_REAUDIT_ROUND2.md`
   - `rust/reports/2026-03-18_PARITY_FIX_REPORT_ROUND2.md` only if you are explicitly correcting an inaccurate claim

Do not mark an issue fixed just because:
- the code is no longer stubbed
- an existing weak test passes
- the behavior is “good enough” for single-node mode

## Priority Order

1. `RAYLET-6`
2. `GCS-17`
3. `GCS-4`
4. `RAYLET-4`
5. `RAYLET-5`
6. `CORE-10`
7. `GCS-12`
8. `GCS-6`
9. `RAYLET-3`

## 1. RAYLET-6: GetNodeStats Parity

### Read First

- C++:
  - `src/ray/raylet/node_manager.cc`
    - `HandleGetNodeStats`
    - store-stats aggregation helpers
  - `src/ray/raylet/local_object_manager.cc`
  - `src/ray/raylet/local_object_manager.h`

- Rust:
  - `rust/ray-raylet/src/grpc_service.rs`
  - `rust/ray-raylet/src/node_manager.rs`
  - `rust/ray-raylet/src/local_object_manager.rs`

### Problem

Current Rust still returns only:
- `num_workers`
- thin `store_stats`

It does not match the C++ multi-source stats collection path.

### Tests To Add First

- `test_get_node_stats_includes_core_worker_stats`
- `test_get_node_stats_includes_spill_and_restore_metrics`
- `test_get_node_stats_handles_no_worker_replies_without_hanging`
- `test_get_node_stats_reports_memory_info_when_requested`

### Fix Guidance

- Extend the Rust reply to include the richer stats payload, not just pinned-object counters.
- If the Rust architecture cannot yet do full worker fanout, make that limitation explicit and close the contract gap in slices:
  - worker/core-worker stats
  - richer store stats
  - optional memory info
- Strengthen the existing weak tests. The current “stats still exist after draining” test is not parity evidence.

### Acceptance Criteria

- The new tests fail before the fix.
- The reply structure is materially closer to the C++ reply.
- At least one test checks an actual field that used to be absent.

## 2. GCS-17: Cluster Status Payload Parity

### Read First

- C++:
  - `src/ray/gcs/gcs_autoscaler_state_manager.cc`
    - `HandleGetClusterStatus`
    - `MakeClusterResourceStateInternal`

- Rust:
  - `rust/ray-gcs/src/autoscaler_state_manager.rs`
  - `rust/ray-gcs/src/grpc_services.rs`

### Problem

Rust now includes `autoscaling_state` and `cluster_session_name`, but it still omits large parts of `ClusterResourceState` that C++ fills.

### Tests To Add First

- `test_get_cluster_status_includes_last_seen_autoscaler_state_version`
- `test_get_cluster_status_includes_pending_resource_requests`
- `test_get_cluster_status_includes_pending_gang_resource_requests`
- `test_get_cluster_status_includes_cluster_resource_constraints`

### Fix Guidance

- Match the C++ shape produced by `MakeClusterResourceStateInternal`.
- Do not limit the fix to top-level reply fields.
- Add the missing demand / gang-request / constraint population in the Rust autoscaler manager.

### Acceptance Criteria

- The reply contains the same major sections C++ emits.
- Tests verify content, not just field presence.

## 3. GCS-4: GetAllJobInfo Enrichment / Filtering

### Read First

- C++:
  - `src/ray/gcs/gcs_job_manager.cc`
    - `HandleGetAllJobInfo`
  - `src/ray/gcs/tests/gcs_job_manager_test.cc`

- Rust:
  - `rust/ray-gcs/src/job_manager.rs`
  - `rust/ray-gcs/src/grpc_services.rs`

### Problem

Rust added filters, but they still do not match the C++ filter/enrichment contract.

### Tests To Add First

- `test_get_all_job_info_filters_by_submission_id_metadata`
- `test_get_all_job_info_populates_job_info_from_internal_kv`
- `test_get_all_job_info_populates_is_running_tasks_via_worker_rpc`
- `test_get_all_job_info_skip_flags_match_cpp`

### Fix Guidance

- Filter on `config.metadata["job_submission_id"]`, not surrogate fields.
- Reproduce the Jobs API KV enrichment path.
- Reproduce the async `is_running_tasks` resolution path.

### Acceptance Criteria

- Submission-ID filtering behaves like C++.
- The enriched fields are populated from the correct sources.

## 4. RAYLET-4: Object Pinning Semantics

### Read First

- C++:
  - `src/ray/raylet/node_manager.cc`
    - `HandlePinObjectIDs`
  - `src/ray/raylet/tests/node_manager_test.cc`
    - pin-object tests

- Rust:
  - `rust/ray-raylet/src/grpc_service.rs`
  - `rust/ray-raylet/src/local_object_manager.rs`

### Problem

Rust currently checks whether an object is already locally pinned. That is weaker than the C++ pin-and-wait lifetime semantics.

### Tests To Add First

- `test_pin_object_ids_pins_object_and_retains_it`
- `test_pin_object_ids_fails_for_pending_deletion`
- `test_pin_object_ids_uses_owner_address_and_generator_id_path`

### Fix Guidance

- Implement real pinning semantics instead of returning a status based on current pin state.
- Match the C++ pending-deletion behavior.

### Acceptance Criteria

- Success means the object has actually been pinned according to the Rust lifetime model.
- Missing or pending-deletion objects fail.

## 5. RAYLET-5: ResizeLocalResourceInstances

### Read First

- C++:
  - `src/ray/raylet/node_manager.cc`
    - `HandleResizeLocalResourceInstances`
  - `src/ray/raylet/tests/node_manager_test.cc`
    - resize tests

- Rust:
  - `rust/ray-raylet/src/grpc_service.rs`
  - `rust/ray-raylet/src/local_resource_manager.rs`

### Problem

Rust resizes resources, but it still misses important C++ validation and semantics.

### Tests To Add First

- `test_resize_local_resource_instances_rejects_gpu`
- `test_resize_local_resource_instances_preserves_in_use_capacity`
- `test_resize_local_resource_instances_matches_cpp_clamping`

### Fix Guidance

- Add explicit unit-instance resource rejection.
- Match the C++ delta-based update semantics.
- Verify partially allocated resources behave correctly after resize.

### Acceptance Criteria

- The behavior matches the C++ tests on both valid and invalid cases.

## 6. CORE-10: Distributed Ownership Semantics

### Read First

- C++:
  - `src/ray/core_worker/core_worker.cc`
    - `HandleUpdateObjectLocationBatch`
    - `HandleGetObjectLocationsOwner`
  - `src/ray/object_manager/ownership_object_directory.cc`
  - `src/ray/object_manager/tests/ownership_object_directory_test.cc`

- Rust:
  - `rust/ray-core-worker/src/grpc_service.rs`
  - `rust/ray-core-worker/src/reference_counter.rs`
  - `rust/ray-core-worker/src/ownership_directory.rs`

### Problem

Rust now has some local helpers and mixed add/remove/spill handling, but it still does not reproduce the full distributed ownership protocol.

### Tests To Add First

- `test_get_object_locations_owner_returns_full_owner_information`
- `test_update_object_location_batch_filters_dead_nodes`
- `test_object_location_subscription_snapshot_and_incremental_updates`
- `test_owner_death_cleanup_end_to_end`
- `test_gcs_restart_reregistration_restores_location_queries`

### Fix Guidance

- Do not stop at local maps.
- Recreate the owner-facing object-info contract first.
- Then add subscription/update behavior.
- Then add node-death / restart behavior.

### Acceptance Criteria

- `GetObjectLocationsOwner` returns the fuller C++-style information, not just node IDs.
- Subscription/update behavior is observable through tests, not just internal state.

## 7. GCS-12: Placement Group Lifecycle

### Read First

- C++:
  - `src/ray/gcs/gcs_placement_group_manager.cc`
  - `src/ray/gcs/tests/gcs_placement_group_manager_test.cc`

- Rust:
  - `rust/ray-gcs/src/grpc_services.rs`
  - `rust/ray-gcs/src/placement_group_manager.rs`

### Problem

Rust still immediately transitions a newly created placement group to `Created` in the gRPC path.

### Tests To Add First

- `test_pg_create_remains_pending_until_scheduler_success`
- `test_wait_placement_group_until_ready_waits_for_explicit_transition`

### Fix Guidance

- Remove the immediate transition from the request handler.
- Drive the transition through the placement-group manager state machine.

### Acceptance Criteria

- Registration alone does not create readiness.
- `WaitPlacementGroupUntilReady` only resolves after the explicit success transition.

## 8. GCS-6: Drain Node Side Effects

### Read First

- C++:
  - `src/ray/gcs/gcs_node_manager.cc`
  - `src/ray/gcs/gcs_autoscaler_state_manager.cc`

- Rust:
  - `rust/ray-gcs/src/node_manager.rs`
  - `rust/ray-gcs/src/autoscaler_state_manager.rs`
  - `rust/ray-gcs/src/grpc_services.rs`

### Problem

Rust still looks like partial cross-wiring rather than a full drain protocol.

### Tests To Add First

- `test_node_drain_rpc_updates_autoscaler_and_node_manager_state`
- `test_autoscaler_drain_rpc_updates_same_state_as_node_drain`
- `test_drain_node_deadline_is_preserved_consistently`

### Fix Guidance

- Unify the drain state transitions.
- Verify both entry points produce the same observable state.

### Acceptance Criteria

- Both drain RPC surfaces behave consistently.

## 9. RAYLET-3: Worker Backlog Reporting

### Read First

- C++:
  - `src/ray/raylet/node_manager.cc`
    - `HandleReportWorkerBacklog`
  - `src/ray/raylet/tests/node_manager_test.cc`

- Rust:
  - `rust/ray-raylet/src/grpc_service.rs`
  - `rust/ray-raylet/src/node_manager.rs`

### Problem

Rust lost the per-worker backlog semantics.

### Tests To Add First

- `test_report_worker_backlog_clears_previous_worker_backlog`
- `test_report_worker_backlog_ignores_unknown_worker`
- `test_report_worker_backlog_tracks_per_worker_per_class`

### Fix Guidance

- Store backlog per worker, not only per scheduling class.
- Match the C++ clear-then-set behavior.

### Acceptance Criteria

- Disconnect/reconnect and repeated reports do not leave stale backlog state behind.

## Final Validation

After all fixes:

1. Run targeted crate tests for each fixed item.
2. Run:
   - `cargo test -p ray-gcs`
   - `cargo test -p ray-raylet`
   - `cargo test -p ray-core-worker`
3. Re-run the Python parity suite.
4. Re-run the Tier 1 cluster scaffold.

If cargo target locking is a problem, continue using isolated `CARGO_TARGET_DIR` values.
