# Claude Fix Plan: Still-Open Backend Parity Issues

Date: 2026-03-18

Primary references:

- [`2026-03-18_BACKEND_PORT_REAUDIT.md`](/Users/istoica/src/ray/rust/reports/2026-03-18_BACKEND_PORT_REAUDIT.md)
- [`2026-03-18_BACKEND_PORT_REAUDIT_EXEC_SUMMARY.md`](/Users/istoica/src/ray/rust/reports/2026-03-18_BACKEND_PORT_REAUDIT_EXEC_SUMMARY.md)
- [`2026-03-17_BACKEND_PORT_TEST_MATRIX.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md)

## Purpose

This document is a focused execution plan for the issues that still appear open
 after the parity-fix commit `52034c2ab8`.

Claude should use this as a worklist for the remaining parity gaps. The goal is
not to improve code style. The goal is to match the observable C++ behavior.

## Working Rules

Claude should follow these rules for every issue below:

1. Read the corresponding C++ implementation and tests first.
2. Write the smallest Rust test that would fail if the Rust behavior still
   differs.
3. Run that test and confirm the failure mode.
4. Fix the Rust implementation.
5. Rerun the targeted test.
6. Update the tracking reports.

Claude should not mark an issue closed until there is:

- a behavior-level test,
- a passing result,
- and an updated note in the status documents.

## Remaining Open Issues

These are the still-open items that should be addressed next:

1. `GCS-1`: internal KV persistence mismatch
2. `GCS-4`: `GetAllJobInfo` enrichment/filter behavior
3. `GCS-6`: GCS drain-node side effects
4. `GCS-8`: pubsub unsubscribe semantics
5. `GCS-12`: placement-group create lifecycle
6. `GCS-16`: autoscaler version-handling
7. `GCS-17`: cluster-status payload parity
8. `RAYLET-3`: worker backlog reporting integration
9. `RAYLET-4`: object pinning semantics
10. `RAYLET-5`: resize-local-resource-instances semantics
11. `RAYLET-6`: node-stats parity
12. `CORE-10`: distributed ownership semantics end to end

## Recommended Execution Order

Claude should work in this order:

1. `GCS-1`
2. `GCS-8`
3. `GCS-12`
4. `GCS-16`
5. `GCS-17`
6. `GCS-4`
7. `GCS-6`
8. `RAYLET-3`
9. `RAYLET-4`
10. `RAYLET-5`
11. `RAYLET-6`
12. `CORE-10`

Why this order:

- fix persistence and pubsub semantics first because they can invalidate higher
  level assumptions
- then fix placement groups and autoscaler-facing semantics
- then finish the remaining Raylet operational gaps
- finish with the largest remaining ownership/distributed-semantics gap

---

## 1. GCS-1: Internal KV Persistence Mismatch

### Problem

The Rust GCS server still appears to create the internal KV manager with
`InMemoryInternalKV` even when the server storage type is Redis.

Current likely source:

- [`rust/ray-gcs/src/server.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/server.rs)

C++ references Claude should read first:

- [`src/ray/gcs/gcs_server.cc`](/Users/istoica/src/ray/src/ray/gcs/gcs_server.cc)
- [`src/ray/gcs/usage_stats_client.cc`](/Users/istoica/src/ray/src/ray/gcs/usage_stats_client.cc)
- internal KV wiring in C++ GCS startup

### What to verify

- whether internal KV in C++ persists through the configured backend when Redis
  is enabled
- whether the Rust `GcsInternalKVManager` can be backed by Redis or a store
  abstraction instead of a separate in-memory map

### Fix strategy

1. Inspect how `GcsInternalKVManager` is currently constructed.
2. Decide whether to:
   - back it by the main `StoreClient`, or
   - add a Redis-backed internal-KV implementation that matches C++
3. Ensure the Redis-backed path is used when `storage_type == Redis`.
4. Keep the in-memory path only for explicit in-memory mode.

### Tests to add first

- `test_internal_kv_persists_across_gcs_restart_with_redis`
- `test_internal_kv_multi_get_after_restart_with_redis`

Suggested test shape:

1. start a Rust GCS server configured for Redis
2. put KV entries
3. stop and recreate the GCS server
4. verify the entries still exist

### Acceptance criteria

- internal KV data survives restart under Redis
- targeted Rust integration test passes
- test matrix status for `GCS-1` changes from `spec` to `added`

---

## 2. GCS-8: Pubsub Unsubscribe Semantics

### Problem

Rust `handle_unsubscribe_command()` appears to remove the whole subscriber
state, not a specific channel/key subscription.

Current likely source:

- [`rust/ray-gcs/src/pubsub_handler.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/pubsub_handler.rs)
- [`rust/ray-gcs/src/grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)

C++ references Claude should read first:

- [`src/ray/gcs/pubsub_handler.cc`](/Users/istoica/src/ray/src/ray/gcs/pubsub_handler.cc)
- [`src/ray/protobuf/pubsub.proto`](/Users/istoica/src/ray/src/ray/protobuf/pubsub.proto)
- C++ pubsub handler tests

### What to verify

- whether C++ unsubscribe is scoped by channel and key
- how C++ handles multiple subscriptions for a single subscriber
- whether sender/subscriber mappings matter for cleanup

### Fix strategy

1. Change Rust unsubscribe handling from “drop the whole subscriber” to
   channel/key-scoped removal.
2. Preserve the subscriber if it still has other active subscriptions.
3. Update the channel subscriber index only for the affected subscription.
4. Recheck long-poll behavior after partial unsubscribe.

### Tests to add first

- `test_pubsub_unsubscribe_only_removes_target_channel`
- `test_pubsub_unsubscribe_only_removes_target_key`
- `test_pubsub_long_poll_still_receives_other_subscriptions_after_partial_unsubscribe`

### Acceptance criteria

- unsubscribing from one channel/key does not remove unrelated subscriptions
- long-poll behavior matches the expected C++ contract afterward

---

## 3. GCS-12: Placement-Group Create Lifecycle

### Problem

Rust placement-group creation still appears to set the PG state to `Created`
immediately in the gRPC layer.

Current likely source:

- [`rust/ray-gcs/src/grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)
- [`rust/ray-gcs/src/placement_group_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/placement_group_manager.rs)

C++ references Claude should read first:

- [`src/ray/gcs/gcs_placement_group_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/gcs_placement_group_manager.cc)
- placement-group manager tests in C++

### What to verify

- what initial PG state C++ uses
- when the state transitions to `Created`
- how waiters are notified
- how `REMOVED` and `PENDING` states interact

### Fix strategy

1. Remove the immediate `Created` assignment from the gRPC layer.
2. Make creation go through the manager’s proper initial state.
3. Transition to `Created` only when the corresponding ready condition is met.
4. Ensure `WaitPlacementGroupUntilReady` still works with the corrected state
   machine.

### Tests to add first

- `test_pg_create_starts_pending_not_created`
- `test_pg_wait_until_ready_transitions_from_pending`
- `test_pg_removed_before_ready_does_not_appear_created`

### Acceptance criteria

- newly created PGs do not appear `Created` synchronously unless the C++ path
  truly does so
- wait semantics and `REMOVED` semantics remain correct

---

## 4. GCS-16: Autoscaler Version-Handling

### Problem

Rust autoscaler state handling still appears to store reported state directly,
and the gRPC layer appears to pass version `0`.

Current likely sources:

- [`rust/ray-gcs/src/autoscaler_state_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/autoscaler_state_manager.rs)
- [`rust/ray-gcs/src/grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)

C++ references Claude should read first:

- [`src/ray/gcs/gcs_autoscaler_state_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/gcs_autoscaler_state_manager.cc)
- autoscaler state tests in C++

### What to verify

- whether C++ ignores stale autoscaler versions
- whether C++ preserves last-seen version ordering
- which field in the protobuf carries the relevant version

### Fix strategy

1. Thread the real autoscaler state version from gRPC into the manager.
2. Reject or ignore stale versions according to the C++ contract.
3. Add tests for increasing and decreasing version sequences.

### Tests to add first

- `test_report_autoscaling_state_rejects_stale_versions`
- `test_report_autoscaling_state_accepts_monotonic_versions`
- `test_report_autoscaling_state_preserves_latest_state_only`

### Acceptance criteria

- stale state does not overwrite newer state
- version behavior matches C++ ordering rules

---

## 5. GCS-17: Cluster-Status Payload Parity

### Problem

Rust `GetClusterStatus` still appears to return a much thinner payload than C++.

Current likely source:

- [`rust/ray-gcs/src/grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)
- [`rust/ray-gcs/src/autoscaler_state_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/autoscaler_state_manager.rs)

C++ references Claude should read first:

- [`src/ray/gcs/gcs_autoscaler_state_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/gcs_autoscaler_state_manager.cc)
- autoscaler proto definitions

### What to verify

- exact payload fields included by C++
- whether cluster status includes more than cluster resource state
- whether versioning, idle nodes, drain state, and autoscaling state are
  included

### Fix strategy

1. Enumerate the exact C++ payload fields and semantics.
2. Compare the Rust reply field-by-field.
3. Add the missing fields and populate them from the appropriate managers.

### Tests to add first

- `test_get_cluster_status_payload_matches_cpp_shape`
- `test_get_cluster_status_includes_drain_and_idle_information`
- `test_get_cluster_status_reflects_latest_autoscaling_state`

### Acceptance criteria

- Rust cluster status is not just a minimal resource-state snapshot
- field presence and meaning match C++

---

## 6. GCS-4: GetAllJobInfo Enrichment / Filters

### Problem

Rust `GetAllJobInfo` still appears to return raw cached `JobTableData` only,
without the richer C++ enrichment/filter behavior.

Current likely source:

- [`rust/ray-gcs/src/job_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs)
- corresponding gRPC handler in [`rust/ray-gcs/src/grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)

C++ references Claude should read first:

- [`src/ray/gcs/gcs_job_manager.cc`](/Users/istoica/src/ray/src/ray/gcs/gcs_job_manager.cc)
- C++ GCS job manager tests

### What to verify

- which fields C++ enriches before returning job info
- what filters are applied
- whether running/dead state and driver reachability influence the returned
  payload

### Fix strategy

1. Extract the exact C++ reply-building logic.
2. Port the missing enrichment/filter behavior into Rust.
3. Do not just add fields blindly; match the C++ rules.

### Tests to add first

- `test_get_all_job_info_matches_cpp_metadata_and_filters`
- `test_get_all_job_info_running_vs_dead_state_parity`
- `test_get_all_job_info_driver_metadata_parity`

### Acceptance criteria

- the Rust reply matches the C++ reply for the same job state inputs

---

## 7. GCS-6: GCS Drain-Node Side Effects

### Problem

Rust GCS node drain still appears to only mark local drain state, which is much
weaker than the C++ drain flow.

Current likely source:

- [`rust/ray-gcs/src/node_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/node_manager.rs)
- possibly autoscaler and raylet wiring

C++ references Claude should read first:

- node manager and autoscaler drain flow in C++

### What to verify

- how C++ propagates drain intent from GCS to raylet
- what persistence and callback semantics exist
- when drain is just bookkeeping versus when it triggers operational behavior

### Fix strategy

1. Trace the full C++ control flow.
2. Identify the missing Rust side effects.
3. Add the missing coordination, not just extra bookkeeping.

### Tests to add first

- `test_gcs_drain_node_triggers_raylet_side_effects`
- `test_gcs_drain_node_persists_and_propagates_state`

### Acceptance criteria

- drain from the GCS side produces the same visible cluster behavior as C++

---

## 8. RAYLET-3: Worker Backlog Reporting

### Problem

Rust `ReportWorkerBacklog` still appears acknowledged-only.

Current likely source:

- [`rust/ray-raylet/src/grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)

C++ references Claude should read first:

- [`src/ray/raylet/node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)
- node manager backlog tests

### What to verify

- what scheduler/autoscaler-visible state C++ updates when backlog is reported
- whether backlog is stored per worker, per scheduling class, or both

### Fix strategy

1. Port the minimum necessary backlog state update path.
2. Wire it into the same consumers that rely on it in C++.

### Tests to add first

- `test_report_worker_backlog_changes_scheduler_state`
- `test_report_worker_backlog_updates_existing_entry`
- `test_report_worker_backlog_removal_or_zeroing_matches_cpp`

### Acceptance criteria

- reporting backlog changes real raylet-visible state rather than only returning
  success

---

## 9. RAYLET-4: PinObjectIDs Semantics

### Problem

Rust still appears to return blanket success for `PinObjectIDs`.

Current likely source:

- [`rust/ray-raylet/src/grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)

C++ references Claude should read first:

- [`src/ray/raylet/node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)
- C++ node manager tests for pinning

### What to verify

- when C++ returns success/failure
- idempotency semantics
- behavior for nonexistent objects

### Fix strategy

1. Port the actual pinning validation logic.
2. Preserve reply shape and per-object result semantics.

### Tests to add first

- `test_pin_object_ids_fails_for_missing_objects`
- `test_pin_object_ids_matches_cpp_idempotency`
- `test_pin_object_ids_partial_success_behavior`

### Acceptance criteria

- Rust does not blanket-return success anymore
- reply semantics match C++

---

## 10. RAYLET-5: ResizeLocalResourceInstances

### Problem

Rust still appears to return default for resource resizing.

Current likely source:

- [`rust/ray-raylet/src/grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)

C++ references Claude should read first:

- [`src/ray/raylet/node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)
- C++ resource-resize tests

### What to verify

- validation rules
- clamping behavior
- side effects on the local resource manager

### Fix strategy

1. Port validation and mutation logic into the Rust raylet.
2. Ensure the local scheduler/resource manager observes the resized instances.

### Tests to add first

- `test_resize_local_resource_instances_successful`
- `test_resize_local_resource_instances_invalid_argument`
- `test_resize_local_resource_instances_clamps_like_cpp`

### Acceptance criteria

- resizing changes actual resource state
- invalid inputs are rejected like C++

---

## 11. RAYLET-6: GetNodeStats

### Problem

Rust node stats still appear drastically weaker than C++.

Current likely source:

- [`rust/ray-raylet/src/grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)

C++ references Claude should read first:

- [`src/ray/raylet/node_manager.cc`](/Users/istoica/src/ray/src/ray/raylet/node_manager.cc)
- local object manager stats path
- C++ node stats tests

### What to verify

- which fields C++ includes
- whether it aggregates store, worker, and memory information
- timeout/collection semantics

### Fix strategy

1. Enumerate the fields currently missing from Rust.
2. Wire the necessary sources into the Rust reply.
3. Match the reply shape closely enough that downstream code sees equivalent
   data.

### Tests to add first

- `test_get_node_stats_contains_worker_and_store_detail`
- `test_get_node_stats_matches_cpp_required_fields`
- `test_get_node_stats_timeout_or_partial_collection_semantics`

### Acceptance criteria

- Rust `GetNodeStats` is no longer just `num_workers`

---

## 12. CORE-10: Distributed Ownership Semantics End To End

### Problem

Rust ownership handling is still only partially fixed. The ownership directory
still looks much more local than the full C++ owner-driven distributed
ownership/location-update protocol.

Current likely sources:

- [`rust/ray-core-worker/src/ownership_directory.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs)
- [`rust/ray-core-worker/src/grpc_service.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/grpc_service.rs)
- [`rust/ray-core-worker/src/reference_counter.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/reference_counter.rs)

C++ references Claude should read first:

- [`src/ray/object_manager/ownership_object_directory.cc`](/Users/istoica/src/ray/src/ray/object_manager/ownership_object_directory.cc)
- [`src/ray/core_worker/reference_counter.cc`](/Users/istoica/src/ray/src/ray/core_worker/reference_counter.cc)
- owner-side RPC handling in C++

### What to verify

- who owns location truth
- how borrowers are registered and cleaned up
- how owner death is propagated
- how add/remove/spill location batches are applied
- whether the Rust changes are still too local to match C++

### Fix strategy

Claude should split this into sub-issues:

1. owner location add/remove/spill batch parity
2. borrower registration and cleanup parity
3. owner-death propagation parity
4. re-registration / restart parity

Claude should not try to “solve ownership” in one opaque patch. Close each
sub-issue with tests.

### Tests to add first

- `test_owner_death_propagates_object_location_failure`
- `test_borrower_cleanup_updates_owner_state`
- `test_object_location_batch_add_remove_spill_matches_cpp`
- `test_owner_reregistration_restores_known_locations`

### Acceptance criteria

- at least the above four distributed-ownership tests pass
- the status report explicitly says which pieces of ownership parity are now
  fixed and which remain open

---

## After Each Issue

After Claude fixes each issue, Claude should do all of the following:

1. update [`2026-03-17_BACKEND_PORT_TEST_MATRIX.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md)
2. update [`2026-03-18_BACKEND_PORT_STATUS_REPORT.md`](/Users/istoica/src/ray/rust/reports/2026-03-18_BACKEND_PORT_STATUS_REPORT.md)
3. update [`2026-03-18_BACKEND_PORT_REAUDIT.md`](/Users/istoica/src/ray/rust/reports/2026-03-18_BACKEND_PORT_REAUDIT.md)
4. record:
   - exact files changed
   - exact tests added
   - exact test commands run
   - whether the finding is now `fixed` or only `partial`

## Final Validation Sweep

After the still-open issues above are addressed, Claude should run:

- Python parity tests
- Tier 1 cluster scaffold
- targeted `ray-gcs` tests
- targeted `ray-raylet` tests
- targeted `ray-core-worker` tests
- targeted Object Manager tests

If cargo locking remains a problem, Claude should use isolated
`CARGO_TARGET_DIR`s consistently.
