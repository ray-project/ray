# Backend Port Re-Audit: Current `cc-to-rust-experimental` State

Date: 2026-03-18

Reference materials:

- [`2026-03-17_BACKEND_PORT_REVIEW.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_REVIEW.md)
- [`2026-03-17_BACKEND_PORT_TEST_MATRIX.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_TEST_MATRIX.md)
- [`2026-03-18_BACKEND_PORT_STATUS_REPORT.md`](/Users/istoica/src/ray/rust/reports/2026-03-18_BACKEND_PORT_STATUS_REPORT.md)
- [`2026-03-18_PARITY_FIX_REPORT.md`](/Users/istoica/src/ray/rust/reports/2026-03-18_PARITY_FIX_REPORT.md)

Primary branch state reviewed:

- `HEAD = 52034c2ab8` (`Close 20 C++/Rust parity gaps: ownership, raylet, object-mgr, GCS`)

## Scope

This report updates the earlier backend audit after Claude’s large parity-fix
commit. It is not a full from-scratch re-review of every single backend line.
It is a focused re-audit of:

- the areas explicitly changed by `52034c2ab8`,
- the highest-risk findings from the original audit,
- and the remaining areas most likely to still block functional equivalence.

## Bottom Line

The Rust backend is still not functionally equivalent to the original C++
backend.

However, the branch is materially better than it was at the time of the
original audit. Several explicit stubs and reduced RPCs have now been replaced
with real behavior, and some earlier findings should now be reclassified from
`open` to `fixed` or `partially fixed`.

The current state is:

- multiple previously confirmed gaps are now genuinely narrowed,
- Python/native compatibility remains much better than before,
- Tier 1 scaffold tests remain useful and green,
- but several important control-plane, autoscaler, Raylet, and distributed
  ownership/data-plane semantics still diverge from C++.

## Review Method

This re-audit used four inputs:

1. The original March 17 audit findings as a checklist.
2. The actual code touched by `52034c2ab8`.
3. The current Rust code in the affected subsystems.
4. The corresponding C++ implementation and proto contracts for comparison.

The review standard remains the same:

- implementation style may differ,
- but observable behavior must match the C++ contract to count as equivalent.

## Commit Delta Reviewed

The parity-fix commit touched these main areas:

- Core Worker:
  - [`reference_counter.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/reference_counter.rs)
  - [`ownership_directory.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs)
  - [`future_resolver.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/future_resolver.rs)
  - [`object_recovery_manager.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/object_recovery_manager.rs)
  - submitter code
- Raylet:
  - [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
  - [`wait_manager.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/wait_manager.rs)
- Object Manager:
  - [`object_buffer_pool.rs`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_buffer_pool.rs)
  - [`object_manager.rs`](/Users/istoica/src/ray/rust/ray-object-manager/src/object_manager.rs)
  - [`pull_manager.rs`](/Users/istoica/src/ray/rust/ray-object-manager/src/pull_manager.rs)
- GCS:
  - [`job_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs)
  - [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/node_manager.rs)
  - [`placement_group_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/placement_group_manager.rs)
  - [`worker_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/worker_manager.rs)
  - [`actor_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/actor_manager.rs)
  - [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)

## Updated Finding Status

### Findings that now look genuinely improved

These findings appear materially narrowed relative to the original audit:

- `GCS-2`: `ReportJobError`
- `GCS-3`: job cleanup on node death
- `GCS-5`: unregister-node death info preservation
- `GCS-7`: `GetAllNodeAddressAndLiveness` filtering/limit support
- `GCS-9`: lineage-reconstruction actor restart RPC
- `GCS-10`: `ReportActorOutOfScope`
- `GCS-11`: worker lookup/update RPCs
- `GCS-13`: placement-group removal persisting `REMOVED`
- `GCS-14`: async wait for PG readiness
- `GCS-18`: autoscaler `DrainNode` validation
- `RAYLET-1`: `ReturnWorkerLease`
- `RAYLET-7`: `DrainRaylet` rejection path
- `RAYLET-9`: cleanup RPCs
- `RAYLET-11`: `WaitManager` timeout/validation
- `OBJECT-2`: duplicate pull-bundle ID handling
- `OBJECT-3`: deferred push support
- `OBJECT-4`: chunk validation path
- `OBJECT-5`: spill path integration
- part of `CORE-8`: timeout behavior in future resolution
- part of `CORE-9`: lineage recovery integration
- part of `CORE-10`: owner-side location update handling

These should not automatically be treated as fully closed, but they are no
longer in the same clearly stubbed state they were in during the original
audit.

### Findings that still appear open from current source inspection

The following still appear open or only partially addressed.

#### GCS-1: Internal KV persistence mismatch still appears open

Evidence:

- [`server.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/server.rs) still creates
  `GcsInternalKVManager` using `InMemoryInternalKV::new()` even when
  `storage_type == Redis`.

Impact:

- Internal KV behavior still appears to diverge from C++ persistence semantics
  across GCS restart.

Status:

- still open

#### GCS-4: `GetAllJobInfo` still appears materially weaker than C++

Evidence:

- [`job_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/job_manager.rs)
  still returns cached `JobTableData` with only optional limit handling.
- No obvious equivalent of the C++ enrichment/filter behavior from the original
  audit is visible in the current Rust path.

Impact:

- Jobs API semantics still likely diverge for metadata enrichment and filtering.

Status:

- still open

#### GCS-6: GCS node drain side effects still appear weaker than C++

Evidence:

- [`node_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/node_manager.rs)
  `handle_drain_node()` still just marks the node as draining if alive.

Impact:

- This is still much weaker than the broader C++ drain flow and does not look
  like a full behavioral match.

Status:

- still open

#### GCS-8: Pubsub unsubscribe semantics still appear different

Evidence:

- [`pubsub_handler.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/pubsub_handler.rs)
  `handle_unsubscribe_command()` removes the entire subscriber state, not a
  channel/key-scoped subscription.

Impact:

- This still appears behaviorally different from the more granular C++ pubsub
  command handling.

Status:

- still open

#### GCS-12: Placement-group creation still collapses to immediate `Created`

Evidence:

- [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)
  still sets PG state to `Created` during `create_placement_group`.

Impact:

- This still appears to bypass the richer C++ PG lifecycle.

Status:

- still open

#### GCS-16: Autoscaler version-handling still appears weak

Evidence:

- [`autoscaler_state_manager.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/autoscaler_state_manager.rs)
  `handle_report_autoscaling_state()` still writes the state directly and stores
  the version.
- [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)
  still passes `0` into `handle_report_autoscaling_state()` in
  `report_autoscaling_state`.

Impact:

- Stale-version rejection behavior still does not appear equivalent to C++.

Status:

- still open

#### GCS-17: `GetClusterStatus` still appears thinner than C++

Evidence:

- [`grpc_services.rs`](/Users/istoica/src/ray/rust/ray-gcs/src/grpc_services.rs)
  `get_cluster_status()` appears to return a simplified `cluster_resource_state`
  snapshot only.

Impact:

- Cluster status payload likely still diverges from the C++ autoscaler-facing
  contract.

Status:

- still open

#### RAYLET-3: `ReportWorkerBacklog` still appears acknowledged-only

Evidence:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
  still logs and returns success, with a comment saying integration is deferred.

Impact:

- Scheduler/autoscaler-visible backlog semantics still likely diverge.

Status:

- still open

#### RAYLET-4: `PinObjectIDs` still appears to return blanket success

Evidence:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
  still returns success for all objects with a comment noting real pinning
  requires deeper integration.

Impact:

- Object pinning semantics still appear non-equivalent.

Status:

- still open

#### RAYLET-5: `ResizeLocalResourceInstances` still appears stubbed

Evidence:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
  still returns default with no visible resource validation or mutation.

Impact:

- Resource-resize behavior still appears non-equivalent.

Status:

- still open

#### RAYLET-6: `GetNodeStats` still appears drastically weaker than C++

Evidence:

- [`grpc_service.rs`](/Users/istoica/src/ray/rust/ray-raylet/src/grpc_service.rs)
  still returns essentially `num_workers` only.

Impact:

- Observability and stats semantics still likely diverge significantly.

Status:

- still open

#### CORE-10: Ownership remains only partially fixed

Evidence:

- [`ownership_directory.rs`](/Users/istoica/src/ray/rust/ray-core-worker/src/ownership_directory.rs)
  is still fundamentally a local map of owned and borrowed objects.
- This remains much weaker than the full owner-driven distributed protocol used
  by C++.

Impact:

- Distributed ownership, owner-death handling, and location propagation are
  still likely not equivalent end to end.

Status:

- partial only

### Findings still likely needing careful re-review even if improved

These are not clearly still open from the quick source scan, but the new code
should not be trusted without deeper verification:

- `RAYLET-1`
- `RAYLET-7`
- `RAYLET-9`
- `RAYLET-11`
- `OBJECT-2`
- `OBJECT-3`
- `OBJECT-4`
- `OBJECT-5`
- `CORE-2`
- `CORE-3`
- `CORE-4`
- `CORE-8`
- `CORE-9`
- `GCS-3`
- `GCS-5`
- `GCS-7`
- `GCS-13`
- `GCS-14`
- `GCS-18`

These areas moved from “obvious stub” to “needs behavioral verification.”

## Current Functional Equivalence Assessment

### Control plane

Better than before, but not equivalent.

Reason:

- several explicit GCS stubs are gone,
- but persistence, autoscaler versioning, cluster status richness, and some
  drain/pubsub/placement-group semantics still differ.

### Raylet behavior

Still not equivalent.

Reason:

- some lease/drain/cleanup behavior improved,
- but backlog, pinning, resource resizing, and stats are still visibly weaker.

### Core Worker semantics

Still not equivalent.

Reason:

- some recovery/future/ownership slices improved,
- but distributed ownership and several deeper task/actor semantics remain
  insufficiently matched.

### Object Manager

Likely improved, but not yet proven equivalent.

Reason:

- chunk validation and deferred push behavior improved,
- but end-to-end equivalence still needs more active, non-ignored conformance
  tests.

### Python/native surface

Much closer than before.

Reason:

- Python parity and Tier 1 scaffold tests remain strong signals of improvement,
- but they do not prove backend equivalence by themselves.

## Additional Tests Recommended

These are the highest-value tests to add now.

### GCS

- `test_internal_kv_persists_across_gcs_restart_with_redis`
- `test_get_all_job_info_matches_cpp_metadata_and_filters`
- `test_gcs_drain_node_triggers_raylet_side_effects`
- `test_pubsub_unsubscribe_is_channel_and_key_scoped`
- `test_pg_create_starts_pending_not_created`
- `test_report_autoscaling_state_rejects_stale_versions`
- `test_get_cluster_status_payload_matches_cpp_shape`

### Raylet

- `test_report_worker_backlog_changes_scheduler_state`
- `test_pin_object_ids_fails_for_missing_or_unpinnable_objects`
- `test_resize_local_resource_instances_matches_cpp_validation_and_clamping`
- `test_get_node_stats_contains_store_worker_and_memory_detail`

### Core Worker / Ownership

- `test_owner_death_propagates_object_location_failure`
- `test_borrower_cleanup_updates_owner_state`
- `test_object_location_batch_add_remove_spill_matches_cpp`
- `test_dynamic_return_location_update_matches_cpp`

### Object Manager

- `test_remote_push_readback_exact_bytes_and_metadata`
- `test_spilled_object_fetch_via_standard_path`
- `test_duplicate_pull_requests_trigger_expected_resend_behavior`
- `test_out_of_order_chunk_receive_matches_cpp_completion_rules`

## Detailed Plan To Address Remaining Differences

### Phase 1: Reclassify every original finding against current code

For each finding from the original audit:

1. mark as `fixed`, `partial`, `open`, or `needs deeper verification`
2. attach current file/line references
3. attach current test coverage

This should update the test matrix and status report.

### Phase 2: Finish the remaining GCS parity gaps

Order:

1. `GCS-1`
2. `GCS-4`
3. `GCS-6`
4. `GCS-8`
5. `GCS-12`
6. `GCS-16`
7. `GCS-17`

For each:

1. read the C++ implementation and test
2. add a failing Rust parity test
3. fix the Rust implementation
4. rerun targeted tests

### Phase 3: Finish the remaining Raylet parity gaps

Order:

1. `RAYLET-3`
2. `RAYLET-4`
3. `RAYLET-5`
4. `RAYLET-6`
5. re-verify `RAYLET-1/7/9/11`

### Phase 4: Finish distributed ownership parity

Focus:

- complete `CORE-10`
- re-review `CORE-2`, `CORE-8`, `CORE-9`
- verify actual distributed behavior, not only local bookkeeping

### Phase 5: Re-verify Object Manager end-to-end

Focus:

- convert parity-gap tests from ignored/spec to active pass/fail where runtime
  behavior is now fixed
- add exact-byte end-to-end transfer and spill tests

### Phase 6: Broader regression sweep

Run:

- Python parity tests
- Tier 1 scaffold
- targeted `ray-gcs` tests
- targeted `ray-raylet` tests
- targeted `ray-core-worker` tests
- targeted Object Manager tests

Use isolated `CARGO_TARGET_DIR`s if cargo locking remains a problem.

## Conclusion

This branch is in a significantly better state than the one reviewed on
2026-03-17.

But the current evidence still does not support calling the Rust backend
functionally equivalent to the C++ backend.

The correct updated conclusion is:

- several important high-severity gaps have been narrowed or fixed,
- several others remain open,
- and a number of formerly stubbed areas now need deeper behavioral
  verification rather than blanket trust.
