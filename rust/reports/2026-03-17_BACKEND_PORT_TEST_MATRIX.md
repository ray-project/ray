# Backend Port Test Matrix

This matrix maps every confirmed audit finding to at least one concrete test.
Statuses:

- `present`: test exists in code
- `added`: test added in this pass
- `spec`: concrete test defined here but not yet implemented in code

Reference audit:

- [`2026-03-17_BACKEND_PORT_REVIEW.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_REVIEW.md)

| finding_id | area | test_name | test_type | status | notes |
|---|---|---|---|---|---|
| GCS-1 | GCS | `test_internal_kv_persists_across_gcs_restart` | integration | spec | must verify Redis-backed restart behavior |
| GCS-2 | GCS | `test_report_job_error_reaches_python_and_dashboard` | integration/e2e | added | Rust GCS now publishes `ReportJobError` to `RAY_ERROR_INFO_CHANNEL`; manager + gRPC Rust tests added |
| GCS-3 | GCS | `test_job_cleanup_on_node_death` | integration | spec | node death should clean affected jobs |
| GCS-4 | GCS | `test_get_all_job_info_matches_cpp_metadata_and_filters` | conformance | spec | compare enriched fields |
| GCS-5 | GCS | `test_unregister_node_preserves_node_death_info` | integration | spec | compare node death metadata |
| GCS-6 | GCS | `test_drain_node_triggers_raylet_shutdown_flow` | integration | spec | autoscaler + raylet |
| GCS-7 | GCS | `test_get_all_node_address_and_liveness_filters` | unit/integration | spec | filtered query parity |
| GCS-8 | GCS | `test_pubsub_long_poll_and_unsubscribe_semantics` | integration | spec | repeated subscribe/unsubscribe |
| GCS-9 | GCS | `test_actor_lineage_reconstruction_rpc` | integration | added | Rust actor manager + gRPC handler now implement lineage-restart requests with stale-request suppression and restart scheduling tests |
| GCS-10 | GCS | `test_report_actor_out_of_scope` | integration | added | Rust actor manager + gRPC handler now implement out-of-scope transition to DEAD with stale-report suppression tests |
| GCS-11 | GCS | `test_worker_info_update_rpcs` | integration | added | Rust worker manager + gRPC handler now implement single-worker lookup, debugger-port updates, paused-thread updates, filters, and count/limit semantics |
| GCS-12 | GCS | `test_placement_group_full_lifecycle` | integration | spec | includes pending->created |
| GCS-13 | GCS | `test_remove_placement_group_persists_removed_state` | integration | spec | state should remain queryable |
| GCS-14 | GCS | `test_wait_placement_group_until_ready_async_semantics` | integration | spec | timeout/readiness behavior |
| GCS-15 | GCS | `test_resource_manager_reporting_and_state` | integration | spec | resources parity |
| GCS-16 | GCS | `test_autoscaler_state_rejects_stale_versions` | integration | spec | stale version handling |
| GCS-17 | GCS | `test_get_cluster_status_parity` | conformance | spec | compare cluster status payload |
| GCS-18 | GCS | `test_autoscaler_drain_node_acceptance_matches_raylet_feedback` | integration | spec | should not always accept |
| RAYLET-1 | Raylet | `test_return_worker_lease_releases_resources` | integration | spec | resource accounting |
| RAYLET-2 | Raylet | `test_request_worker_lease_lifecycle_and_retry` | integration | spec | request queue behavior |
| RAYLET-3 | Raylet | `test_report_worker_backlog_affects_scheduler_state` | integration | spec | backlog signal propagation |
| RAYLET-4 | Raylet | `test_object_pinning_semantics` | integration | spec | pinned objects not evicted |
| RAYLET-5 | Raylet | `test_resize_local_resource_instances` | integration | spec | runtime resource changes |
| RAYLET-6 | Raylet | `test_get_node_stats_parity` | conformance | spec | node stats content |
| RAYLET-7 | Raylet | `test_drain_raylet_accept_reject_semantics` | integration | spec | drain should not always accept |
| RAYLET-8 | Raylet | `test_shutdown_raylet_semantics` | integration | spec | orderly shutdown |
| RAYLET-9 | Raylet | `test_cleanup_rpcs_have_effect` | integration | spec | task/actor cleanup |
| RAYLET-10 | Raylet | `test_dependency_manager_fetch_and_owner_semantics` | integration | spec | wait/get/lease request types |
| RAYLET-11 | Raylet | `test_wait_manager_timeout_and_ordering` | integration | spec | user-visible `ray.wait` |
| RAYLET-12 | Raylet | `test_worker_pool_matching_runtime_env_and_reuse` | integration | spec | worker selection parity |
| RAYLET-13 | Raylet | `test_local_object_manager_spill_restore_owner_subscriptions` | integration | spec | spill/owner semantics |
| CORE-1 | Core Worker | `test_core_worker_remote_integration_paths` | integration | spec | avoid local-store-only success |
| CORE-2 | Core Worker | `test_reference_count_owner_borrower_protocol` | integration | spec | borrower cleanup and ownership |
| CORE-3 | Core Worker | `test_normal_task_submission_parity` | integration | spec | lease/retry semantics |
| CORE-4 | Core Worker | `test_actor_task_submission_restart_cancel_semantics` | integration | spec | actor lifecycle |
| CORE-5 | Core Worker | `test_task_execution_ordering_and_cancellation` | integration | spec | actor task receiver parity |
| CORE-6 | Core Worker | `test_core_worker_rpc_handler_parity` | integration | spec | handlers should not be stubs |
| CORE-7 | Core Worker | `test_dependency_resolution_nonlocal_and_actor_aware` | integration | spec | inlining + actor awareness |
| CORE-8 | Core Worker | `test_future_resolution_via_owner_rpc` | integration | spec | future owner behavior |
| CORE-9 | Core Worker | `test_object_recovery_lineage_and_pinning` | integration | spec | recovery semantics |
| CORE-10 | Core Worker | `test_distributed_ownership_tracking` | integration | added | owner-side `UpdateObjectLocationBatch` now validates recipient worker and applies spilled-location updates; Rust tests added for spill and wrong-recipient handling |
| CORE-11 | Core Worker | `test_direct_actor_transport_fault_injection` | integration | spec | restart/backpressure |
| CORE-12 | Core Worker | `test_actor_manager_named_actor_and_state_updates` | integration | spec | named actor lifecycle |
| OBJECT-1 | Object Manager | `test_object_directory_owner_driven_updates` | integration | spec | owner/pubsub semantics |
| OBJECT-2 | Object Manager | `test_pull_manager_deduplicates_bundle_entries_like_cpp` | unit | added | ignored parity-gap test in Rust |
| OBJECT-2 | Object Manager | `test_pull_manager_priority_quota_cancel_semantics_match_cpp` | integration | spec | port `pull_manager_test.cc` |
| OBJECT-3 | Object Manager | `test_pull_rpc_for_nonlocal_object_defers_and_later_serves` | integration | added | ignored parity-gap test in Rust |
| OBJECT-3 | Object Manager | `test_duplicate_pull_requests_trigger_resend_behavior` | integration | spec | compare resend semantics |
| OBJECT-4 | Object Manager | `test_out_of_order_chunks_do_not_complete_object` | unit | added | ignored parity-gap test in Rust |
| OBJECT-4 | Object Manager | `test_duplicate_chunk_does_not_complete_object` | unit | added | ignored parity-gap test in Rust |
| OBJECT-4 | Object Manager | `test_parity_duplicate_chunk_push_does_not_complete_object` | integration | added | ignored gRPC parity-gap test |
| OBJECT-4 | Object Manager | `test_parity_undersized_chunk_push_does_not_complete_object` | integration | added | ignored gRPC parity-gap test |
| OBJECT-4 | Object Manager | `test_parity_out_of_order_chunk_push_does_not_complete_object` | integration | added | ignored gRPC parity-gap test |
| OBJECT-4 | Object Manager | `test_remote_push_readback_exact_bytes_and_metadata` | integration | spec | end-to-end readback |
| OBJECT-5 | Object Manager | `test_spilled_object_fetch_via_standard_object_manager_path` | integration | spec | spill integration |
| PY-1 | Python | `test_raylet_symbol_parity_smoke` | pytest | added | verified passing after `ray._raylet` compatibility export fix |
| PY-2 | Python | `test_core_worker_ctor_contract_parity` | pytest | added | Python-surface constructor/shim parity checks now pass |
| PY-3 | Python | `test_object_ref_async_contract_parity` | pytest | added | verified passing after adding `future`, `as_future`, `_on_completed`, and constructor compatibility args |
| PY-4 | Python | `test_core_worker_get_accepts_object_refs_and_preserves_error_shape` | pytest | added | current parity file passes the Python-surface coverage now in place |
| PY-5 | Python | `test_gcs_client_async_and_drain_contract_parity` | pytest | added | verified passing after wiring `drain_nodes` and adding async/multi-get compatibility methods |
| PY-6 | Python/RDT | `test_rdt_store_two_mode_behavior` | pytest | added | source-level parity now passes after removing the explicit Python fallback path |
| TIER1-1 | Cluster scaffold | `test_tier1_cluster_startup_smoke` | pytest/integration | added | verified passing with built `_raylet` under `rust/.venv/bin/python` |
| TIER1-1 | Cluster scaffold | `test_tier1_put_get_roundtrip_baseline` | pytest/integration | added | verified passing with built `_raylet` under `rust/.venv/bin/python` |
| TIER1-2 | Cluster scaffold | `test_tier1_object_fetch_wait_timeout_conformance` | pytest/integration | added | now promoted from xfail and verified passing with built `_raylet` |
| TIER1-3 | Cluster scaffold | `test_tier1_actor_task_ordering_restart_cancel_conformance` | pytest/integration | added | verified passing after per-actor serial dispatch fix |
| TIER1-4 | Cluster scaffold | `test_tier1_placement_group_lifecycle_conformance` | pytest/integration | added | verified passing after adding Rust `ray.util` placement-group helpers |
| TIER1-5 | Cluster scaffold | `test_tier1_worker_lease_and_drain_conformance` | pytest/integration | added | now promoted from xfail and verified passing with wired `drain_nodes` RPC |
| PY-1 | Python | `test_raylet_shim_exports_cluster_and_client_entrypoints` | pytest | added | now passing after `ray._raylet` compatibility export fix |
| GCS-12 | Python/GCS | `test_ray_util_placement_group_helpers_present` | pytest | added | now passing after adding Rust placement-group helper surface |
