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
| GCS-4 | GCS | `test_get_all_job_info_matches_cpp_metadata_and_filters` | conformance | added | filters by config.metadata["job_submission_id"] (Round 3) |
| GCS-4 | GCS | `test_get_all_job_info_filters_by_submission_id_metadata` | unit | added | Round 3: C++ filter parity |
| GCS-4 | GCS | `test_get_all_job_info_skip_flags_match_cpp` | unit | added | Round 3: skip flags parity |
| GCS-5 | GCS | `test_unregister_node_preserves_node_death_info` | integration | spec | compare node death metadata |
| GCS-6 | GCS | `test_drain_node_triggers_raylet_shutdown_flow` | integration | spec | autoscaler + raylet (legacy NodeInfo path — uses ShutdownRaylet) |
| GCS-6 | GCS | `test_autoscaler_drain_rpc_updates_same_state_as_node_drain` | unit | added | Round 3: both drain surfaces consistent |
| GCS-6 | GCS | `test_drain_node_deadline_is_preserved_consistently` | unit | added | Round 3: deadline preservation |
| GCS-6 | GCS | `test_set_node_draining_fires_listeners_and_stores_request` | unit | added | Round 11: C++ SetNodeDraining parity — full request + listeners |
| GCS-6 | GCS | `test_set_node_draining_non_alive_is_noop` | unit | added | Round 11: C++ SetNodeDraining no-op for removed node |
| GCS-6 | GCS | `test_set_node_draining_overwrite_fires_listener_again` | unit | added | Round 11: C++ overwrite behavior |
| GCS-6 | GCS | `test_gcs_drain_node_dead_node_accepted` | unit | added | Round 11: C++ dead node → is_accepted=true |
| GCS-6 | GCS | `test_gcs_drain_node_cross_manager_side_effects_match_cpp` | unit | added | Round 11: full request stored + listeners fire |
| GCS-6 | GCS | `test_gcs_drain_node_matches_autoscaler_and_node_manager_state_transitions` | unit | added | Round 11: both managers consistent + cleanup on death |
| GCS-6 | GCS | `test_gcs_drain_node_runtime_observable_effects_match_cpp` | unit | added | Round 11: negative deadline, dead/unknown/alive/overwrite all match C++ |
| GCS-6 | GCS | `test_autoscaler_drain_node_not_alive_accepted` | unit | added | Round 11: C++ parity (was _rejected) |
| GCS-6 | GCS | `test_autoscaler_drain_node_negative_deadline_rejected` | unit | added | Round 11: C++ parity negative deadline |
| GCS-6 | GCS | `test_autoscaler_drain_updates_resource_manager_in_live_server_wiring` | unit | added | GCS-6 R2: live listener → resource_manager |
| GCS-6 | GCS | `test_get_draining_nodes_reflects_autoscaler_drain_without_test_local_listener` | unit | added | GCS-6 R2: listener mechanism, not ad-hoc |
| GCS-6 | GCS | `test_autoscaler_drain_busy_node_rejected_by_raylet` | unit | added | GCS-6 R2: raylet rejection path |
| GCS-6 | GCS | `test_autoscaler_drain_idle_node_accepted_by_raylet` | unit | added | GCS-6 R2: raylet acceptance path |
| GCS-6 | GCS | `test_autoscaler_drain_rejection_reason_propagated` | unit | added | GCS-6 R2: rejection reason pass-through |
| GCS-6 | GCS | `test_autoscaler_drain_state_committed_only_after_raylet_accepts` | unit | added | GCS-6 R2: no state commit on rejection |
| GCS-6 | GCS | `test_autoscaler_drain_marks_node_actors_preempted` | unit | added | GCS-6 R2: SetPreemptedAndPublish parity |
| GCS-6 | GCS | `test_autoscaler_drain_publishes_actor_preemption_state` | unit | added | GCS-6 R2: actor preemption published via pubsub |
| GCS-6 | GCS | `test_preempted_actor_state_persisted_to_actor_table` | unit | added | GCS-6 R3: preempted persisted to actor table storage |
| GCS-6 | GCS | `test_node_preemption_increments_num_restarts_due_to_node_preemption` | unit | added | GCS-6 R3: preemption restart increments counter |
| GCS-6 | GCS | `test_node_preemption_restart_does_not_consume_regular_restart_budget` | unit | added | GCS-6 R3: preemption restarts excluded from max_restarts budget |
| GCS-6 | GCS | `test_preempted_actor_state_survives_gcs_restart_until_consumed` | unit | added | GCS-6 R3: preempted state survives GCS restart via storage |
| GCS-6 | GCS | `test_on_actor_creation_success_persists_preempted_reset` | unit | added | GCS-6 R4: real creation-success path persists preempted=false to storage |
| GCS-6 | GCS | `test_preempted_reset_survives_gcs_restart_after_creation_success` | unit | added | GCS-6 R4: preempted=false survives GCS restart after creation success |
| GCS-6 | GCS | `test_real_creation_success_path_clears_preempted_in_storage_not_just_memory` | unit | added | GCS-6 R4: full preempt→kill→restart→creation-success flow persists correctly |
| GCS-6 | GCS | `test_preempted_publish_occurs_only_after_actor_table_persist` | unit | added | GCS-6 R5: proves preempt publication is ordered after persistence (C++ Put callback parity) |
| GCS-6 | GCS | `test_creation_success_publish_occurs_only_after_actor_table_persist` | unit | added | GCS-6 R5: proves ALIVE publication is ordered after persistence (C++ Put callback parity) |
| GCS-6 | GCS | `test_pubsub_observer_cannot_see_unpersisted_actor_state` | unit | added | GCS-6 R5: proves pubsub observers only see state already present in storage |
| GCS-6 | GCS | `test_creation_success_callbacks_fire_only_after_actor_table_persist` | unit | added | GCS-6 R6: creation callbacks gated on persistence (C++ Put callback parity) |
| GCS-6 | GCS | `test_creation_success_callbacks_do_not_run_while_persist_blocked` | unit | added | GCS-6 R6: callbacks remain pending while persistence is blocked |
| GCS-6 | GCS | `test_creation_success_callbacks_are_ordered_after_publication` | unit | added | GCS-6 R6: callbacks fire after publication (C++ persist→publish→callback ordering) |
| GCS-6 | GCS | `test_actor_export_event_emitted_on_creation_success_when_enabled` | unit | added | GCS-6 R8: export event emitted on ALIVE transition when enabled |
| GCS-6 | GCS | `test_actor_export_event_emitted_on_restart_when_enabled` | unit | added | GCS-6 R8: export event emitted on RESTARTING transition when enabled |
| GCS-6 | GCS | `test_actor_export_event_emitted_on_death_when_enabled` | unit | added | GCS-6 R8: export event emitted on DEAD transition when enabled |
| GCS-6 | GCS | `test_actor_export_event_not_emitted_when_disabled` | unit | added | GCS-6 R8: no events when export disabled (default) |
| GCS-6 | GCS | `test_actor_export_event_emitted_to_file_when_export_api_enabled` | unit | added | GCS-6 R9: file output at export_events/event_EXPORT_ACTOR.log with valid JSON |
| GCS-6 | GCS | `test_actor_export_event_honors_export_api_write_config_actor_only` | unit | added | GCS-6 R9: selective EXPORT_ACTOR config enables file output; EXPORT_TASK does not |
| GCS-6 | GCS | `test_actor_export_event_payload_matches_expected_fields` | unit | added | GCS-6 R9: all 16 ExportActorData proto fields in file output |
| GCS-6 | GCS | `test_actor_ray_event_path_emits_when_enable_ray_event_is_enabled` | unit | added | GCS-6 R9: aggregator path buffers events; file export bypassed |
| GCS-6 | GCS | `test_actor_ray_event_path_uses_real_sink_when_enabled` | unit | added | GCS-6 R10: real sink attached, events delivered through flush |
| GCS-6 | GCS | `test_actor_ray_event_path_flushes_through_live_runtime` | unit | added | GCS-6 R10: periodic flush loop drains buffer through sink |
| GCS-6 | GCS | `test_actor_ray_event_path_is_not_just_buffered_in_memory` | unit | added | GCS-6 R10: proves events leave memory via sink, not just sit in buffer |
| GCS-6 | GCS | `test_actor_ray_event_path_disabled_means_no_sink_activity` | unit | added | GCS-6 R10: disabled path has no sink, no buffer, no flush output |
| GCS-6 | GCS | `test_actor_ray_event_path_uses_cpp_equivalent_output_channel` | unit | added | GCS-6 R11: EventAggregatorSink writes AddEventsRequest JSON with RayEvent protos |
| GCS-6 | GCS | `test_actor_ray_event_path_preserves_expected_structured_event_delivery` | unit | added | GCS-6 R11: ActorLifecycleEvent with state_transitions, source_type=GCS, event_type=10 |
| GCS-6 | GCS | `test_actor_ray_event_path_not_just_tracing_logs` | unit | added | GCS-6 R11: output is AddEventsRequest JSON file, not plain log lines |
| GCS-6 | GCS | `test_actor_ray_event_path_disabled_means_no_output_channel_activity` | unit | added | GCS-6 R11: disabled path creates no ray_events dir or output file |
| GCS-7 | GCS | `test_get_all_node_address_and_liveness_filters` | unit/integration | spec | filtered query parity |
| GCS-8 | GCS | `test_pubsub_long_poll_and_unsubscribe_semantics` | integration | spec | repeated subscribe/unsubscribe |
| GCS-9 | GCS | `test_actor_lineage_reconstruction_rpc` | integration | added | Rust actor manager + gRPC handler now implement lineage-restart requests with stale-request suppression and restart scheduling tests |
| GCS-10 | GCS | `test_report_actor_out_of_scope` | integration | added | Rust actor manager + gRPC handler now implement out-of-scope transition to DEAD with stale-report suppression tests |
| GCS-11 | GCS | `test_worker_info_update_rpcs` | integration | added | Rust worker manager + gRPC handler now implement single-worker lookup, debugger-port updates, paused-thread updates, filters, and count/limit semantics |
| GCS-12 | GCS | `test_placement_group_full_lifecycle` | integration | spec | includes pending->created |
| GCS-12 | GCS | `test_pg_create_remains_pending_until_scheduler_success` | unit | added | Round 3: no auto-transition |
| GCS-12 | GCS | `test_wait_placement_group_until_ready_waits_for_explicit_transition` | unit | added | Round 3: wait blocks until explicit mark |
| GCS-13 | GCS | `test_remove_placement_group_persists_removed_state` | integration | spec | state should remain queryable |
| GCS-14 | GCS | `test_wait_placement_group_until_ready_async_semantics` | integration | spec | timeout/readiness behavior |
| GCS-15 | GCS | `test_resource_manager_reporting_and_state` | integration | spec | resources parity |
| GCS-16 | GCS | `test_autoscaler_state_rejects_stale_versions` | integration | spec | stale version handling |
| GCS-17 | GCS | `test_get_cluster_status_parity` | conformance | added | Round 3: full payload parity |
| GCS-17 | GCS | `test_get_cluster_status_includes_last_seen_autoscaler_state_version` | unit | added | Round 3 |
| GCS-17 | GCS | `test_get_cluster_status_includes_pending_resource_requests` | unit | added | Round 3 |
| GCS-17 | GCS | `test_get_cluster_status_includes_cluster_resource_constraints` | unit | added | Round 3 |
| GCS-18 | GCS | `test_autoscaler_drain_node_acceptance_matches_raylet_feedback` | integration | spec | should not always accept |
| RAYLET-1 | Raylet | `test_return_worker_lease_releases_resources` | integration | spec | resource accounting |
| RAYLET-2 | Raylet | `test_request_worker_lease_lifecycle_and_retry` | integration | spec | request queue behavior |
| RAYLET-3 | Raylet | `test_report_worker_backlog_affects_scheduler_state` | integration | added | Round 3: per-worker clear-then-set |
| RAYLET-3 | Raylet | `test_report_worker_backlog_clears_previous_worker_backlog` | unit | added | Round 3 |
| RAYLET-3 | Raylet | `test_report_worker_backlog_ignores_unknown_worker` | unit | added | Round 3 |
| RAYLET-3 | Raylet | `test_report_worker_backlog_tracks_per_worker_per_class` | unit | added | Round 3 |
| RAYLET-4 | Raylet | `test_object_pinning_semantics` | integration | added | Round 3: pending-deletion + owner context |
| RAYLET-4 | Raylet | `test_pin_object_ids_fails_for_pending_deletion` | unit | added | Round 3 |
| RAYLET-4 | Raylet | `test_pin_object_ids_uses_owner_address_and_generator_id_path` | unit | added | Round 3 |
| RAYLET-5 | Raylet | `test_resize_local_resource_instances` | integration | added | Round 3: GPU rejection + clamping |
| RAYLET-5 | Raylet | `test_resize_local_resource_instances_rejects_gpu` | unit | added | Round 3 |
| RAYLET-5 | Raylet | `test_resize_local_resource_instances_preserves_in_use_capacity` | unit | added | Round 3 |
| RAYLET-5 | Raylet | `test_resize_local_resource_instances_matches_cpp_clamping` | unit | added | Round 3 |
| RAYLET-6 | Raylet | `test_get_node_stats_parity` | conformance | added | Round 3: full spill/restore metrics |
| RAYLET-6 | Raylet | `test_get_node_stats_includes_spill_and_restore_metrics` | unit | added | Round 3 |
| RAYLET-6 | Raylet | `test_get_node_stats_handles_no_worker_replies_without_hanging` | unit | added | Round 3 |
| RAYLET-6 | Raylet | `test_get_node_stats_reports_memory_info_when_requested` | unit | added | Round 3 |
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
| CORE-10 | Core Worker | `test_get_object_locations_owner_returns_full_owner_information` | unit | added | Round 3: full WorkerObjectLocationsPubMessage |
| CORE-10 | Core Worker | `test_update_object_location_batch_filters_dead_nodes` | unit | added | Round 3: add/remove semantics |
| CORE-10 | Core Worker | `test_owner_death_cleanup_end_to_end` | unit | added | Round 3: owner death propagation |
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
| GCS-17 | GCS | `test_request_cluster_resource_constraint_roundtrips_through_real_rpc` | unit | added | Round 4: real RPC path roundtrip (catches encoding bug) |
| GCS-17 | GCS | `test_get_cluster_status_includes_pending_gang_resource_requests_from_pg_load` | unit | added | Round 4: PG load → gang resource requests |
| RAYLET-3 | Raylet | `test_report_worker_backlog_rejects_unregistered_worker_even_with_valid_length` | unit | added | Round 4: worker pool existence check |
| RAYLET-3 | Raylet | `test_report_worker_backlog_accepts_registered_driver_or_worker` | unit | added | Round 4: registered worker accepted |
| CORE-10 | Core Worker | `test_get_object_locations_owner_returns_spilled_node_id` | unit | added | Round 4: spilled_node_id correct after spill |
| CORE-10 | Core Worker | `test_update_object_location_batch_ignores_dead_node_additions` | unit | added | Round 4: dead-node filtering |
| RAYLET-6 | Raylet | `test_get_node_stats_populates_core_workers_stats` | unit | added | Round 4: non-empty core_workers_stats |
| RAYLET-6 | Raylet | `test_get_node_stats_propagates_include_memory_info` | unit | added | Round 4: include_memory_info accepted |
| RAYLET-6 | Raylet | `test_get_node_stats_collects_worker_stats_without_regression` | unit | added | Round 4: store stats + worker stats together |
| RAYLET-4 | Raylet | `test_pin_object_ids_establishes_pin_lifetime` | unit | added | Round 4: real pin retention with owner |
| RAYLET-4 | Raylet | `test_pin_object_ids_releases_pin_only_when_owner_condition_is_met` | unit | added | Round 4: pin release on owner free |
| RAYLET-5 | Raylet | `test_resize_local_resource_instances_rejects_all_unit_instance_resources` | unit | added | Round 4: generic resource-type validation |
| RAYLET-5 | Raylet | `test_parse_unit_instance_resources_default` | unit | added | Round 4: config parsing default |
| RAYLET-5 | Raylet | `test_parse_unit_instance_resources_custom_config` | unit | added | Round 4: config parsing custom |
| RAYLET-5 | Raylet | `test_parse_unit_instance_resources_with_custom_resources` | unit | added | Round 4: config parsing custom_unit |
| RAYLET-3 | Raylet | `test_report_worker_backlog_accepts_registered_driver` | unit | added | Round 5: proves driver WorkerType accepted |
| RAYLET-3 | Raylet | `test_report_worker_backlog_rejects_unknown_driver_id` | unit | added | Round 5: proves unknown driver ID rejected |
| GCS-4 | GCS | `test_get_all_job_info_alive_job_with_no_pending_tasks_reports_false` | unit | added | Round 5: real NumPendingTasks RPC, not heuristic |
| GCS-4 | GCS | `test_get_all_job_info_uses_driver_pending_tasks_rpc_for_is_running_tasks` | unit | added | Round 5: proves alive job with tasks → true |
| RAYLET-6 | Raylet | `test_get_node_stats_include_memory_info_changes_worker_stats_payload` | unit | added | Round 5: include_memory_info has semantic effect |
| RAYLET-6 | Raylet | `test_get_node_stats_core_worker_stats_include_nontrivial_fields` | unit | added | Round 5: real task counts in worker stats |
| RAYLET-4 | Raylet | `test_pin_object_ids_keeps_pin_until_owner_free_event` | unit | added | Round 5: owner-driven pin lifecycle |
| RAYLET-4 | Raylet | `test_pin_object_ids_owner_free_event_releases_retained_pin` | unit | added | Round 5: per-object free releases pin |
| RAYLET-4 | Raylet | `test_pin_objects_owner_death_releases_all_pins` | unit | added | Round 5: owner death releases all pins |
| CORE-10 | Core Worker | `test_object_location_point_in_time_query_works` | unit | added | Round 5: documents working point-in-time query |
| CORE-10 | Core Worker | `test_object_location_subscription_receives_initial_snapshot` | unit | added | Round 8: subscribe triggers initial snapshot with current locations |
| CORE-10 | Core Worker | `test_object_location_subscription_receives_incremental_add_remove_updates` | unit | added | Round 8: ADD/REMOVE updates published to subscribers |
| CORE-10 | Core Worker | `test_object_location_subscription_owner_death_notifies_subscriber` | unit | added | Round 8: owner death stops message delivery |
| CORE-10 | Core Worker | `test_object_location_unsubscribe_stops_updates` | unit | added | Round 8: unsubscribe stops incremental updates |
| RAYLET-5 | Raylet | `test_resize_local_resource_instances_matches_cpp_for_custom_unit_resources` | unit | added | Round 5: custom unit resource rejection |
| RAYLET-5 | Raylet | `test_resize_local_resource_instances_matches_cpp_delta_semantics_under_use` | unit | added | Round 5: delta semantics with in-use resources |
| GCS-4 | GCS | `test_get_all_job_info_limit_zero_returns_zero_jobs` | unit | added | Round 6: limit=0 returns 0 jobs |
| GCS-4 | GCS | `test_get_all_job_info_negative_limit_is_invalid` | unit | added | Round 6: negative limit returns error |
| GCS-4 | GCS | `test_get_all_job_info_enriches_full_jobs_api_info_from_internal_kv` | unit | added | Round 6: all 15 JobsApiInfo fields from KV JSON |
| RAYLET-4 | Raylet | `test_pin_object_ids_owner_eviction_signal_releases_pin_end_to_end` | unit | added | Round 6: eviction via protocol path |
| RAYLET-4 | Raylet | `test_pin_object_ids_owner_death_signal_releases_pin_end_to_end` | unit | added | Round 6: owner death via protocol path |
| RAYLET-6 | Raylet | `test_get_node_stats_queries_live_worker_stats_path` | unit | added | Round 6: live provider collection for workers + drivers |
| RAYLET-6 | Raylet | `test_get_node_stats_stale_tracker_does_not_mask_missing_provider_reply` | unit | added | Round 6: stale data not used when provider returns None |
| RAYLET-4 | Raylet | `test_pin_object_ids_registers_real_subscriber_subscription` | unit | added | Round 8: subscription in real ray-pubsub Subscriber at pin time |
| RAYLET-4 | Raylet | `test_pin_object_ids_missing_subscription_does_not_release_object` | unit | added | Round 8: no subscription → no release (no fallback) |
| RAYLET-4 | Raylet | `test_pin_object_ids_release_requires_real_subscription_delivery_path` | unit | added | Round 8: eviction delivered through subscriber handle_poll_response |
| RAYLET-4 | Raylet | `test_pin_object_ids_real_subscriber_callback_releases_pin` | unit | added | Round 8: end-to-end subscriber callback releases pin |
| RAYLET-4 | Raylet | `test_pin_object_ids_owner_death_via_subscriber_failure` | unit | added | Round 8: owner death via subscriber handle_publisher_failure |
| RAYLET-4 | Raylet | `test_pin_object_ids_subscriber_commands_are_drained_for_owner` | unit | added | Round 9: drain_commands returns subscribe command (transport lifecycle) |
| RAYLET-4 | Raylet | `test_pin_object_ids_long_poll_delivery_releases_pin_without_notify_helper` | unit | added | Round 9: handle_poll_response directly releases pin (no helper) |
| RAYLET-4 | Raylet | `test_pin_object_ids_owner_death_via_real_subscriber_failure_path` | unit | added | Round 9: handle_publisher_failure directly releases pins (no helper) |
| CORE-10 | Core Worker | `test_object_location_subscription_owner_death_via_publisher_failure` | unit | added | Round 9: subscriber-side handle_publisher_failure fires failure callback |
| CORE-10 | Core Worker | `test_object_location_subscription_failure_path_uses_subscriber_failure_mechanism` | unit | added | Round 9: multi-object failure callback fires for all subscriptions |
| CORE-10 | Core Worker | `test_object_location_subscription_real_long_poll_failure_path` | unit | added | Round 9: long-poll timeout + subscriber failure = failure callback fires |
| RAYLET-4 | Raylet | `test_pin_object_ids_eviction_subscriber_is_driven_by_production_transport_loop` | unit | added | Round 10: production SubscriberTransport.poll_publisher drives eviction delivery |
| RAYLET-4 | Raylet | `test_pin_object_ids_owner_failure_reaches_subscriber_without_manual_failure_call` | unit | added | Round 10: production SubscriberTransport detects failure, releases pins |
| CORE-10 | Core Worker | `test_object_location_subscription_owner_failure_is_detected_by_production_loop` | unit | added | Round 10: production SubscriberTransport detects owner failure, fires callback |
| CORE-10 | Core Worker | `test_object_location_subscription_failure_callback_runs_without_manual_handle_publisher_failure` | unit | added | Round 10: production SubscriberTransport delivers location update via callback |
| RAYLET-4 | Raylet | `test_raylet_eviction_transport_loop_runs_in_runtime` | integration | added | Round 11: runtime poll loop drives eviction delivery |
| RAYLET-4 | Raylet | `test_raylet_owner_failure_reaches_eviction_subscriber_in_runtime` | integration | added | Round 11: runtime poll loop detects owner failure |
| CORE-10 | Core Worker | `test_object_location_transport_loop_runs_in_runtime` | integration | added | Round 11: runtime poll loop drives location update delivery |
| CORE-10 | Core Worker | `test_object_location_owner_failure_reaches_subscriber_in_runtime` | integration | added | Round 11: runtime poll loop detects owner failure, fires callback |
| RAYLET-6 | Raylet | `test_get_node_stats_uses_real_worker_stats_provider_by_default` | unit | added | Round 7: default is GrpcWorkerStatsProvider |
| RAYLET-6 | Raylet | `test_get_node_stats_rpc_failure_does_not_fallback_to_tracker_data` | unit | added | Round 8: RPC failure returns default/zero stats, not tracker data |
| RAYLET-6 | Raylet | `test_get_node_stats_unreachable_driver_does_not_fallback_to_tracker_data` | unit | added | Round 8: unreachable driver gets default stats, not tracker |
| RAYLET-6 | Raylet | `test_get_node_stats_live_rpc_success_still_populates_runtime_fields` | unit | added | Round 8: successful stats collection populates all fields |
| RAYLET-6 | Raylet | `test_get_node_stats_collects_driver_stats_through_same_live_path` | unit | added | Round 7: drivers use same collection path |
| GCS-6 | GCS | `test_actor_registration_emits_definition_and_lifecycle_events` | unit | added | GCS-6 R12: registration emits 2 separate events (definition + lifecycle) matching C++ |
| GCS-6 | GCS | `test_actor_registration_event_types_match_cpp_cardinality` | unit | added | GCS-6 R12: definition event has no lifecycle_event nested — separate protos |
| GCS-6 | GCS | `test_actor_definition_event_includes_required_resources` | unit | added | GCS-6 R12: required_resources populated in ActorDefinitionEvent |
| GCS-6 | GCS | `test_actor_definition_event_includes_placement_group_and_label_selector` | unit | added | GCS-6 R12: placement_group_id and label_selector populated |
| GCS-6 | GCS | `test_actor_definition_event_includes_call_site_parent_and_ref_ids` | unit | added | GCS-6 R12: call_site, parent_id, ref_ids populated |
| GCS-6 | GCS | `test_actor_lifecycle_alive_event_includes_worker_id_and_port` | unit | added | GCS-6 R12: ALIVE transition has worker_id and port |
| GCS-6 | GCS | `test_actor_lifecycle_dead_event_includes_death_cause` | unit | added | GCS-6 R12: DEAD transition has death_cause |
| GCS-6 | GCS | `test_actor_lifecycle_restarting_event_includes_restart_reason` | unit | added | GCS-6 R12: RESTARTING transition has restart_reason=NODE_PREEMPTION |
| GCS-6 | GCS | `test_enable_ray_event_without_structured_sink_is_not_treated_as_full_parity_path` | unit | added | GCS-6 R12: no LoggingEventSink fallback in parity path |
| GCS-6 | Observability | `test_actor_event_export_merges_same_actor_same_type_events_like_cpp` | unit | added | GCS-6 R13: grouping/merge of same-actor events before export |
| GCS-6 | Observability | `test_actor_event_export_preserves_cpp_grouping_order` | unit | added | GCS-6 R13: insertion order preserved during grouping |
| GCS-6 | Observability | `test_actor_event_export_does_not_overlap_in_flight_flushes` | unit | added | GCS-6 R13: flush_in_progress prevents overlapping exports |
| GCS-6 | Observability | `test_actor_event_export_flushes_on_shutdown` | unit | added | GCS-6 R13: shutdown() does final flush of remaining events |
| GCS-6 | Observability | `test_actor_event_export_stop_semantics_match_cpp_closely` | unit | added | GCS-6 R13: enabled=false blocks new events after shutdown |
| GCS-6 | Observability | `test_actor_event_export_uses_runtime_delivery_mechanism_matching_claimed_parity` | unit | added | GCS-6 R13: file output round-trips to AddEventsRequest proto |
| GCS-6 | GCS Server | `test_gcs_server_shutdown_flushes_actor_events` | integration | added | GCS-6 R14: live server stop() flushes buffered events to output file |
| GCS-6 | GCS Server | `test_gcs_server_shutdown_rejects_new_actor_events_after_shutdown_boundary` | integration | added | GCS-6 R14: events rejected after live server stop() |
| GCS-6 | GCS Server | `test_actor_event_output_channel_matches_claimed_full_parity_contract` | integration | added | GCS-6 R14: live server output round-trips to AddEventsRequest proto with correct fields |
| GCS-6 | GCS Server | `test_gcs_server_stop_stops_periodic_actor_event_export_loop` | integration | added | GCS-6 R15: periodic flush handle stored and aborted on stop() |
| GCS-6 | GCS Server | `test_gcs_server_stop_cancels_or_quiesces_periodic_flush_task` | integration | added | GCS-6 R15: exporter rejects events after stop — task cancelled |
| GCS-6 | GCS Server | `test_actor_event_output_channel_matches_full_parity_claim_without_transport_gap` | integration | added | GCS-6 R15: comprehensive live-server output verification with definition+lifecycle events |
| GCS-6 | GCS Server | `test_actor_ray_event_path_uses_real_aggregator_service_client` | integration | added | GCS-6 R16: gRPC EventAggregatorService client sends events, no file output |
| GCS-6 | GCS Server | `test_live_gcs_server_actor_events_flow_through_service_path` | integration | added | GCS-6 R16: end-to-end proto verification at mock service |
| GCS-6 | GCS Server | `test_actor_ray_event_path_no_longer_relies_on_file_output_for_full_parity` | integration | added | GCS-6 R17: no file fallback — no sink when no port (C++ parity) |
| GCS-6 | GCS Server | `test_enable_ray_event_with_unavailable_aggregator_does_not_fallback_to_file` | integration | added | GCS-6 R17: gRPC failure → no file fallback, no export |
| GCS-6 | GCS Server | `test_enable_ray_event_without_metrics_agent_port_does_not_claim_full_parity_path` | integration | added | GCS-6 R17: no port → no sink, no export, no file |
