load("//bazel:python.bzl", "py_test_module_list")

py_library(
  name = "conftest",
  srcs = glob(["**/conftest.py"]),
  visibility = [
    "//python/ray/tests:__subpackages__",
    "//python/ray/dashboard:__pkg__",
    "//python/ray/data:__pkg__",
  ],
)

py_test_module_list(
  files = [
    "test_dashboard.py",
    "test_ray_cluster_with_external_redis.py",
  ],
  size = "large",
  tags = ["exclusive", "manual", "team:serverless"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_actor_pool.py",
    "test_async.py",
    "test_asyncio.py",
    "test_actor_lifetime.py",
    "test_advanced.py",
    "test_advanced_2.py",
    "test_advanced_3.py",
    "test_advanced_4.py",
    "test_advanced_5.py",
    "test_advanced_6.py",
    "test_advanced_7.py",
    "test_advanced_8.py",
    "test_advanced_9.py",
    "test_array.py",
    "test_autoscaling_policy.py",
    "test_basic.py",
    "test_basic_2.py",
    "test_basic_4.py",
    "test_basic_5.py",
    "test_cancel.py",
    "test_dashboard_profiler.py",
    "test_gcs_fault_tolerance.py",
    "test_gcs_utils.py",
    "test_generators.py",
    "test_metrics_agent.py",
    "test_metrics_head.py",
    "test_component_failures_2.py",
    "test_component_failures_3.py",
    "test_error_ray_not_initialized.py",
    "test_gcs_pubsub.py",
    "test_global_gc.py",
    "test_grpc_client_credentials.py",
    "test_job.py",
    "test_get_locations.py",
    "test_global_state.py",
    "test_healthcheck.py",
    "test_kill_raylet_signal_log.py",
    "test_memstat.py",
    "test_protobuf_compatibility.py"
  ],
  size = "medium",
  tags = ["exclusive", "medium_size_python_tests_a_to_j", "team:core"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test(
  name = "test_joblib",
  srcs = ["test_joblib.py"],
  data = ["mnist_784_100_samples.pkl"],
  size = "medium",
  tags = ["exclusive", "medium_size_python_tests_a_to_j", "team:core"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_client.py",
    "test_client_reconnect.py",
  ],
  size = "large",
  tags = ["exclusive", "client_tests", "team:serverless"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_client_builder.py",
    "test_client_compat.py",
    "test_client_init.py",
    "test_client_multi.py",
    "test_client_proxy.py",
    "test_client_references.py",
    "test_client_warnings.py",
    "test_client_library_integration.py",
  ],
  size = "medium",
  tags = ["exclusive", "client_tests", "team:serverless"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_autoscaler_fake_scaledown.py",  # Temporarily owned by core.
    "test_logging.py",
    "test_memory_scheduling.py",
    "test_memory_pressure.py",
    "test_nested_task.py",
    "test_metrics.py",
    "test_task_events.py",
    "test_task_metrics.py",
    "test_task_metrics_reconstruction.py",
    "test_actor_state_metrics.py",
    "test_placement_group_metrics.py",
    "test_object_store_metrics.py",
    "test_multi_node_2.py",
    "test_multinode_failures.py",
    "test_multinode_failures_2.py",
    "test_object_assign_owner.py",
    "test_placement_group.py",
    "test_placement_group_2.py",
    "test_placement_group_4.py",
    "test_placement_group_failover.py",
    "test_ray_init.py",
    "test_ray_init_2.py",
    "test_ray_shutdown.py",
    "test_resource_demand_scheduler.py",
    "test_resource_metrics.py",
    "test_runtime_context.py",
    "test_runtime_env_env_vars.py",
    "test_runtime_env_packaging.py",
    "test_runtime_env_plugin.py",
    "test_runtime_env_strong_type.py",
    "test_runtime_env_fork_process.py",
    "test_serialization.py",
    "test_shuffle.py",
    "test_state_api_log.py",
    "test_state_api_summary.py",
    "test_stress.py",
    "test_stress_sharded.py",
    "test_tempfile.py",
    "test_tensorflow.py",
    "test_tls_auth.py",
    "test_ray_debugger.py",
    "test_worker_capping.py",
    "test_object_manager.py",
    "test_multi_tenancy.py",
    "test_namespace.py",
    "test_scheduling.py",
    "test_traceback.py",
    "test_queue.py",
  ],
  size = "medium",
  tags = ["exclusive", "medium_size_python_tests_k_to_z", "team:core"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_actor_out_of_order.py",
    "test_autoscaler_fake_multinode.py",  # Temporarily owned by core.
    "test_args.py",
    "test_asyncio_cluster.py",
    "test_concurrency_group.py",
    "test_component_failures.py",
    "test_cross_language.py",
    "test_debug_tools.py",
    "test_distributed_sort.py",
    "test_environ.py",
    "test_get_or_create_actor.py",
    "test_list_actors.py",
    "test_list_actors_2.py",
    "test_list_actors_3.py",
    "test_list_actors_4.py",
    "test_metrics_agent_2.py",
    "test_microbenchmarks.py",
    "test_mini.py",
    "test_node_manager.py",
    "test_numba.py",
    "test_redis_tls.py",
    "test_raylet_output.py",
    "test_scheduling_performance.py",
    "test_top_level_api.py",
    "test_unhandled_error.py",
    "test_utils.py",
  ],
  size = "small",
  tags = ["exclusive", "small_size_python_tests", "team:core"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_gcs_ha_e2e.py",
  ],
  size = "medium",
  tags = ["exclusive", "ray_ha", "team:core"],
  deps = ["//:ray_lib", ":conftest"],
)


py_test_module_list(
  files = [
    "test_batch_node_provider_unit.py",
    "test_batch_node_provider_integration.py",
    "test_autoscaler.py",
    "test_autoscaler_drain_node_api.py",
    "test_autoscaler_e2e.py",
    "test_autoscaler_gcp.py",
    "test_cli_logger.py",
    "test_client_metadata.py",
    "test_client_terminate.py",
    "test_command_runner.py",
    "test_coordinator_server.py",
    "test_monitor.py",
    "test_node_provider_availability_tracker.py",
    "test_response_cache.py",
    "kuberay/test_autoscaling_config.py",
    "kuberay/test_kuberay_node_provider.py"
  ],
  size = "small",
  tags = ["exclusive", "small_size_python_tests", "team:serverless"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test(
  name = "test_autoscaler_yaml",
  size = "small",
  srcs = ["test_autoscaler_yaml.py"],
  tags = ["exclusive", "small_size_python_tests", "team:serverless"],
  deps = ["//:ray_lib", ":conftest"],
  data = [
    "additional_property.yaml",
    "test_cli_patterns/test_multi_node.yaml",
    "test_cli_patterns/test_no_head.yaml",
    "test_cli_patterns/test_no_workers.yaml",
    "//python/ray/autoscaler/aws:example",
    "//python/ray/autoscaler/azure:example",
    "//python/ray/autoscaler/gcp:example",
    "//python/ray/autoscaler/local:example",
  ],
)

py_test_module_list(
  files = [
    "test_dataclient_disconnect.py",
  ],
  size = "medium",
  tags = ["exclusive", "medium_size_python_tests_a_to_j", "team:serverless"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_actor.py",
    "test_actor_failures.py",
    "test_actor_resources.py",
    "test_failure.py",
    "test_actor_advanced.py",
    "test_memory_deadlock.py",
    "test_multi_node.py",
    "test_threaded_actor.py",
    "test_stress_failure.py",
    "test_state_api.py",
    "test_reconstruction.py",
    "test_reconstruction_2.py",
    "test_reconstruction_stress.py",
    "test_reconstruction_stress_spill.py",
    "test_failure_2.py",
    "test_failure_3.py",
    "test_chaos.py",
    "test_storage.py",
    "test_reference_counting_2.py",
    "test_exit_observability.py",
    "test_usage_stats.py",
    "test_placement_group_3.py",
    "test_placement_group_5.py",
  ],
  size = "large",
  tags = ["exclusive", "large_size_python_tests_shard_0", "team:core"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test(
  name = "test_cli",
  srcs = ["test_cli.py"],
  data = glob(["test_cli_patterns/*.txt", "test_cli_patterns/*.yaml"]),
  size = "large",
  tags = ["exclusive", "large_size_python_tests_shard_0", "team:core"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_basic_3.py",
    "test_output.py",
    "test_out_of_disk_space.py",
    "test_failure_4.py",
    "test_iter.py",
    "test_object_spilling.py",
    "test_object_spilling_no_asan.py",
    "test_object_spilling_2.py",
    "test_object_spilling_3.py",
    "test_plasma_unlimited.py",
    "test_placement_group_mini_integration.py",
    "test_scheduling_2.py",
    "test_multi_node_3.py",
    "test_multiprocessing.py",
    "test_reference_counting.py",
  ],
  size = "large",
  tags = ["exclusive", "large_size_python_tests_shard_1", "team:core"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_runtime_env.py",
    "test_runtime_env_2.py",
    "test_runtime_env_failure.py",
    "test_runtime_env_working_dir.py",
    "test_runtime_env_working_dir_2.py",
    "test_runtime_env_working_dir_3.py",
    "test_runtime_env_working_dir_4.py",
    "test_runtime_env_working_dir_remote_uri.py"
  ],
  size = "large",
  tags = ["exclusive", "large_size_python_tests_shard_2", "team:serve"],
  deps = ["//:ray_lib", ":conftest"],
  data = ["pip_install_test-0.5-py3-none-any.whl"],
)

py_test_module_list(
  files = [
    "test_runtime_env_conda_and_pip.py",
    "test_runtime_env_conda_and_pip_2.py",
    "test_runtime_env_conda_and_pip_3.py",
    "test_runtime_env_conda_and_pip_4.py",
    "test_runtime_env_conda_and_pip_5.py",
  ],
  size = "large",
  tags = ["exclusive", "post_wheel_build", "team:serve"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test(
  name = "test_runtime_env_complicated",
  size = "large",
  srcs = ["test_runtime_env_complicated.py"],
  tags = ["exclusive", "post_wheel_build", "team:serve"],
  deps = ["//:ray_lib", ":conftest"],
  data = ["//python/ray/experimental/packaging/example_pkg"],
)

py_test(
    name = "test_actor_group",
    size = "medium",
    srcs = ["test_actor_group.py"],
    tags = ["exclusive", "medium_size_python_tests_a_to_j", "team:serve"],
    deps = ["//:ray_lib", ":conftest"]
)

# TODO(barakmich): aws/ might want its own buildfile, or
#    py_test_module_list should support subdirectories.
py_test(
    name = "test_autoscaler_aws",
    size = "small",
    srcs = ["aws/test_autoscaler_aws.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serverless"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_aws_batch_tag_update",
    size = "small",
    srcs = ["aws/test_aws_batch_tag_update.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serverless"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_gcp_node_provider",
    size = "small",
    srcs = ["gcp/test_gcp_node_provider.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serverless"],
    deps = ["//:ray_lib", ":conftest"],
)

# Note(simon): typing tests are not included in module list
#    because they requires globs and it might be refactored in the future.
py_test(
    name = "test_typing",
    size = "medium",
    srcs = ["test_typing.py", "typing_files/check_typing_bad.py",
            "typing_files/check_typing_good.py"],
    tags = ["exclusive", "small_size_python_tests", "team:core"],
    deps = ["//:ray_lib", ":conftest"],
)

# TODO: use py_test(env = ...) in the build file with bazel 4.0
py_test(
  name = "test_tracing",
  size = "medium",
  srcs = ["test_tracing.py"],
  tags = ["exclusive", "medium_size_python_tests_k_to_z", "team:serve"],
  deps = ["//:ray_lib", ":conftest"]
)

py_test(
    name = "test_pydantic_serialization",
    size = "small",
    srcs = ["test_pydantic_serialization.py", "pydantic_module.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serve"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_runtime_env_validation",
    size = "small",
    srcs = ["test_runtime_env_validation.py"],
    data = [
        "test_runtime_env_validation_1_schema.json",
        "test_runtime_env_validation_2_schema.json",
    ],
    tags = ["exclusive", "small_size_python_tests", "team:serve"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_runtime_env_ray_minimal",
    size = "medium",
    srcs = ["test_runtime_env_ray_minimal.py"],
    tags = ["exclusive", "medium_size_python_tests_k_to_z", "team:serve"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_serve_ray_minimal",
    size = "small",
    srcs = ["test_serve_ray_minimal.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serve"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "kuberay/test_autoscaling_e2e",
    size = "large",
    srcs = ["kuberay/test_autoscaling_e2e.py"],
    tags = ["exclusive", "kuberay_operator", "team:serverless"],
    deps = ["//:ray_lib", ":conftest"],
)

# TODO(ekl) we can't currently support tagging these as flaky since there's
# no way to filter by both flaky and client mode tests in bazel.
py_test_module_list(
  files = [
    "test_actor.py",
    "test_advanced.py",
    "test_basic.py",
    "test_basic_2.py",
    "test_basic_3.py",
    "test_basic_4.py",
    "test_basic_5.py",
    "test_asyncio.py",
    "test_object_assign_owner.py",
    "test_multiprocessing.py",
    "test_list_actors.py",
    "test_list_actors_2.py",
    "test_list_actors_3.py",
    "test_list_actors_4.py",
  ],
  size = "large",
  name_suffix = "_client_mode",
  env = {"RAY_CLIENT_MODE": "1", "RAY_PROFILING": "1"},
  tags = ["exclusive", "client_tests", "team:serverless"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_actor_in_container.py",
  ],
  size = "large",
  tags = ["exclusive", "worker-container", "team:serve"],
  # Now we run this test in a container which has installed ray
  deps = [],
)

py_test_module_list(
  files = [
    "test_object_spilling.py",
    "test_object_spilling_2.py",
    "test_object_spilling_3.py",
    "test_scheduling.py",
    "test_scheduling_2.py",
    "test_array.py"
  ],
  size = "large",
  name_suffix = "_debug_mode",
  tags = ["exclusive", "debug_tests", "team:core"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "test_object_spilling.py",
    "test_object_spilling_2.py",
    "test_object_spilling_3.py",
    "test_scheduling.py",
    "test_scheduling_2.py",
    "test_array.py"
  ],
  size = "large",
  name_suffix = "_asan",
  tags = ["exclusive", "asan_tests", "team:core"],
  deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = [
    "spark/test_basic.py",
    "spark/test_GPU.py",
    "spark/test_multicores_per_task.py",
    "spark/test_utils.py",
  ],
  size = "large",
  tags = ["exclusive", "spark_plugin_tests", "team:serverless"],
  deps = ["//:ray_lib", ":conftest"],
  data = [
    "spark/discover_2_gpu.sh",
    "spark/discover_4_gpu.sh"
  ],
)
