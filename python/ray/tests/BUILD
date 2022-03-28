load("//bazel:python.bzl", "py_test_module_list")

SRCS = [] + select({
    "@bazel_tools//src/conditions:windows": glob([
        # TODO(mehrdadn): This should be added for all platforms once resulting errors are fixed
        "**/conftest.py",
    ]),
    "//conditions:default": [],
})

py_test_module_list(
  files = [
    "test_dashboard.py",
    "test_ray_cluster_with_external_redis.py",
    "test_k8s_cluster_launcher.py",
  ],
  size = "large",
  extra_srcs = SRCS,
  tags = ["exclusive", "manual", "team:serverless"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_actor_pool.py",
    "test_async.py",
    "test_actor_failures.py",
    "test_actor_resources.py",
    "test_actor_lifetime.py",
    "test_advanced.py",
    "test_advanced_2.py",
    "test_advanced_3.py",
    "test_advanced_4.py",
    "test_array.py",
    "test_autoscaling_policy.py",
    "test_basic.py",
    "test_basic_2.py",
    "test_basic_4.py",
    "test_basic_5.py",
    "test_cancel.py",
    "test_gcs_fault_tolerance.py",
    "test_metrics_agent.py",
    "test_component_failures_2.py",
    "test_component_failures_3.py",
    "test_error_ray_not_initialized.py",
    "test_gcs_pubsub.py",
    "test_global_gc.py",
    "test_grpc_client_credentials.py",
    "test_iter.py",
    "test_job.py",
    "test_joblib.py",
    "test_get_locations.py",
    "test_global_state.py",
    "test_healthcheck.py",
    "test_kill_raylet_signal_log.py",
    "test_memstat.py",
    "test_mldataset.py",
  ],
  size = "medium",
  extra_srcs = SRCS,
  tags = ["exclusive", "medium_size_python_tests_a_to_j", "team:core"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_client.py",
  ],
  size = "large",
  extra_srcs = SRCS,
  tags = ["exclusive", "client_tests", "team:serverless"],
  deps = ["//:ray_lib"],
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
    "test_client_reconnect.py",
  ],
  size = "medium",
  extra_srcs = SRCS,
  tags = ["exclusive", "client_tests", "team:serverless"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_usage_stats.py",
    "test_autoscaler_fake_scaledown.py",  # Temporarily owned by core.
    "test_logging.py",
    "test_memory_scheduling.py",
    "test_metrics.py",
    "test_multi_node.py",
    "test_multi_node_2.py",
    "test_multi_node_3.py",
    "test_multinode_failures.py",
    "test_multinode_failures_2.py",
    "test_multiprocessing.py",
    "test_object_assign_owner.py",
    "test_placement_group.py",
    "test_placement_group_2.py",
    "test_placement_group_3.py",
    "test_placement_group_4.py",
    "test_placement_group_5.py",
    "test_ray_init.py",
    "test_reference_counting.py",
    "test_resource_demand_scheduler.py",
    "test_runtime_context.py",
    "test_runtime_env_env_vars.py",
    "test_runtime_env_packaging.py",
    "test_runtime_env_plugin.py",
    "test_runtime_env_fork_process.py",
    "test_serialization.py",
    "test_shuffle.py",
    "test_stress.py",
    "test_storage.py",
    "test_stress_sharded.py",
    "test_tempfile.py",
    "test_tensorflow.py",
    "test_threaded_actor.py",
    "test_tls_auth.py",
    "test_ray_debugger.py",
    "test_worker_capping.py",
    "test_object_manager.py",
    "test_multi_tenancy.py",
    "test_namespace.py",
    "test_scheduling.py",
    "test_scheduling_2.py",
    "test_traceback.py",
  ],
  size = "medium",
  extra_srcs = SRCS,
  tags = ["exclusive", "medium_size_python_tests_k_to_z", "team:core"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_autoscaler_fake_multinode.py",  # Temporarily owned by core.
    "test_args.py",
    "test_asyncio_cluster.py",
    "test_asyncio.py",
    "test_concurrency_group.py",
    "test_component_failures.py",
    "test_debug_tools.py",
    "test_distributed_sort.py",
    "test_microbenchmarks.py",
    "test_mini.py",
    "test_node_manager.py",
    "test_numba.py",
    "test_queue.py",
    "test_ray_shutdown.py",
    "test_unhandled_error.py",
    "test_top_level_api.py",
    "test_list_actors.py",
    "test_list_actors_2.py",
    "test_list_actors_3.py",
    "test_list_actors_4.py",
    "test_actor_out_of_order.py",
    "test_cross_language.py",
    "test_environ.py",
    "test_raylet_output.py",
    "test_scheduling_performance.py",
    "test_get_or_create_actor.py",
  ],
  size = "small",
  extra_srcs = SRCS,
  tags = ["exclusive", "small_size_python_tests", "team:core"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_autoscaler.py",
    "test_autoscaler_drain_node_api.py",
    "test_autoscaler_gcp.py",
    "test_autoscaler_yaml.py",
    "test_cli_logger.py",
    "test_client_metadata.py",
    "test_client_terminate.py",
    "test_command_runner.py",
    "test_coordinator_server.py",
    "test_k8s_operator_unit_tests.py",
    "test_monitor.py",
    "test_response_cache.py",
    "kuberay/test_autoscaling_config.py"
  ],
  size = "small",
  extra_srcs = SRCS,
  tags = ["exclusive", "small_size_python_tests", "team:serverless"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_dataclient_disconnect.py",
  ],
  size = "medium",
  extra_srcs = SRCS,
  tags = ["exclusive", "medium_size_python_tests_a_to_j", "team:serverless"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_actor.py",
    "test_cli.py",
    "test_failure.py",
    "test_actor_advanced.py",
    "test_stress_failure.py",
    "test_reconstruction.py",
    "test_failure_2.py",
    "test_failure_3.py",
    "test_chaos.py",
    "test_reference_counting_2.py",
  ],
  size = "large",
  extra_srcs = SRCS,
  tags = ["exclusive", "large_size_python_tests_shard_0", "team:core"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_basic_3.py",
    "test_output.py",
    "test_failure_4.py",
    "test_object_spilling.py",
    "test_object_spilling_2.py",
    "test_object_spilling_3.py",
    "test_plasma_unlimited.py",
    "test_placement_group_mini_integration.py",
  ],
  size = "large",
  extra_srcs = SRCS,
  tags = ["exclusive", "large_size_python_tests_shard_1", "team:core"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_runtime_env.py",
    "test_runtime_env_2.py",
    "test_runtime_env_working_dir.py",
    "test_runtime_env_working_dir_2.py",
    "test_runtime_env_working_dir_3.py",
    "test_runtime_env_working_dir_remote_uri.py"
  ],
  size = "large",
  extra_srcs = SRCS,
  tags = ["exclusive", "large_size_python_tests_shard_2", "team:serve"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_runtime_env_conda_and_pip.py",
    "test_runtime_env_conda_and_pip_2.py",
    "test_runtime_env_conda_and_pip_3.py",
    "test_runtime_env_conda_and_pip_4.py",
    "test_runtime_env_conda_and_pip_5.py",
    "test_runtime_env_complicated.py"
  ],
  size = "large",
  extra_srcs = SRCS,
  tags = ["exclusive", "post_wheel_build", "team:serve"],
  deps = ["//:ray_lib"],
)

py_test(
    name = "test_actor_group",
    size = "medium",
    srcs = SRCS + ["test_actor_group.py"],
    tags = ["exclusive", "medium_size_python_tests_a_to_j", "team:serve"],
    deps = ["//:ray_lib"]
)

# TODO(barakmich): aws/ might want its own buildfile, or
#    py_test_module_list should support subdirectories.
py_test(
    name = "test_autoscaler_aws",
    size = "small",
    srcs = SRCS + ["aws/test_autoscaler_aws.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serverless"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_aws_batch_tag_update",
    size = "small",
    srcs = SRCS + ["aws/test_aws_batch_tag_update.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serverless"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_gcp_node_provider",
    size = "small",
    srcs = SRCS + ["gcp/test_gcp_node_provider.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serverless"],
    deps = ["//:ray_lib"],
)

# Note(simon): typing tests are not included in module list
#    because they requires globs and it might be refactored in the future.
py_test(
    name = "test_typing",
    size = "medium",
    srcs = SRCS + ["test_typing.py", "typing_files/check_typing_bad.py",
                   "typing_files/check_typing_good.py"],
    tags = ["exclusive", "small_size_python_tests", "team:core"],
    deps = ["//:ray_lib"],
)

# TODO: use py_test(env = ...) in the build file with bazel 4.0
py_test(
  name = "test_tracing",
  size = "medium",
  srcs = SRCS + ["test_tracing.py"],
  tags = ["exclusive", "medium_size_python_tests_k_to_z", "team:serve"],
  deps = ["//:ray_lib"]
)

py_test(
    name = "test_pydantic_serialization",
    size = "small",
    srcs = SRCS + ["test_pydantic_serialization.py", "pydantic_module.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serve"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_runtime_env_validation",
    size = "small",
    srcs = SRCS + ["test_runtime_env_validation.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serve"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_runtime_env_ray_minimal",
    size = "medium",
    srcs = SRCS + ["test_runtime_env_ray_minimal.py"],
    tags = ["exclusive", "medium_size_python_tests_k_to_z", "team:serve"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_serve_ray_minimal",
    size = "small",
    srcs = SRCS + ["test_serve_ray_minimal.py"],
    tags = ["exclusive", "small_size_python_tests", "team:serve"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "kuberay/test_autoscaling_e2e",
    size = "medium",
    srcs = SRCS + ["kuberay/test_autoscaling_e2e.py"],
    tags = ["exclusive", "kuberay_operator", "team:serverless"],
    deps = ["//:ray_lib"],
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
    "test_multiprocessing.py",
    "test_list_actors.py",
    "test_list_actors_2.py",
    "test_list_actors_3.py",
    "test_list_actors_4.py",
  ],
  size = "large",
  extra_srcs = SRCS,
  name_suffix = "_client_mode",
  env = {"RAY_CLIENT_MODE": "1", "RAY_PROFILING": "1"},
  tags = ["exclusive", "client_tests", "team:serverless"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_actor_in_container.py",
  ],
  size = "large",
  extra_srcs = SRCS,
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
  size = "medium",
  extra_srcs = SRCS,
  name_suffix = "_debug_mode",
  tags = ["exclusive", "debug_tests", "team:core"],
  deps = ["//:ray_lib"],
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
  extra_srcs = SRCS,
  name_suffix = "_asan",
  tags = ["exclusive", "asan_tests", "team:core"],
  deps = ["//:ray_lib"],
)
