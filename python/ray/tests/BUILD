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
#    "test_dynres.py",  # dyn res not implemented
    "test_async.py",
    "test_actor.py",
    "test_actor_advanced.py",
    "test_actor_failures.py",
    "test_actor_resources.py",
    "test_advanced.py",
    "test_advanced_2.py",
    "test_advanced_3.py",
    "test_advanced_4.py",
    "test_array.py",
    "test_autoscaling_policy.py",
    "test_basic.py",
    "test_basic_2.py",
    "test_basic_3.py",
    "test_cancel.py",
    "test_client.py",
    "test_client_init.py",
    "test_component_failures_2.py",
    "test_component_failures_3.py",
    "test_error_ray_not_initialized.py",
    "test_gcs_fault_tolerance.py",
    "test_iter.py",
    "test_joblib.py",
    "test_global_state.py",
    "test_global_gc.py",
    "test_mldataset.py",
  ],
  size = "medium",
  extra_srcs = SRCS,
  tags = ["exclusive", "medium_size_python_tests_a_to_j"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_memory_scheduling.py",
    "test_metrics.py",
    "test_multi_node.py",
    "test_multi_node_2.py",
    "test_multi_node_3.py",
    "test_multi_tenancy.py",
    "test_multinode_failures.py",
    "test_multinode_failures_2.py",
    "test_multiprocessing.py",
    "test_object_manager.py",
    "test_output.py",
    "test_reconstruction.py",
    "test_reference_counting.py",
    "test_reference_counting_2.py",
    "test_resource_demand_scheduler.py",
    "test_scheduling.py",
    "test_serialization.py",
    "test_stress.py",
    "test_stress_sharded.py",
    "test_tensorflow.py",
  ],
  size = "medium",
  extra_srcs = SRCS,
  tags = ["exclusive", "medium_size_python_tests_k_to_z"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_actor_pool.py",
    "test_args.py",
    "test_asyncio_cluster.py",
    "test_asyncio.py",
    "test_autoscaler.py",
    "test_autoscaler_yaml.py",
    "test_cli_logger.py",
    "test_client_metadata.py",
    "test_client_references.py",
    "test_client_terminate.py",
    "test_command_runner.py",
    "test_component_failures.py",
    "test_coordinator_server.py",
    "test_dask_callback.py",
    "test_dask_scheduler.py",
    "test_debug_tools.py",
    "test_job.py",
    "test_k8s_operator_mock.py",
    "test_kv.py",
    "test_memstat.py",
    "test_metrics_agent.py",
    "test_microbenchmarks.py",
    "test_mini.py",
    "test_monitor.py",
    "test_node_manager.py",
    "test_numba.py",
    "test_queue.py",
    "test_ray_debugger.py",
    "test_ray_init.py",
    "test_shuffle.py",
    "test_tempfile.py",
  ],
  size = "small",
  extra_srcs = SRCS,
  tags = ["exclusive"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_cli.py",
    "test_failure.py",
    "test_stress_failure.py",
  ],
  size = "large",
  extra_srcs = SRCS,
  tags = ["exclusive"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_placement_group.py",
    "test_object_spilling.py",
    "test_runtime_env.py",
  ],
  size = "large",
  extra_srcs = SRCS,
  deps = ["//:ray_lib"],
)

# TODO(barakmich): aws/ might want its own buildfile, or
#    py_test_module_list should support subdirectories.
py_test(
    name = "test_autoscaler_aws",
    size = "small",
    srcs = SRCS + ["aws/test_autoscaler_aws.py"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_aws_batch_tag_update",
    size = "small",
    srcs = SRCS + ["aws/test_aws_batch_tag_update.py"],
    deps = ["//:ray_lib"],
)

# Note(simon): typing tests are not included in module list
#    because they requires globs and it might be refactored in the future.
py_test(
    name = "test_typing",
    size = "small",
    srcs = SRCS + ["test_typing.py"] + glob(["typing_files"]),
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)


py_test_module_list(
  files = [
    "test_actor.py",
    "test_advanced.py",
    "test_basic.py",
    "test_basic_2.py",
    "test_basic_3.py",
  ],
  size = "medium",
  extra_srcs = SRCS,
  name_suffix = "_client_mode",
  # TODO(barakmich): py_test will support env in Bazel 4.0.0...
  # Until then, we can use tags.
  #env = {"RAY_CLIENT_MODE": "1"},
  tags = ["exclusive", "client_tests"],
  deps = ["//:ray_lib"],
)
