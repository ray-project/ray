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
    "test_actor_advanced.py",
    "test_actor_failures.py",
    "test_actor.py",
    "test_actor_resources.py",
    "test_advanced_2.py",
    "test_advanced_3.py",
    "test_advanced.py",
    "test_array.py",
    "test_async.py",
    "test_basic_2.py",
    "test_basic.py",
    "test_cancel.py",
    "test_cli.py",
    "test_component_failures_2.py",
    "test_component_failures_3.py",
    "test_dynres.py",
    "test_error_ray_not_initialized.py",
    "test_gcs_fault_tolerance.py",
    "test_global_gc.py",
    "test_iter.py",
    "test_joblib.py",
    "test_resource_demand_scheduler.py",
  ],
  size = "medium",
  extra_srcs = SRCS,
  tags = ["exclusive", "medium_size_python_tests_a_to_j"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_memory_limits.py",
    "test_memory_scheduling.py",
    "test_metrics.py",
    "test_multi_node_2.py",
    "test_multi_tenancy.py",
    "test_multinode_failures_2.py",
    "test_multinode_failures.py",
    "test_multi_node.py",
    "test_multiprocessing.py",
    "test_object_manager.py",
    "test_output.py",
    "test_placement_group.py",
    "test_reconstruction.py",
    "test_reference_counting_2.py",
    "test_reference_counting.py",
    "test_stress.py",
    "test_stress_sharded.py",
    "test_unreconstructable_errors.py",
    "test_tensorflow.py",
    "test_object_spilling.py",
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
    "test_asyncio.py",
    "test_autoscaler.py",
    "test_autoscaler_yaml.py",
    "test_component_failures.py",
    "test_command_runner.py",
    "test_coordinator_server.py",
    "test_dask_scheduler.py",
    "test_dask_callback.py",
    "test_debug_tools.py",
    "test_global_state.py",
    "test_job.py",
    "test_memstat.py",
    "test_metrics_agent.py",
    "test_microbenchmarks.py",
    "test_mini.py",
    "test_node_manager.py",
    "test_numba.py",
    "test_queue.py",
    "test_ray_init.py",
    "test_serialization.py",
    "test_tempfile.py",
    "test_webui.py",
  ],
  size = "small",
  extra_srcs = SRCS,
  tags = ["exclusive"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_stress_failure.py",
    "test_failure.py"
  ],
  size = "large",
  extra_srcs = SRCS,
  tags = ["exclusive"],
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

# Note(simon): typing tests are not included in module list
#    because they requires globs and it might be refactored in the future.
py_test(
    name = "test_typing",
    size = "small",
    srcs = SRCS + ["test_typing.py"] + glob(["typing_files"]),
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)
