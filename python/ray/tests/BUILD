load("//bazel:python.bzl", "py_test_module_list")

SRCS = [] + select({
    "@bazel_tools//src/conditions:windows": glob([
        # TODO(mehrdadn): This should be added for all platforms once resulting errors are fixed
        "**/conftest.py",
    ]),
    "//conditions:default": [],
})

py_test(
    name = "test_actor",
    size = "medium",
    srcs = SRCS + ["test_actor.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_actor_advanced",
    size = "medium",
    srcs = SRCS + ["test_actor_advanced.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_actor_pool",
    size = "small",
    srcs = SRCS + ["test_actor_pool.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_memstat",
    size = "small",
    srcs = SRCS + ["test_memstat.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_dask_scheduler",
    size = "small",
    srcs = SRCS + ["test_dask_scheduler.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_iter",
    size = "medium",
    srcs = SRCS + ["test_iter.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_actor_resources",
    size = "medium",
    srcs = SRCS + ["test_actor_resources.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_actor_failures",
    size = "medium",
    srcs = SRCS + ["test_actor_failures.py"],
    # TODO(ekl) enable this once we support actor reconstruction again
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_serialization",
    size = "small",
    srcs = SRCS + ["test_serialization.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_basic",
    size = "medium",
    srcs = SRCS + ["test_basic.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_basic_2",
    size = "medium",
    srcs = SRCS + ["test_basic_2.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_advanced",
    size = "medium",
    srcs = SRCS + ["test_advanced.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_advanced_2",
    size = "medium",
    srcs = SRCS + ["test_advanced_2.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_advanced_3",
    size = "medium",
    srcs = SRCS + ["test_advanced_3.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_multi_tenancy",
    size = "small",
    srcs = SRCS + ["test_multi_tenancy.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_component_failures",
    size = "small",
    srcs = SRCS + ["test_component_failures.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_component_failures_2",
    size = "medium",
    srcs = SRCS + ["test_component_failures_2.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_component_failures_3",
    size = "medium",
    srcs = SRCS + ["test_component_failures_3.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_multinode_failures",
    size = "medium",
    srcs = SRCS + ["test_multinode_failures.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_multinode_failures_2",
    size = "medium",
    srcs = SRCS + ["test_multinode_failures_2.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_stress",
    size = "medium",
    srcs = SRCS + ["test_stress.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_stress_sharded",
    size = "medium",
    srcs = SRCS + ["test_stress_sharded.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_stress_failure",
    size = "large",
    srcs = SRCS + ["test_stress_failure.py"],
    # TODO(ekl) enable again once we support direct call reconstruction
    tags = ["exclusive", "manual"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_array",
    size = "medium",
    srcs = SRCS + ["test_array.py"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_autoscaler",
    size = "small",
    srcs = SRCS + ["test_autoscaler.py"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_resource_demand_scheduler",
    size = "small",
    srcs = SRCS + ["test_resource_demand_scheduler.py"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_coordinator_server",
    size = "small",
    srcs = SRCS + ["test_coordinator_server.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

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
    "test_component_failures_2.py",
    "test_component_failures_3.py",
    "test_dynres.py",
    "test_gcs_fault_tolerance.py",
    "test_global_gc.py",
    "test_iter.py",
    "test_joblib.py",
    "test_memory_limits.py",
    "test_memory_scheduling.py",
    "test_metrics.py",
    "test_multi_node_2.py",
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
  ],
  size = "medium",
  extra_srcs = SRCS,
  tags = ["exclusive"],
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
    "test_coordinator_server.py",
    "test_dask_scheduler.py",
    "test_debug_tools.py",
    "test_global_state.py",
    "test_job.py",
    "test_memstat.py",
    "test_metrics_agent.py",
    "test_microbenchmarks.py",
    "test_mini.py",
    "test_multi_tenancy.py",
    "test_node_manager.py",
    "test_numba.py",
    "test_projects.py",
    "test_ray_init.py",
    "test_serialization.py",
    "test_tempfile.py",
    "test_tensorflow.py",
    "test_webui.py",
  ],
  size = "small",
  extra_srcs = SRCS,
  tags = ["exclusive"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
    "test_stress_faiure.py",
    "test_failure.py"
  ],
  size = "large",
  extra_srcs = SRCS,
  # TODO(ekl) enable again once we support direct call reconstruction
  tags = ["exclusive", "manual"],
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

