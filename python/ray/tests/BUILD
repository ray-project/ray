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
    name = "test_coordinator_server",
    size = "small",
    srcs = SRCS + ["test_coordinator_server.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_autoscaler_aws",
    size = "small",
    srcs = SRCS + ["aws/test_autoscaler_aws.py"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_autoscaler_yaml",
    size = "small",
    srcs = SRCS + ["test_autoscaler_yaml.py"],
    data = ["additional_property.yaml"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_debug_tools",
    size = "small",
    srcs = SRCS + ["test_debug_tools.py"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_dynres",
    size = "medium",
    srcs = SRCS + ["test_dynres.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_failure",
    size = "large",
    srcs = SRCS + ["test_failure.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_reconstruction",
    size = "medium",
    srcs = SRCS + ["test_reconstruction.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_reference_counting",
    size = "medium",
    srcs = SRCS + ["test_reference_counting.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_reference_counting_2",
    size = "medium",
    srcs = SRCS + ["test_reference_counting_2.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_global_gc",
    size = "medium",
    srcs = SRCS + ["test_global_gc.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_global_state",
    size = "small",
    srcs = SRCS + ["test_global_state.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_memory_limits",
    size = "medium",
    srcs = SRCS + ["test_memory_limits.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_memory_scheduling",
    size = "medium",
    srcs = SRCS + ["test_memory_scheduling.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_metrics",
    size = "small",
    srcs = SRCS + ["test_metrics.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_metrics_agent",
    size = "small",
    srcs = SRCS + ["test_metrics_agent.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_microbenchmarks",
    size = "small",
    srcs = SRCS + ["test_microbenchmarks.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_mini",
    size = "small",
    srcs = SRCS + ["test_mini.py"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_multiprocessing",
    size = "medium",
    srcs = SRCS + ["test_multiprocessing.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_job",
    size = "small",
    srcs = SRCS + ["test_job.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_joblib",
    size = "medium",
    srcs = SRCS + ["test_joblib.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_multi_node_2",
    size = "medium",
    srcs = SRCS + ["test_multi_node_2.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_multi_node",
    size = "medium",
    srcs = SRCS + ["test_multi_node.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_node_manager",
    size = "small",
    srcs = SRCS + ["test_node_manager.py"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_object_manager",
    size = "medium",
    srcs = SRCS + ["test_object_manager.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_projects",
    size = "small",
    srcs = SRCS + ["test_projects.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_queue",
    size = "small",
    srcs = SRCS + ["test_queue.py"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_ray_init",
    size = "small",
    srcs = SRCS + ["test_ray_init.py"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_tempfile",
    size = "small",
    srcs = SRCS + ["test_tempfile.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_tensorflow",
    size = "small",
    srcs = SRCS + ["test_tensorflow.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_unreconstructable_errors",
    size = "medium",
    srcs = SRCS + ["test_unreconstructable_errors.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_webui",
    size = "small",
    srcs = SRCS + ["test_webui.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_args",
    size = "small",
    srcs = SRCS + ["test_args.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_asyncio",
    size = "small",
    srcs = SRCS + ["test_asyncio.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_numba",
    size = "small",
    srcs = SRCS + ["test_numba.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_cancel",
    size = "medium",
    srcs = SRCS + ["test_cancel.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_gcs_fault_tolerance",
    size = "medium",
    srcs = SRCS + ["test_gcs_fault_tolerance.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_output",
    size = "medium",
    srcs = SRCS + ["test_output.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_async",
    size = "medium",
    srcs = SRCS + ["test_async.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)

py_test(
    name = "test_placement_group",
    size = "medium",
    srcs = SRCS + ["test_placement_group.py"],
    tags = ["exclusive"],
    deps = ["//:ray_lib"],
)
