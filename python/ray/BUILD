py_test(
    name = "test_actor",
    size = "large",
    srcs = ["tests/test_actor.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_array",
    size = "medium",
    srcs = ["tests/test_array.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_autoscaler",
    size = "small",
    srcs = ["tests/test_autoscaler.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_autoscaler_yaml",
    size = "small",
    srcs = ["tests/test_autoscaler_yaml.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_basic",
    size = "large",
    srcs = ["tests/test_basic.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_component_failures",
    size = "large",
    srcs = ["tests/test_component_failures.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_cython",
    size = "small",
    srcs = ["tests/test_cython.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_debug_tools",
    size = "small",
    srcs = ["tests/test_debug_tools.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_dynres",
    size = "large",
    srcs = ["tests/test_dynres.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_failure",
    size = "large",
    srcs = ["tests/test_failure.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_garbage_collection",
    size = "medium",
    srcs = ["tests/test_garbage_collection.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_global_state",
    size = "medium",
    srcs = ["tests/test_global_state.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_logical_graph",
    size = "medium",
    srcs = ["tests/test_logical_graph.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_memory_limits",
    size = "medium",
    srcs = ["tests/test_memory_limits.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_memory_scheduling",
    size = "medium",
    srcs = ["tests/test_memory_scheduling.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_metrics",
    size = "small",
    srcs = ["tests/test_metrics.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_microbenchmarks",
    size = "small",
    srcs = ["tests/test_microbenchmarks.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_mini",
    size = "small",
    srcs = ["tests/test_mini.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_monitors",
    size = "medium",
    srcs = ["tests/test_monitors.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_multi_node_2",
    size = "medium",
    srcs = ["tests/test_multi_node_2.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_multi_node",
    size = "medium",
    srcs = ["tests/test_multi_node.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_node_manager",
    size = "small",
    srcs = ["tests/test_node_manager.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_object_manager",
    size = "medium",
    srcs = ["tests/test_object_manager.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_projects",
    size = "small",
    srcs = ["tests/test_projects.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_queue",
    size = "small",
    srcs = ["tests/test_queue.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_ray_init",
    size = "small",
    srcs = ["tests/test_ray_init.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_signal",
    size = "medium",
    srcs = ["tests/test_signal.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_stress",
    size = "large",
    srcs = ["tests/test_stress.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_tempfile",
    size = "small",
    srcs = ["tests/test_tempfile.py"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_tensorflow",
    size = "medium",
    srcs = ["tests/test_tensorflow.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_unreconstructable_errors",
    size = "medium",
    srcs = ["tests/test_unreconstructable_errors.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

py_test(
    name = "test_webui",
    size = "medium",
    srcs = ["tests/test_webui.py"],
    tags = ["exclusive"],
    deps = [":ray_lib"],
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "ray_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
