load("@rules_python//python:defs.bzl", "py_library")
load("//bazel:python.bzl", "py_test_module_list", "py_test_run_all_subdirectory")

py_library(
    name = "conftest",
    srcs = ["conftest.py"],
)

py_test_run_all_subdirectory(
    size = "small",
    include = glob(["test_*.py"]),
    exclude = [],
    extra_srcs = [],
    tags = ["team:serve"],
    deps = [
        ":conftest",
        "//python/ray/serve:serve_lib",
        "//python/ray/serve/tests:common",
    ],
)

py_test_module_list(
    size = "medium",
    env = {"RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS": "0"},
    files = [
        "test_deployment_state.py",
    ],
    name_suffix = "_with_stop_fully_then_start_behavior",
    tags = [
        "no_windows",
        "team:serve",
    ],
    deps = [
        ":conftest",
        "//python/ray/serve:serve_lib",
        "//python/ray/serve/tests:common",
    ],
)

py_test_module_list(
    size = "medium",
    env = {"RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY": "1"},
    files = [
        "test_deployment_scheduler.py",
        "test_deployment_state.py",
    ],
    name_suffix = "_with_compact_scheduling",
    tags = [
        "no_windows",
        "team:serve",
    ],
    deps = [
        ":conftest",
        "//python/ray/serve:serve_lib",
        "//python/ray/serve/tests:common",
    ],
)

py_test_module_list(
    size = "medium",
    env = {"RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE": "0"},
    files = [
        "test_autoscaling_policy.py",
        "test_deployment_state.py",
        "test_router.py",
    ],
    name_suffix = "_with_metr_disab",
    tags = [
        "no_windows",
        "team:serve",
    ],
    deps = [
        ":conftest",
        "//python/ray/serve:serve_lib",
        "//python/ray/serve/tests:common",
    ],
)
