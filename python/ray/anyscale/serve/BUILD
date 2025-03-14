load("@rules_python//python:defs.bzl", "py_library")
load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
    size = "medium",
    data = glob(["tests/fixtures/*.*"]),
    env = {"RAY_SERVE_NODE_COMPACTION_DELAY_S": "1"},
    files = glob(["tests/test_*.py"]),
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
)

# Test without compact scheduling
py_test_module_list(
    size = "medium",
    data = glob(["tests/fixtures/*.*"]),
    env = {"RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY": "0"},
    files = [
        "tests/test_az_routing.py",
        "tests/test_deployment_scheduler.py",
        "tests/test_draining.py",
        "tests/test_metrics.py",
        "tests/test_telemetry.py",
        "tests/test_tracing_utils.py",
    ],
    name_suffix = "_without_compact_scheduling",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test_module_list(
    size = "medium",
    data = glob(["tests/fixtures/*.*"]),
    env = {"ANYSCALE_RAY_SERVE_ENABLE_DIRECT_INGRESS": "1"},
    files = [
        "tests/test_direct_ingress.py",
    ],
    name_suffix = "_with_direct_ingress_enabled",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)
