load("@rules_python//python:defs.bzl", "py_library")

# --------------------------------------------------------------------
# Tests from the python/ray/autoscaler/v2/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
load("//bazel:python.bzl", "py_test_module_list")

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
    deps = ["//python/ray/tests:conftest"],
)

# Unit test. (These should not have ray started)
py_test_module_list(
    size = "small",
    files = [
        "tests/test_config.py",
        "tests/test_event_logger.py",
        "tests/test_instance_manager.py",
        "tests/test_instance_storage.py",
        "tests/test_instance_util.py",
        "tests/test_metrics_reporter.py",
        "tests/test_node_provider.py",
        "tests/test_ray_installer.py",
        "tests/test_reconciler.py",
        "tests/test_scheduler.py",
        "tests/test_schema.py",
        "tests/test_storage.py",
        "tests/test_subscribers.py",
        "tests/test_threaded_ray_installer.py",
        "tests/test_utils.py",
    ],
    tags = ["team:core"],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

# Integration test.
py_test_module_list(
    size = "medium",
    files = [
        "tests/test_autoscaler.py",
        "tests/test_sdk.py",
    ],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test_module_list(
    size = "large",
    files = [
        "tests/test_e2e.py",
    ],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)
