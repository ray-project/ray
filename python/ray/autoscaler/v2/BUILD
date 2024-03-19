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
    files = [
        "tests/test_instance_storage.py",
        "tests/test_storage.py",
        "tests/test_config.py",
        "tests/test_node_provider.py",
        "tests/test_ray_installer.py",
        "tests/test_subscribers.py",
        "tests/test_reconciler.py",
        "tests/test_threaded_ray_installer.py",
        "tests/test_utils.py",
        "tests/test_scheduler.py",
        "tests/test_instance_manager.py",
        "tests/test_instance_util.py",
        "tests/test_schema.py",
        "tests/test_event_logger.py",
        "tests/test_metrics_reporter.py",
    ],
    size = "small",
    tags = ["team:core"],
    deps = ["//:ray_lib", ":conftest"],
)

# Integration test.
py_test_module_list(
    files=[
        "tests/test_sdk.py",
        "tests/test_autoscaler.py",
    ],
    size="medium",
    tags=["team:core", "exclusive"],
    deps=["//:ray_lib", ":conftest"],
)

py_test_module_list(
    files=[
        "tests/test_e2e.py",
    ],
    size="large",
    tags=["team:core", "exclusive"],
    deps=["//:ray_lib", ":conftest"],
)
