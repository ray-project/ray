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
        "test_instance_storage.py",
        "test_storage.py",
        "test_config.py",
        "test_node_provider.py",
        "test_ray_installer.py",
        "test_subscribers.py",
        "test_reconciler.py",
        "test_threaded_ray_installer.py",
        "test_utils.py",
        "test_scheduler.py",
        "test_instance_manager.py",
        "test_instance_util.py",
        "test_schema.py",
        "test_event_logger.py",
        "test_metrics_reporter.py",
    ],
    size = "small",
    tags = ["team:core"],
    deps = ["//:ray_lib", ":conftest"],
)

# Integration test.
py_test_module_list(
    files=[
        "test_sdk.py",
        "test_e2e.py",
        "test_autoscaler.py",
    ],
    size="medium",
    tags=["team:core", "exclusive"],
    deps=["//:ray_lib", ":conftest"],
)
