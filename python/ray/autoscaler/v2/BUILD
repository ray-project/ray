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
        "test_instance_storage",
        "test_storage",
        "test_config",
        "test_node_provider",
        "test_ray_installer",
        "test_subscribers",
        "test_reconciler",
        "test_threaded_ray_installer",
        "test_utils",
        "test_scheduler",
        "test_instance_manager",
        "test_instance_util",
        "test_schema",
        "test_event_logger",
        "test_metrics_reporter",
    ],
    size = "small",
    tags = ["team:core"],
    deps = ["//:ray_lib", ":conftest"],
)

# Integration test.
py_test_module_list(
    files=[
        "test_sdk",
        "test_e2e",
        "test_autoscaler",
    ],
    size="medium",
    tags=["team:core", "exclusive"],
    deps=[":ray_lib", ":conftest"],
)
