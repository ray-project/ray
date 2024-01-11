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

py_test(
    name = "test_instance_storage",
    size = "small",
    srcs = ["tests/test_instance_storage.py"],
    tags = ["team:core"],
    deps = ["//:ray_lib",],
)

py_test(
    name = "test_storage",
    size = "small",
    srcs = ["tests/test_storage.py"],
    tags = ["team:core"],
    deps = ["//:ray_lib",],
)

py_test(
    name = "test_config",
    size = "small",
    srcs = ["tests/test_config.py"],
    tags = ["team:core"],
    deps = [
        "//:ray_lib",
    ]
)

py_test(
    name = "test_node_provider",
    size = "small",
    srcs = ["tests/test_node_provider.py"],
    tags = ["team:core"],
    deps = ["//:ray_lib",],
)

py_test(
    name = "test_ray_installer",
    size = "small",
    srcs = ["tests/test_ray_installer.py"],
    tags = ["team:core"],
    deps = ["//:ray_lib",],
)

py_test(
    name = "test_instance_launcher",
    size = "small",
    srcs = ["tests/test_instance_launcher.py"],
    tags = ["team:core"],
    deps = ["//:ray_lib",],
)

py_test(
    name = "test_reconciler",
    size = "small",
    srcs = ["tests/test_reconciler.py"],
    tags = ["team:core"],
    deps = ["//:ray_lib",],
)

py_test(
    name = "test_threaded_ray_installer",
    size = "small",
    srcs = ["tests/test_threaded_ray_installer.py"],
    tags = ["team:core"],
    deps = ["//:ray_lib",],
)

py_test(
    name = "test_sdk",
    size = "medium",
    srcs = ["tests/test_sdk.py"],
    tags = ["team:core", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_utils",
    size = "small",
    srcs = ["tests/test_utils.py"],
    tags = ["team:core"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_e2e",
    size = "medium",
    srcs = ["tests/test_e2e.py"],
    tags = ["team:core", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_scheduler",
    size = "small",
    srcs = ["tests/test_scheduler.py"],
    tags = ["team:core", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)


py_test(
    name = "test_instance_util",
    size = "small",
    srcs = ["tests/test_instance_util.py"],
    tags = ["team:core", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_schema",
    size = "small",
    srcs = ["tests/test_schema.py"],
    tags = ["team:core", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)
