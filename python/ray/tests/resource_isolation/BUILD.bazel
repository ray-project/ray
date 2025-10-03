load("@rules_python//python:defs.bzl", "py_library", "py_test")

py_library(
    name = "conftest",
    srcs = ["conftest.py"],
    deps = ["//python/ray/tests:conftest"],
)

# TODO(#54703): The tests in this file are being tagged
# as manual because they shouldn't be run as part of
# bazel test //python/ray/tests/...
py_test(
    name = "test_resource_isolation_integration",
    size = "medium",
    srcs = ["test_resource_isolation_integration.py"],
    tags = [
        "cgroup",
        "exclusive",
        "manual",
        "team:core",
    ],
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_resource_isolation_config",
    size = "medium",
    srcs = ["test_resource_isolation_config.py"],
    tags = [
        "exclusive",
        "manual",
        "team:core",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)
