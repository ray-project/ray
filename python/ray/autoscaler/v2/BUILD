# --------------------------------------------------------------------
# Tests from the python/ray/autoscaler/v2/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
load("//bazel:python.bzl", "py_test_module_list")

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