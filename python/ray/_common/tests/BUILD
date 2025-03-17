load("//bazel:python.bzl", "py_test_module_list")

# Small tests.
py_test_module_list(
    size = "small",
    files = [
        "test_utils.py",
    ],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = [
        "//:ray_lib",
    ],
)
