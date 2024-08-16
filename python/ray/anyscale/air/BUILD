load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
    files = glob(["tests/test_*.py"]),
    size = "small",
    tags = ["team:ml", "exclusive"],
    deps = ["//:ray_lib"],
)

