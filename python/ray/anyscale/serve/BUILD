load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
    files = glob(["tests/test_*.py"]),
    size = "medium",
    tags = ["team:serve", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
)
