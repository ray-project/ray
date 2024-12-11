load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
    files = glob(
        ["tests/test_*.py"],
        exclude=["tests/test_checkpoint.py"]
    ),
    size = "medium",
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_checkpoint",
    size = "large",
    srcs = ["tests/test_checkpoint.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
)
