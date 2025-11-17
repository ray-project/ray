load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//bazel:python.bzl", "py_test_run_all_subdirectory")

py_library(
    name = "conftest",
    srcs = ["conftest.py"],
)

py_test_run_all_subdirectory(
    size = "small",
    include = glob(["test_*.py"]),
    # test_runtime_env_validation.py requires data files so it has its own target below.
    exclude = ["test_runtime_env_validation.py"],
    extra_srcs = [],
    tags = [
        "exclusive",
        "small_size_python_tests",
        "team:core",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_runtime_env_validation",
    size = "small",
    srcs = ["test_runtime_env_validation.py"],
    data = glob([
        "test_runtime_env_validation_*_schema.json",
    ]),
    tags = [
        "exclusive",
        "small_size_python_tests",
        "team:core",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)
