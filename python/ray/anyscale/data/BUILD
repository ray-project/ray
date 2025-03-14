load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
    size = "medium",
    files = glob(
        [
            "tests/test_*.py",
            "tests/*/test_*.py",
        ],
        exclude = [
            "tests/test_checkpoint.py",
            "tests/test_snowflake.py",
            "tests/test_delta.py",
        ],
    ),
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_snowflake",
    size = "medium",
    srcs = ["tests/test_snowflake.py"],
    tags = [
        "exclusive",
        "manual",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_checkpoint",
    size = "large",
    srcs = ["tests/test_checkpoint.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
)

py_test(
    name = "test_delta",
    size = "small",
    srcs = ["tests/test_delta.py"],
    tags = [
        "exclusive",
        "manual",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)
