load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//bazel:python.bzl", "py_test_module_list")

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
)

py_test_module_list(
    size = "small",
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    files = glob(
        ["tests/test_*.py"],
        exclude = ["tests/test_elastic_e2e.py"],
    ),
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_elastic_e2e",
    size = "medium",
    srcs = ["tests/test_elastic_e2e.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)
