load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "test_data_head",
    size = "small",
    srcs = ["data/tests/test_data_head.py"],
    tags = ["team:data"],
    deps = ["//python/ray/dashboard:conftest"],
)
