load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "test_runtime_env_validation",
    srcs = ["test_runtime_env_validation.py"],
    tags = ["team:core"],
    deps = [
        "//python/ray/_private/runtime_env:validation",
    ],
)

py_test(
    name = "test_runtime_env_uv",
    srcs = ["test_runtime_env_uv.py"],
    tags = ["team:core"],
    deps = [
        "//python/ray/_private/runtime_env:uv",
    ],
)
