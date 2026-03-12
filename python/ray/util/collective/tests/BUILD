load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "test_backend_registry",
    srcs = ["test_backend_registry.py"],
    tags = [
        "ci_unit",
        "team:ci",
    ],
    deps = [
        "//python/ray/util/collective",
    ],
)
