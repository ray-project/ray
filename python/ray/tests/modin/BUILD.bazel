load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "test_modin",
    size = "small",
    srcs = ["test_modin.py"],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/tests:conftest",
    ],
)
