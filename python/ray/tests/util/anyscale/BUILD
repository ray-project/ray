load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "zmq_channel_test",
    size = "small",
    srcs = ["zmq_channel_test.py"],
    tags = [
        "exclusive",
        "no_main",
        "no_windows",
        "small_size_python_tests",
        "team:core",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/tests:conftest",
    ],
)
