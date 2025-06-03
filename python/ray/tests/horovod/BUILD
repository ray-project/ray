load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "test_horovod",
    size = "medium",
    srcs = ["test_horovod.py"],
    tags = [
        "compat",
        "exclusive",
        "manual",
        "team:ml",
    ],
    deps = ["//:ray_lib"],
)
