load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "test_ludwig",
    size = "medium",
    srcs = ["test_ludwig.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = ["//:ray_lib"],
)
