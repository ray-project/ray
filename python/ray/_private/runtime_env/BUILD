load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = ["//visibility:public"])

py_library(
    name = "validation",
    srcs = ["validation.py"],
)

py_test(
    name = "validation_test",
    srcs = ["validation_test.py"],
    tags = ["team:core"],
    deps = [
        ":validation",
    ],
)
