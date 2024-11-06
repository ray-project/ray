load("@rules_python//python:defs.bzl", "py_library")

package(default_visibility = ["//visibility:public"])

py_library(
    name = "validation",
    srcs = ["validation.py"],
)

py_library(
    name = "physical_mode_context",
    srcs = ["physical_mode_context.py"],
)

py_library(
    name = "physical_mode_execution_utils",
    srcs = ["physical_mode_execution_utils.py"],
    deps = [
        ":physical_mode_context",
    ],
)
