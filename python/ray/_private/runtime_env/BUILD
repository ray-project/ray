# TODO(hjiang): All existing pythons are not using bazel as build system, which leads to missing BUILD file and targets.
# Revisit if we decide to support bazel build in the future.

load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = ["//visibility:public"])

py_library(
    name = "validation",
    srcs = ["validation.py"],
)

py_library(
    name = "utils",
    srcs = ["utils.py"],
)

py_library(
    name = "virtualenv_utils",
    srcs = ["virtualenv_utils.py"],
    deps = [
        ":utils",
    ],
)

py_library(
    name = "dependency_utils",
    srcs = ["dependency_utils.py"],
    deps = [
        ":utils",
    ],
)

py_library(
    name = "uv",
    srcs= ["uv.py"],
    deps = [
        ":utils",
        ":virtualenv_utils",
        ":dependency_utils",
    ],
)
