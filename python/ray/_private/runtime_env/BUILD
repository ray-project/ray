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
    name = "path_utils",
    srcs = ["path_utils.py"],
    deps = [
        ":env_utils",
    ],
)

py_library(
    name = "env_utils",
    srcs = ["env_utils.py"],
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
        ":env_utils",
        ":path_utils",
        ":dependency_utils",
    ],
)
