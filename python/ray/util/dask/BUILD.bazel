load("@rules_python//python:defs.bzl", "py_library")

py_library(
    name = "dask_lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["tests/*.py"],
    ),
    visibility = ["__subpackages__"],
)
