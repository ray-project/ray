load("//bazel:python.bzl", "py_test_run_all_subdirectory")
load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "air_examples",
    srcs = glob([
        "*.ipynb",
        "*.py",
    ]),
    visibility = ["//doc:__subpackages__"],
)

# --------------------------------------------------------------------
# Test all doc/source/ray-air/examples notebooks.
# --------------------------------------------------------------------

# GPU Tests

py_test_run_all_notebooks(
    size = "large",
    include = [
        "gptj_serving.ipynb",
    ],
    data = ["//doc/source/ray-air/examples:air_examples"],
    exclude = [],
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:ml",
    ],
)
