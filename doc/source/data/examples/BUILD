load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "data_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"]
)

# --------------------------------------------------------------------
# Test all doc/source/data/examples notebooks.
# --------------------------------------------------------------------

py_test_run_all_notebooks(
    size = "large",
    include = ["*.ipynb"],
        exclude = [],
        data = ["//doc/source/data/examples:data_examples"],
    tags = ["exclusive", "team:data", "gpu"],
)
