load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "tune_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"]
)

# --------------------------------------------------------------------
# Test all doc/source/tune/examples notebooks.
# --------------------------------------------------------------------

py_test_run_all_notebooks(
    size = "medium",
    include = ["*.ipynb"],
    exclude = [],
    data = ["//doc/source/tune/examples:tune_examples"],
    tags = ["exclusive", "team:ml"],
)