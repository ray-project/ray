load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "usecase_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"]
)

# --------------------------------------------------------------------
# Test all doc/source/ray-overview/examples notebooks.
# --------------------------------------------------------------------

py_test_run_all_notebooks(
    size = "medium",
    include = ["*.ipynb"],
    exclude = [],
    data = ["//doc/source/ray-overview/examples:usecase_examples"],
    tags = ["exclusive", "team:ml"],
)