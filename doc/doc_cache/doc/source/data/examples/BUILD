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
    # FIXME: https://github.com/ray-project/ray/issues/36618 and
    # https://github.com/ray-project/ray/issues/36619
    exclude = ["ocr_example.ipynb", "nyc_taxi_basic_processing.ipynb"],
    data = ["//doc/source/data/examples:data_examples"],
    tags = ["exclusive", "team:data", "gpu"],
)
