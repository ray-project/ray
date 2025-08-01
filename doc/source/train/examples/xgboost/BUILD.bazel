load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "xgboost_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"],
)

py_test_run_all_notebooks(
    size = "medium",
    include = ["*.ipynb"],
    data = ["//doc/source/train/examples/xgboost:xgboost_examples"],
    exclude = [],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
)
