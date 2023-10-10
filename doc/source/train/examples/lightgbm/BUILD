load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name="lightgbm_examples",
    srcs=glob(["*.ipynb"]),
    visibility=["//doc:__subpackages__"],
)

py_test_run_all_notebooks(
    size = "medium",
    include = ["*.ipynb"],
    exclude = [],
    data=["//doc/source/train/examples/lightgbm:lightgbm_examples"],
    tags=["exclusive", "team:ml", "ray_air"],
)