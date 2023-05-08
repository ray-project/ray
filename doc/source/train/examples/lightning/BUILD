load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name="lightning_examples",
    srcs=glob(["*.ipynb"]),
    visibility=["//doc:__subpackages__"],
)

py_test_run_all_notebooks(
    size="large",
    include=["*.ipynb"],
    exclude=[],
    data=["//doc/source/train/examples/lightning:lightning_examples"],
    tags=["exclusive", "team:ml", "gpu", "ray_air"],
)
