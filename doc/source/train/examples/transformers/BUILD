load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name="huggingface_transformers_examples",
    srcs=glob(["*.ipynb"]),
    visibility=["//doc:__subpackages__"],
)

# GPU Tests
py_test_run_all_notebooks(
    size = "large",
    include = ["*.ipynb"],
    exclude = [],
    data=["//doc/source/train/examples/transformers:huggingface_transformers_examples"],
    tags=["exclusive", "team:ml", "gpu", "ray_air"],
)