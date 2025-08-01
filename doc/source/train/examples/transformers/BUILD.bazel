load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "huggingface_transformers_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"],
)

# GPU Tests
py_test_run_all_notebooks(
    size = "large",
    include = ["*.ipynb"],
    data = ["//doc/source/train/examples/transformers:huggingface_transformers_examples"],
    exclude = [],
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:ml",
    ],
)
