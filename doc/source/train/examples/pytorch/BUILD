load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "train_pytorch_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"]
)

# GPU Tests
py_test_run_all_notebooks(
    size = "large",
    include = ["*.ipynb"],
    exclude = ["convert_existing_pytorch_code_to_ray_train.ipynb"], # CPU test
    data=["//doc/source/train/examples/pytorch:train_pytorch_examples"],
    tags=["exclusive", "team:ml", "gpu", "ray_air"],
)

# CPU Tests
py_test_run_all_notebooks(
    size = "large",
    include = ["convert_existing_pytorch_code_to_ray_train.ipynb"],
    exclude = [],
    data=["//doc/source/train/examples/pytorch:train_pytorch_examples"],
    tags=["exclusive", "team:ml", "ray_air"],
)
