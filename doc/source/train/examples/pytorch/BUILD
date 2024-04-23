load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "train_pytorch_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"],
)

# GPU Tests
py_test_run_all_notebooks(
    size = "large",
    include = ["*.ipynb"],
    data = ["//doc/source/train/examples/pytorch:train_pytorch_examples"],
    exclude = ["convert_existing_pytorch_code_to_ray_train.ipynb"],  # CPU test
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:ml",
    ],
)

# CPU Tests
py_test_run_all_notebooks(
    size = "large",
    include = ["convert_existing_pytorch_code_to_ray_train.ipynb"],
    data = ["//doc/source/train/examples/pytorch:train_pytorch_examples"],
    exclude = [],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
)
