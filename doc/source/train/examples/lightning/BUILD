load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "lightning_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"],
)

# GPU tests
py_test_run_all_notebooks(
    size = "large",
    include = ["*.ipynb"],
    data = ["//doc/source/train/examples/lightning:lightning_examples"],
    exclude = [
        "dolly_lightning_fsdp_finetuning.ipynb",  # Release Test
        "vicuna_13b_lightning_deepspeed_finetune.ipynb",  # Release Test
    ],
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:ml",
    ],
)
