load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name="lightning_examples",
    srcs=glob(["*.ipynb"]),
    visibility=["//doc:__subpackages__"],
)

# GPU tests
py_test_run_all_notebooks(
    size="large",
    include=["*.ipynb"],
    exclude=[
        "vicuna_13b_lightning_deepspeed_finetune.ipynb", # Release Test
    ],
    data=["//doc/source/train/examples/lightning:lightning_examples"],
    tags=["exclusive", "team:ml", "gpu", "ray_air"],
)
