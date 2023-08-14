load("//bazel:python.bzl", "py_test_run_all_subdirectory")
load("//bazel:python.bzl", "py_test_run_all_notebooks")


filegroup(
    name = "air_examples",
    srcs = glob(["*.ipynb", "*.py"]),
    visibility = ["//doc:__subpackages__"]
)

# --------------------------------------------------------------------
# Test all doc/source/ray-air/examples code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "medium",
    include = ["*.py"],
    exclude = [],
    extra_srcs = [],
    data = ["//doc/source/ray-air/examples:air_examples"],
    tags = ["exclusive", "team:ml", "ray_air"],
)

# --------------------------------------------------------------------
# Test all doc/source/ray-air/examples notebooks.
# --------------------------------------------------------------------

py_test_run_all_notebooks(
    size = "large",
    include = ["*.ipynb"],
    exclude = [
        "huggingface_text_classification.ipynb",
        "upload_to_comet_ml.ipynb",  # Needs credentials
        "upload_to_wandb.ipynb",  # Needs credentials
        "torch_detection.ipynb",  # Requires GPUs
        "gptj_batch_prediction.ipynb",  # Requires GPUs
        "gptj_serving.ipynb",  # Requires GPUs
        "stablediffusion_batch_prediction.ipynb",  # Requires GPUs
        "gptj_deepspeed_fine_tuning.ipynb",  # Requires release test
        "dolly_lightning_fsdp_finetuning.ipynb", # Requires release test
    ],
    data = ["//doc/source/ray-air/examples:air_examples"],
    tags = ["exclusive", "team:ml", "ray_air"],
)

py_test_run_all_notebooks(
    size = "large",
    include = ["upload_to_*.ipynb"],
    exclude = [],
    data = ["//doc/source/ray-air/examples:air_examples"],
    tags = ["exclusive", "team:ml", "ray_air", "needs_credentials"],
)

# GPU Tests

py_test_run_all_notebooks(
    size = "large",
    include = [
        "huggingface_text_classification.ipynb",
        "torch_detection.ipynb",
        "gptj_batch_prediction.ipynb",
        "gptj_serving.ipynb",
        "stablediffusion_batch_prediction.ipynb",
    ],
    exclude = [],
    data = ["//doc/source/ray-air/examples:air_examples"],
    tags = ["exclusive", "team:ml", "gpu", "ray_air"],
)
