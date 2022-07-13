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
        "torch_incremental_learning.ipynb",
        "feast_example.ipynb",  # REGRESSION
        "rl_offline_example.ipynb",  # REGRESSION
        "rl_online_example.ipynb",  # REGRESSION
        "convert_existing_pytorch_code_to_ray_air.ipynb", # REGRESSION
        "tfx_tabular_train_to_serve.ipynb", # REGRESSION
        "torch_image_example.ipynb", # REGRESSION
    ],
    data = ["//doc/source/ray-air/examples:air_examples"],
    tags = ["exclusive", "team:ml", "ray_air"],
)


# GPU Tests

py_test_run_all_notebooks(
    size = "large",
    include = ["huggingface_text_classification.ipynb"],
    exclude = [
        "torch_incremental_learning.ipynb" # REGRESSION!
    ],
    data = ["//doc/source/ray-air/examples:air_examples"],
    tags = ["exclusive", "team:ml", "gpu", "ray_air"],
)
