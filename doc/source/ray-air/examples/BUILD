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
        "automl_with_ray_air.ipynb",  # Requires timeseries_libs
        "batch_forecasting.ipynb",  # Requires timeseries_libs
        "huggingface_text_classification.ipynb",
        "feast_example.ipynb",  # REGRESSION
        "upload_to_comet_ml.ipynb",  # Needs credentials
        "upload_to_wandb.ipynb",  # Needs credentials
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

py_test_run_all_notebooks(
    size = "medium",
    include = [
        "automl_with_ray_air.ipynb",
        "batch_forecasting.ipynb",
    ],
    exclude = [],
    data = ["//doc/source/ray-air/examples:air_examples"],
    tags = ["exclusive", "team:ml", "ray_air", "timeseries_libs"],
)

# GPU Tests

py_test_run_all_notebooks(
    size = "large",
    include = ["huggingface_text_classification.ipynb"],
    exclude = [],
    data = ["//doc/source/ray-air/examples:air_examples"],
    tags = ["exclusive", "team:ml", "gpu", "ray_air"],
)
