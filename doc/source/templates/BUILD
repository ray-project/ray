load("//bazel:python.bzl", "py_test_run_all_notebooks")


# --------------------------------------------------------------------
# Test all workspace template notebooks as smoke tests.
# --------------------------------------------------------------------

py_test_run_all_notebooks(
    size = "large",
    include = ["tests/batch_inference.ipynb", "tests/serving_stable_diffusion.ipynb"],
    exclude = [],
    tags = ["exclusive", "team:ml", "ray_air", "gpu", "doc_tested"],
    env = {"SMOKE_TEST": "1"},
)

py_test_run_all_notebooks(
    size = "large",
    include = ["tests/many_model_training.ipynb"],
    exclude = [],
    tags = ["exclusive", "team:ml", "ray_air", "timeseries_libs", "doc_tested"],
    env = {"SMOKE_TEST": "1"},
)
