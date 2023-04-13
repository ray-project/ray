load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "workspace_templates",
    srcs = glob(["**/*.ipynb"]),
    visibility = ["//doc:__subpackages__"]
)


# --------------------------------------------------------------------
# Test all workspace template notebooks as smoke tests.
# --------------------------------------------------------------------

py_test_run_all_notebooks(
    size = "large",
    include = ["01_batch_inference/*.ipynb", "03_serving_stable_diffusion/*.ipynb"],
    exclude = [],
    data = ["//doc/source/templates:workspace_templates"],
    tags = ["exclusive", "team:ml", "ray_air", "gpu"],
    env = {"SMOKE_TEST": "1"},
)

py_test_run_all_notebooks(
    size = "large",
    include = ["02_many_model_training/*.ipynb"],
    exclude = [],
    data = ["//doc/source/templates:workspace_templates"],
    tags = ["exclusive", "team:ml", "ray_air", "timeseries_libs"],
    env = {"SMOKE_TEST": "1"},
)
