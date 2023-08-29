load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "tune_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"]
)

# --------------------------------------------------------------------
# Test all doc/source/tune/examples notebooks.
# --------------------------------------------------------------------

# pbt_ppo_example.ipynb is not tested right now due to large resource
# requirements

py_test_run_all_notebooks(
    size = "medium",
    include = ["*.ipynb"],
    exclude = [
        "pbt_ppo_example.ipynb",
        "tune-xgboost.ipynb",
        "pbt_transformers.ipynb",  # Transformers uses legacy Tune APIs.
        # TODO(justinvyu): tune_sklearn uses removed experiment analysis method
        "tune-sklearn.ipynb",
        # TODO(krfricke): This depends on the ptl callback PR landing.
        "tune-vanilla-pytorch-lightning.ipynb",
        "sigopt_example.ipynb", # REGRESSION: no credentials
    ],
    data = ["//doc/source/tune/examples:tune_examples"],
    tags = ["exclusive", "team:ml"],
)

# GPU tests
py_test_run_all_notebooks(
    size = "large",
    include = ["tune-xgboost.ipynb"],
    exclude = [],
    data = ["//doc/source/tune/examples:tune_examples"],
    tags = ["exclusive", "team:ml", "gpu"],
)
