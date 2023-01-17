load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name="tune_examples", srcs=glob(["*.ipynb"]), visibility=["//doc:__subpackages__"]
)

# --------------------------------------------------------------------
# Test all doc/source/tune/examples notebooks.
# --------------------------------------------------------------------

# pbt_ppo_example.ipynb is not tested right now due to large resource
# requirements

py_test_run_all_notebooks(
    size="medium",
    include=["*.ipynb"],
    exclude=[
        "pbt_ppo_example.ipynb",
        "tune-xgboost.ipynb",
        "sigopt_example.ipynb",  # REGRESSION: no credentials
    ],
    data=["//doc/source/tune/examples:tune_examples"],
    args=["--smoke-test"],
    tags=["exclusive", "team:ml"],
)

# GPU tests
py_test_run_all_notebooks(
    size="large",
    include=["tune-xgboost.ipynb"],
    exclude=[],
    data=["//doc/source/tune/examples:tune_examples"],
    tags=["exclusive", "team:ml", "gpu"],
)
