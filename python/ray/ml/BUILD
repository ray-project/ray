# --------------------------------------------------------------------
# Tests from the python/ray/ml/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_checkpoints",
    size = "small",
    srcs = ["tests/test_checkpoints.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_lightgbm_predictor",
    size = "small",
    srcs = ["tests/test_lightgbm_predictor.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_preprocessors",
    size = "small",
    srcs = ["tests/test_preprocessors.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_trainer",
    size = "medium",
    srcs = ["tests/test_trainer.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_tensorflow_predictor",
    size = "small",
    srcs = ["tests/test_tensorflow_predictor.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_torch_predictor",
    size = "small",
    srcs = ["tests/test_torch_predictor.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_torch_utils",
    size = "small",
    srcs = ["tests/test_torch_utils.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_xgboost_predictor",
    size = "small",
    srcs = ["tests/test_xgboost_predictor.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "ml_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
