# --------------------------------------------------------------------
# Tests from the python/ray/ml/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "custom_trainer",
    size = "small",
    main = "examples/custom_trainer.py",
    srcs = ["examples/custom_trainer.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test (
    name = "lightgbm_example",
    size = "medium",
    srcs = ["examples/lightgbm_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "tensorflow_linear_dataset_example",
    size = "medium",
    main = "examples/tensorflow/tensorflow_linear_dataset_example.py",
    srcs = ["examples/tensorflow/tensorflow_linear_dataset_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "tensorflow_mnist_example",
    size = "medium",
    main = "examples/tensorflow/tensorflow_mnist_example.py",
    srcs = ["examples/tensorflow/tensorflow_mnist_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "torch_fashion_mnist_example",
    size = "medium",
    main = "examples/pytorch/torch_fashion_mnist_example.py",
    srcs = ["examples/pytorch/torch_fashion_mnist_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "torch_fashion_mnist_example_gpu",
    size = "medium",
    main = "examples/pytorch/torch_fashion_mnist_example.py",
    srcs = ["examples/pytorch/torch_fashion_mnist_example.py"],
    tags = ["team:ml", "exclusive", "gpu"],
    deps = [":ml_lib"],
    args = ["--use-gpu"]
)

py_test(
    name = "torch_linear_dataset_example",
    size = "medium",
    main = "examples/pytorch/torch_linear_dataset_example.py",
    srcs = ["examples/pytorch/torch_linear_dataset_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "torch_linear_example",
    size = "medium",
    main = "examples/pytorch/torch_linear_example.py",
    srcs = ["examples/pytorch/torch_linear_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "tune_tensorflow_mnist_example",
    size = "medium",
    main = "examples/tensorflow/tune_tensorflow_mnist_example.py",
    srcs = ["examples/tensorflow/tune_tensorflow_mnist_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "tune_torch_linear_dataset_example.py",
    size = "medium",
    main = "examples/pytorch/tune_torch_linear_dataset_example.py",
    srcs = ["examples/pytorch/tune_torch_linear_dataset_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "xgboost_example",
    size = "medium",
    srcs = ["examples/xgboost_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

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
    name = "test_data_parallel_trainer",
    size = "medium",
    srcs = ["tests/test_data_parallel_trainer.py"],
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
    name = "test_lightgbm_trainer",
    size = "medium",
    srcs = ["tests/test_lightgbm_trainer.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_predictor",
    size = "small",
    srcs = ["tests/test_predictor.py"],
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
    name = "test_tensorflow_trainer",
    size = "medium",
    srcs = ["tests/test_tensorflow_trainer.py"],
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
    name = "test_torch_trainer",
    size = "medium",
    srcs = ["tests/test_torch_trainer.py"],
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

py_test(
    name = "test_xgboost_trainer",
    size = "medium",
    srcs = ["tests/test_xgboost_trainer.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "ml_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
