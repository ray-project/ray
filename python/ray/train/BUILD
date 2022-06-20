# --------------------------------------------------------------------
# Tests from the python/ray/train/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "mlflow_fashion_mnist_example",
    size = "medium",
    main = "examples/mlflow_fashion_mnist_example.py",
    srcs = ["examples/mlflow_fashion_mnist_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "mlflow_simple_example",
    size = "medium",
    main = "examples/mlflow_simple_example.py",
    srcs = ["examples/mlflow_simple_example.py"],
    tags = ["team:ml", "exclusive", "no_main"],
    deps = [":train_lib"],
)

py_test(
    name = "tensorflow_quick_start",
    size = "medium",
    main = "examples/tensorflow_quick_start.py",
    srcs = ["examples/tensorflow_quick_start.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "torch_quick_start",
    size = "medium",
    main = "examples/torch_quick_start.py",
    srcs = ["examples/torch_quick_start.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "torch_tensorboard_profiler_example",
    size = "small",
    main = "examples/torch_tensorboard_profiler_example.py",
    srcs = ["examples/torch_tensorboard_profiler_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "transformers_example_gpu",
    size = "large",
    main = "examples/transformers/transformers_example.py",
    srcs = ["examples/transformers/transformers_example.py"],
    tags = ["team:ml", "exclusive", "tune", "gpu_only"],
    deps = [":train_lib"],
    args = ["--model_name_or_path=bert-base-cased", "--task_name=mrpc",
    "--max_length=32", "--per_device_train_batch_size=64",
    "--max_train_steps=2", "--start_local", "--num_workers=2", "--use_gpu"]
)

py_test(
    name = "transformers_example_cpu",
    size = "large",
    main = "examples/transformers/transformers_example.py",
    srcs = ["examples/transformers/transformers_example.py"],
    tags = ["team:ml", "exclusive", "tune"],
    deps = [":train_lib"],
    args = ["--model_name_or_path=bert-base-cased", "--task_name=mrpc",
    "--max_length=32", "--per_device_train_batch_size=64",
    "--max_train_steps=2", "--start_local", "--num_workers=2"]
)

py_test(
    name = "tune_cifar_pytorch_pbt_example",
    size = "medium",
    main = "examples/tune_cifar_pytorch_pbt_example.py",
    srcs = ["examples/tune_cifar_pytorch_pbt_example.py"],
    tags = ["team:ml", "exclusive", "pytorch", "tune"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "tune_linear_dataset_example",
    size = "medium",
    main = "examples/tune_linear_dataset_example.py",
    srcs = ["examples/tune_linear_dataset_example.py"],
    tags = ["team:ml", "exclusive", "gpu_only", "tune"],
    deps = [":train_lib"],
    args = ["--smoke-test", "--use-gpu"]
)

py_test(
    name = "tune_linear_example",
    size = "medium",
    main = "examples/tune_linear_example.py",
    srcs = ["examples/tune_linear_example.py"],
    tags = ["team:ml", "exclusive", "tune"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

# --------------------------------------------------------------------
# Tests from the python/ray/train/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "pytorch_pbt_failure",
    size = "medium",
    srcs = ["tests/pytorch_pbt_failure.py"],
    tags = ["team:ml", "exlusive", "no_main"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "test_backend",
    size = "large",
    srcs = ["tests/test_backend.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_base_trainer",
    size = "medium",
    srcs = ["tests/test_base_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_batch_predictor",
    size = "small",
    srcs = ["tests/test_batch_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_batchpredictor_runtime",
    size = "medium",
    srcs = ["tests/test_batchpredictor_runtime.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_callbacks",
    size = "medium",
    srcs = ["tests/test_callbacks.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_data_parallel_trainer",
    size = "medium",
    srcs = ["tests/test_data_parallel_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_examples",
    size = "large",
    srcs = ["tests/test_examples.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_gpu",
    size = "large",
    srcs = ["tests/test_gpu.py"],
    tags = ["team:ml", "exclusive", "gpu_only"],
    deps = [":train_lib"]
)

py_test(
    name = "test_horovod_trainer",
    size = "large",
    srcs = ["tests/test_horovod_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_huggingface_predictor",
    size = "medium",
    srcs = ["tests/test_huggingface_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_huggingface_trainer",
    size = "medium",
    srcs = ["tests/test_huggingface_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_lightgbm_predictor",
    size = "small",
    srcs = ["tests/test_lightgbm_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_lightgbm_trainer",
    size = "medium",
    srcs = ["tests/test_lightgbm_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_minimal",
    size = "small",
    srcs = ["tests/test_minimal.py"],
    tags = ["team:ml", "exclusive", "minimal"],
    deps = [":train_lib"]
)

py_test(
    name = "test_predictor",
    size = "small",
    srcs = ["tests/test_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_results_preprocessors",
    size = "small",
    srcs = ["tests/test_results_preprocessors.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_rl_predictor",
    size = "small",
    srcs = ["tests/test_rl_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_session",
    size = "small",
    srcs = ["tests/test_session.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_sklearn_predictor",
    size = "small",
    srcs = ["tests/test_sklearn_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_sklearn_trainer",
    size = "medium",
    srcs = ["tests/test_sklearn_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_trainer",
    size = "large",
    srcs = ["tests/test_trainer.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_tensorflow_predictor",
    size = "small",
    srcs = ["tests/test_tensorflow_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_tensorflow_trainer",
    size = "medium",
    srcs = ["tests/test_tensorflow_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_tensorflow_utils",
    size = "small",
    srcs = ["tests/test_tensorflow_utils.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_torch_predictor",
    size = "small",
    srcs = ["tests/test_torch_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_torch_trainer",
    size = "medium",
    srcs = ["tests/test_torch_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_torch_utils",
    size = "small",
    srcs = ["tests/test_torch_utils.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_tune",
    size = "medium",
    srcs = ["tests/test_tune.py"],
    tags = ["team:ml", "exclusive", "tune"],
    deps = [":train_lib"]
)

py_test(
    name = "test_utils",
    size = "small",
    srcs = ["tests/test_utils.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_worker_group",
    size = "medium",
    srcs = ["tests/test_worker_group.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_xgboost_predictor",
    size = "small",
    srcs = ["tests/test_xgboost_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_xgboost_trainer",
    size = "medium",
    srcs = ["tests/test_xgboost_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "train_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
