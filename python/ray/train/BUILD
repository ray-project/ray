# --------------------------------------------------------------------
# Tests from the python/ray/train/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"]
)

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
    size = "small",
    main = "examples/mlflow_simple_example.py",
    srcs = ["examples/mlflow_simple_example.py"],
    tags = ["team:ml", "exclusive", "no_main"],
    deps = [":train_lib"],
)

py_test(
    name = "tensorflow_quick_start",
    size = "medium",
    main = "examples/tf/tensorflow_quick_start.py",
    srcs = ["examples/tf/tensorflow_quick_start.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "torch_quick_start",
    size = "small",
    main = "examples/pytorch/torch_quick_start.py",
    srcs = ["examples/pytorch/torch_quick_start.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "transformers_example_gpu",
    size = "medium",
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
    size = "medium",
    main = "examples/transformers/transformers_example.py",
    srcs = ["examples/transformers/transformers_example.py"],
    tags = ["team:ml", "exclusive", "tune"],
    deps = [":train_lib"],
    args = ["--model_name_or_path=bert-base-cased", "--task_name=mrpc",
    "--max_length=32", "--per_device_train_batch_size=64",
    "--max_train_steps=2", "--start_local", "--num_workers=2"]
)

py_test(
    name = "tune_cifar_torch_pbt_example",
    size = "medium",
    main = "examples/pytorch/tune_cifar_torch_pbt_example.py",
    srcs = ["examples/pytorch/tune_cifar_torch_pbt_example.py"],
    tags = ["team:ml", "exclusive", "pytorch", "tune"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "tune_torch_regression_example",
    size = "small",
    main = "examples/pytorch/tune_torch_regression_example.py",
    srcs = ["examples/pytorch/tune_torch_regression_example.py"],
    tags = ["team:ml", "exclusive", "tune"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

# Formerly AIR examples

py_test(
    name = "distributed_sage_example",
    size = "small",
    main = "examples/pytorch_geometric/distributed_sage_example.py",
    srcs = ["examples/pytorch_geometric/distributed_sage_example.py"],
    tags = ["team:ml", "exclusive", "gpu"],
    deps = [":train_lib"],
    args = ["--use-gpu", "--num-workers=2", "--epochs=1", "--dataset=fake"]
)

py_test(
    name = "horovod_cifar_pbt_example",
    size = "small",
    srcs = ["examples/horovod/horovod_cifar_pbt_example.py"],
    tags = ["team:ml", "exlusive"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "horovod_pytorch_example",
    size = "small",
    srcs = ["examples/horovod/horovod_pytorch_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"],
    args = ["--num-epochs=1"]
)

py_test(
    name = "horovod_tune_example",
    size = "small",
    srcs = ["examples/horovod/horovod_tune_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

py_test (
    name = "huggingface_basic_language_modeling_example",
    size = "medium",
    srcs = ["examples/huggingface/huggingface_basic_language_modeling_example.py"],
    args = ["--smoke-test", "--num-epochs 3"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "tensorflow_regression_example",
    size = "medium",
    main = "examples/tf/tensorflow_regression_example.py",
    srcs = ["examples/tf/tensorflow_regression_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

# This is tested in test_examples!
# py_test(
#     name = "tensorflow_mnist_example",
#     size = "medium",
#     main = "examples/tf/tensorflow_mnist_example.py",
#     srcs = ["examples/tf/tensorflow_mnist_example.py"],
#     tags = ["team:ml", "exclusive"],
#     deps = [":train_lib"],
#     args = ["--smoke-test"]
# )

# This is tested in test_examples!
# py_test(
#     name = "torch_fashion_mnist_example",
#     size = "medium",
#     main = "examples/pytorch/torch_fashion_mnist_example.py",
#     srcs = ["examples/pytorch/torch_fashion_mnist_example.py"],
#     tags = ["team:ml", "exclusive"],
#     deps = [":train_lib"],
#     args = ["--smoke-test"]
# )

# This is tested in test_gpu_examples!
# py_test(
#     name = "torch_fashion_mnist_example_gpu",
#     size = "medium",
#     main = "examples/pytorch/torch_fashion_mnist_example.py",
#     srcs = ["examples/pytorch/torch_fashion_mnist_example.py"],
#     tags = ["team:ml", "exclusive", "gpu"],
#     deps = [":train_lib"],
#     args = ["--use-gpu"]
# )

py_test(
    name = "torch_regression_example",
    size = "medium",
    main = "examples/pytorch/torch_regression_example.py",
    srcs = ["examples/pytorch/torch_regression_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

# This is tested in test_examples!
# py_test(
#     name = "torch_linear_example",
#     size = "small",
#     main = "examples/pytorch/torch_linear_example.py",
#     srcs = ["examples/pytorch/torch_linear_example.py"],
#     tags = ["team:ml", "exclusive"],
#     deps = [":train_lib"],
#     args = ["--smoke-test"]
# )

py_test(
    name = "tune_tensorflow_mnist_example",
    size = "medium",
    main = "examples/tf/tune_tensorflow_mnist_example.py",
    srcs = ["examples/tf/tune_tensorflow_mnist_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

# --------------------------------------------------------------------
# Tests from the python/ray/train/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "pytorch_pbt_failure",
    size = "small",
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
    deps = [":train_lib",  ":conftest"]
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
    size = "medium",
    srcs = ["tests/test_batch_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air", "gpu"],
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
    name = "test_checkpoints",
    size = "small",
    srcs = ["tests/test_checkpoints.py"],
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
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_gpu",
    size = "large",
    srcs = ["tests/test_gpu.py"],
    tags = ["team:ml", "exclusive", "gpu_only"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_gpu_auto_transfer",
    size = "medium",
    srcs = ["tests/test_gpu_auto_transfer.py"],
    tags = ["team:ml", "exclusive", "gpu_only"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_gpu_examples",
    size = "large",
    srcs = ["tests/test_gpu_examples.py"],
    tags = ["team:ml", "exclusive", "gpu_only"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_gpu_amp",
    size = "large",
    srcs = ["tests/test_gpu_amp.py"],
    tags = ["team:ml", "exclusive", "gpu_only"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_torch_fsdp",
    size = "small",
    srcs = ["tests/test_torch_fsdp.py"],
    tags = ["team:ml", "exclusive", "gpu_only", "torch_1_11"],
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
    name = "test_huggingface_checkpoint",
    size = "small",
    srcs = ["tests/test_huggingface_checkpoint.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_huggingface_predictor",
    size = "medium",
    srcs = ["tests/test_huggingface_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_huggingface_trainer",
    size = "large",
    srcs = ["tests/test_huggingface_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_huggingface_trainer_steps",
    size = "large",
    srcs = ["tests/test_huggingface_trainer_steps.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_mosaic_trainer",
    size = "medium",
    srcs = ["tests/test_mosaic_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air", "torch_1_11"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_lightgbm_predictor",
    size = "small",
    srcs = ["tests/test_lightgbm_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib", ":conftest"]
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
    name = "test_rl_trainer",
    size = "medium",
    srcs = ["tests/test_rl_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_rl_predictor",
    size = "small",
    srcs = ["tests/test_rl_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_session",
    size = "small",
    srcs = ["tests/test_session.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_sklearn_predictor",
    size = "small",
    srcs = ["tests/test_sklearn_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_sklearn_trainer",
    size = "medium",
    srcs = ["tests/test_sklearn_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_tensorflow_checkpoint",
    size = "small",
    srcs = ["tests/test_tensorflow_checkpoint.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_tensorflow_predictor",
    size = "small",
    srcs = ["tests/test_tensorflow_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air", "gpu"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_tensorflow_trainer",
    size = "medium",
    srcs = ["tests/test_tensorflow_trainer.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_torch_checkpoint",
    size = "small",
    srcs = ["tests/test_torch_checkpoint.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_torch_predictor",
    size = "small",
    srcs = ["tests/test_torch_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air", "gpu"],
    deps = [":train_lib", ":conftest"]
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
    name = "test_training_iterator",
    size = "large",
    srcs = ["tests/test_training_iterator.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_tune",
    size = "large",
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
    deps = [":train_lib", ":conftest"]
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
    visibility = [
        "//python/ray/air:__pkg__",
        "//python/ray/air:__subpackages__",
        "//python/ray/train:__pkg__",
        "//python/ray/train:__subpackages__",
    ],
)
