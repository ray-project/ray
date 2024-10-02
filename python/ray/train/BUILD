load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(
        ["**/*.py"],
        exclude=[
            "examples/**",
            "tests/**",
            "horovod/**",  # CI do not have horovod installed
            "mosaic/**",  # CI do not have mosaicml installed
            # GPU tests
            "tensorflow/tensorflow_trainer.py",
            "_internal/session.py",
            "context.py"
        ]
    ),
    size = "large",
    tags = ["team:ml"],
)

doctest(
    files = [
        "tensorflow/tensorflow_trainer.py",
        "_internal/session.py",
        "context.py"
    ],
    size = "large",
    tags = ["team:ml"],
    gpu = True
)

# --------------------------------------------------------------------
# Tests from the python/ray/train/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"]
)

############ Experiment tracking examples start ############

# no credentials needed
py_test(
    name = "lightning_exp_tracking_mlflow",
    size = "small",
    main = "examples/experiment_tracking/lightning_exp_tracking_mlflow.py",
    srcs = ["examples/experiment_tracking/lightning_exp_tracking_mlflow.py", "examples/experiment_tracking/lightning_exp_tracking_model_dl.py"],
    tags = ["team:ml", "exclusive", "no_main", "new_storage"],
    deps = [":train_lib"],
)

py_test(
    name = "lightning_exp_tracking_tensorboard",
    size = "small",
    main = "examples/experiment_tracking/lightning_exp_tracking_tensorboard.py",
    srcs = ["examples/experiment_tracking/lightning_exp_tracking_tensorboard.py", "examples/experiment_tracking/lightning_exp_tracking_model_dl.py"],
    tags = ["team:ml", "exclusive", "no_main", "new_storage"],
    deps = [":train_lib"],
)

py_test(
    name = "torch_exp_tracking_mlflow",
    size = "medium",
    main = "examples/experiment_tracking/torch_exp_tracking_mlflow.py",
    srcs = ["examples/experiment_tracking/torch_exp_tracking_mlflow.py"],
    tags = ["team:ml", "exclusive", "no_main", "new_storage"],
    deps = [":train_lib"],
)

# credentials needed
py_test(
    name = "lightning_exp_tracking_wandb",
    size = "medium",
    main = "examples/experiment_tracking/lightning_exp_tracking_wandb.py",
    srcs = ["examples/experiment_tracking/lightning_exp_tracking_wandb.py", "examples/experiment_tracking/lightning_exp_tracking_model_dl.py"],
    tags = ["team:ml", "exclusive", "no_main", "needs_credentials", "new_storage"],
    deps = [":train_lib"],
)

py_test(
    name = "lightning_exp_tracking_comet",
    size = "medium",
    main = "examples/experiment_tracking/lightning_exp_tracking_comet.py",
    srcs = ["examples/experiment_tracking/lightning_exp_tracking_comet.py", "examples/experiment_tracking/lightning_exp_tracking_model_dl.py"],
    tags = ["team:ml", "exclusive", "no_main", "needs_credentials", "new_storage"],
    deps = [":train_lib"],
)

py_test(
    name = "torch_exp_tracking_wandb",
    size = "medium",
    main = "examples/experiment_tracking/torch_exp_tracking_wandb.py",
    srcs = ["examples/experiment_tracking/torch_exp_tracking_wandb.py"],
    tags = ["team:ml", "exclusive", "no_main", "needs_credentials", "new_storage"],
    deps = [":train_lib"],
)

############ Experiment tracking examples end ############

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
    size = "medium",
    main = "examples/pytorch/torch_quick_start.py",
    srcs = ["examples/pytorch/torch_quick_start.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
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
    tags = ["team:ml", "exlusive", "manual"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "horovod_pytorch_example",
    size = "small",
    srcs = ["examples/horovod/horovod_pytorch_example.py"],
    tags = ["team:ml", "exclusive", "manual"],
    deps = [":train_lib"],
    args = ["--num-epochs=1"]
)

py_test(
    name = "horovod_tune_example",
    size = "small",
    srcs = ["examples/horovod/horovod_tune_example.py"],
    tags = ["team:ml", "exclusive", "manual"],
    deps = [":train_lib"],
    args = ["--smoke-test"]
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
    name = "test_torch_accelerate",
    size = "large",
    srcs = ["tests/test_torch_accelerate.py"],
    tags = ["team:ml", "exclusive", "gpu_only"],
    deps = [":train_lib",  ":conftest"]
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
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_checkpoint",
    size = "small",
    srcs = ["tests/test_checkpoint.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_checkpoint_manager",
    size = "small",
    srcs = ["tests/test_checkpoint_manager.py"],
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
    name = "test_data_parallel_trainer_checkpointing",
    size = "medium",
    srcs = ["tests/test_data_parallel_trainer_checkpointing.py"],
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
    name = "test_gpu_2",
    size = "medium",
    srcs = ["tests/test_gpu_2.py"],
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
    tags = ["team:ml", "exclusive", "ray_air", "manual"],
    deps = [":train_lib"]
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
    name = "test_torch_lightning_train",
    size = "large",
    srcs = ["tests/test_torch_lightning_train.py"],
    tags = ["team:ml", "exclusive", "ray_air", "gpu", "ptl_v2"],
    deps = [":train_lib"]
)

py_test(
    name = "test_torch_transformers_train",
    size = "large",
    srcs = ["tests/test_torch_transformers_train.py"],
    tags = ["team:ml", "exclusive", "ray_air", "gpu"],
    deps = [":train_lib"]
)

py_test(
    name = "accelerate_torch_trainer",
    size = "large",
    srcs = ["examples/accelerate/accelerate_torch_trainer.py"],
    tags = ["team:ml", "exclusive", "gpu"],
    deps = [":train_lib"]
)

py_test(
    name = "accelerate_torch_trainer_no_raydata",
    size = "large",
    srcs = ["examples/accelerate/accelerate_torch_trainer_no_raydata.py"],
    tags = ["team:ml", "exclusive", "gpu"],
    deps = [":train_lib"]
)

py_test(
    name = "deepspeed_torch_trainer",
    size = "large",
    srcs = ["examples/deepspeed/deepspeed_torch_trainer.py"],
    tags = ["team:ml", "exclusive", "gpu"],
    deps = [":train_lib"]
)

py_test(
    name = "deepspeed_torch_trainer_no_raydata",
    size = "large",
    srcs = ["examples/deepspeed/deepspeed_torch_trainer_no_raydata.py"],
    tags = ["team:ml", "exclusive", "gpu"],
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
    name = "test_new_persistence",
    size = "large",
    srcs = ["tests/test_new_persistence.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_predictor",
    size = "small",
    srcs = ["tests/test_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = [":train_lib"]
)

py_test(
    name = "test_result",
    size = "medium",
    srcs = ["tests/test_result.py"],
    tags = ["team:ml", "exclusive"],
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
    name = "test_storage",
    size = "small",
    srcs = ["tests/test_storage.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_state",
    size = "medium",
    srcs = ["tests/test_state.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib", ":conftest"]
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
    size = "medium",
    srcs = ["tests/test_torch_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air", "gpu"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_torch_detection_predictor",
    size = "medium",
    srcs = ["tests/test_torch_detection_predictor.py"],
    tags = ["team:ml", "exclusive", "ray_air", "gpu"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_torch_device_manager",
    size = "small",
    srcs = ["tests/test_torch_device_manager.py"],
    tags = ["team:ml", "exclusive", "ray_air", "gpu_only"],
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_torch_trainer",
    size = "large",
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
    name = "test_train_usage",
    size = "medium",
    srcs = ["tests/test_train_usage.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"],
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
    deps = [":train_lib", ":conftest"]
)

py_test(
    name = "test_utils",
    size = "small",
    srcs = ["tests/test_utils.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_lib"]
)

py_test(
    name = "test_e2e_wandb_integration",
    size = "small",
    srcs = ["tests/test_e2e_wandb_integration.py"],
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
    name = "test_windows",
    size = "small",
    srcs = ["tests/test_windows.py"],
    tags = ["team:ml", "exclusive", "minimal"],
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

py_test(
    name = "test_trainer_restore",
    size = "large",
    srcs = ["tests/test_trainer_restore.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
        "new_storage",
    ],
    deps = [":train_lib", ":conftest"],
)

### E2E Data + Train
py_test(
    name = "test_datasets_train",
    size = "medium",
    srcs = ["tests/test_datasets_train.py"],
    tags = ["exclusive", "team:ml", "datasets_train", "gpu"],
    args = ["--smoke-test", "--num-workers=2", "--use-gpu"]
)

### Train Dashboard
py_test(
    name = "test_train_head",
    size = "small",
    srcs = ["tests/test_train_head.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib", ":conftest"],
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
