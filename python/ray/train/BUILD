load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//bazel:python.bzl", "doctest")

doctest(
    name = "py_doctest[train]",
    size = "large",
    files = glob(
        ["**/*.py"],
        exclude = [
            "collective/**",  # Only works on v2
            "examples/**",
            "tests/**",
            "horovod/**",  # CI do not have horovod installed
            "mosaic/**",  # CI do not have mosaicml installed
            # GPU tests
            "tensorflow/tensorflow_trainer.py",
            "_internal/session.py",
            "context.py",
        ],
    ),
    tags = ["team:ml"],
)

doctest(
    name = "py_doctest[train-gpu]",
    size = "large",
    files = [
        "_internal/session.py",
        "context.py",
        "tensorflow/tensorflow_trainer.py",
    ],
    gpu = True,
    tags = ["team:ml"],
)

# --------------------------------------------------------------------
# Tests from the python/ray/train/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
)

############ Experiment tracking examples start ############

# no credentials needed
py_test(
    name = "lightning_exp_tracking_mlflow",
    size = "small",
    srcs = [
        "examples/experiment_tracking/lightning_exp_tracking_mlflow.py",
        "examples/experiment_tracking/lightning_exp_tracking_model_dl.py",
    ],
    main = "examples/experiment_tracking/lightning_exp_tracking_mlflow.py",
    tags = [
        "exclusive",
        "new_storage",
        "no_main",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "lightning_exp_tracking_tensorboard",
    size = "small",
    srcs = [
        "examples/experiment_tracking/lightning_exp_tracking_model_dl.py",
        "examples/experiment_tracking/lightning_exp_tracking_tensorboard.py",
    ],
    main = "examples/experiment_tracking/lightning_exp_tracking_tensorboard.py",
    tags = [
        "exclusive",
        "new_storage",
        "no_main",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "torch_exp_tracking_mlflow",
    size = "medium",
    srcs = ["examples/experiment_tracking/torch_exp_tracking_mlflow.py"],
    main = "examples/experiment_tracking/torch_exp_tracking_mlflow.py",
    tags = [
        "exclusive",
        "new_storage",
        "no_main",
        "team:ml",
    ],
    deps = [":train_lib"],
)

# credentials needed
py_test(
    name = "lightning_exp_tracking_wandb",
    size = "medium",
    srcs = [
        "examples/experiment_tracking/lightning_exp_tracking_model_dl.py",
        "examples/experiment_tracking/lightning_exp_tracking_wandb.py",
    ],
    main = "examples/experiment_tracking/lightning_exp_tracking_wandb.py",
    tags = [
        "exclusive",
        "needs_credentials",
        "new_storage",
        "no_main",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "lightning_exp_tracking_comet",
    size = "medium",
    srcs = [
        "examples/experiment_tracking/lightning_exp_tracking_comet.py",
        "examples/experiment_tracking/lightning_exp_tracking_model_dl.py",
    ],
    main = "examples/experiment_tracking/lightning_exp_tracking_comet.py",
    tags = [
        "exclusive",
        "needs_credentials",
        "new_storage",
        "no_main",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "torch_exp_tracking_wandb",
    size = "medium",
    srcs = ["examples/experiment_tracking/torch_exp_tracking_wandb.py"],
    main = "examples/experiment_tracking/torch_exp_tracking_wandb.py",
    tags = [
        "exclusive",
        "needs_credentials",
        "new_storage",
        "no_main",
        "team:ml",
    ],
    deps = [":train_lib"],
)

############ Experiment tracking examples end ############

py_test(
    name = "mlflow_simple_example",
    size = "small",
    srcs = ["examples/mlflow_simple_example.py"],
    main = "examples/mlflow_simple_example.py",
    tags = [
        "exclusive",
        "no_main",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "tensorflow_quick_start",
    size = "medium",
    srcs = ["examples/tf/tensorflow_quick_start.py"],
    main = "examples/tf/tensorflow_quick_start.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "torch_quick_start",
    size = "medium",
    srcs = ["examples/pytorch/torch_quick_start.py"],
    main = "examples/pytorch/torch_quick_start.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "tune_cifar_torch_pbt_example",
    size = "medium",
    srcs = ["examples/pytorch/tune_cifar_torch_pbt_example.py"],
    args = ["--smoke-test"],
    main = "examples/pytorch/tune_cifar_torch_pbt_example.py",
    tags = [
        "exclusive",
        "pytorch",
        "team:ml",
        "tune",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "tune_torch_regression_example",
    size = "small",
    srcs = ["examples/pytorch/tune_torch_regression_example.py"],
    args = ["--smoke-test"],
    main = "examples/pytorch/tune_torch_regression_example.py",
    tags = [
        "exclusive",
        "team:ml",
        "tune",
    ],
    deps = [":train_lib"],
)

# Formerly AIR examples

py_test(
    name = "distributed_sage_example",
    size = "small",
    srcs = ["examples/pytorch_geometric/distributed_sage_example.py"],
    args = [
        "--use-gpu",
        "--num-workers=2",
        "--epochs=1",
        "--dataset=fake",
    ],
    main = "examples/pytorch_geometric/distributed_sage_example.py",
    tags = [
        "exclusive",
        "gpu",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "horovod_pytorch_example",
    size = "small",
    srcs = ["examples/horovod/horovod_pytorch_example.py"],
    args = ["--num-epochs=1"],
    tags = [
        "exclusive",
        "manual",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "horovod_tune_example",
    size = "small",
    srcs = ["examples/horovod/horovod_tune_example.py"],
    args = ["--smoke-test"],
    tags = [
        "exclusive",
        "manual",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "tensorflow_regression_example",
    size = "medium",
    srcs = ["examples/tf/tensorflow_regression_example.py"],
    args = ["--smoke-test"],
    main = "examples/tf/tensorflow_regression_example.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
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
    srcs = ["examples/pytorch/torch_regression_example.py"],
    args = ["--smoke-test"],
    main = "examples/pytorch/torch_regression_example.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
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
    srcs = ["examples/tf/tune_tensorflow_mnist_example.py"],
    args = ["--smoke-test"],
    main = "examples/tf/tune_tensorflow_mnist_example.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
)

# --------------------------------------------------------------------
# Tests from the python/ray/train/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_torch_accelerate",
    size = "large",
    srcs = ["tests/test_torch_accelerate.py"],
    tags = [
        "exclusive",
        "gpu_only",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_api_migrations",
    size = "small",
    srcs = ["tests/test_api_migrations.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_backend",
    size = "large",
    srcs = ["tests/test_backend.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_base_trainer",
    size = "medium",
    srcs = ["tests/test_base_trainer.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_checkpoint",
    size = "small",
    srcs = ["tests/test_checkpoint.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_checkpoint_manager",
    size = "small",
    srcs = ["tests/test_checkpoint_manager.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_data_parallel_trainer",
    size = "medium",
    srcs = ["tests/test_data_parallel_trainer.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_data_parallel_trainer_checkpointing",
    size = "medium",
    srcs = ["tests/test_data_parallel_trainer_checkpointing.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_examples",
    size = "large",
    srcs = ["tests/test_examples.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_gpu",
    size = "large",
    srcs = ["tests/test_gpu.py"],
    tags = [
        "exclusive",
        "gpu_only",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_gpu_2",
    size = "medium",
    srcs = ["tests/test_gpu_2.py"],
    tags = [
        "exclusive",
        "gpu_only",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_iter_torch_batches_gpu",
    size = "medium",
    srcs = ["tests/test_iter_torch_batches_gpu.py"],
    tags = [
        "exclusive",
        "gpu_only",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_gpu_auto_transfer",
    size = "medium",
    srcs = ["tests/test_gpu_auto_transfer.py"],
    tags = [
        "exclusive",
        "gpu_only",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_gpu_examples",
    size = "large",
    srcs = ["tests/test_gpu_examples.py"],
    tags = [
        "exclusive",
        "gpu_only",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_torch_fsdp",
    size = "small",
    srcs = ["tests/test_torch_fsdp.py"],
    tags = [
        "exclusive",
        "gpu_only",
        "team:ml",
        "torch_1_11",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_horovod_trainer",
    size = "large",
    srcs = ["tests/test_horovod_trainer.py"],
    tags = [
        "exclusive",
        "manual",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_lightgbm_predictor",
    size = "small",
    srcs = ["tests/test_lightgbm_predictor.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_lightgbm_trainer",
    size = "medium",
    srcs = ["tests/test_lightgbm_trainer.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_torch_lightning_train",
    size = "large",
    srcs = ["tests/test_torch_lightning_train.py"],
    tags = [
        "exclusive",
        "gpu",
        "ptl_v2",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_torch_transformers_train",
    size = "large",
    srcs = ["tests/test_torch_transformers_train.py"],
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "accelerate_torch_trainer",
    size = "large",
    srcs = ["examples/accelerate/accelerate_torch_trainer.py"],
    tags = [
        "exclusive",
        "gpu",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "accelerate_torch_trainer_no_raydata",
    size = "large",
    srcs = ["examples/accelerate/accelerate_torch_trainer_no_raydata.py"],
    tags = [
        "exclusive",
        "gpu",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "deepspeed_torch_trainer",
    size = "large",
    srcs = ["examples/deepspeed/deepspeed_torch_trainer.py"],
    tags = [
        "exclusive",
        "gpu",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "deepspeed_torch_trainer_no_raydata",
    size = "large",
    srcs = ["examples/deepspeed/deepspeed_torch_trainer_no_raydata.py"],
    tags = [
        "exclusive",
        "gpu",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_minimal",
    size = "small",
    srcs = ["tests/test_minimal.py"],
    tags = [
        "exclusive",
        "minimal",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_new_persistence",
    size = "large",
    srcs = ["tests/test_new_persistence.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_predictor",
    size = "small",
    srcs = ["tests/test_predictor.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_result",
    size = "medium",
    srcs = ["tests/test_result.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_session",
    size = "small",
    srcs = ["tests/test_session.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_storage",
    size = "small",
    srcs = ["tests/test_storage.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_state",
    size = "medium",
    srcs = ["tests/test_state.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_state_export",
    size = "medium",
    srcs = ["tests/test_state_export.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_tensorflow_checkpoint",
    size = "small",
    srcs = ["tests/test_tensorflow_checkpoint.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_tensorflow_predictor",
    size = "small",
    srcs = ["tests/test_tensorflow_predictor.py"],
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_tensorflow_trainer",
    size = "medium",
    srcs = ["tests/test_tensorflow_trainer.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_torch_checkpoint",
    size = "small",
    srcs = ["tests/test_torch_checkpoint.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_torch_predictor",
    size = "medium",
    srcs = ["tests/test_torch_predictor.py"],
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_torch_detection_predictor",
    size = "medium",
    srcs = ["tests/test_torch_detection_predictor.py"],
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_torch_device_manager",
    size = "medium",
    srcs = ["tests/test_torch_device_manager.py"],
    tags = [
        "exclusive",
        "gpu_only",
        "ray_air",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_torch_trainer",
    size = "large",
    srcs = ["tests/test_torch_trainer.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_torch_utils",
    size = "small",
    srcs = ["tests/test_torch_utils.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_train_usage",
    size = "medium",
    srcs = ["tests/test_train_usage.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_training_iterator",
    size = "large",
    srcs = ["tests/test_training_iterator.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_tune",
    size = "large",
    srcs = ["tests/test_tune.py"],
    tags = [
        "exclusive",
        "team:ml",
        "tune",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_utils",
    size = "small",
    srcs = ["tests/test_utils.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_e2e_wandb_integration",
    size = "small",
    srcs = ["tests/test_e2e_wandb_integration.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_worker_group",
    size = "medium",
    srcs = ["tests/test_worker_group.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_windows",
    size = "small",
    srcs = ["tests/test_windows.py"],
    tags = [
        "exclusive",
        "minimal",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_xgboost_predictor",
    size = "small",
    srcs = ["tests/test_xgboost_predictor.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_xgboost_trainer",
    size = "medium",
    srcs = ["tests/test_xgboost_trainer.py"],
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [":train_lib"],
)

py_test(
    name = "test_trainer_restore",
    size = "large",
    srcs = ["tests/test_trainer_restore.py"],
    tags = [
        "exclusive",
        "new_storage",
        "ray_air",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

py_test(
    name = "test_telemetry",
    size = "medium",
    srcs = ["tests/test_telemetry.py"],
    tags = ["team:ml"],
    deps = [":train_lib"],
)

### E2E Data + Train
py_test(
    name = "test_datasets_train",
    size = "medium",
    srcs = ["tests/test_datasets_train.py"],
    args = [
        "--smoke-test",
        "--num-workers=2",
        "--use-gpu",
    ],
    tags = [
        "datasets_train",
        "exclusive",
        "gpu",
        "team:ml",
    ],
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
    deps = [
        ":conftest",
        ":train_lib",
    ],
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "train_lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["tests/*.py"],
    ),
    visibility = [
        "//python/ray/air:__pkg__",
        "//python/ray/air:__subpackages__",
        "//python/ray/train:__pkg__",
        "//python/ray/train:__subpackages__",
    ],
)
