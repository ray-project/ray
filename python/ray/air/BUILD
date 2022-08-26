# --------------------------------------------------------------------
# Tests from the python/ray/air/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "custom_trainer",
    size = "small",
    main = "examples/custom_trainer.py",
    srcs = ["examples/custom_trainer.py"],
    tags = ["team:ml", "exclusive", "no_main"],
    deps = [":ml_lib"]
)

py_test(
    name = "check_ingest",
    size = "large",
    main = "util/check_ingest.py",
    srcs = ["util/check_ingest.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)


py_test(
    name = "distributed_sage_example",
    size = "large",
    main = "examples/pytorch_geometric/distributed_sage_example.py",
    srcs = ["examples/pytorch_geometric/distributed_sage_example.py"],
    tags = ["team:ml", "exclusive", "gpu"],
    deps = [":ml_lib"],
    args = ["--use-gpu", "--num-workers=2", "--epochs=1", "--dataset=fake"]
)

py_test(
    name = "horovod_cifar_pbt_example",
    size = "medium",
    srcs = ["examples/horovod/horovod_cifar_pbt_example.py"],
    tags = ["team:ml", "exlusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "horovod_pytorch_example",
    size = "medium",
    srcs = ["examples/horovod/horovod_pytorch_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--num-epochs=1"]
)

py_test(
    name = "horovod_tune_example",
    size = "medium",
    srcs = ["examples/horovod/horovod_tune_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test (
    name = "huggingface_basic_language_modeling_example",
    size = "medium",
    srcs = ["examples/huggingface/huggingface_basic_language_modeling_example.py"],
    args = ["--smoke-test", "--num-epochs 3"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "tensorflow_regression_example",
    size = "medium",
    main = "examples/tf/tensorflow_regression_example.py",
    srcs = ["examples/tf/tensorflow_regression_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "tensorflow_mnist_example",
    size = "medium",
    main = "examples/tf/tensorflow_mnist_example.py",
    srcs = ["examples/tf/tensorflow_mnist_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

# py_test(
#     name = "tensorflow_autoencoder_example", # REGRESSION
#     size = "medium",
#     main = "examples/tf/tensorflow_autoencoder_example.py",
#     srcs = ["examples/tf/tensorflow_autoencoder_example.py"],
#     tags = ["team:ml", "exclusive"],
#     deps = [":ml_lib"],
#     args = ["--smoke-test"]
# )

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
    name = "torch_regression_example",
    size = "medium",
    main = "examples/pytorch/torch_regression_example.py",
    srcs = ["examples/pytorch/torch_regression_example.py"],
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
    main = "examples/tf/tune_tensorflow_mnist_example.py",
    srcs = ["examples/tf/tune_tensorflow_mnist_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "tune_torch_regression_example.py",
    size = "medium",
    main = "examples/pytorch/tune_torch_regression_example.py",
    srcs = ["examples/pytorch/tune_torch_regression_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"],
    args = ["--smoke-test"]
)

# --------------------------------------------------------------------
# Tests from the python/ray/air/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_api",
    size = "small",
    srcs = ["tests/test_api.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_checkpoints",
    size = "small",
    srcs = ["tests/test_checkpoints.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_checkpoint_manager",
    size = "small",
    srcs = ["tests/test_checkpoint_manager.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_configs",
    size = "small",
    srcs = ["tests/test_configs.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_data_batch_conversion",
    size = "small",
    srcs = ["tests/test_data_batch_conversion.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_dataset_config",
    size = "medium",
    srcs = ["tests/test_dataset_config.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_keras_callback",
    size = "small",
    srcs = ["tests/test_keras_callback.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_mlflow",
    size = "medium",
    srcs = ["tests/test_mlflow.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_remote_storage",
    size = "small",
    srcs = ["tests/test_remote_storage.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_resource_changing",
    size = "medium",
    srcs = ["tests/test_resource_changing.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_tensor_extension",
    size = "small",
    srcs = ["tests/test_tensor_extension.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_tracebacks",
    size = "small",
    srcs = ["tests/test_tracebacks.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "ml_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
    visibility = [
        "//python/ray/air:__pkg__",
        "//python/ray/air:__subpackages__",
        "//python/ray/train:__pkg__",
        "//python/ray/train:__subpackages__",
    ],
)
