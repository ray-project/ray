# --------------------------------------------------------------------
# Tests from the python/ray/util/sgd/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "test_ptl",
    size = "large",
    srcs = ["tests/test_ptl.py"],
    tags = ["exclusive", "pytorch-lightning", "pytorch"],
    deps = [":sgd_lib"],
)

py_test(
    name = "test_tensorflow",
    size = "small",
    srcs = ["tests/test_tensorflow.py"],
    tags = ["exclusive", "tf"],
    deps = [":sgd_lib"],
)

py_test(
    name = "test_torch",
    size = "large",
    srcs = ["tests/test_torch.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
)

py_test(
    name = "test_torch_failure",
    size = "large",
    srcs = ["tests/test_torch_failure.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
)

py_test(
    name = "test_torch_runner",
    size = "small",
    srcs = ["tests/test_torch_runner.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
)

# --------------------------------------------------------------------
# Tests from the python/ray/util/sgd/tf/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "cifar_tf_example_1",
    size = "medium",
    main = "tf/examples/cifar_tf_example.py",
    srcs = ["tf/examples/cifar_tf_example.py"],
    tags = ["exclusive", "tf"],
    deps = [":sgd_lib"],
    args = ["--smoke-test", "--num-replicas=1"]
)

py_test(
    name = "cifar_tf_example_2",
    size = "medium",
    main = "tf/examples/cifar_tf_example.py",
    srcs = ["tf/examples/cifar_tf_example.py"],
    tags = ["exclusive", "tf"],
    deps = [":sgd_lib"],
    args = ["--smoke-test", "--num-replicas=2"]
)

py_test(
    name = "cifar_tf_example_2b",
    size = "small",
    main = "tf/examples/cifar_tf_example.py",
    srcs = ["tf/examples/cifar_tf_example.py"],
    tags = ["exclusive", "tf"],
    deps = [":sgd_lib"],
    args = ["--smoke-test", "--num-replicas=2", "--augment-data"]
)

py_test(
    name = "tensorflow_train_example_1",
    size = "small",
    main = "tf/examples/tensorflow_train_example.py",
    srcs = ["tf/examples/tensorflow_train_example.py"],
    tags = ["exclusive", "tf"],
    deps = [":sgd_lib"],
    args = ["--num-replicas=1", "--smoke-test"]
)

py_test(
    name = "tensorflow_train_example_2",
    size = "small",
    main = "tf/examples/tensorflow_train_example.py",
    srcs = ["tf/examples/tensorflow_train_example.py"],
    tags = ["exclusive", "tf"],
    deps = [":sgd_lib"],
    args = ["--num-replicas=2", "--smoke-test"]
)

py_test(
    name = "tensorflow_train_example_tune",
    size = "small",
    main = "tf/examples/tensorflow_train_example.py",
    srcs = ["tf/examples/tensorflow_train_example.py"],
    tags = ["exclusive", "tf"],
    deps = [":sgd_lib"],
    args = ["--tune", "--smoke-test"]
)

# --------------------------------------------------------------------
# Tests from the python/ray/util/sgd/torch/examples directory.
# Does not include subdirectories.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "cifar_pytorch_example_1",
    size = "medium",
    main = "torch/examples/cifar_pytorch_example.py",
    srcs = ["torch/examples/cifar_pytorch_example.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
    args = ["--smoke-test", "--num-workers=1"]
)

py_test(
    name = "cifar_pytorch_example_2",
    size = "medium",
    main = "torch/examples/cifar_pytorch_example.py",
    srcs = ["torch/examples/cifar_pytorch_example.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
    args = ["--smoke-test", "--num-workers=2"]
)

py_test(
    name = "cifar_pytorch_pbt",
    size = "medium",
    srcs = ["torch/examples/cifar_pytorch_pbt.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
    args = ["--smoke-test"]
)

# py_test(
#     name = "dcgan",
#     size = "small",
#     srcs = ["torch/examples/dcgan.py"],
#     tags = ["exclusive", "pytorch"],
#     deps = [":sgd_lib"],
#     args = ["--smoke-test", "--num-workers=2"]
# )

py_test(
    name = "raysgd_torch_signatures",
    size = "small",
    srcs = ["torch/examples/raysgd_torch_signatures.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"]
)

py_test(
    name = "train_example_1",
    size = "small",
    main = "torch/examples/train_example.py",
    srcs = ["torch/examples/train_example.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
    args = ["--num-workers=1", "--smoke-test"]
)

py_test(
    name = "train_example_2",
    size = "small",
    main = "torch/examples/train_example.py",
    srcs = ["torch/examples/train_example.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
    args = ["--num-workers=2", "--smoke-test"]
)

py_test(
    name = "tune_example_1",
    size = "small",
    main = "torch/examples/tune_example.py",
    srcs = ["torch/examples/tune_example.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
    args = ["--num-workers=1", "--smoke-test"]
)

py_test(
    name = "tune_example_2",
    size = "small",
    main = "torch/examples/tune_example.py",
    srcs = ["torch/examples/tune_example.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
    args = ["--num-workers=2", "--smoke-test"]
)

py_test(
    name = "tune_example_3",
    size = "small",
    main = "torch/examples/tune_example.py",
    srcs = ["torch/examples/tune_example.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
    args = ["--num-workers=2", "--smoke-test", "--lr-reduce-on-plateau"]
)


# --------------------------------------------------------------------
# Tests from the python/ray/util/sgd/torch/examples/* directories.
# Only covers subdirectories.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "benchmark",
    size = "small",
    srcs = ["torch/examples/benchmarks/benchmark.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "image_models",
    size = "small",
    main = "torch/examples/image_models/train.py",
    srcs = ["torch/examples/image_models/train.py"],
    tags = ["exclusive", "pytorch"],
    deps = [":sgd_lib"],
    args = ["--no-gpu", "--mock-data", "--smoke-test", "--ray-num-workers=2", "--model=mobilenetv3_small_075", "data"]
)

# py_test(
#     name = "mnist-ptl",
#     size = "small",
#     srcs = ["torch/examples/pytorch-lightning/mnist-ptl.py"],
#     tags = ["exclusive", "pytorch", "pytorch-lightning"],
#     deps = [":sgd_lib"],
#     args = ["--smoke-test"]
# )

# --------------------------------------------------------------------
# SGD related tests from the ../../../../release directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "pytorch_pbt_failure",
    size = "medium",
    srcs = ["torch/examples/pytorch_pbt_failure.py"],
    tags = ["exlusive", "pytorch", "release"],
    deps = [":sgd_lib"],
    args = ["--smoke-test"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "sgd_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)