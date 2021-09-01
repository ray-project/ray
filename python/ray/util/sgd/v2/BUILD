# --------------------------------------------------------------------
# Tests from the python/ray/util/sgd/v2/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "mlflow_fashion_mnist",
    size = "medium",
    main = "examples/mlflow_fashion_mnist.py",
    srcs = ["examples/mlflow_fashion_mnist.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":sgd_v2_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "transformers_example",
    size = "large",
    main = "examples/transformers/transformers_example.py",
    srcs = ["examples/transformers/transformers_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":sgd_v2_lib"],
    args = ["--model_name_or_path=bert-base-cased", "--task_name=mrpc",
    "--max_length=32", "--per_device_train_batch_size=64",
    "--max_train_steps=2", "--start_local", "--num_workers=2"]
)

py_test(
    name = "tune_linear",
    size = "medium",
    main = "examples/tune_linear.py",
    srcs = ["examples/tune_linear.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":sgd_v2_lib"],
    args = ["--smoke-test"]
)

# --------------------------------------------------------------------
# Tests from the python/ray/util/sgd/v2/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_backend",
    size = "medium",
    srcs = ["tests/test_backend.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":sgd_v2_lib"]
)

py_test(
    name = "test_session",
    size = "small",
    srcs = ["tests/test_session.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":sgd_v2_lib"]
)

py_test(
    name = "test_trainer",
    size = "large",
    srcs = ["tests/test_trainer.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":sgd_v2_lib"]
)

py_test(
    name = "test_tune",
    size = "medium",
    srcs = ["tests/test_tune.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":sgd_v2_lib"]
)

py_test(
    name = "test_worker_group",
    size = "small",
    srcs = ["tests/test_worker_group.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":sgd_v2_lib"]
)



# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "sgd_v2_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
