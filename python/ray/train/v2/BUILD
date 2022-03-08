# --------------------------------------------------------------------
# Tests from the python/ray/train/v2/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "torch_linear_dataset_example",
    size = "medium",
    main = "examples/torch_linear_dataset_example.py",
    srcs = ["examples/torch_linear_dataset_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_v2_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "torch_tune_linear_dataset_example",
    size = "medium",
    main = "examples/torch_tune_linear_dataset_example.py",
    srcs = ["examples/torch_tune_linear_dataset_example.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_v2_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "test_trainer",
    size = "large",
    srcs = ["tests/test_trainer.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":train_v2_lib"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "train_v2_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)