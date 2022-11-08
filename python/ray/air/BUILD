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
    tags = ["team:ml", "exclusive", "ray_data"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_dataset_config",
    size = "large",
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
    size = "small",
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
    size = "large",
    srcs = ["tests/test_resource_changing.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_tensor_extension",
    size = "small",
    srcs = ["tests/test_tensor_extension.py"],
    tags = ["team:ml", "exclusive", "ray_data"],
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
        "//release:__pkg__"
    ],
)
