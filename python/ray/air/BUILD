load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(
        ["**/*.py"],
        exclude = glob([
            "examples/**/*",
            "tests/**/*",
            "callbacks/*.py",
        ]) + ["integrations/wandb.py"],
    ),  # TODO: Add note for callbacks
    tags = ["team:ml"],
)

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
)

# --------------------------------------------------------------------
# Tests from the python/ray/air/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "custom_trainer",
    size = "small",
    srcs = ["examples/custom_trainer.py"],
    main = "examples/custom_trainer.py",
    tags = [
        "exclusive",
        "no_main",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "check_ingest",
    size = "large",
    srcs = ["util/check_ingest.py"],
    main = "util/check_ingest.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
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
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_arrow",
    size = "small",
    srcs = [
        "conftest",
        "tests/test_arrow.py",
    ],
    tags = [
        "exclusive",
        "ray_data",
        "team:data",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_air_usage",
    size = "small",
    srcs = ["tests/test_air_usage.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_configs",
    size = "small",
    srcs = ["tests/test_configs.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_data_batch_conversion",
    size = "small",
    srcs = ["tests/test_data_batch_conversion.py"],
    tags = [
        "exclusive",
        "ray_data",
        "team:data",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_new_dataset_config",
    size = "large",
    srcs = ["tests/test_new_dataset_config.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_experiment_restore",
    size = "large",
    srcs = [
        "tests/_test_experiment_restore_run.py",
        "tests/test_experiment_restore.py",
    ],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_errors",
    size = "medium",
    srcs = ["tests/test_errors.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_integration_comet",
    size = "small",
    srcs = ["tests/test_integration_comet.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_integration_wandb",
    size = "small",
    srcs = ["tests/test_integration_wandb.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_integration_mlflow",
    size = "small",
    srcs = ["tests/test_integration_mlflow.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_keras_callback",
    size = "medium",
    srcs = ["tests/test_keras_callback.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_remote_storage_hdfs",
    size = "small",
    srcs = ["tests/test_remote_storage_hdfs.py"],
    tags = [
        "exclusive",
        "hdfs",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_tensor_extension",
    size = "small",
    srcs = ["tests/test_tensor_extension.py"],
    tags = [
        "exclusive",
        "ray_data",
        "team:data",
        "team:ml",
    ],
    deps = [
        ":conftest",
        ":ml_lib",
    ],
)

py_test(
    name = "test_object_extension",
    size = "small",
    srcs = ["tests/test_object_extension.py"],
    tags = [
        "exclusive",
        "ray_data",
        "team:data",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_tracebacks",
    size = "small",
    srcs = ["tests/test_tracebacks.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_utils",
    size = "small",
    srcs = ["tests/test_utils.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_util_torch_dist",
    size = "small",
    srcs = ["tests/test_util_torch_dist.py"],
    tags = [
        "exclusive",
        "gpu",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

# --------------------------------------------------------------------
# Tests from the python/ray/air/tests/execution directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_barrier",
    size = "small",
    srcs = ["tests/execution/test_barrier.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_e2e_train_flow",
    size = "medium",
    srcs = ["tests/execution/test_e2e_train_flow.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_e2e_tune_flow",
    size = "medium",
    srcs = ["tests/execution/test_e2e_tune_flow.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_event_manager",
    size = "medium",
    srcs = ["tests/execution/test_event_manager.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_resource_manager_fixed",
    size = "small",
    srcs = ["tests/execution/test_resource_manager_fixed.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_resource_manager_placement_group",
    size = "small",
    srcs = ["tests/execution/test_resource_manager_placement_group.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_resource_request",
    size = "small",
    srcs = ["tests/execution/test_resource_request.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_tracked_actor",
    size = "small",
    srcs = ["tests/execution/test_tracked_actor.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

py_test(
    name = "test_tracked_actor_task",
    size = "small",
    srcs = ["tests/execution/test_tracked_actor_task.py"],
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [":ml_lib"],
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "ml_lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["tests/*.py"],
    ),
    visibility = [
        "//python/ray/air:__pkg__",
        "//python/ray/air:__subpackages__",
        "//python/ray/train:__pkg__",
        "//python/ray/train:__subpackages__",
        "//release:__pkg__",
    ],
)
