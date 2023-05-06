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
    name = "test_experiment_restore",
    size = "large",
    srcs = [
        "tests/test_experiment_restore.py",
        "tests/_test_experiment_restore_run.py"
    ],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_errors",
    size = "small",
    srcs = ["tests/test_errors.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_integration_comet",
    size = "small",
    srcs = ["tests/test_integration_comet.py"],
    deps = [":ml_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_integration_wandb",
    size = "small",
    srcs = ["tests/test_integration_wandb.py"],
    deps = [":ml_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_integration_mlflow",
    size = "small",
    srcs = ["tests/test_integration_mlflow.py"],
    deps = [":ml_lib"],
    tags = ["team:ml", "exclusive"]
)

py_test(
    name = "test_keras_callback",
    size = "small",
    srcs = ["tests/test_keras_callback.py"],
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
    name = "test_remote_storage_hdfs",
    size = "small",
    srcs = ["tests/test_remote_storage_hdfs.py"],
    tags = ["team:ml", "exclusive", "hdfs"],
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

py_test(
    name = "test_utils",
    size = "small",
    srcs = ["tests/test_utils.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_util_torch_dist",
    size = "small",
    srcs = ["tests/test_util_torch_dist.py"],
    tags = ["team:ml", "gpu", "exclusive"],
    deps = [":ml_lib"]
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
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_e2e_train_flow",
    size = "medium",
    srcs = ["tests/execution/test_e2e_train_flow.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_e2e_tune_flow",
    size = "medium",
    srcs = ["tests/execution/test_e2e_tune_flow.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_event_manager",
    size = "medium",
    srcs = ["tests/execution/test_event_manager.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_resource_manager_fixed",
    size = "small",
    srcs = ["tests/execution/test_resource_manager_fixed.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_resource_manager_placement_group",
    size = "small",
    srcs = ["tests/execution/test_resource_manager_placement_group.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_resource_request",
    size = "small",
    srcs = ["tests/execution/test_resource_request.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_tracked_actor",
    size = "small",
    srcs = ["tests/execution/test_tracked_actor.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_lib"]
)

py_test(
    name = "test_tracked_actor_task",
    size = "small",
    srcs = ["tests/execution/test_tracked_actor_task.py"],
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
