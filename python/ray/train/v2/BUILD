load("@rules_python//python:defs.bzl", "py_library", "py_test")

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
)

py_test(
    name = "test_accelerator_utils",
    size = "small",
    srcs = ["tests/test_accelerator_utils.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_checkpoint_manager",
    size = "small",
    srcs = ["tests/test_checkpoint_manager.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_collective",
    size = "small",
    srcs = ["tests/test_collective.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_controller",
    size = "small",
    srcs = ["tests/test_controller.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_data_integration",
    size = "small",
    srcs = ["tests/test_data_integration.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_data_parallel_trainer",
    size = "medium",
    srcs = ["tests/test_data_parallel_trainer.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_env_callbacks",
    size = "small",
    srcs = ["tests/test_env_callbacks.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_failure_policy",
    size = "small",
    srcs = ["tests/test_failure_policy.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_lightgbm_trainer",
    size = "small",
    srcs = ["tests/test_lightgbm_trainer.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_lightning_integration",
    size = "medium",
    srcs = ["tests/test_lightning_integration.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_logging",
    size = "small",
    srcs = ["tests/test_logging.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_metrics",
    size = "small",
    srcs = ["tests/test_metrics.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_persistence",
    size = "medium",
    srcs = ["tests/test_persistence.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_report_handler",
    size = "small",
    srcs = ["tests/test_report_handler.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_result",
    size = "small",
    srcs = ["tests/test_result.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_scheduling",
    size = "medium",
    srcs = ["tests/test_scheduling.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_serialization",
    size = "small",
    srcs = ["tests/test_serialization.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_state",
    size = "medium",
    srcs = ["tests/test_state.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_state_export",
    size = "medium",
    srcs = ["tests/test_state_export.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_storage",
    size = "small",
    srcs = ["tests/test_storage.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_sync_actor",
    size = "small",
    srcs = ["tests/test_sync_actor.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_telemetry",
    size = "medium",
    srcs = ["tests/test_telemetry.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_tensorflow_trainer",
    size = "medium",
    srcs = ["tests/test_tensorflow_trainer.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_thread_runner",
    size = "small",
    srcs = ["tests/test_thread_runner.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_torch_trainer",
    size = "small",
    srcs = ["tests/test_torch_trainer.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_torch_transformers_train",
    size = "medium",
    srcs = ["tests/test_torch_transformers_train.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_util",
    size = "small",
    srcs = ["tests/test_util.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_v2_api",
    size = "small",
    srcs = ["tests/test_v2_api.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_worker_group",
    size = "medium",
    srcs = ["tests/test_worker_group.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_xgboost_trainer",
    size = "small",
    srcs = ["tests/test_xgboost_trainer.py"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
    tags = [
        "exclusive",
        "team:ml",
        "train_v2",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)
