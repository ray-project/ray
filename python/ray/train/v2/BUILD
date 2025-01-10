py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"]
)

py_test(
    name = "test_accelerator_utils",
    size = "small",
    srcs = ["tests/test_accelerator_utils.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_checkpoint_handler",
    size = "small",
    srcs = ["tests/test_checkpoint_handler.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_checkpoint_manager",
    size = "small",
    srcs = ["tests/test_checkpoint_manager.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_controller",
    size = "small",
    srcs = ["tests/test_controller.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_data_integration",
    size = "small",
    srcs = ["tests/test_data_integration.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)
py_test(
    name = "test_data_parallel_trainer",
    size = "small",
    srcs = ["tests/test_data_parallel_trainer.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_failure_policy",
    size = "small",
    srcs = ["tests/test_failure_policy.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_lightgbm_trainer",
    size = "small",
    srcs = ["tests/test_lightgbm_trainer.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_lightning_integration",
    size = "medium",
    srcs = ["tests/test_lightning_integration.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_logging",
    size = "small",
    srcs = ["tests/test_logging.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_metrics",
    size = "small",
    srcs = ["tests/test_metrics.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_persistence",
    size = "medium",
    srcs = ["tests/test_persistence.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_result",
    size = "small",
    srcs = ["tests/test_result.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_scheduling",
    size = "medium",
    srcs = ["tests/test_scheduling.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_storage",
    size = "small",
    srcs = ["tests/test_storage.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_sync_actor",
    size = "small",
    srcs = ["tests/test_sync_actor.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_tensorflow_trainer",
    size = "medium",
    srcs = ["tests/test_tensorflow_trainer.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_thread_runner",
    size = "small",
    srcs = ["tests/test_thread_runner.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_torch_trainer",
    size = "small",
    srcs = ["tests/test_torch_trainer.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_torch_transformers_train",
    size = "medium",
    srcs = ["tests/test_torch_transformers_train.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_util",
    size = "small",
    srcs = ["tests/test_util.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_v2_api",
    size = "small",
    srcs = ["tests/test_v2_api.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_worker_group",
    size = "medium",
    srcs = ["tests/test_worker_group.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)

py_test(
    name = "test_xgboost_trainer",
    size = "small",
    srcs = ["tests/test_xgboost_trainer.py"],
    tags = ["exclusive", "team:ml", "train_v2"],
    deps = ["//:ray_lib", ":conftest"],
    env = {"RAY_TRAIN_V2_ENABLED": "1"},
)
