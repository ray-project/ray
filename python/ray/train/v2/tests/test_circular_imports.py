"""
This file tests previously known circular imports to prevent regressions, isolating each import in a Ray task.
"""

import sys

import pytest

import ray


@pytest.fixture(scope="session", autouse=True)
def ray_session():
    """Initialize Ray at the start of the test session and shutdown at the end."""
    if not ray.is_initialized():
        ray.init(runtime_env={"env_vars": {"RAY_TRAIN_V2_ENABLED": "1"}})
    yield
    if ray.is_initialized():
        ray.shutdown()


def run_import_task(task_func):
    """
    Helper function to run a Ray import task and handle errors.
    """
    try:
        future = task_func.remote()
        ray.get(future)
    except Exception as e:
        raise AssertionError(f"Import failed: {e}")


def test_train_import():
    # Ray tasks for train imports
    @ray.remote
    def import_user_callback():
        from ray.train.v2.api.callback import UserCallback  # noqa: F401

    @ray.remote
    def import_train_configs():
        from ray.train.v2.api.config import (  # noqa: F401
            FailureConfig,
            RunConfig,
            ScalingConfig,
        )

    @ray.remote
    def import_checkpoint_upload_mode():
        from ray.train.v2.api.report_config import CheckpointUploadMode  # noqa: F401

    @ray.remote
    def import_reported_checkpoint():
        from ray.train.v2.api.reported_checkpoint import (
            ReportedCheckpoint,  # noqa: F401
        )

    @ray.remote
    def import_result():
        from ray.train.v2.api.result import Result  # noqa: F401

    @ray.remote
    def import_train_fn_utils():
        from ray.train.v2.api.train_fn_utils import (  # noqa: F401
            get_all_reported_checkpoints,
            get_checkpoint,
            get_context,
            get_dataset_shard,
            report,
        )

    run_import_task(import_user_callback)
    run_import_task(import_train_configs)
    run_import_task(import_checkpoint_upload_mode)
    run_import_task(import_reported_checkpoint)
    run_import_task(import_result)
    run_import_task(import_train_fn_utils)


def test_tensorflow_import():
    # Ray tasks for tensorflow imports
    @ray.remote
    def import_tensorflow_trainer():
        from ray.train.v2.tensorflow.tensorflow_trainer import (  # noqa: F401
            TensorflowTrainer,
        )

    run_import_task(import_tensorflow_trainer)


def test_collective_import():
    # Ray tasks for collective imports
    @ray.remote
    def import_collectives():
        from ray.train.collective.collectives import (  # noqa: F401
            barrier,
            broadcast_from_rank_zero,
        )

    run_import_task(import_collectives)


def test_lightgbm_import():
    # Ray tasks for lightgbm imports
    @ray.remote
    def import_lightgbm_trainer():
        from ray.train.v2.lightgbm.lightgbm_trainer import LightGBMTrainer  # noqa: F401

    run_import_task(import_lightgbm_trainer)


def test_torch_import():
    # Ray tasks for torch imports
    @ray.remote
    def import_torch_trainer():
        from ray.train.v2.torch.torch_trainer import TorchTrainer  # noqa: F401

    @ray.remote
    def import_torch_train_loop_utils():
        from ray.train.v2.torch.train_loop_utils import (  # noqa: F401
            accelerate,
            backward,
            enable_reproducibility,
            get_device,
            get_devices,
            prepare_data_loader,
            prepare_model,
            prepare_optimizer,
        )

    run_import_task(import_torch_trainer)
    run_import_task(import_torch_train_loop_utils)


def test_xgboost_import():
    # Ray tasks for xgboost imports
    @ray.remote
    def import_xgboost_config():
        from ray.train.v2.xgboost.config import XGBoostConfig  # noqa: F401

    @ray.remote
    def import_xgboost_trainer():
        from ray.train.v2.xgboost.xgboost_trainer import XGBoostTrainer  # noqa: F401

    run_import_task(import_xgboost_config)
    run_import_task(import_xgboost_trainer)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
