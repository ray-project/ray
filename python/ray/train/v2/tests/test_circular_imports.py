"""
This file tests previously known circular imports to prevent regressions.
"""

import subprocess
import sys

import pytest


def run_isolated(code: str):
    """
    Helper function to run code in separate subprocesses to isolate module imports.
    """
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"Subprocess failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )


def test_train_import():
    run_isolated("from ray.train.v2.api.callback import UserCallback")
    run_isolated(
        "from ray.train.v2.api.config import FailureConfig, RunConfig, ScalingConfig"
    )
    run_isolated("from ray.train.v2.api.report_config import CheckpointUploadMode")
    run_isolated("from ray.train.v2.api.reported_checkpoint import ReportedCheckpoint")
    run_isolated("from ray.train.v2.api.result import Result")
    run_isolated(
        "from ray.train.v2.api.train_fn_utils import get_all_reported_checkpoints, get_checkpoint, get_context, get_dataset_shard, report"
    )


def test_tensorflow_import():
    run_isolated(
        "from ray.train.v2.tensorflow.tensorflow_trainer import TensorflowTrainer"
    )


def test_collective_import():
    run_isolated(
        "from ray.train.collective.collectives import barrier, broadcast_from_rank_zero"
    )


def test_lightgbm_import():
    run_isolated("from ray.train.v2.lightgbm.lightgbm_trainer import LightGBMTrainer")


def test_torch_import():
    run_isolated("from ray.train.v2.torch.torch_trainer import TorchTrainer")
    run_isolated(
        "from ray.train.v2.torch.train_loop_utils import accelerate, backward, enable_reproducibility, get_device, get_devices, prepare_data_loader, prepare_model, prepare_optimizer"
    )


def test_xgboost_import():
    run_isolated("from ray.train.v2.xgboost.config import XGBoostConfig")
    run_isolated("from ray.train.v2.xgboost.xgboost_trainer import XGBoostTrainer")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
