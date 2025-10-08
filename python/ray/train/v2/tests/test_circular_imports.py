"""
This file tests previously known circular imports to prevent regressions.
"""

import pytest


def test_train_import():
    from ray.train.v2.api.callback import UserCallback  # noqa: F401
    from ray.train.v2.api.config import (  # noqa: F401
        FailureConfig,
        RunConfig,
        ScalingConfig,
    )
    from ray.train.v2.api.report_config import CheckpointUploadMode  # noqa: F401
    from ray.train.v2.api.reported_checkpoint import ReportedCheckpoint  # noqa: F401
    from ray.train.v2.api.result import Result  # noqa: F401
    from ray.train.v2.api.train_fn_utils import (  # noqa: F401
        get_all_reported_checkpoints,
        get_checkpoint,
        get_context,
        get_dataset_shard,
        report,
    )


def test_tensorflow_import():
    from ray.train.v2.tensorflow.tensorflow_trainer import (  # noqa: F401
        TensorflowTrainer,
    )


def test_collective_import():
    from ray.train.collective.collectives import (  # noqa: F401
        barrier,
        broadcast_from_rank_zero,
    )


def test_lightgbm_import():
    from ray.train.v2.lightgbm.lightgbm_trainer import LightGBMTrainer  # noqa: F401


def test_torch_import():
    from ray.train.v2.torch.torch_trainer import TorchTrainer  # noqa: F401
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


def test_xgboost_import():
    from ray.train.v2.xgboost.config import XGBoostConfig  # noqa: F401
    from ray.train.v2.xgboost.xgboost_trainer import XGBoostTrainer  # noqa: F401


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
