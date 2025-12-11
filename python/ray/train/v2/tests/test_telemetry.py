import sys

import pytest

import ray
import ray._common.usage.usage_lib as ray_usage_lib
from ray._common.test_utils import TelemetryCallsite, check_library_usage_telemetry
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer


@pytest.fixture
def mock_record(monkeypatch):
    import ray.air._internal.usage

    recorded = {}

    def mock_record_extra_usage_tag(key: ray_usage_lib.TagKey, value: str):
        recorded[key] = value

    monkeypatch.setattr(
        ray.air._internal.usage,
        "record_extra_usage_tag",
        mock_record_extra_usage_tag,
    )
    yield recorded


@pytest.fixture
def reset_usage_lib():
    yield
    ray.shutdown()
    ray_usage_lib.reset_global_state()


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_not_used_on_import(reset_usage_lib, callsite: TelemetryCallsite):
    def _import_ray_train():
        from ray import train  # noqa: F401

    check_library_usage_telemetry(
        _import_ray_train, callsite=callsite, expected_library_usages=[set(), {"core"}]
    )


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_used_on_trainer_fit(reset_usage_lib, callsite: TelemetryCallsite):
    def _call_trainer_fit():
        def train_fn():
            pass

        trainer = DataParallelTrainer(train_fn)
        trainer.fit()

    check_library_usage_telemetry(
        _call_trainer_fit,
        callsite=callsite,
        expected_library_usages=[{"train"}, {"core", "train"}],
        expected_extra_usage_tags={
            "train_trainer": "DataParallelTrainer",
        },
    )


@pytest.mark.skipif(
    sys.version_info.major == 3 and sys.version_info.minor >= 12,
    reason="Python 3.12+ does not have Tensorflow installed on CI due to dependency conflicts.",
)
def test_tag_train_entrypoint(mock_record):
    """Test that Train v2 entrypoints are recorded correctly."""
    from ray.train.v2.lightgbm.lightgbm_trainer import LightGBMTrainer
    from ray.train.v2.tensorflow.tensorflow_trainer import TensorflowTrainer
    from ray.train.v2.torch.torch_trainer import TorchTrainer
    from ray.train.v2.xgboost.xgboost_trainer import XGBoostTrainer

    trainer_classes = [
        TorchTrainer,
        TensorflowTrainer,
        XGBoostTrainer,
        LightGBMTrainer,
    ]
    for trainer_cls in trainer_classes:
        trainer = trainer_cls(
            lambda: None,
            scaling_config=ray.train.ScalingConfig(num_workers=2),
        )
        assert (
            mock_record[ray_usage_lib.TagKey.TRAIN_TRAINER]
            == trainer.__class__.__name__
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
