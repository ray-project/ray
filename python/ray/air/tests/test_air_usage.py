"""Unit tests for AIR telemetry."""

import json
import pytest

import ray
from ray._private.usage.usage_lib import TagKey
from ray.air._internal import usage as air_usage
from ray.air.integrations import wandb, mlflow, comet
from ray.tune.callback import Callback
from ray.tune.logger import LoggerCallback
from ray.tune.logger.aim import AimLoggerCallback
from ray.tune.utils.callback import DEFAULT_CALLBACK_CLASSES


@pytest.fixture
def mock_record(monkeypatch):
    import ray.air._internal.usage

    recorded = {}

    def mock_record_extra_usage_tag(key: TagKey, value: str):
        recorded[key] = value

    monkeypatch.setattr(
        ray.air._internal.usage,
        "record_extra_usage_tag",
        mock_record_extra_usage_tag,
    )
    yield recorded


@pytest.fixture(scope="module")
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    ray.shutdown()


class _CustomLoggerCallback(LoggerCallback):
    pass


class _CustomCallback(Callback):
    pass


@pytest.mark.parametrize(
    "callback_classes",
    [
        None,
        [],
        [lambda: None],
        DEFAULT_CALLBACK_CLASSES,
        [
            wandb.WandbLoggerCallback,
            mlflow.MLflowLoggerCallback,
            comet.CometLoggerCallback,
            AimLoggerCallback,
        ]
        + [_CustomLoggerCallback, _CustomCallback] * 2,
    ],
)
def test_tag_callbacks(mock_record, callback_classes):

    callbacks = (
        [callback_cls() for callback_cls in callback_classes]
        if callback_classes
        else None
    )

    recorded = air_usage.tag_callbacks(callbacks)
    expected_callback_counts = air_usage._count_callbacks(callbacks)

    if not callbacks or not expected_callback_counts:
        # Handles: None, [], [None]
        assert not recorded and not mock_record
    else:
        callback_usage_str = mock_record[TagKey.AIR_CALLBACKS]
        callback_counts = json.loads(callback_usage_str)
        assert callback_counts == expected_callback_counts
        assert sum(callback_counts.values()) == len(callbacks)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
