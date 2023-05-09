"""Unit tests for AIR telemetry."""

import json

import pytest
from unittest.mock import MagicMock

import ray
from ray._private.usage.usage_lib import TagKey
from ray.air._internal import usage as air_usage
from ray.air.integrations import wandb, mlflow, comet
from ray.tune.callback import Callback
from ray.tune.logger import LoggerCallback
from ray.tune.logger.aim import AimLoggerCallback
from ray.tune.utils.callback import DEFAULT_CALLBACK_CLASSES


def _mock_record_from_module(module, monkeypatch):
    recorded = {}

    def mock_record_extra_usage_tag(key: TagKey, value: str):
        recorded[key] = value

    monkeypatch.setattr(
        module,
        "record_extra_usage_tag",
        mock_record_extra_usage_tag,
    )
    return recorded


@pytest.fixture
def mock_record(monkeypatch):
    import ray.air._internal.usage

    yield _mock_record_from_module(ray.air._internal.usage, monkeypatch=monkeypatch)


@pytest.fixture(scope="module")
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    ray.shutdown()


class _CustomLoggerCallback(LoggerCallback):
    pass


class _CustomCallback(Callback):
    pass


_TEST_CALLBACKS = [
    wandb.WandbLoggerCallback,
    mlflow.MLflowLoggerCallback,
    comet.CometLoggerCallback,
    AimLoggerCallback,
    _CustomLoggerCallback,
    _CustomLoggerCallback,
    _CustomCallback,
]


def test_tag_setup_wandb(mock_record, monkeypatch):
    from ray.air.integrations.wandb import _setup_wandb

    _setup_wandb(trial_id="a", trial_name="b", config={}, _wandb=MagicMock())
    assert mock_record[TagKey.AIR_SETUP_WANDB_INTEGRATION_USED] == "1"


def test_tag_setup_mlflow(mock_record, monkeypatch):
    from ray.air.integrations.mlflow import setup_mlflow

    monkeypatch.setattr(ray.air.integrations.mlflow, "_MLflowLoggerUtil", MagicMock())
    setup_mlflow()
    assert mock_record[TagKey.AIR_SETUP_MLFLOW_INTEGRATION_USED] == "1"


@pytest.mark.parametrize(
    "callback_classes_expected",
    [
        (None, None),
        ([], None),
        ([lambda: None], None),
        (
            DEFAULT_CALLBACK_CLASSES,
            {cls.__name__: 1 for cls in DEFAULT_CALLBACK_CLASSES},
        ),
        (
            _TEST_CALLBACKS,
            {
                "WandbLoggerCallback": 1,
                "MLflowLoggerCallback": 1,
                "CometLoggerCallback": 1,
                "AimLoggerCallback": 1,
                "CustomLoggerCallback": 2,
                "CustomCallback": 1,
            },
        ),
    ],
)
def test_tag_callbacks(mock_record, callback_classes_expected):
    callback_classes, expected = callback_classes_expected

    callbacks = (
        [callback_cls() for callback_cls in callback_classes]
        if callback_classes
        else None
    )

    air_usage.tag_callbacks(callbacks)

    callback_usage_str = mock_record.pop(TagKey.AIR_CALLBACKS, None)
    callback_counts = json.loads(callback_usage_str) if callback_usage_str else None
    assert callback_counts == expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
