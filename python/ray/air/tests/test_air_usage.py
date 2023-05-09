"""Unit tests for AIR telemetry."""

import json
import os

import pytest
from unittest.mock import MagicMock, patch

import ray
from ray import air, tune
from ray._private.usage.usage_lib import TagKey
from ray.air import session
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


def train_fn(config):
    session.report({"score": 1})


@pytest.fixture
def tuner(tmp_path):
    yield tune.Tuner(train_fn, run_config=air.RunConfig(storage_path=str(tmp_path)))


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


def test_tag_setup_wandb(mock_record):
    from ray.air.integrations.wandb import _setup_wandb

    with patch.dict(os.environ, {wandb.WANDB_MODE_ENV_VAR: "disabled"}):
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


def test_tag_env_vars(ray_start_2_cpus, mock_record, tuner):
    """Test that env vars are recorded properly, and arbitrary user environment
    variables are ignored."""
    env_vars_to_record = {
        "RAY_AIR_LOCAL_CACHE_DIR": "~/ray_results",
        "TUNE_DISABLE_AUTO_CALLBACK_SYNCER": "1",
    }
    untracked_env_vars = {"RANDOM_USER_ENV_VAR": "asdf"}

    with patch.dict(os.environ, {**env_vars_to_record, **untracked_env_vars}):
        tuner.fit()

    recorded_env_vars = json.loads(mock_record[TagKey.AIR_ENV_VARS])
    assert sorted(env_vars_to_record) == sorted(recorded_env_vars)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
