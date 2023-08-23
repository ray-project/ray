"""Unit tests for AIR telemetry."""

from collections import namedtuple
import json
import os

import pytest
from unittest.mock import MagicMock, patch

import ray
from ray import train, tune
from ray.air._internal import usage as air_usage
from ray.air._internal.usage import AirEntrypoint
from ray.air.integrations import wandb, mlflow, comet
from ray.tune.callback import Callback
from ray.tune.experiment.experiment import Experiment
from ray.tune.logger import LoggerCallback
from ray.tune.logger.aim import AimLoggerCallback
from ray.tune.utils.callback import DEFAULT_CALLBACK_CLASSES
from ray._private.usage.usage_lib import TagKey


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
    train.report({"score": 1})


@pytest.fixture
def tuner(tmp_path):
    yield tune.Tuner(train_fn, run_config=train.RunConfig(storage_path=str(tmp_path)))


@pytest.fixture
def trainer(tmp_path):
    from ray.train.data_parallel_trainer import DataParallelTrainer

    yield DataParallelTrainer(
        train_loop_per_worker=train_fn,
        scaling_config=train.ScalingConfig(num_workers=2),
        run_config=train.RunConfig(storage_path=str(tmp_path)),
    )


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


# (nfs: bool, remote_path: str | None, syncing_disabled: bool, expected: str)
_StorageTestConfig = namedtuple(
    "StorageTestConfig", ["nfs", "remote_path", "syncing_disabled", "expected"]
)

_storage_test_configs = [
    # Local
    _StorageTestConfig(False, None, False, "driver"),
    _StorageTestConfig(False, None, True, "local"),
    # Remote
    _StorageTestConfig(False, "s3://mock/bucket?param=1", False, "s3"),
    _StorageTestConfig(False, "gs://mock/bucket?param=1", False, "gs"),
    _StorageTestConfig(False, "hdfs://mock/bucket?param=1", False, "hdfs"),
    _StorageTestConfig(False, "file://mock/bucket?param=1", False, "local_uri"),
    _StorageTestConfig(False, "memory://mock/bucket?param=1", False, "memory"),
    _StorageTestConfig(
        False, "custom://mock/bucket?param=1", False, "custom_remote_storage"
    ),
    # NFS
    _StorageTestConfig(True, None, True, "nfs"),
]


@pytest.mark.parametrize(
    "storage_test_config",
    _storage_test_configs,
    ids=[str(config) for config in _storage_test_configs],
)
def test_tag_ray_air_storage_config(
    tmp_path, storage_test_config, mock_record, monkeypatch
):
    if storage_test_config.nfs:
        import ray.air._internal.remote_storage

        monkeypatch.setattr(
            ray.air._internal.remote_storage,
            "_get_network_mounts",
            lambda: [str(tmp_path)],
        )

    local_path = str(tmp_path / "local_path")
    sync_config = (
        tune.SyncConfig(syncer=None)
        if storage_test_config.syncing_disabled
        else tune.SyncConfig()
    )

    air_usage.tag_ray_air_storage_config(
        local_path=local_path,
        remote_path=storage_test_config.remote_path,
        sync_config=sync_config,
    )
    assert storage_test_config.expected == mock_record[TagKey.AIR_STORAGE_CONFIGURATION]


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


def test_tag_env_vars(ray_start_4_cpus, mock_record, tuner):
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


@pytest.mark.parametrize("entrypoint", list(AirEntrypoint))
def test_tag_air_entrypoint(ray_start_4_cpus, mock_record, entrypoint, tuner, trainer):
    if entrypoint == AirEntrypoint.TUNE_RUN:
        tune.run(train_fn)
    elif entrypoint == AirEntrypoint.TUNE_RUN_EXPERIMENTS:
        experiment_spec = Experiment("experiment", train_fn)
        tune.run_experiments(experiments=experiment_spec)
    elif entrypoint == AirEntrypoint.TUNER:
        tuner.fit()
    elif entrypoint == AirEntrypoint.TRAINER:
        trainer.fit()

    assert mock_record[TagKey.AIR_ENTRYPOINT] == entrypoint.value


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
