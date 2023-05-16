"""Unit tests for AIR telemetry."""

from collections import namedtuple
import json
import os

import pytest
from unittest import mock

import ray
from ray import air, tune
from ray.air import session
from ray._private.usage.usage_lib import TagKey
from ray.air._internal import usage as air_usage


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


def test_tag_env_vars(ray_start_2_cpus, mock_record, tuner):
    """Test that env vars are recorded properly, and arbitrary user environment
    variables are ignored."""
    env_vars_to_record = {
        "RAY_AIR_LOCAL_CACHE_DIR": "~/ray_results",
        "TUNE_DISABLE_AUTO_CALLBACK_SYNCER": "1",
    }
    untracked_env_vars = {"RANDOM_USER_ENV_VAR": "asdf"}

    with mock.patch.dict(os.environ, {**env_vars_to_record, **untracked_env_vars}):
        tuner.fit()

    recorded_env_vars = json.loads(mock_record[TagKey.AIR_ENV_VARS])
    assert sorted(env_vars_to_record) == sorted(recorded_env_vars)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
