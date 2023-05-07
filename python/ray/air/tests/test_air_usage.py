"""Unit tests for AIR telemetry."""

import json
import os

import pytest
from unittest import mock

import ray
from ray import air, tune
from ray._private.usage.usage_lib import TagKey
from ray.air import session
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


@pytest.fixture(scope="module")
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    ray.shutdown()


from collections import namedtuple


# (nfs: bool, remote_path: str | None, syncing_disabled: bool, expected: str)
_StorageTestConfig = namedtuple(
    "StorageTestConfig", ["nfs", "remote_path", "syncing_disabled", "expected"]
)

_storage_test_configs = [
    # Local
    _StorageTestConfig(False, None, False, "local+sync"),
    _StorageTestConfig(False, None, True, "local+no_sync"),
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
def test_tag_ray_air_storage_config(tmp_path, storage_test_config, mock_record):
    local_path = (
        "/dev/a/b/c" if storage_test_config.nfs else str(tmp_path / "local_path")
    )
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
