"""Unit tests for AIR telemetry."""

import json
import os

import pytest
from unittest import mock

import ray
from ray import air, tune
from ray._private.usage.usage_lib import TagKey
from ray.air import session


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


def train_fn(config):
    session.report({"score": 1})


@pytest.fixture
def tuner(tmp_path):
    yield tune.Tuner(train_fn, run_config=air.RunConfig(storage_path=str(tmp_path)))


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
