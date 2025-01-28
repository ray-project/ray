import sys
import warnings

import pytest

import ray.train
import ray.tune
from ray.train.constants import ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.util.annotations import RayDeprecationWarning


@pytest.fixture(autouse=True)
def enable_v2_migration_deprecation_messages(monkeypatch):
    monkeypatch.setenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, "1")
    yield
    monkeypatch.delenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR)


def test_trainer_restore():
    with pytest.warns(RayDeprecationWarning, match=""):
        try:
            DataParallelTrainer.restore("dummy")
        except Exception as e:
            pass

    with pytest.warns(RayDeprecationWarning, match=""):
        try:
            DataParallelTrainer.can_restore("dummy")
        except Exception as e:
            pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
