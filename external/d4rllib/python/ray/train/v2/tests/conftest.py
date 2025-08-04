import logging

import pytest

import ray
from ray.train.v2._internal.constants import (
    ENABLE_STATE_ACTOR_RECONCILIATION_ENV_VAR,
)


@pytest.fixture()
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.fixture(autouse=True)
def setup_logging():
    logger = logging.getLogger("ray.train")
    orig_level = logger.getEffectiveLevel()
    logger.setLevel(logging.INFO)
    yield
    logger.setLevel(orig_level)


@pytest.fixture
def shutdown_only():
    yield None
    ray.shutdown()


@pytest.fixture(autouse=True)
def disable_state_actor_polling(monkeypatch):
    monkeypatch.setenv(ENABLE_STATE_ACTOR_RECONCILIATION_ENV_VAR, "0")
    yield
