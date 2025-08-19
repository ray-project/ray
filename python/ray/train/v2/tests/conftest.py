import logging
from unittest.mock import create_autospec

import pytest

import ray
from ray.train.v2._internal.constants import (
    ENABLE_STATE_ACTOR_RECONCILIATION_ENV_VAR,
)
from ray.train.v2._internal.execution.worker_group import worker_group


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


@pytest.fixture(autouse=True)
def mock_get_current_actor(monkeypatch, request):
    @ray.remote
    class DummyActor:
        pass

    mock_get_current_actor = create_autospec(worker_group.get_current_actor)

    # Must return real actor handle so it can get passed to other actors
    # Cannot create actor here since ray has not been initialized yet
    mock_get_current_actor.side_effect = lambda: DummyActor.remote()

    # Note that actors do not use mocked methods
    # This means that if worker_group is not in in actor, it will use the dummy actor,
    # but if it is in an actor, it will use the real actor
    monkeypatch.setattr(worker_group, "get_current_actor", mock_get_current_actor)

    yield
