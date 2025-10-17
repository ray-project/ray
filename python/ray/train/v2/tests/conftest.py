import logging

import pytest

import ray
from ray import runtime_context
from ray.cluster_utils import Cluster
from ray.train.v2._internal.constants import (
    ENABLE_STATE_ACTOR_RECONCILIATION_ENV_VAR,
)


@pytest.fixture()
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.fixture()
def ray_start_4_cpus_2_gpus():
    ray.init(num_cpus=4, num_gpus=2)
    yield
    ray.shutdown()


@pytest.fixture
def ray_start_2x2_gpu_cluster():
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=4, num_gpus=2)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


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


@pytest.fixture
def mock_runtime_context(monkeypatch):
    @ray.remote
    class DummyActor:
        pass

    # Must return real actor handle so it can get passed to other actors
    # Cannot create actor here since ray has not been initialized yet
    def mock_current_actor(self):
        return DummyActor.remote()

    # In unit tests where the controller is not an actor, current_actor is
    # a DummyActor, which is ok because it won't be called in those tests.
    # In unit tests where the controller is an actor, current_actor is the
    # controller actor because monkeypatch doesn't propagate to the actor
    # process. Those tests can successfully test methods on that actor.
    monkeypatch.setattr(
        runtime_context.RuntimeContext, "current_actor", property(mock_current_actor)
    )

    yield
