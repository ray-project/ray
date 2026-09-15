import logging
import os

import pytest

import ray
from ray import runtime_context
from ray._common import utils as ray_utils
from ray.cluster_utils import Cluster

# * `propagate_logs` is re-exported so tests in this directory can request it
#   to use caplog/capsys with Ray's loggers, which do not propagate to the root
#   logger by default.
# * Importing `pytest_runtest_makereport` registers the hook that zips
#   /tmp/ray/session_latest/logs into RAY_TEST_FAILURE_LOGS_ARCHIVE_DIR
#   (the Buildkite artifact mount) whenever a test fails.
from ray.tests.conftest import (  # noqa: F401
    propagate_logs,
    pytest_runtest_makereport,
)
from ray.train.v2._internal.constants import (
    ENABLE_STATE_ACTOR_RECONCILIATION_ENV_VAR,
)

# Keep the footer-reader pool tiny for Data-integrated train tests. Mirrored
# in python/ray/train/v2/BUILD.bazel for bazel test targets.
os.environ.setdefault("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")


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


@pytest.fixture(autouse=True)
def reset_ray_address(monkeypatch):
    ray_utils.reset_ray_address()
    yield
    ray_utils.reset_ray_address()


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
    @ray.remote(num_cpus=0)
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
