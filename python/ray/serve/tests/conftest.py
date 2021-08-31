import os
from typing import Tuple
from unittest.mock import patch, Mock

import pytest

import ray
from ray import serve
from ray.serve.backend_state import (
    BackendState,
    CHECKPOINT_KEY,
)
from ray.serve.storage.kv_store import RayLocalKVStore
from ray.serve.async_goal_manager import AsyncGoalManager
from ray.serve.tests.test_backend_state import (
    MockTimer,
    MockReplicaActorWrapper,
)

if os.environ.get("RAY_SERVE_INTENTIONALLY_CRASH", False) == 1:
    serve.controller._CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.5


@pytest.fixture(scope="session")
def _shared_serve_instance():
    # Note(simon):
    # This line should be not turned on on master because it leads to very
    # spammy and not useful log in case of a failure in CI.
    # To run locally, please use this instead.
    # SERVE_LOG_DEBUG=1 pytest -v -s test_api.py
    # os.environ["SERVE_LOG_DEBUG"] = "1" <- Do not uncomment this.

    # Overriding task_retry_delay_ms to relaunch actors more quickly
    ray.init(
        num_cpus=36,
        namespace="default_test_namespace",
        _metrics_export_port=9999,
        _system_config={
            "metrics_report_interval_ms": 1000,
            "task_retry_delay_ms": 50
        })
    yield serve.start(detached=True)


@pytest.fixture
def serve_instance(_shared_serve_instance):
    yield _shared_serve_instance
    # Clear all state between tests to avoid naming collisions.
    for deployment in serve.list_deployments().values():
        deployment.delete()


# For all test in tests or subdir of tests that needs to mock backend_state
@pytest.fixture
def mock_backend_state() -> Tuple[BackendState, Mock, Mock]:
    timer = MockTimer()
    with patch(
            "ray.serve.backend_state.ActorReplicaWrapper",
            new=MockReplicaActorWrapper), patch(
                "time.time", new=timer.time), patch(
                    "ray.serve.long_poll.LongPollHost"
                ) as mock_long_poll:

        kv_store = RayLocalKVStore("TEST_DB", "test_kv_store.db")
        goal_manager = AsyncGoalManager()
        backend_state = BackendState("name", True, kv_store,
                                     mock_long_poll, goal_manager)

        yield backend_state, timer, goal_manager
        # Clear checkpoint at the end of each test
        kv_store.delete(CHECKPOINT_KEY)
