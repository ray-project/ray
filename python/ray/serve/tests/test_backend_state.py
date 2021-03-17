import time
from typing import Dict, Optional, Tuple
from unittest.mock import patch, Mock

import pytest

from ray.actor import ActorHandle
from ray.serve.async_goal_manager import AsyncGoalManager
from ray.serve.common import (
    BackendConfig,
    BackendInfo,
    BackendTag,
    ReplicaConfig,
    ReplicaTag,
)
from ray.serve.backend_state import BackendState, ReplicaState


def mock_replica_factory(mock_replicas):
    class MockReplicaActorWrapper:
        def __init__(self, actor_name: str, detached: bool,
                     controller_name: str, replica_tag: ReplicaTag,
                     backend_tag: BackendTag):
            self._actor_name = actor_name
            self._replica_tag = replica_tag
            self._backend_tag = backend_tag
            self._state = ReplicaState.SHOULD_START

            mock_replicas.append(self)

            # Will be set when `start()` is called.
            self.started = False
            # Expected to be set in the test.
            self.ready = False
            # Will be set when `graceful_stop()` is called.
            self.stopped = False
            # Expected to be set in the test.
            self.done_stopping = False
            # Will be set when `force_stop()` is called.
            self.force_stopped_counter = 0

        def __del__(self):
            mock_replicas.remove(self)

        @property
        def actor_handle(self) -> ActorHandle:
            return None

        def set_ready(self):
            self.ready = True

        def set_done_stopping(self):
            self.done_stopping = True

        def start(self, backend_info: BackendInfo):
            self.started = True

        def check_ready(self) -> bool:
            assert self.started
            return self.ready

        def resource_requirements(
                self) -> Tuple[Dict[str, float], Dict[str, float]]:
            assert self.started
            return {"AVAILABLE_RESOURCE": 1.0}, {"REQUIRED_RESOURCE": 1.0}

        def graceful_stop(self) -> None:
            assert self.started
            self.stopped = True

        def check_stopped(self) -> bool:
            return self.done_stopping

        def force_stop(self):
            self.force_stopped_counter += 1

    return MockReplicaActorWrapper


def generate_configs(num_replicas: Optional[int] = 1
                     ) -> Tuple[BackendConfig, ReplicaConfig]:
    return BackendConfig(num_replicas=num_replicas), ReplicaConfig(lambda x: x)


class MockTimer:
    def __init__(self, start_time=None):
        if start_time is None:
            start_time = time.time()
        self._curr = start_time

    def time(self):
        return self._curr

    def advance(self, by):
        self._curr += by


@pytest.fixture
def mock_backend_state() -> Tuple[BackendState, Mock, Mock]:
    timer = MockTimer()
    mock_replicas = []
    with patch(
            "ray.serve.backend_state.ActorReplicaWrapper",
            new=mock_replica_factory(mock_replicas)), patch(
                "time.time",
                new=timer.time), patch("ray.serve.kv_store.RayInternalKVStore"
                                       ) as mock_kv_store, patch(
                                           "ray.serve.long_poll.LongPollHost"
                                       ) as mock_long_poll, patch.object(
                                           BackendState,
                                           "_checkpoint") as mock_checkpoint:
        mock_kv_store.get = Mock(return_value=None)
        goal_manager = AsyncGoalManager()
        backend_state = BackendState("name", True, mock_kv_store,
                                     mock_long_poll, goal_manager)
        mock_checkpoint.return_value = None
        yield backend_state, timer, mock_replicas, goal_manager


def replica_count(backend_state, backend=None, states=None):
    total = 0
    for backend_tag, state_dict in backend_state._replicas.items():
        if backend is None or backend_tag == backend:
            for state, replica_list in state_dict.items():
                if states is None or state in states:
                    total += len(replica_list)

    return total


def test_create_delete_single_replica(mock_backend_state):
    backend_state, timer, mock_replicas, goal_manager = mock_backend_state

    assert replica_count(backend_state) == 0

    b_config_1, r_config_1 = generate_configs()
    create_goal = backend_state.create_backend("tag1", b_config_1, r_config_1)

    # Single replica should be created.
    backend_state.update()
    assert replica_count(backend_state) == 1
    assert replica_count(backend_state, states=[ReplicaState.STARTING]) == 1
    assert mock_replicas[0].started

    # update() should not transition the state if the replica isn't ready.
    backend_state.update()
    assert replica_count(backend_state) == 1
    assert replica_count(backend_state, states=[ReplicaState.STARTING]) == 1
    mock_replicas[0].set_ready()
    assert not goal_manager.check_complete(create_goal)

    # Now the replica should be marked running.
    backend_state.update()
    assert replica_count(backend_state) == 1
    assert replica_count(backend_state, states=[ReplicaState.RUNNING]) == 1

    # TODO(edoakes): can we remove this extra update period for completing it?
    backend_state.update()
    assert goal_manager.check_complete(create_goal)

    # Removing the replica should transition it to stopping.
    delete_goal = backend_state.delete_backend("tag1")
    backend_state.update()
    assert replica_count(backend_state) == 1
    assert replica_count(backend_state, states=[ReplicaState.STOPPING]) == 1
    assert mock_replicas[0].stopped
    assert not goal_manager.check_complete(delete_goal)

    # Once it's done stopping, replica should be removed.
    mock_replicas[0].set_done_stopping()
    backend_state.update()
    assert replica_count(backend_state) == 0

    # TODO(edoakes): can we remove this extra update period for completing it?
    backend_state.update()
    assert goal_manager.check_complete(delete_goal)


def test_force_kill(mock_backend_state):
    backend_state, timer, mock_replicas, goal_manager = mock_backend_state

    assert replica_count(backend_state) == 0

    grace_period_s = 10
    b_config_1, r_config_1 = generate_configs()
    b_config_1.experimental_graceful_shutdown_timeout_s = grace_period_s

    # Create and delete the backend.
    backend_state.create_backend("tag1", b_config_1, r_config_1)
    backend_state.update()
    mock_replicas[0].set_ready()
    backend_state.update()
    delete_goal = backend_state.delete_backend("tag1")
    backend_state.update()

    # Replica should remain in STOPPING until it finishes.
    assert replica_count(backend_state) == 1
    assert replica_count(backend_state, states=[ReplicaState.STOPPING]) == 1
    assert mock_replicas[0].stopped

    backend_state.update()
    backend_state.update()

    # force_stop shouldn't be called until after the timer.
    assert not mock_replicas[0].force_stopped_counter
    assert replica_count(backend_state) == 1
    assert replica_count(backend_state, states=[ReplicaState.STOPPING]) == 1

    # Advance the timer, now the replica should be force stopped.
    timer.advance(grace_period_s + 0.1)
    backend_state.update()
    assert mock_replicas[0].force_stopped_counter == 1
    assert replica_count(backend_state) == 1
    assert replica_count(backend_state, states=[ReplicaState.STOPPING]) == 1
    assert not goal_manager.check_complete(delete_goal)

    # Force stop should be called repeatedly until the replica stops.
    backend_state.update()
    assert mock_replicas[0].force_stopped_counter == 2
    assert replica_count(backend_state) == 1
    assert replica_count(backend_state, states=[ReplicaState.STOPPING]) == 1
    assert not goal_manager.check_complete(delete_goal)

    # Once the replica is done stopping, it should be removed.
    mock_replicas[0].set_done_stopping()
    backend_state.update()
    assert replica_count(backend_state) == 0

    # TODO(edoakes): can we remove this extra update period for completing it?
    backend_state.update()
    assert goal_manager.check_complete(delete_goal)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
