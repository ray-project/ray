import pytest
from typing import Optional, Tuple
from unittest.mock import patch, Mock
from uuid import uuid4

from ray.serve.common import BackendConfig, BackendInfo, ReplicaConfig
from ray.serve.backend_state import BackendState


def generate_mock_backend_info(
        num_replicas: Optional[int] = None) -> BackendInfo:
    backend_info = BackendInfo(
        worker_class=lambda x: x,
        backend_config=BackendConfig(),
        replica_config=ReplicaConfig(lambda x: x))
    if num_replicas:
        backend_info.backend_config.num_replicas = num_replicas

    return backend_info


@pytest.fixture
def mock_backend_state_inputs() -> Tuple[BackendState, Mock, Mock]:
    with patch(
            "ray.serve.kv_store.RayInternalKVStore") as mock_kv_store, patch(
                "ray.serve.long_poll.LongPollHost") as mock_long_poll, patch(
                    "ray.serve.async_goal_manager.AsyncGoalManager"
                ) as mock_goal_manager:
        mock_kv_store.get = Mock(return_value=None)
        backend_state = BackendState("name", True, mock_kv_store,
                                     mock_long_poll, mock_goal_manager)
        yield backend_state, mock_kv_store, mock_long_poll, mock_goal_manager


def test_completed_goals_deleted_backend(mock_backend_state_inputs):
    backend_state = mock_backend_state_inputs[0]
    b1 = "backend_one"
    backend_state.backends[b1] = None
    backend_state.backend_replicas[b1] = {}
    result_uuid_b1 = uuid4()
    backend_state.backend_goals[b1] = result_uuid_b1

    assert backend_state._completed_goals() == [result_uuid_b1]

    backend_state.backend_goals = {}

    b2 = "backend_two"
    backend_state.backends[b2] = None
    result_uuid_b2 = uuid4()
    backend_state.backend_goals[b2] = result_uuid_b2

    assert backend_state._completed_goals() == [result_uuid_b2]


def test_completed_goals_delta_backend(mock_backend_state_inputs):
    backend_state = mock_backend_state_inputs[0]
    b1 = "backend_one"
    backend_state.backends[b1] = generate_mock_backend_info()
    backend_state.backend_replicas[b1] = {i: i for i in range(1)}
    # NOTE(ilr): This test made it clear that the _completed_goals function
    # should (.get) from the dict.
    assert len(backend_state._completed_goals()) == 0

    backend_state.backends[b1] = generate_mock_backend_info(30)
    result_uuid = uuid4()
    backend_state.backend_goals[b1] = result_uuid
    assert len(backend_state._completed_goals()) == 0

    backend_state.backend_replicas[b1] = {i: i for i in range(30)}
    assert backend_state._completed_goals() == [result_uuid]


def test_completed_goals_created_backend(mock_backend_state_inputs):
    backend_state = mock_backend_state_inputs[0]
    assert len(backend_state._completed_goals()) == 0

    b1 = "backend_one"
    backend_state.backends[b1] = generate_mock_backend_info()
    result_uuid = uuid4()
    backend_state.backend_goals[b1] = result_uuid

    assert len(backend_state._completed_goals()) == 0

    backend_state.backend_replicas[b1] = {i: i for i in range(1)}

    assert backend_state._completed_goals() == [result_uuid]


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
