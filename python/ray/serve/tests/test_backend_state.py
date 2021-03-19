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


class MockReplicaActorWrapper:
    def __init__(self, actor_name: str, detached: bool, controller_name: str,
                 replica_tag: ReplicaTag, backend_tag: BackendTag):
        self._actor_name = actor_name
        self._replica_tag = replica_tag
        self._backend_tag = backend_tag
        self._state = ReplicaState.SHOULD_START

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
        # Will be cleaned up when `cleanup()` is called.
        self.cleaned_up = False

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
        return {"REQUIRED_RESOURCE": 1.0}, {"AVAILABLE_RESOURCE": 1.0}

    def graceful_stop(self) -> None:
        assert self.started
        self.stopped = True

    def check_stopped(self) -> bool:
        return self.done_stopping

    def force_stop(self):
        self.force_stopped_counter += 1

    def cleanup(self):
        self.cleaned_up = True


def generate_configs(
        num_replicas: Optional[int] = 1,
        **config_opts,
) -> Tuple[BackendConfig, ReplicaConfig]:
    return BackendConfig(
        num_replicas=num_replicas, **config_opts), ReplicaConfig(lambda x: x)


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
    with patch(
            "ray.serve.backend_state.ActorReplicaWrapper",
            new=MockReplicaActorWrapper), patch(
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
        yield backend_state, timer, goal_manager


def replicas(backend_state, backend=None, states=None):
    replicas = []
    for backend_tag, state_dict in backend_state._replicas.items():
        if backend is None or backend_tag == backend:
            for state, replica_list in state_dict.items():
                if states is None or state in states:
                    replicas.extend(replica_list)

    return replicas


def test_override_goals(mock_backend_state):
    backend_state, _, goal_manager = mock_backend_state

    b_config_1, r_config_1 = generate_configs()
    initial_goal = backend_state.deploy_backend("tag1", b_config_1, r_config_1)
    assert not goal_manager.check_complete(initial_goal)

    b_config_2, r_config_2 = generate_configs(num_replicas=2)
    new_goal = backend_state.deploy_backend("tag1", b_config_2, r_config_2)
    assert goal_manager.check_complete(initial_goal)
    assert not goal_manager.check_complete(new_goal)


def test_return_existing_goal(mock_backend_state):
    backend_state, _, goal_manager = mock_backend_state

    b_config_1, r_config_1 = generate_configs()
    initial_goal = backend_state.deploy_backend("tag1", b_config_1, r_config_1)
    assert not goal_manager.check_complete(initial_goal)

    new_goal = backend_state.deploy_backend("tag1", b_config_1, r_config_1)
    assert initial_goal == new_goal
    assert not goal_manager.check_complete(initial_goal)


def test_create_delete_single_replica(mock_backend_state):
    backend_state, timer, goal_manager = mock_backend_state

    assert len(replicas(backend_state)) == 0

    b_config_1, r_config_1 = generate_configs()
    create_goal = backend_state.deploy_backend("tag1", b_config_1, r_config_1)

    # Single replica should be created.
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STARTING])) == 1
    assert replicas(backend_state)[0]._actor.started

    # update() should not transition the state if the replica isn't ready.
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STARTING])) == 1
    replicas(backend_state)[0]._actor.set_ready()
    assert not goal_manager.check_complete(create_goal)

    # Now the replica should be marked running.
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.RUNNING])) == 1

    # TODO(edoakes): can we remove this extra update period for completing it?
    backend_state.update()
    assert goal_manager.check_complete(create_goal)

    # Removing the replica should transition it to stopping.
    delete_goal = backend_state.delete_backend("tag1")
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STOPPING])) == 1
    assert replicas(backend_state)[0]._actor.stopped
    assert not replicas(backend_state)[0]._actor.cleaned_up
    assert not goal_manager.check_complete(delete_goal)

    # Once it's done stopping, replica should be removed.
    replica = replicas(backend_state)[0]
    replica._actor.set_done_stopping()
    backend_state.update()
    assert len(replicas(backend_state)) == 0

    # TODO(edoakes): can we remove this extra update period for completing it?
    backend_state.update()
    assert goal_manager.check_complete(delete_goal)
    replica._actor.cleaned_up


def test_force_kill(mock_backend_state):
    backend_state, timer, goal_manager = mock_backend_state

    assert len(replicas(backend_state)) == 0

    grace_period_s = 10
    b_config_1, r_config_1 = generate_configs()
    b_config_1.experimental_graceful_shutdown_timeout_s = grace_period_s

    # Create and delete the backend.
    backend_state.deploy_backend("tag1", b_config_1, r_config_1)
    backend_state.update()
    replicas(backend_state)[0]._actor.set_ready()
    backend_state.update()
    delete_goal = backend_state.delete_backend("tag1")
    backend_state.update()

    # Replica should remain in STOPPING until it finishes.
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STOPPING])) == 1
    assert replicas(backend_state)[0]._actor.stopped

    backend_state.update()
    backend_state.update()

    # force_stop shouldn't be called until after the timer.
    assert not replicas(backend_state)[0]._actor.force_stopped_counter
    assert not replicas(backend_state)[0]._actor.cleaned_up
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STOPPING])) == 1

    # Advance the timer, now the replica should be force stopped.
    timer.advance(grace_period_s + 0.1)
    backend_state.update()
    assert replicas(backend_state)[0]._actor.force_stopped_counter == 1
    assert not replicas(backend_state)[0]._actor.cleaned_up
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STOPPING])) == 1
    assert not goal_manager.check_complete(delete_goal)

    # Force stop should be called repeatedly until the replica stops.
    backend_state.update()
    assert replicas(backend_state)[0]._actor.force_stopped_counter == 2
    assert not replicas(backend_state)[0]._actor.cleaned_up
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STOPPING])) == 1
    assert not goal_manager.check_complete(delete_goal)

    # Once the replica is done stopping, it should be removed.
    replica = replicas(backend_state)[0]
    replica._actor.set_done_stopping()
    backend_state.update()
    assert len(replicas(backend_state)) == 0

    # TODO(edoakes): can we remove this extra update period for completing it?
    backend_state.update()
    assert goal_manager.check_complete(delete_goal)
    assert replica._actor.cleaned_up


def test_redeploy_same_version(mock_backend_state):
    # Redeploying with the same version and code should do nothing.
    backend_state, timer, goal_manager = mock_backend_state

    assert len(replicas(backend_state)) == 0

    b_config_1, r_config_1 = generate_configs()
    goal_1 = backend_state.deploy_backend(
        "tag1", b_config_1, r_config_1, version="1")

    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STARTING])) == 1
    assert replicas(backend_state)[0].version == "1"
    assert not goal_manager.check_complete(goal_1)

    # Test redeploying while the initial deployment is still pending.
    _, r_config_2 = generate_configs()
    goal_2 = backend_state.deploy_backend(
        "tag1", b_config_1, r_config_2, version="1")
    assert goal_1 == goal_2
    assert not goal_manager.check_complete(goal_1)

    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STARTING])) == 1
    assert replicas(backend_state)[0].version == "1"

    # Mark the replica ready. After this, the initial goal should be complete.
    replicas(backend_state)[0]._actor.set_ready()
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.RUNNING])) == 1
    assert replicas(backend_state)[0].version == "1"

    backend_state.update()
    assert goal_manager.check_complete(goal_1)

    # Test redeploying after the initial deployment has finished.
    same_version_goal = backend_state.deploy_backend(
        "tag1", b_config_1, r_config_1, version="1")
    assert goal_manager.check_complete(same_version_goal)
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.RUNNING])) == 1
    assert replicas(backend_state)[0].version == "1"
    assert goal_manager.check_complete(goal_2)


def test_redeploy_new_version(mock_backend_state):
    # Redeploying with a new version should start a new replica.
    backend_state, timer, goal_manager = mock_backend_state

    assert len(replicas(backend_state)) == 0

    b_config_1, r_config_1 = generate_configs()
    goal_1 = backend_state.deploy_backend(
        "tag1", b_config_1, r_config_1, version="1")

    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STARTING])) == 1
    assert replicas(backend_state)[0].version == "1"
    assert not goal_manager.check_complete(goal_1)

    # Test redeploying while the initial deployment is still pending.
    _, r_config_2 = generate_configs()
    goal_2 = backend_state.deploy_backend(
        "tag1", b_config_1, r_config_2, version="2")
    assert goal_1 != goal_2
    assert goal_manager.check_complete(goal_1)
    assert not goal_manager.check_complete(goal_2)

    # The initial replica should be stopping and the new replica starting.
    backend_state.update()
    assert len(replicas(backend_state)) == 2
    assert len(replicas(backend_state, states=[ReplicaState.STARTING])) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STOPPING])) == 1
    assert replicas(
        backend_state, states=[ReplicaState.STOPPING])[0].version == "1"
    assert replicas(
        backend_state, states=[ReplicaState.STARTING])[0].version == "2"

    # The initial replica should be gone and the new replica running.
    replicas(
        backend_state,
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    replicas(
        backend_state, states=[ReplicaState.STARTING])[0]._actor.set_ready()
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.RUNNING])) == 1
    assert replicas(
        backend_state, states=[ReplicaState.RUNNING])[0].version == "2"

    backend_state.update()
    assert goal_manager.check_complete(goal_2)

    # Now deploy a third version after the transition has finished.
    _, r_config_3 = generate_configs()
    goal_3 = backend_state.deploy_backend(
        "tag1", b_config_1, r_config_3, version="3")
    assert not goal_manager.check_complete(goal_3)

    backend_state.update()
    assert len(replicas(backend_state)) == 2
    assert len(replicas(backend_state, states=[ReplicaState.STOPPING])) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STARTING])) == 1
    assert replicas(
        backend_state, states=[ReplicaState.STOPPING])[0].version == "2"
    assert replicas(
        backend_state, states=[ReplicaState.STARTING])[0].version == "3"

    replicas(
        backend_state,
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    replicas(
        backend_state, states=[ReplicaState.STARTING])[0]._actor.set_ready()
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.RUNNING])) == 1
    assert replicas(
        backend_state, states=[ReplicaState.RUNNING])[0].version == "3"

    backend_state.update()
    assert goal_manager.check_complete(goal_3)


def test_deploy_new_config_same_version(mock_backend_state):
    # Deploying a new config with the same version should not deploy a new
    # replica.
    backend_state, timer, goal_manager = mock_backend_state

    assert len(replicas(backend_state)) == 0

    b_config_1, r_config_1 = generate_configs()
    create_goal = backend_state.deploy_backend(
        "tag1", b_config_1, r_config_1, version="1")

    # Create the replica initially.
    backend_state.update()
    replicas(backend_state)[0]._actor.set_ready()
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.RUNNING])) == 1
    backend_state.update()
    assert goal_manager.check_complete(create_goal)

    # Update to a new config without changing the version.
    b_config_2, _ = generate_configs(user_config={"hello": "world"})
    update_goal = backend_state.deploy_backend(
        "tag1", b_config_2, r_config_1, version="1")
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.RUNNING])) == 1
    backend_state.update()
    assert goal_manager.check_complete(update_goal)


def test_deploy_new_config_new_version(mock_backend_state):
    # Deploying a new config with a new version should deploy a new replica.
    backend_state, timer, goal_manager = mock_backend_state

    assert len(replicas(backend_state)) == 0

    b_config_1, r_config_1 = generate_configs()
    create_goal = backend_state.deploy_backend(
        "tag1", b_config_1, r_config_1, version="1")

    # Create the replica initially.
    backend_state.update()
    replicas(backend_state)[0]._actor.set_ready()
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.RUNNING])) == 1
    backend_state.update()
    assert goal_manager.check_complete(create_goal)

    # Update to a new config and a new version.
    b_config_2, _ = generate_configs(user_config={"hello": "world"})
    update_goal = backend_state.deploy_backend(
        "tag1", b_config_2, r_config_1, version="2")

    backend_state.update()
    assert len(replicas(backend_state)) == 2
    assert len(replicas(backend_state, states=[ReplicaState.STOPPING])) == 1
    assert len(replicas(backend_state, states=[ReplicaState.STARTING])) == 1
    assert replicas(
        backend_state, states=[ReplicaState.STOPPING])[0].version == "1"
    assert replicas(
        backend_state, states=[ReplicaState.STARTING])[0].version == "2"

    replicas(
        backend_state,
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    replicas(
        backend_state, states=[ReplicaState.STARTING])[0]._actor.set_ready()
    backend_state.update()
    assert len(replicas(backend_state)) == 1
    assert len(replicas(backend_state, states=[ReplicaState.RUNNING])) == 1
    assert replicas(
        backend_state, states=[ReplicaState.RUNNING])[0].version == "2"

    backend_state.update()
    assert goal_manager.check_complete(update_goal)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
