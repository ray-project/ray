import time
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch, Mock

import pytest

from ray.actor import ActorHandle
from ray.serve.common import (
    BackendConfig,
    BackendInfo,
    BackendTag,
    ReplicaConfig,
    ReplicaTag,
)
from ray.serve.backend_state import (
    BackendState,
    BackendStateManager,
    BackendVersion,
    ReplicaStartupStatus,
    ReplicaState,
    ReplicaStateContainer,
    VersionedReplica,
    CHECKPOINT_KEY,
)
from ray.serve.async_goal_manager import AsyncGoalManager
from ray.serve.storage.kv_store import RayLocalKVStore
from ray.serve.utils import get_random_letters


class TestUserConfigHash:
    def test_validation(self):
        # Code version must be a string.
        with pytest.raises(TypeError):
            BackendVersion(123, None)

        # Can't pass unhashable type as user config.
        with pytest.raises(TypeError):
            BackendVersion(123, set())

        # Can't pass nested unhashable type as user config.
        with pytest.raises(TypeError):
            BackendVersion(123, {"set": set()})

    def test_code_version(self):
        v1 = BackendVersion("1", None)
        v2 = BackendVersion("1", None)
        v3 = BackendVersion("2", None)

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_basic(self):
        v1 = BackendVersion("1", "1")
        v2 = BackendVersion("1", "1")
        v3 = BackendVersion("1", "2")

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_hashable(self):
        v1 = BackendVersion("1", ("1", "2"))
        v2 = BackendVersion("1", ("1", "2"))
        v3 = BackendVersion("1", ("1", "3"))

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_list(self):
        v1 = BackendVersion("1", ["1", "2"])
        v2 = BackendVersion("1", ["1", "2"])
        v3 = BackendVersion("1", ["1", "3"])

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_dict_keys(self):
        v1 = BackendVersion("1", {"1": "1"})
        v2 = BackendVersion("1", {"1": "1"})
        v3 = BackendVersion("1", {"2": "1"})

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_dict_vals(self):
        v1 = BackendVersion("1", {"1": "1"})
        v2 = BackendVersion("1", {"1": "1"})
        v3 = BackendVersion("1", {"1": "2"})

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_nested(self):
        v1 = BackendVersion("1", [{"1": "2"}, {"1": "2"}])
        v2 = BackendVersion("1", [{"1": "2"}, {"1": "2"}])
        v3 = BackendVersion("1", [{"1": "2"}, {"1": "3"}])

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_nested_in_hashable(self):
        v1 = BackendVersion("1", ([{"1": "2"}, {"1": "2"}], ))
        v2 = BackendVersion("1", ([{"1": "2"}, {"1": "2"}], ))
        v3 = BackendVersion("1", ([{"1": "2"}, {"1": "3"}], ))

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)


class MockReplicaActorWrapper:
    def __init__(self, actor_name: str, detached: bool, controller_name: str,
                 replica_tag: ReplicaTag, backend_tag: BackendTag):
        self._actor_name = actor_name
        self._replica_tag = replica_tag
        self._backend_tag = backend_tag

        # Will be set when `start()` is called.
        self.started = False
        # Will be set when `recover()` is called.
        self.recovering = False
        # Will be set when `start()` is called.
        self.version = None
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
        # Will be set when `check_health()` is called.
        self.health_check_called = False
        # Returned by the health check.
        self.healthy = True

    @property
    def replica_tag(self) -> str:
        return str(self._replica_tag)

    @property
    def backend_tag(self) -> str:
        return self._backend_tag

    @property
    def actor_handle(self) -> ActorHandle:
        return None

    def set_ready(self):
        self.ready = ReplicaStartupStatus.SUCCEEDED

    def set_failed_to_start(self):
        self.ready = ReplicaStartupStatus.FAILED

    def set_done_stopping(self):
        self.done_stopping = True

    def set_unhealthy(self):
        self.healthy = False

    def set_starting_version(self, version: BackendVersion):
        """Mocked backend_worker return version from reconfigure()"""
        self.starting_version = version

    def start(self, backend_info: BackendInfo, version: BackendVersion):
        self.started = True
        self.version = version

    def update_user_config(self, user_config: Any):
        self.started = True
        self.version = BackendVersion(
            self.version.code_version, user_config=user_config)

    def recover(self):
        self.recovering = True
        self.started = False
        self.version = None

    def check_ready(self) -> ReplicaStartupStatus:
        ready = self.ready
        self.ready = ReplicaStartupStatus.PENDING
        if ready == ReplicaStartupStatus.SUCCEEDED and self.recovering:
            self.recovering = False
            self.started = True
            self.version = self.starting_version
        return ready, self.version

    def resource_requirements(
            self) -> Tuple[Dict[str, float], Dict[str, float]]:
        assert self.started
        return {"REQUIRED_RESOURCE": 1.0}, {"AVAILABLE_RESOURCE": 1.0}

    @property
    def actor_resources(self) -> Dict[str, float]:
        return {"CPU": 0.1}

    @property
    def available_resources(self) -> Dict[str, float]:
        # Only used to print a warning.
        return {}

    def graceful_stop(self) -> None:
        assert self.started
        self.stopped = True

    def check_stopped(self) -> bool:
        return self.done_stopping

    def force_stop(self):
        self.force_stopped_counter += 1

    def cleanup(self):
        self.cleaned_up = True

    def check_health(self):
        self.health_check_called = True
        return self.healthy


def backend_info(version: Optional[str] = None,
                 num_replicas: Optional[int] = 1,
                 user_config: Optional[Any] = None,
                 **config_opts) -> Tuple[BackendInfo, BackendVersion]:
    info = BackendInfo(
        actor_def=None,
        version=version,
        start_time_ms=0,
        backend_config=BackendConfig(
            num_replicas=num_replicas, user_config=user_config, **config_opts),
        replica_config=ReplicaConfig(lambda x: x))

    if version is not None:
        code_version = version
    else:
        code_version = get_random_letters()

    version = BackendVersion(code_version, info.backend_config.user_config)

    return info, version


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
                "time.time", new=timer.time), patch(
                    "ray.serve.long_poll.LongPollHost") as mock_long_poll:

        goal_manager = AsyncGoalManager()
        backend_state = BackendState("name", "name", True, mock_long_poll,
                                     goal_manager, lambda: None)
        yield backend_state, timer, goal_manager


def replica(version: Optional[BackendVersion] = None) -> VersionedReplica:
    if version is None:
        version = BackendVersion(get_random_letters(), None)

    class MockVersionedReplica(VersionedReplica):
        def __init__(self, version: BackendVersion):
            self._version = version

        @property
        def version(self):
            return self._version

    return MockVersionedReplica(version)


class TestReplicaStateContainer:
    def test_count(self):
        c = ReplicaStateContainer()
        r1, r2, r3 = replica(BackendVersion("1")), replica(
            BackendVersion("2")), replica(BackendVersion("2"))
        c.add(ReplicaState.STARTING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.STOPPING, r3)
        assert c.count() == 3

        # Test filtering by state.
        assert c.count() == c.count(
            states=[ReplicaState.STARTING, ReplicaState.STOPPING])
        assert c.count(states=[ReplicaState.STARTING]) == 2
        assert c.count(states=[ReplicaState.STOPPING]) == 1

        # Test filtering by version.
        assert c.count(version=BackendVersion("1")) == 1
        assert c.count(version=BackendVersion("2")) == 2
        assert c.count(version=BackendVersion("3")) == 0
        assert c.count(exclude_version=BackendVersion("1")) == 2
        assert c.count(exclude_version=BackendVersion("2")) == 1
        assert c.count(exclude_version=BackendVersion("3")) == 3

        # Test filtering by state and version.
        assert c.count(
            version=BackendVersion("1"), states=[ReplicaState.STARTING]) == 1
        assert c.count(
            version=BackendVersion("3"), states=[ReplicaState.STARTING]) == 0
        assert c.count(
            version=BackendVersion("2"),
            states=[ReplicaState.STARTING, ReplicaState.STOPPING]) == 2
        assert c.count(
            exclude_version=BackendVersion("1"),
            states=[ReplicaState.STARTING]) == 1
        assert c.count(
            exclude_version=BackendVersion("3"),
            states=[ReplicaState.STARTING]) == 2
        assert c.count(
            exclude_version=BackendVersion("2"),
            states=[ReplicaState.STARTING, ReplicaState.STOPPING]) == 1

    def test_get(self):
        c = ReplicaStateContainer()
        r1, r2, r3 = replica(), replica(), replica()

        c.add(ReplicaState.STARTING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.STOPPING, r3)
        assert c.get() == [r1, r2, r3]
        assert c.get() == c.get([ReplicaState.STARTING, ReplicaState.STOPPING])
        assert c.get([ReplicaState.STARTING]) == [r1, r2]
        assert c.get([ReplicaState.STOPPING]) == [r3]

    def test_pop_basic(self):
        c = ReplicaStateContainer()
        r1, r2, r3 = replica(), replica(), replica()

        c.add(ReplicaState.STARTING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.STOPPING, r3)
        assert c.pop() == [r1, r2, r3]
        assert not c.pop()

    def test_pop_exclude_version(self):
        c = ReplicaStateContainer()
        r1, r2, r3 = replica(BackendVersion("1")), replica(
            BackendVersion("1")), replica(BackendVersion("2"))

        c.add(ReplicaState.STARTING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.STARTING, r3)
        assert c.pop(exclude_version=BackendVersion("1")) == [r3]
        assert not c.pop(exclude_version=BackendVersion("1"))
        assert c.pop(exclude_version=BackendVersion("2")) == [r1, r2]
        assert not c.pop(exclude_version=BackendVersion("2"))
        assert not c.pop()

    def test_pop_max_replicas(self):
        c = ReplicaStateContainer()
        r1, r2, r3 = replica(), replica(), replica()

        c.add(ReplicaState.STARTING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.STOPPING, r3)
        assert not c.pop(max_replicas=0)
        assert len(c.pop(max_replicas=1)) == 1
        assert len(c.pop(max_replicas=2)) == 2
        c.add(ReplicaState.STARTING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.STOPPING, r3)
        assert len(c.pop(max_replicas=10)) == 3

    def test_pop_states(self):
        c = ReplicaStateContainer()
        r1, r2, r3, r4 = replica(), replica(), replica(), replica()

        # Check popping single state.
        c.add(ReplicaState.STOPPING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.STOPPING, r3)
        assert c.pop(states=[ReplicaState.STARTING]) == [r2]
        assert not c.pop(states=[ReplicaState.STARTING])
        assert c.pop(states=[ReplicaState.STOPPING]) == [r1, r3]
        assert not c.pop(states=[ReplicaState.STOPPING])

        # Check popping multiple states. Ordering of states should be
        # preserved.
        c.add(ReplicaState.STOPPING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.STOPPING, r3)
        c.add(ReplicaState.STARTING, r4)
        assert c.pop(
            states=[ReplicaState.STOPPING, ReplicaState.STARTING]) == [
                r1, r3, r2, r4
            ]
        assert not c.pop(states=[ReplicaState.STOPPING, ReplicaState.STARTING])
        assert not c.pop(states=[ReplicaState.STOPPING])
        assert not c.pop(states=[ReplicaState.STARTING])
        assert not c.pop()

    def test_pop_integration(self):
        c = ReplicaStateContainer()
        r1, r2, r3, r4 = replica(BackendVersion("1")), replica(
            BackendVersion("2")), replica(BackendVersion("2")), replica(
                BackendVersion("3"))

        c.add(ReplicaState.STOPPING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.RUNNING, r3)
        c.add(ReplicaState.RUNNING, r4)
        assert not c.pop(
            exclude_version=BackendVersion("1"),
            states=[ReplicaState.STOPPING])
        assert c.pop(
            exclude_version=BackendVersion("1"),
            states=[ReplicaState.RUNNING],
            max_replicas=1) == [r3]
        assert c.pop(
            exclude_version=BackendVersion("1"),
            states=[ReplicaState.RUNNING],
            max_replicas=1) == [r4]
        c.add(ReplicaState.RUNNING, r3)
        c.add(ReplicaState.RUNNING, r4)
        assert c.pop(
            exclude_version=BackendVersion("1"),
            states=[ReplicaState.RUNNING]) == [r3, r4]
        assert c.pop(
            exclude_version=BackendVersion("1"),
            states=[ReplicaState.STARTING]) == [r2]
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.RUNNING, r3)
        c.add(ReplicaState.RUNNING, r4)
        assert c.pop(
            exclude_version=BackendVersion("1"),
            states=[ReplicaState.RUNNING,
                    ReplicaState.STARTING]) == [r3, r4, r2]
        assert c.pop(
            exclude_version=BackendVersion("nonsense"),
            states=[ReplicaState.STOPPING]) == [r1]


def test_override_goals(mock_backend_state):
    backend_state, _, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info()
    initial_goal, updating = backend_state.deploy(b_info_1)
    assert updating
    assert not goal_manager.check_complete(initial_goal)

    b_info_2, b_version_2 = backend_info(num_replicas=2)
    new_goal, updating = backend_state.deploy(b_info_2)
    assert updating
    assert goal_manager.check_complete(initial_goal)
    assert not goal_manager.check_complete(new_goal)


def test_return_existing_goal(mock_backend_state):
    backend_state, _, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(version="1")
    initial_goal, updating = backend_state.deploy(b_info_1)
    assert updating
    assert not goal_manager.check_complete(initial_goal)

    new_goal, updating = backend_state.deploy(b_info_1)
    assert not updating
    assert initial_goal == new_goal
    assert not goal_manager.check_complete(initial_goal)


def check_counts(backend_state: BackendState,
                 total: Optional[int] = None,
                 version: Optional[str] = None,
                 by_state: Optional[List[Tuple[ReplicaState, int]]] = None):
    if total is not None:
        assert backend_state._replicas.count(version=version) == total

    if by_state is not None:
        for state, count in by_state:
            assert isinstance(state, ReplicaState)
            assert isinstance(count, int) and count >= 0
            curr_count = backend_state._replicas.count(
                version=version, states=[state])
            msg = f"Expected {count} for state {state} but got {curr_count}."
            assert curr_count == count, msg


def test_create_delete_single_replica(mock_backend_state):
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info()
    create_goal, updating = backend_state.deploy(b_info_1)
    assert updating

    # Single replica should be created.
    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STARTING, 1)])

    # update() should not transition the state if the replica isn't ready.
    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STARTING, 1)])
    backend_state._replicas.get()[0]._actor.set_ready()
    assert not goal_manager.check_complete(create_goal)

    # Now the replica should be marked running.
    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])

    # TODO(edoakes): can we remove this extra update period for completing it?
    backend_state.update()
    assert goal_manager.check_complete(create_goal)

    # Removing the replica should transition it to stopping.
    delete_goal = backend_state.delete()
    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])
    assert backend_state._replicas.get()[0]._actor.stopped
    assert not backend_state._replicas.get()[0]._actor.cleaned_up
    assert not goal_manager.check_complete(delete_goal)

    # Once it's done stopping, replica should be removed.
    replica = backend_state._replicas.get()[0]
    replica._actor.set_done_stopping()
    backend_state.update()
    check_counts(backend_state, total=0)

    # TODO(edoakes): can we remove this extra update period for completing it?
    deleted = backend_state.update()
    assert deleted
    assert goal_manager.check_complete(delete_goal)
    assert replica._actor.cleaned_up


def test_force_kill(mock_backend_state):
    backend_state, timer, goal_manager = mock_backend_state

    grace_period_s = 10
    b_info_1, b_version_1 = backend_info(
        experimental_graceful_shutdown_timeout_s=grace_period_s)

    # Create and delete the backend.
    backend_state.deploy(b_info_1)
    backend_state.update()
    backend_state._replicas.get()[0]._actor.set_ready()
    backend_state.update()
    delete_goal = backend_state.delete()
    backend_state.update()

    # Replica should remain in STOPPING until it finishes.
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])
    assert backend_state._replicas.get()[0]._actor.stopped

    backend_state.update()
    backend_state.update()

    # force_stop shouldn't be called until after the timer.
    assert not backend_state._replicas.get()[0]._actor.force_stopped_counter
    assert not backend_state._replicas.get()[0]._actor.cleaned_up
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])

    # Advance the timer, now the replica should be force stopped.
    timer.advance(grace_period_s + 0.1)
    backend_state.update()
    assert backend_state._replicas.get()[0]._actor.force_stopped_counter == 1
    assert not backend_state._replicas.get()[0]._actor.cleaned_up
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])
    assert not goal_manager.check_complete(delete_goal)

    # Force stop should be called repeatedly until the replica stops.
    backend_state.update()
    assert backend_state._replicas.get()[0]._actor.force_stopped_counter == 2
    assert not backend_state._replicas.get()[0]._actor.cleaned_up
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])
    assert not goal_manager.check_complete(delete_goal)

    # Once the replica is done stopping, it should be removed.
    replica = backend_state._replicas.get()[0]
    replica._actor.set_done_stopping()
    backend_state.update()
    check_counts(backend_state, total=0)

    # TODO(edoakes): can we remove this extra update period for completing it?
    deleted = backend_state.update()
    assert deleted
    assert goal_manager.check_complete(delete_goal)
    assert replica._actor.cleaned_up


def test_redeploy_same_version(mock_backend_state):
    # Redeploying with the same version and code should do nothing.
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(version="1")
    goal_1, updating = backend_state.deploy(b_info_1)
    assert updating

    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)])
    assert not goal_manager.check_complete(goal_1)

    # Test redeploying while the initial deployment is still pending.
    goal_2, updating = backend_state.deploy(b_info_1)
    assert not updating
    assert goal_1 == goal_2
    assert not goal_manager.check_complete(goal_1)

    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)])

    # Mark the replica ready. After this, the initial goal should be complete.
    backend_state._replicas.get()[0]._actor.set_ready()
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    backend_state.update()
    assert goal_manager.check_complete(goal_1)

    # Test redeploying after the initial deployment has finished.
    same_version_goal, updating = backend_state.deploy(b_info_1)
    assert not updating
    assert goal_manager.check_complete(same_version_goal)
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])
    assert goal_manager.check_complete(goal_2)


def test_redeploy_no_version(mock_backend_state):
    # Redeploying with no version specified (`None`) should always redeploy
    # the replicas.
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(version=None)
    goal_1, updating = backend_state.deploy(b_info_1)
    assert updating

    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STARTING, 1)])
    assert not goal_manager.check_complete(goal_1)

    # Test redeploying while the initial deployment is still pending.
    goal_2, updating = backend_state.deploy(b_info_1)
    assert updating
    assert goal_1 != goal_2
    assert goal_manager.check_complete(goal_1)
    assert not goal_manager.check_complete(goal_2)

    # The initial replica should be stopping. The new replica shouldn't start
    # until the old one has completely stopped.
    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])

    backend_state.update()
    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    backend_state.update()
    check_counts(backend_state, total=0)

    # Now that the old replica has stopped, the new replica should be started.
    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STARTING, 1)])
    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]._actor.set_ready()

    # Check that the new replica has started.
    backend_state.update()
    check_counts(backend_state, total=1)
    check_counts(backend_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])

    backend_state.update()
    assert goal_manager.check_complete(goal_2)

    # Now deploy a third version after the transition has finished.
    b_info_3, b_version_3 = backend_info(version="3")
    goal_3, updating = backend_state.deploy(b_info_3)
    assert updating
    assert not goal_manager.check_complete(goal_3)

    backend_state.update()
    check_counts(backend_state, total=1)
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])

    backend_state.update()
    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()

    backend_state.update()
    check_counts(backend_state, total=0)

    backend_state.update()
    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]._actor.set_ready()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STARTING, 1)])

    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])

    deleted = backend_state.update()
    assert goal_manager.check_complete(goal_3)
    assert not deleted


def test_redeploy_new_version(mock_backend_state):
    # Redeploying with a new version should start a new replica.
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(version="1")
    goal_1, updating = backend_state.deploy(b_info_1)
    assert updating

    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)])
    assert not goal_manager.check_complete(goal_1)

    # Test redeploying while the initial deployment is still pending.
    b_info_2, b_version_2 = backend_info(version="2")
    goal_2, updating = backend_state.deploy(b_info_2)
    assert updating
    assert goal_1 != goal_2
    assert goal_manager.check_complete(goal_1)
    assert not goal_manager.check_complete(goal_2)

    # The initial replica should be stopping. The new replica shouldn't start
    # until the old one has completely stopped.
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)])

    backend_state.update()
    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    backend_state.update()
    check_counts(backend_state, total=0)

    # Now that the old replica has stopped, the new replica should be started.
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)])
    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]._actor.set_ready()

    # Check that the new replica has started.
    backend_state.update()
    check_counts(backend_state, total=1)
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    backend_state.update()
    assert goal_manager.check_complete(goal_2)

    # Now deploy a third version after the transition has finished.
    b_info_3, b_version_3 = backend_info(version="3")
    goal_3, updating = backend_state.deploy(b_info_3)
    assert updating
    assert not goal_manager.check_complete(goal_3)

    backend_state.update()
    check_counts(backend_state, total=1)
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)])

    backend_state.update()
    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()

    backend_state.update()
    check_counts(backend_state, total=0)

    backend_state.update()
    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]._actor.set_ready()
    check_counts(
        backend_state,
        version=b_version_3,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)])

    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_3,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    deleted = backend_state.update()
    assert goal_manager.check_complete(goal_3)
    assert not deleted


def test_deploy_new_config_same_version(mock_backend_state):
    # Deploying a new config with the same version should not deploy a new
    # replica.
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(version="1")
    goal_id, updated = backend_state.deploy(b_info_1)

    # Create the replica initially.
    backend_state.update()
    backend_state._replicas.get()[0]._actor.set_ready()
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])
    backend_state.update()
    assert goal_manager.check_complete(goal_id)

    # Update to a new config without changing the version.
    b_info_2, b_version_2 = backend_info(
        version="1", user_config={"hello": "world"})
    goal_id, updated = backend_state.deploy(b_info_2)
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    backend_state.update()
    check_counts(backend_state, total=1)
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.UPDATING, 1)])

    # Mark the replica as ready.
    backend_state._replicas.get()[0]._actor.set_ready()

    backend_state.update()
    check_counts(backend_state, total=1)
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    backend_state.update()
    assert goal_manager.check_complete(goal_id)


def test_deploy_new_config_new_version(mock_backend_state):
    # Deploying a new config with a new version should deploy a new replica.
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(version="1")
    create_goal = backend_state.deploy(b_info_1)

    # Create the replica initially.
    backend_state.update()
    backend_state._replicas.get()[0]._actor.set_ready()
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])
    backend_state.update()
    assert goal_manager.check_complete(create_goal)

    # Update to a new config and a new version.
    b_info_2, b_version_2 = backend_info(
        version="2", user_config={"hello": "world"})
    update_goal = backend_state.deploy(b_info_2)

    # New version shouldn't start until old version is stopped.
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)])
    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    backend_state.update()
    assert backend_state._replicas.count() == 0
    check_counts(backend_state, total=0)

    # Now the new version should be started.
    backend_state.update()
    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]._actor.set_ready()
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)])

    # Check that the new version is now running.
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    backend_state.update()
    assert goal_manager.check_complete(update_goal)


def test_initial_deploy_no_throttling(mock_backend_state):
    # All replicas should be started at once for a new deployment.
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(num_replicas=10, version="1")
    goal_1, updating = backend_state.deploy(b_info_1)
    assert updating

    backend_state.update()
    check_counts(
        backend_state, total=10, by_state=[(ReplicaState.STARTING, 10)])
    assert not goal_manager.check_complete(goal_1)

    for replica in backend_state._replicas.get():
        replica._actor.set_ready()

    backend_state.update()

    # Check that the new replicas have started.
    backend_state.update()
    check_counts(
        backend_state, total=10, by_state=[(ReplicaState.RUNNING, 10)])
    assert goal_manager.check_complete(goal_1)


def test_new_version_deploy_throttling(mock_backend_state):
    # All replicas should be started at once for a new deployment.
    # When the version is updated, it should be throttled. The throttling
    # should apply to both code version and user config updates.
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(
        num_replicas=10, version="1", user_config="1")
    goal_1, updating = backend_state.deploy(b_info_1)
    assert updating

    backend_state.update()
    check_counts(
        backend_state, total=10, by_state=[(ReplicaState.STARTING, 10)])
    assert not goal_manager.check_complete(goal_1)

    for replica in backend_state._replicas.get():
        replica._actor.set_ready()

    backend_state.update()

    # Check that the new replicas have started.
    backend_state.update()
    check_counts(
        backend_state, total=10, by_state=[(ReplicaState.RUNNING, 10)])
    assert goal_manager.check_complete(goal_1)

    # Now deploy a new version. Two old replicas should be stopped.
    b_info_2, b_version_2 = backend_info(
        num_replicas=10, version="2", user_config="2")
    goal_2, updating = backend_state.deploy(b_info_2)
    assert updating
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=10,
        by_state=[(ReplicaState.RUNNING, 8), (ReplicaState.STOPPING, 2)])

    # Mark only one of the replicas as done stopping.
    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()

    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=9,
        by_state=[(ReplicaState.RUNNING, 8), (ReplicaState.STOPPING, 1)])

    # Now one of the new version replicas should start up.
    backend_state.update()
    check_counts(backend_state, total=10)
    check_counts(
        backend_state,
        version=b_version_1,
        total=9,
        by_state=[(ReplicaState.RUNNING, 8), (ReplicaState.STOPPING, 1)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)])

    # Mark the new version replica as ready. Another old version replica
    # should subsequently be stopped.
    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]._actor.set_ready()
    backend_state.update()

    backend_state.update()
    check_counts(backend_state, total=10)
    check_counts(
        backend_state,
        version=b_version_1,
        total=9,
        by_state=[(ReplicaState.RUNNING, 7), (ReplicaState.STOPPING, 2)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    # Mark the old replicas as done stopping.
    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[1]._actor.set_done_stopping()

    assert not goal_manager.check_complete(goal_2)

    # Old replicas should be stopped and new versions started in batches of 2.
    new_replicas = 1
    old_replicas = 9
    while old_replicas > 3:
        backend_state.update()

        check_counts(backend_state, total=8)
        check_counts(
            backend_state,
            version=b_version_1,
            total=old_replicas - 2,
            by_state=[(ReplicaState.RUNNING, old_replicas - 2)])
        check_counts(
            backend_state,
            version=b_version_2,
            total=new_replicas,
            by_state=[(ReplicaState.RUNNING, new_replicas)])

        # Replicas starting up.
        backend_state.update()
        check_counts(backend_state, total=10)
        check_counts(
            backend_state,
            version=b_version_1,
            total=old_replicas - 2,
            by_state=[(ReplicaState.RUNNING, old_replicas - 2)])
        check_counts(
            backend_state,
            version=b_version_2,
            total=new_replicas + 2,
            by_state=[(ReplicaState.RUNNING, new_replicas),
                      (ReplicaState.STARTING, 2)])

        # Set both ready.
        backend_state._replicas.get(
            states=[ReplicaState.STARTING])[0]._actor.set_ready()
        backend_state._replicas.get(
            states=[ReplicaState.STARTING])[1]._actor.set_ready()
        new_replicas += 2

        backend_state.update()
        check_counts(backend_state, total=10)
        check_counts(
            backend_state,
            version=b_version_1,
            total=old_replicas - 2,
            by_state=[(ReplicaState.RUNNING, old_replicas - 2)])
        check_counts(
            backend_state,
            version=b_version_2,
            total=new_replicas,
            by_state=[(ReplicaState.RUNNING, new_replicas)])

        # Two more old replicas should be stopped.
        old_replicas -= 2
        backend_state.update()
        check_counts(backend_state, total=10)
        check_counts(
            backend_state,
            version=b_version_1,
            total=old_replicas,
            by_state=[(ReplicaState.RUNNING, old_replicas - 2),
                      (ReplicaState.STOPPING, 2)])
        check_counts(
            backend_state,
            version=b_version_2,
            total=new_replicas,
            by_state=[(ReplicaState.RUNNING, new_replicas)])

        backend_state._replicas.get(
            states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
        backend_state._replicas.get(
            states=[ReplicaState.STOPPING])[1]._actor.set_done_stopping()

        assert not goal_manager.check_complete(goal_2)

    # 2 left to update.
    backend_state.update()
    check_counts(backend_state, total=8)
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=new_replicas,
        by_state=[(ReplicaState.RUNNING, 7)])

    # Replicas starting up.
    backend_state.update()
    check_counts(backend_state, total=10)
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=9,
        by_state=[(ReplicaState.RUNNING, 7), (ReplicaState.STARTING, 2)])

    # Set both ready.
    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]._actor.set_ready()
    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[1]._actor.set_ready()

    # One replica remaining to update.
    backend_state.update()
    check_counts(backend_state, total=10)
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=9,
        by_state=[(ReplicaState.RUNNING, 9)])

    # The last replica should be stopped.
    backend_state.update()
    check_counts(backend_state, total=10)
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=9,
        by_state=[(ReplicaState.RUNNING, 9)])

    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()

    backend_state.update()
    check_counts(backend_state, total=9)
    check_counts(
        backend_state,
        version=b_version_2,
        total=9,
        by_state=[(ReplicaState.RUNNING, 9)])

    # The last replica should start up.
    backend_state.update()
    check_counts(backend_state, total=10)
    check_counts(
        backend_state,
        version=b_version_2,
        total=10,
        by_state=[(ReplicaState.RUNNING, 9), (ReplicaState.STARTING, 1)])

    assert not goal_manager.check_complete(goal_2)

    # Set both ready.
    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]._actor.set_ready()
    backend_state.update()
    check_counts(backend_state, total=10)
    check_counts(
        backend_state,
        version=b_version_2,
        total=10,
        by_state=[(ReplicaState.RUNNING, 10)])

    backend_state.update()
    assert goal_manager.check_complete(goal_2)


def test_reconfigure_throttling(mock_backend_state):
    # All replicas should be started at once for a new deployment.
    # When the version is updated, it should be throttled.
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(
        num_replicas=2, version="1", user_config="1")
    goal_1, updating = backend_state.deploy(b_info_1)
    assert updating

    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert not goal_manager.check_complete(goal_1)

    for replica in backend_state._replicas.get():
        replica._actor.set_ready()

    backend_state.update()

    # Check that the new replicas have started.
    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])
    assert goal_manager.check_complete(goal_1)

    # Now deploy a new user_config. One replica should be updated.
    b_info_2, b_version_2 = backend_info(
        num_replicas=2, version="1", user_config="2")
    goal_2, updating = backend_state.deploy(b_info_2)
    assert updating
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.UPDATING, 1)])

    # Mark the updating replica as ready.
    backend_state._replicas.get(
        states=[ReplicaState.UPDATING])[0]._actor.set_ready()

    # The updated replica should now be RUNNING.
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    # The second replica should now be updated.
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_2,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.UPDATING, 1)])

    # Mark the updating replica as ready.
    backend_state._replicas.get(
        states=[ReplicaState.UPDATING])[0]._actor.set_ready()

    # Both replicas should now be RUNNING.
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_2,
        total=2,
        by_state=[(ReplicaState.RUNNING, 2)])

    backend_state.update()
    assert goal_manager.check_complete(goal_1)


def test_new_version_and_scale_down(mock_backend_state):
    # Test the case when we reduce the number of replicas and change the
    # version at the same time. First the number of replicas should be
    # turned down, then the rolling update should happen.
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(num_replicas=10, version="1")
    goal_1, updating = backend_state.deploy(b_info_1)
    assert updating

    backend_state.update()
    check_counts(
        backend_state, total=10, by_state=[(ReplicaState.STARTING, 10)])
    assert not goal_manager.check_complete(goal_1)

    for replica in backend_state._replicas.get():
        replica._actor.set_ready()

    backend_state.update()

    # Check that the new replicas have started.
    backend_state.update()
    check_counts(
        backend_state, total=10, by_state=[(ReplicaState.RUNNING, 10)])
    assert goal_manager.check_complete(goal_1)

    # Now deploy a new version and scale down the number of replicas to 2.
    # First, 8 old replicas should be stopped to bring it down to the target.
    b_info_2, b_version_2 = backend_info(num_replicas=2, version="2")
    goal_2, updating = backend_state.deploy(b_info_2)
    assert updating
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=10,
        by_state=[(ReplicaState.RUNNING, 2), (ReplicaState.STOPPING, 8)])

    # Mark only one of the replicas as done stopping.
    # This should not yet trigger the rolling update because there are still
    # stopping replicas.
    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()

    backend_state.update()
    check_counts(backend_state, total=9)
    check_counts(
        backend_state,
        version=b_version_1,
        total=9,
        by_state=[(ReplicaState.RUNNING, 2), (ReplicaState.STOPPING, 7)])

    # Stop the remaining replicas.
    for replica in backend_state._replicas.get(states=[ReplicaState.STOPPING]):
        replica._actor.set_done_stopping()

    # Now the rolling update should trigger, stopping one of the old replicas.
    backend_state.update()
    check_counts(backend_state, total=2)
    check_counts(
        backend_state,
        version=b_version_1,
        total=2,
        by_state=[(ReplicaState.RUNNING, 2)])

    backend_state.update()
    check_counts(backend_state, total=2)
    check_counts(
        backend_state,
        version=b_version_1,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STOPPING, 1)])

    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()

    backend_state.update()
    check_counts(backend_state, total=1)
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    # Old version stopped, new version should start up.
    backend_state.update()
    check_counts(backend_state, total=2)
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)])

    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]._actor.set_ready()
    backend_state.update()
    check_counts(backend_state, total=2)
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    # New version is started, final old version replica should be stopped.
    backend_state.update()
    check_counts(backend_state, total=2)
    check_counts(
        backend_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    backend_state.update()
    check_counts(backend_state, total=1)
    check_counts(
        backend_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)])

    # Final old version replica is stopped, final new version replica
    # should be started.
    backend_state.update()
    check_counts(backend_state, total=2)
    check_counts(
        backend_state,
        version=b_version_2,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STARTING, 1)])

    backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]._actor.set_ready()
    backend_state.update()
    check_counts(backend_state, total=2)
    check_counts(
        backend_state,
        version=b_version_2,
        total=2,
        by_state=[(ReplicaState.RUNNING, 2)])

    backend_state.update()
    assert goal_manager.check_complete(goal_2)


def test_new_version_and_scale_up(mock_backend_state):
    # Test the case when we increase the number of replicas and change the
    # version at the same time. The new replicas should all immediately be
    # turned up. When they're up, rolling update should trigger.
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(num_replicas=2, version="1")
    goal_1, updating = backend_state.deploy(b_info_1)
    assert updating

    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert not goal_manager.check_complete(goal_1)

    for replica in backend_state._replicas.get():
        replica._actor.set_ready()

    backend_state.update()

    # Check that the new replicas have started.
    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])
    assert goal_manager.check_complete(goal_1)

    # Now deploy a new version and scale up the number of replicas to 10.
    # 8 new replicas should be started.
    b_info_2, b_version_2 = backend_info(num_replicas=10, version="2")
    goal_2, updating = backend_state.deploy(b_info_2)
    assert updating
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=2,
        by_state=[(ReplicaState.RUNNING, 2)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=8,
        by_state=[(ReplicaState.STARTING, 8)])

    # Mark the new replicas as ready.
    for replica in backend_state._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=2,
        by_state=[(ReplicaState.RUNNING, 2)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=8,
        by_state=[(ReplicaState.RUNNING, 8)])

    # Now that the new version replicas are up, rolling update should start.
    backend_state.update()
    check_counts(
        backend_state,
        version=b_version_1,
        total=2,
        by_state=[(ReplicaState.RUNNING, 0), (ReplicaState.STOPPING, 2)])
    check_counts(
        backend_state,
        version=b_version_2,
        total=8,
        by_state=[(ReplicaState.RUNNING, 8)])

    # Mark the replicas as done stopping.
    for replica in backend_state._replicas.get(states=[ReplicaState.STOPPING]):
        replica._actor.set_done_stopping()

    backend_state.update()
    check_counts(backend_state, total=8)
    check_counts(
        backend_state,
        version=b_version_2,
        total=8,
        by_state=[(ReplicaState.RUNNING, 8)])

    # The remaining replicas should be started.
    backend_state.update()
    check_counts(backend_state, total=10)
    check_counts(
        backend_state,
        version=b_version_2,
        total=10,
        by_state=[(ReplicaState.RUNNING, 8), (ReplicaState.STARTING, 2)])

    # Mark the remaining replicas as ready.
    for replica in backend_state._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()

    # All new replicas should be up and running.
    backend_state.update()
    check_counts(backend_state, total=10)
    check_counts(
        backend_state,
        version=b_version_2,
        total=10,
        by_state=[(ReplicaState.RUNNING, 10)])

    backend_state.update()
    assert goal_manager.check_complete(goal_2)


def test_health_check(mock_backend_state):
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(num_replicas=2, version="1")
    goal_1, updating = backend_state.deploy(b_info_1)
    assert updating

    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert not goal_manager.check_complete(goal_1)

    for replica in backend_state._replicas.get():
        replica._actor.set_ready()
        # Health check shouldn't be called until it's ready.
        assert not replica._actor.health_check_called

    # Check that the new replicas have started.
    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])

    backend_state.update()
    assert goal_manager.check_complete(goal_1)

    backend_state.update()
    for replica in backend_state._replicas.get():
        # Health check shouldn't be called until it's ready.
        assert replica._actor.health_check_called

    # Mark one replica unhealthy. It should be stopped.
    backend_state._replicas.get()[0]._actor.set_unhealthy()
    backend_state.update()
    check_counts(
        backend_state,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STOPPING, 1)])

    replica = backend_state._replicas.get(states=[ReplicaState.STOPPING])[0]
    replica._actor.set_done_stopping()

    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])

    backend_state.update()
    check_counts(
        backend_state,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STARTING, 1)])

    replica = backend_state._replicas.get(states=[ReplicaState.STARTING])[0]
    replica._actor.set_ready()

    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])


def _constructor_failure_loop_two_replica(backend_state, num_loops):
    """Helper function to exact constructor failure loops.
    """
    deleted = False
    for i in range(num_loops):
        # Single replica should be created.
        backend_state.update()
        check_counts(
            backend_state, total=2, by_state=[(ReplicaState.STARTING, 2)])

        assert backend_state._replica_constructor_retry_counter == i * 2

        replica_1 = backend_state._replicas.get()[0]
        replica_2 = backend_state._replicas.get()[1]

        replica_1._actor.set_failed_to_start()
        replica_2._actor.set_failed_to_start()
        # Now the replica should be marked SHOULD_STOP after failure.
        backend_state.update()
        check_counts(
            backend_state, total=2, by_state=[(ReplicaState.STOPPING, 2)])

        # Once it's done stopping, replica should be removed.
        replica_1._actor.set_done_stopping()
        replica_2._actor.set_done_stopping()
        deleted = backend_state.update()
        check_counts(backend_state, total=0)

    return deleted


def test_deploy_with_consistent_constructor_failure(mock_backend_state):
    """
    Test deploy() multiple replicas with consistent constructor failure.
    Ensures:
        1) Async goal manager can correctly recognize deployment goal as
            failed with exception
        2) There should be no hanging replicas running since failed initial
            deploy() is followed by delete_backend()

    Same testing for same test case in test_deploy.py.
    """
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(num_replicas=2)
    create_goal_id, updating = backend_state.deploy(b_info_1)
    goal_obj = goal_manager.get_goal(create_goal_id)
    deleted = _constructor_failure_loop_two_replica(backend_state, 3)

    assert backend_state._replica_constructor_retry_counter == 6
    assert goal_manager.check_complete(create_goal_id)
    check_counts(backend_state, total=0)
    assert deleted
    assert goal_obj.exception is not None


def test_deploy_with_partial_constructor_failure(mock_backend_state):
    """
    Test deploy() multiple replicas with constructor failure exceedining
    pre-set limit but achieved partial success with at least 1 running replica.

    Ensures:
        1) Async goal manager can correctly recognize deployment goal as
            successful
        2) There should be expected # of RUNNING replicas eventually that
            matches user intent
        3) Replica counter set as -1 to stop tracking current goal as it's
            already completed

    Same testing for same test case in test_deploy.py.
    """
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(num_replicas=2)
    create_goal, updating = backend_state.deploy(b_info_1)
    goal_obj = goal_manager.get_goal(create_goal)

    deleted = _constructor_failure_loop_two_replica(backend_state, 2)
    assert not deleted

    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert backend_state._replica_constructor_retry_counter == 4

    # Let one replica reach RUNNING state while the other still fails
    replica_1 = backend_state._replicas.get()[0]
    replica_2 = backend_state._replicas.get()[1]
    replica_1._actor.set_ready()
    replica_2._actor.set_failed_to_start()

    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STOPPING, 1)])

    # Ensure failed to start replica is removed
    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STOPPING, 1)])

    replica_2._actor.set_done_stopping()
    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STARTING, 0)])

    # New update cycle should spawn new replica after previous one is removed
    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STARTING, 1)])

    # Set the starting one to fail again and trigger retry limit
    starting_replica = backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]
    starting_replica._actor.set_failed_to_start()

    backend_state.update()
    # Ensure our goal returned with construtor start counter reset
    assert backend_state._replica_constructor_retry_counter == -1
    # Deploy() goal should NOT be considered complete yet
    assert not goal_manager.check_complete(create_goal)

    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STOPPING, 1)])

    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STOPPING, 1)])
    starting_replica = backend_state._replicas.get(
        states=[ReplicaState.STOPPING])[0]
    starting_replica._actor.set_done_stopping()

    backend_state.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STARTING, 0)])

    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STARTING, 1)])

    starting_replica = backend_state._replicas.get(
        states=[ReplicaState.STARTING])[0]
    starting_replica._actor.set_ready()

    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])

    # Deploy() goal should be considered complete
    assert goal_manager.check_complete(create_goal)
    # No except set on the AsyncGoal object
    assert goal_obj.exception is None


def test_deploy_with_transient_constructor_failure(mock_backend_state):
    """
    Test deploy() multiple replicas with transient constructor failure.
    Ensures:
        1) Async goal manager can correctly recognize deployment goal as
            successful
        2) There should be expected # of RUNNING replicas eventually that
            matches user intent
        3) Replica counter set as -1 to stop tracking current goal as it's
            already completed

    Same testing for same test case in test_deploy.py.
    """
    backend_state, timer, goal_manager = mock_backend_state

    b_info_1, b_version_1 = backend_info(num_replicas=2)
    create_goal, updating = backend_state.deploy(b_info_1)
    goal_obj = goal_manager.get_goal(create_goal)

    # Burn 4 retries from both replicas.
    deleted = _constructor_failure_loop_two_replica(backend_state, 2)
    assert not deleted

    # Let both replicas succeed in last try.
    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.STARTING, 2)])

    assert backend_state._replica_constructor_retry_counter == 4
    replica_1 = backend_state._replicas.get()[0]
    replica_2 = backend_state._replicas.get()[1]

    replica_1._actor.set_ready()
    replica_2._actor.set_ready()
    backend_state.update()
    check_counts(backend_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])

    assert backend_state._replica_constructor_retry_counter == 4
    assert goal_manager.check_complete(create_goal)
    assert goal_obj.exception is None


@pytest.fixture
def mock_backend_state_manager(
) -> Tuple[BackendStateManager, Mock, AsyncGoalManager]:
    timer = MockTimer()
    with patch(
            "ray.serve.backend_state.ActorReplicaWrapper",
            new=MockReplicaActorWrapper), patch(
                "ray.serve.backend_state.CONTROLLER_STARTUP_GRACE_PERIOD_S",
                0), patch(
                    "time.time", new=timer.time), patch(
                        "ray.serve.long_poll.LongPollHost") as mock_long_poll:

        kv_store = RayLocalKVStore("TEST_DB", "test_kv_store.db")
        goal_manager = AsyncGoalManager()
        all_current_actor_names = []
        backend_state_manager = BackendStateManager(
            "name", True, kv_store, mock_long_poll, goal_manager,
            all_current_actor_names)
        yield backend_state_manager, timer, goal_manager
        # Clear checkpoint at the end of each test
        kv_store.delete(CHECKPOINT_KEY)


def test_shutdown(mock_backend_state_manager):
    """
    Test that shutdown waits for all backends to be deleted and the backends
    are force-killed without a grace period.
    """
    backend_state_manager, timer, goal_manager = mock_backend_state_manager

    tag = "test"

    b_info_1, b_version_1 = backend_info()
    create_goal, updating = backend_state_manager.deploy_backend(tag, b_info_1)

    backend_state = backend_state_manager._backend_states[tag]

    # Single replica should be created.
    backend_state_manager.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.STARTING, 1)])
    backend_state._replicas.get()[0]._actor.set_ready()

    # Now the replica should be marked running.
    backend_state_manager.update()
    check_counts(backend_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])

    # Test shutdown flow
    assert not backend_state._replicas.get()[0]._actor.stopped

    shutdown_goal = backend_state_manager.shutdown()[0]

    backend_state_manager.update()

    check_counts(backend_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])
    assert backend_state._replicas.get()[0]._actor.stopped
    assert backend_state._replicas.get()[0]._actor.force_stopped_counter == 1
    assert not backend_state._replicas.get()[0]._actor.cleaned_up
    assert not goal_manager.check_complete(shutdown_goal)

    # Once it's done stopping, replica should be removed.
    replica = backend_state._replicas.get()[0]
    replica._actor.set_done_stopping()
    backend_state.update()
    check_counts(backend_state, total=0)

    # TODO(edoakes): can we remove this extra update period for completing it?
    backend_state_manager.update()
    assert goal_manager.check_complete(shutdown_goal)
    assert replica._actor.cleaned_up

    assert len(backend_state_manager._backend_states) == 0


def test_resume_backend_state_from_replica_tags(mock_backend_state_manager):
    backend_state_manager, timer, goal_manager = mock_backend_state_manager

    tag = "test"

    # Step 1: Create some backend info with actors in running state
    b_info_1, b_version_1 = backend_info(version="1")
    create_goal, updating = backend_state_manager.deploy_backend(tag, b_info_1)

    backend_state = backend_state_manager._backend_states[tag]

    # Single replica should be created.
    backend_state_manager.update()
    check_counts(
        backend_state,
        total=1,
        version=b_version_1,
        by_state=[(ReplicaState.STARTING, 1)])
    backend_state._replicas.get()[0]._actor.set_ready()

    # Now the replica should be marked running.
    backend_state_manager.update()
    check_counts(
        backend_state,
        total=1,
        version=b_version_1,
        by_state=[(ReplicaState.RUNNING, 1)])

    mocked_replica = backend_state._replicas.get(
        states=[ReplicaState.RUNNING])[0]

    # Step 2: Delete _replicas from backend_state
    backend_state._replicas = ReplicaStateContainer()
    # Step 3: Create new backend_state by resuming from passed in replica tags

    backend_state_manager._recover_from_checkpoint(
        [mocked_replica.replica_tag])

    # Step 4: Ensure new backend_state is correct
    # backend state behind "test" is re-created in recovery flow
    backend_state = backend_state_manager._backend_states[tag]
    # Ensure recovering replica begin with no version assigned
    check_counts(
        backend_state,
        total=1,
        version=None,
        by_state=[(ReplicaState.RECOVERING, 1)])
    backend_state._replicas.get()[0]._actor.set_ready()
    backend_state._replicas.get()[0]._actor.set_starting_version(b_version_1)

    # Now the replica should be marked running.
    backend_state_manager.update()
    check_counts(
        backend_state,
        total=1,
        version=b_version_1,
        by_state=[(ReplicaState.RUNNING, 1)])
    # Ensure same replica name is used
    assert backend_state._replicas.get()[
        0].replica_tag == mocked_replica.replica_tag


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
