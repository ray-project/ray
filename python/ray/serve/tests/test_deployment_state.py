import sys
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch, Mock
from collections import defaultdict

import pytest

import ray
from ray.serve._private.common import (
    DeploymentID,
    DeploymentConfig,
    DeploymentInfo,
    DeploymentStatus,
    ReplicaConfig,
    ReplicaTag,
    ReplicaName,
    ReplicaState,
)
from ray.serve._private.deployment_scheduler import (
    ReplicaSchedulingRequest,
)
from ray.serve.tests.utils import MockTimer, MockKVStore
from ray.serve._private.deployment_state import (
    ActorReplicaWrapper,
    DeploymentState,
    DriverDeploymentState,
    DeploymentStateManager,
    DeploymentVersion,
    DeploymentReplica,
    ReplicaStartupStatus,
    ReplicaStateContainer,
    VersionedReplica,
)
from ray.serve._private.constants import (
    DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S,
    DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S,
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_MAX_CONCURRENT_QUERIES,
)
from ray.serve._private.storage.kv_store import RayInternalKVStore
from ray.serve._private.utils import get_random_letters


class FakeRemoteFunction:
    def remote(self):
        pass


class MockActorHandle:
    def __init__(self):
        self._actor_id = "fake_id"
        self.initialize_and_get_metadata_called = False
        self.is_allocated_called = False

    @property
    def initialize_and_get_metadata(self):
        self.initialize_and_get_metadata_called = True
        # return a mock object so that we can call `remote()` on it.
        return FakeRemoteFunction()

    @property
    def is_allocated(self):
        self.is_allocated_called = True
        return FakeRemoteFunction()


class MockReplicaActorWrapper:
    def __init__(
        self,
        actor_name: str,
        detached: bool,
        controller_name: str,
        replica_tag: ReplicaTag,
        deployment_id: DeploymentID,
        version: DeploymentVersion,
    ):
        self._actor_name = actor_name
        self._replica_tag = replica_tag
        self._deployment_id = deployment_id

        # Will be set when `start()` is called.
        self.started = False
        # Will be set when `recover()` is called.
        self.recovering = False
        # Will be set when `start()` is called.
        self.version = version
        # Initial state for a replica is PENDING_ALLOCATION.
        self.ready = ReplicaStartupStatus.PENDING_ALLOCATION
        # Will be set when `graceful_stop()` is called.
        self.stopped = False
        # Expected to be set in the test.
        self.done_stopping = False
        # Will be set when `force_stop()` is called.
        self.force_stopped_counter = 0
        # Will be set when `check_health()` is called.
        self.health_check_called = False
        # Returned by the health check.
        self.healthy = True
        self._is_cross_language = False
        self._actor_handle = MockActorHandle()
        self._node_id = None
        self._node_id_is_set = False

    @property
    def is_cross_language(self) -> bool:
        return self._is_cross_language

    @property
    def replica_tag(self) -> str:
        return str(self._replica_tag)

    @property
    def deployment_name(self) -> str:
        return self._deployment_id.name

    @property
    def actor_handle(self) -> MockActorHandle:
        return self._actor_handle

    @property
    def max_concurrent_queries(self) -> int:
        return self.version.deployment_config.max_concurrent_queries

    @property
    def graceful_shutdown_timeout_s(self) -> float:
        return self.version.deployment_config.graceful_shutdown_timeout_s

    @property
    def health_check_period_s(self) -> float:
        return self.version.deployment_config.health_check_period_s

    @property
    def health_check_timeout_s(self) -> float:
        return self.version.deployment_config.health_check_timeout_s

    @property
    def pid(self) -> Optional[int]:
        return None

    @property
    def actor_id(self) -> Optional[str]:
        return None

    @property
    def worker_id(self) -> Optional[str]:
        return None

    @property
    def node_id(self) -> Optional[str]:
        if self._node_id_is_set:
            return self._node_id
        if self.ready == ReplicaStartupStatus.SUCCEEDED or self.started:
            return "node-id"
        return None

    @property
    def node_ip(self) -> Optional[str]:
        return None

    @property
    def log_file_path(self) -> Optional[str]:
        return None

    def set_ready(self, version: DeploymentVersion = None):
        self.ready = ReplicaStartupStatus.SUCCEEDED
        if version:
            self.version_to_be_fetched_from_actor = version
        else:
            self.version_to_be_fetched_from_actor = self.version

    def set_failed_to_start(self):
        self.ready = ReplicaStartupStatus.FAILED

    def set_done_stopping(self):
        self.done_stopping = True

    def set_unhealthy(self):
        self.healthy = False

    def set_starting_version(self, version: DeploymentVersion):
        """Mocked deployment_worker return version from reconfigure()"""
        self.starting_version = version

    def set_node_id(self, node_id: str):
        self._node_id = node_id
        self._node_id_is_set = True

    def start(self, deployment_info: DeploymentInfo):
        self.started = True
        return ReplicaSchedulingRequest(
            deployment_id=self._deployment_id,
            replica_name=self._replica_tag,
            actor_def=None,
            actor_resources=None,
            actor_options=None,
            actor_init_args=None,
            on_scheduled=None,
        )

    def reconfigure(self, version: DeploymentVersion):
        self.started = True
        updating = self.version.requires_actor_reconfigure(version)
        self.version = version
        return updating

    def recover(self):
        self.recovering = True
        self.started = False

    def check_ready(self) -> ReplicaStartupStatus:
        ready = self.ready
        self.ready = ReplicaStartupStatus.PENDING_INITIALIZATION
        if ready == ReplicaStartupStatus.SUCCEEDED and self.recovering:
            self.recovering = False
            self.started = True
            self.version = self.version_to_be_fetched_from_actor
        return ready, None

    def resource_requirements(self) -> Tuple[str, str]:
        assert self.started
        return str({"REQUIRED_RESOURCE": 1.0}), str({"AVAILABLE_RESOURCE": 1.0})

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
        return self.graceful_shutdown_timeout_s

    def check_stopped(self) -> bool:
        return self.done_stopping

    def force_stop(self):
        self.force_stopped_counter += 1

    def check_health(self):
        self.health_check_called = True
        return self.healthy


class MockDeploymentScheduler:
    def __init__(self, cluster_node_info_cache):
        self.deployments = set()
        self.replicas = defaultdict(set)

    def on_deployment_created(self, deployment_id, scheduling_strategy):
        assert deployment_id not in self.deployments
        self.deployments.add(deployment_id)

    def on_deployment_deleted(self, deployment_id):
        assert deployment_id in self.deployments
        self.deployments.remove(deployment_id)

    def on_replica_stopping(self, deployment_id, replica_name):
        assert replica_name in self.replicas[deployment_id]
        self.replicas[deployment_id].remove(replica_name)

    def on_replica_running(self, deployment_id, replica_name, node_id):
        assert replica_name in self.replicas[deployment_id]

    def on_replica_recovering(self, deployment_id, replica_name):
        assert replica_name not in self.replicas[deployment_id]
        self.replicas[deployment_id].add(replica_name)

    def schedule(self, upscales, downscales):
        for upscale in upscales.values():
            for replica_scheduling_request in upscale:
                assert (
                    replica_scheduling_request.replica_name
                    not in self.replicas[replica_scheduling_request.deployment_id]
                )
                self.replicas[replica_scheduling_request.deployment_id].add(
                    replica_scheduling_request.replica_name
                )

        deployment_to_replicas_to_stop = defaultdict(set)
        for downscale in downscales.values():
            replica_iter = iter(self.replicas[downscale.deployment_id])
            for _ in range(downscale.num_to_stop):
                deployment_to_replicas_to_stop[downscale.deployment_id].add(
                    next(replica_iter)
                )
        return deployment_to_replicas_to_stop


def deployment_info(
    version: Optional[str] = None,
    num_replicas: Optional[int] = 1,
    user_config: Optional[Any] = None,
    is_driver_deployment: bool = False,
    **config_opts,
) -> Tuple[DeploymentInfo, DeploymentVersion]:
    info = DeploymentInfo(
        version=version,
        start_time_ms=0,
        deployment_config=DeploymentConfig(
            num_replicas=num_replicas, user_config=user_config, **config_opts
        ),
        replica_config=ReplicaConfig.create(lambda x: x),
        deployer_job_id="",
        is_driver_deployment=is_driver_deployment,
    )

    if version is not None:
        code_version = version
    else:
        code_version = get_random_letters()

    version = DeploymentVersion(
        code_version, info.deployment_config, info.replica_config.ray_actor_options
    )

    return info, version


def deployment_version(code_version) -> DeploymentVersion:
    return DeploymentVersion(code_version, DeploymentConfig(), {})


class MockClusterNodeInfoCache:
    def __init__(self):
        self.alive_node_ids = set()
        self.draining_node_ids = set()

    def get_alive_node_ids(self):
        return self.alive_node_ids

    def get_draining_node_ids(self):
        return self.draining_node_ids

    def get_active_node_ids(self):
        return self.alive_node_ids - self.draining_node_ids


@pytest.fixture
def mock_deployment_state(request) -> Tuple[DeploymentState, Mock, Mock]:
    timer = MockTimer()
    with patch(
        "ray.serve._private.deployment_state.ActorReplicaWrapper",
        new=MockReplicaActorWrapper,
    ), patch("time.time", new=timer.time), patch(
        "ray.serve._private.long_poll.LongPollHost"
    ) as mock_long_poll:

        def mock_save_checkpoint_fn(*args, **kwargs):
            pass

        cluster_node_info_cache = MockClusterNodeInfoCache()

        # It is a driver deployment test
        if request.param is True:
            deployment_state = DriverDeploymentState(
                DeploymentID("name", "my_app"),
                "name",
                True,
                mock_long_poll,
                MockDeploymentScheduler(cluster_node_info_cache),
                cluster_node_info_cache,
                mock_save_checkpoint_fn,
            )
            yield deployment_state, timer, cluster_node_info_cache
        else:
            deployment_state = DeploymentState(
                DeploymentID("name", "my_app"),
                "name",
                True,
                mock_long_poll,
                MockDeploymentScheduler(cluster_node_info_cache),
                cluster_node_info_cache,
                mock_save_checkpoint_fn,
            )

            yield deployment_state, timer, cluster_node_info_cache


def replica(version: Optional[DeploymentVersion] = None) -> VersionedReplica:
    if version is None:
        version = DeploymentVersion(get_random_letters(), DeploymentConfig(), {})

    class MockVersionedReplica(VersionedReplica):
        def __init__(self, version: DeploymentVersion):
            self._version = version

        @property
        def version(self):
            return self._version

        def update_state(self, state):
            pass

    return MockVersionedReplica(version)


class TestReplicaStateContainer:
    def test_count(self):
        c = ReplicaStateContainer()
        r1, r2, r3 = (
            replica(deployment_version("1")),
            replica(deployment_version("2")),
            replica(deployment_version("2")),
        )
        c.add(ReplicaState.STARTING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.STOPPING, r3)
        assert c.count() == 3

        # Test filtering by state.
        assert c.count() == c.count(
            states=[ReplicaState.STARTING, ReplicaState.STOPPING]
        )
        assert c.count(states=[ReplicaState.STARTING]) == 2
        assert c.count(states=[ReplicaState.STOPPING]) == 1

        # Test filtering by version.
        assert c.count(version=deployment_version("1")) == 1
        assert c.count(version=deployment_version("2")) == 2
        assert c.count(version=deployment_version("3")) == 0
        assert c.count(exclude_version=deployment_version("1")) == 2
        assert c.count(exclude_version=deployment_version("2")) == 1
        assert c.count(exclude_version=deployment_version("3")) == 3

        # Test filtering by state and version.
        assert (
            c.count(version=deployment_version("1"), states=[ReplicaState.STARTING])
            == 1
        )
        assert (
            c.count(version=deployment_version("3"), states=[ReplicaState.STARTING])
            == 0
        )
        assert (
            c.count(
                version=deployment_version("2"),
                states=[ReplicaState.STARTING, ReplicaState.STOPPING],
            )
            == 2
        )
        assert (
            c.count(
                exclude_version=deployment_version("1"), states=[ReplicaState.STARTING]
            )
            == 1
        )
        assert (
            c.count(
                exclude_version=deployment_version("3"), states=[ReplicaState.STARTING]
            )
            == 2
        )
        assert (
            c.count(
                exclude_version=deployment_version("2"),
                states=[ReplicaState.STARTING, ReplicaState.STOPPING],
            )
            == 1
        )

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
        r1, r2, r3 = (
            replica(deployment_version("1")),
            replica(deployment_version("1")),
            replica(deployment_version("2")),
        )

        c.add(ReplicaState.STARTING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.STARTING, r3)
        assert c.pop(exclude_version=deployment_version("1")) == [r3]
        assert not c.pop(exclude_version=deployment_version("1"))
        assert c.pop(exclude_version=deployment_version("2")) == [r1, r2]
        assert not c.pop(exclude_version=deployment_version("2"))
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
        assert c.pop(states=[ReplicaState.STOPPING, ReplicaState.STARTING]) == [
            r1,
            r3,
            r2,
            r4,
        ]
        assert not c.pop(states=[ReplicaState.STOPPING, ReplicaState.STARTING])
        assert not c.pop(states=[ReplicaState.STOPPING])
        assert not c.pop(states=[ReplicaState.STARTING])
        assert not c.pop()

    def test_pop_integration(self):
        c = ReplicaStateContainer()
        r1, r2, r3, r4 = (
            replica(deployment_version("1")),
            replica(deployment_version("2")),
            replica(deployment_version("2")),
            replica(deployment_version("3")),
        )

        c.add(ReplicaState.STOPPING, r1)
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.RUNNING, r3)
        c.add(ReplicaState.RUNNING, r4)
        assert not c.pop(
            exclude_version=deployment_version("1"), states=[ReplicaState.STOPPING]
        )
        assert c.pop(
            exclude_version=deployment_version("1"),
            states=[ReplicaState.RUNNING],
            max_replicas=1,
        ) == [r3]
        assert c.pop(
            exclude_version=deployment_version("1"),
            states=[ReplicaState.RUNNING],
            max_replicas=1,
        ) == [r4]
        c.add(ReplicaState.RUNNING, r3)
        c.add(ReplicaState.RUNNING, r4)
        assert c.pop(
            exclude_version=deployment_version("1"), states=[ReplicaState.RUNNING]
        ) == [r3, r4]
        assert c.pop(
            exclude_version=deployment_version("1"), states=[ReplicaState.STARTING]
        ) == [r2]
        c.add(ReplicaState.STARTING, r2)
        c.add(ReplicaState.RUNNING, r3)
        c.add(ReplicaState.RUNNING, r4)
        assert c.pop(
            exclude_version=deployment_version("1"),
            states=[ReplicaState.RUNNING, ReplicaState.STARTING],
        ) == [r3, r4, r2]
        assert c.pop(
            exclude_version=deployment_version("nonsense"),
            states=[ReplicaState.STOPPING],
        ) == [r1]


def check_counts(
    deployment_state: DeploymentState,
    total: Optional[int] = None,
    version: Optional[str] = None,
    by_state: Optional[List[Tuple[ReplicaState, int]]] = None,
):
    if total is not None:
        assert deployment_state._replicas.count(version=version) == total

    if by_state is not None:
        for state, count in by_state:
            assert isinstance(state, ReplicaState)
            assert isinstance(count, int) and count >= 0
            curr_count = deployment_state._replicas.count(
                version=version, states=[state]
            )
            msg = f"Expected {count} for state {state} but got {curr_count}."
            assert curr_count == count, msg


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_create_delete_single_replica(mock_deployment_state):
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    b_info_1, b_version_1 = deployment_info()
    updating = deployment_state.deploy(b_info_1)
    assert updating

    # Single replica should be created.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STARTING, 1)])

    # update() should not transition the state if the replica isn't ready.
    deployment_state.update()
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STARTING, 1)])
    deployment_state._replicas.get()[0]._actor.set_ready()
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Now the replica should be marked running.
    deployment_state.update()
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    # Removing the replica should transition it to stopping.
    deployment_state.delete()
    deployment_state_update_result = deployment_state.update()
    replicas_to_stop = deployment_state._deployment_scheduler.schedule(
        {},
        {deployment_state._id: deployment_state_update_result.downscale}
        if deployment_state_update_result.downscale
        else {},
    )[deployment_state._id]
    deployment_state.stop_replicas(replicas_to_stop)
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])
    assert deployment_state._replicas.get()[0]._actor.stopped
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Once it's done stopping, replica should be removed.
    replica = deployment_state._replicas.get()[0]
    replica._actor.set_done_stopping()
    deployment_state_update_result = deployment_state.update()
    assert deployment_state_update_result.deleted
    check_counts(deployment_state, total=0)


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_force_kill(mock_deployment_state):
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    grace_period_s = 10
    b_info_1, b_version_1 = deployment_info(graceful_shutdown_timeout_s=grace_period_s)

    # Create and delete the deployment.
    deployment_state.deploy(b_info_1)
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    deployment_state._replicas.get()[0]._actor.set_ready()
    deployment_state.update()
    deployment_state.delete()
    deployment_state_update_result = deployment_state.update()
    replicas_to_stop = deployment_state._deployment_scheduler.schedule(
        {},
        {deployment_state._id: deployment_state_update_result.downscale}
        if deployment_state_update_result.downscale
        else {},
    )[deployment_state._id]
    deployment_state.stop_replicas(replicas_to_stop)

    # Replica should remain in STOPPING until it finishes.
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])
    assert deployment_state._replicas.get()[0]._actor.stopped

    for _ in range(10):
        deployment_state.update()

    # force_stop shouldn't be called until after the timer.
    assert not deployment_state._replicas.get()[0]._actor.force_stopped_counter
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])

    # Advance the timer, now the replica should be force stopped.
    timer.advance(grace_period_s + 0.1)
    deployment_state.update()
    assert deployment_state._replicas.get()[0]._actor.force_stopped_counter == 1
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Force stop should be called repeatedly until the replica stops.
    deployment_state.update()
    assert deployment_state._replicas.get()[0]._actor.force_stopped_counter == 2
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Once the replica is done stopping, it should be removed.
    replica = deployment_state._replicas.get()[0]
    replica._actor.set_done_stopping()
    deployment_state_update_result = deployment_state.update()
    assert deployment_state_update_result.deleted
    check_counts(deployment_state, total=0)


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_redeploy_same_version(mock_deployment_state):
    # Redeploying with the same version and code should do nothing.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    b_info_1, b_version_1 = deployment_info(version="1")
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Test redeploying while the initial deployment is still pending.
    updating = deployment_state.deploy(b_info_1)
    assert not updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )

    # Mark the replica ready. After this, the initial goal should be complete.
    deployment_state._replicas.get()[0]._actor.set_ready()
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    # Test redeploying after the initial deployment has finished.
    updating = deployment_state.deploy(b_info_1)
    assert not updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_redeploy_no_version(mock_deployment_state):
    # Redeploying with no version specified (`None`) should always redeploy
    # the replicas.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    b_info_1, b_version_1 = deployment_info(version=None)
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STARTING, 1)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Test redeploying while the initial deployment is still pending.
    updating = deployment_state.deploy(b_info_1)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # The initial replica should be stopping. The new replica shouldn't start
    # until the old one has completely stopped.
    deployment_state.update()
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])

    deployment_state.update()
    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()
    # Now that the old replica has stopped, the new replica should be started.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STARTING, 1)])
    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Check that the new replica has started.
    deployment_state.update()
    check_counts(deployment_state, total=1)
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])

    deployment_state.update()
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    # Now deploy a third version after the transition has finished.
    b_info_3, b_version_3 = deployment_info(version="3")
    updating = deployment_state.deploy(b_info_3)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    deployment_state.update()
    check_counts(deployment_state, total=1)
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])

    deployment_state.update()
    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STARTING, 1)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    deployment_state_update_result = deployment_state.update()
    assert not deployment_state_update_result.deleted
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_redeploy_new_version(mock_deployment_state):
    # Redeploying with a new version should start a new replica.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    b_info_1, b_version_1 = deployment_info(version="1")
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Test redeploying while the initial deployment is still pending.
    b_info_2, b_version_2 = deployment_info(version="2")
    updating = deployment_state.deploy(b_info_2)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # The initial replica should be stopping. The new replica shouldn't start
    # until the old one has completely stopped.
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)],
    )

    deployment_state.update()
    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()

    # Now that the old replica has stopped, the new replica should be started.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )
    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()

    # Check that the new replica has started.
    deployment_state.update()
    check_counts(deployment_state, total=1)
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )

    deployment_state.update()
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    # Now deploy a third version after the transition has finished.
    b_info_3, b_version_3 = deployment_info(version="3")
    updating = deployment_state.deploy(b_info_3)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    deployment_state.update()
    check_counts(deployment_state, total=1)
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)],
    )

    deployment_state.update()
    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    check_counts(
        deployment_state,
        version=b_version_3,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )

    deployment_state_update_result = deployment_state.update()
    assert not deployment_state_update_result.deleted
    check_counts(
        deployment_state,
        version=b_version_3,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
@pytest.mark.parametrize(
    "option,value",
    [
        ("user_config", {"hello": "world"}),
        ("max_concurrent_queries", 10),
        ("graceful_shutdown_timeout_s", DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S + 1),
        ("graceful_shutdown_wait_loop_s", DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S + 1),
        ("health_check_period_s", DEFAULT_HEALTH_CHECK_PERIOD_S + 1),
        ("health_check_timeout_s", DEFAULT_HEALTH_CHECK_TIMEOUT_S + 1),
    ],
)
def test_deploy_new_config_same_code_version(mock_deployment_state, option, value):
    # Deploying a new config with the same version should not deploy a new
    # replica.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    b_info_1, b_version_1 = deployment_info(version="1")
    updated = deployment_state.deploy(b_info_1)
    assert updated
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Create the replica initially.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    deployment_state._replicas.get()[0]._actor.set_ready()
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    # Update to a new config without changing the code version.
    b_info_2, b_version_2 = deployment_info(version="1", **{option: value})
    updated = deployment_state.deploy(b_info_2)
    assert updated
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )

    if option in ["user_config", "graceful_shutdown_wait_loop_s"]:
        deployment_state.update()
        check_counts(deployment_state, total=1)
        check_counts(
            deployment_state,
            version=b_version_2,
            total=1,
            by_state=[(ReplicaState.UPDATING, 1)],
        )
        # Mark the replica as ready.
        deployment_state._replicas.get()[0]._actor.set_ready()

    deployment_state.update()
    check_counts(deployment_state, total=1)
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_deploy_new_config_same_code_version_2(mock_deployment_state):
    # Make sure we don't transition from STARTING to UPDATING directly.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    b_info_1, b_version_1 = deployment_info(version="1")
    updated = deployment_state.deploy(b_info_1)
    assert updated
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Create the replica initially.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )

    # Update to a new config without changing the code version.
    b_info_2, b_version_2 = deployment_info(version="1", user_config={"hello": "world"})
    updated = deployment_state.deploy(b_info_2)
    assert updated
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    deployment_state.update()
    # Since it's STARTING, we cannot transition to UPDATING
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )

    deployment_state._replicas.get()[0]._actor.set_ready()
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.UPDATING, 1)],
    )

    # Mark the replica as ready.
    deployment_state._replicas.get()[0]._actor.set_ready()
    deployment_state.update()
    check_counts(deployment_state, total=1)
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_deploy_new_config_new_version(mock_deployment_state):
    # Deploying a new config with a new version should deploy a new replica.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    b_info_1, b_version_1 = deployment_info(version="1")
    updating = deployment_state.deploy(b_info_1)
    assert updating

    # Create the replica initially.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    deployment_state._replicas.get()[0]._actor.set_ready()
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    # Update to a new config and a new version.
    b_info_2, b_version_2 = deployment_info(version="2", user_config={"hello": "world"})
    updating = deployment_state.deploy(b_info_2)
    assert updating

    # New version shouldn't start until old version is stopped.
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)],
    )
    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()

    # Now the new version should be started.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Check that the new version is now running.
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_stop_replicas_on_draining_nodes(mock_deployment_state):
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"node-1", "node-2"}

    b_info_1, b_version_1 = deployment_info(num_replicas=2, version="1")
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Drain node-2.
    cluster_node_info_cache.draining_node_ids = {"node-2"}

    # Since the replicas are still starting and we don't know the actor node id
    # yet so nothing happens
    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])

    deployment_state._replicas.get()[0]._actor.set_ready()
    deployment_state._replicas.get()[0]._actor.set_node_id("node-1")
    deployment_state._replicas.get()[1]._actor.set_ready()
    deployment_state._replicas.get()[1]._actor.set_node_id("node-2")

    # The replica running on node-2 will be drained.
    deployment_state.update()
    check_counts(
        deployment_state,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STOPPING, 1)],
    )

    # A new node is started.
    cluster_node_info_cache.alive_node_ids = {
        "node-1",
        "node-2",
        "node-3",
    }

    # The draining replica is stopped and a new one will be started.
    deployment_state._replicas.get()[1]._actor.set_done_stopping()
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        total=2,
        by_state=[(ReplicaState.STARTING, 1), (ReplicaState.RUNNING, 1)],
    )


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_initial_deploy_no_throttling(mock_deployment_state):
    # All replicas should be started at once for a new deployment.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {str(i) for i in range(10)}

    b_info_1, b_version_1 = deployment_info(num_replicas=10, version="1")
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=10, by_state=[(ReplicaState.STARTING, 10)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    for replica in deployment_state._replicas.get():
        replica._actor.set_ready()

    # Check that the new replicas have started.
    deployment_state.update()
    check_counts(deployment_state, total=10, by_state=[(ReplicaState.RUNNING, 10)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_new_version_deploy_throttling(mock_deployment_state):
    # All replicas should be started at once for a new deployment.
    # When the version is updated, it should be throttled. The throttling
    # should apply to both code version and user config updates.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {str(i) for i in range(10)}

    b_info_1, b_version_1 = deployment_info(
        num_replicas=10, version="1", user_config="1"
    )
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=10, by_state=[(ReplicaState.STARTING, 10)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    for replica in deployment_state._replicas.get():
        replica._actor.set_ready()

    # Check that the new replicas have started.
    deployment_state.update()
    check_counts(deployment_state, total=10, by_state=[(ReplicaState.RUNNING, 10)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    # Now deploy a new version. Two old replicas should be stopped.
    b_info_2, b_version_2 = deployment_info(
        num_replicas=10, version="2", user_config="2"
    )
    updating = deployment_state.deploy(b_info_2)
    assert updating
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_1,
        total=10,
        by_state=[(ReplicaState.RUNNING, 8), (ReplicaState.STOPPING, 2)],
    )

    # Mark only one of the replicas as done stopping.
    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()

    # Now one of the new version replicas should start up.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=10)
    check_counts(
        deployment_state,
        version=b_version_1,
        total=9,
        by_state=[(ReplicaState.RUNNING, 8), (ReplicaState.STOPPING, 1)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )

    # Mark the new version replica as ready. Another old version replica
    # should subsequently be stopped.
    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()

    deployment_state.update()
    check_counts(deployment_state, total=10)
    check_counts(
        deployment_state,
        version=b_version_1,
        total=9,
        by_state=[(ReplicaState.RUNNING, 7), (ReplicaState.STOPPING, 2)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )

    # Mark the old replicas as done stopping.
    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()
    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        1
    ]._actor.set_done_stopping()

    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Old replicas should be stopped and new versions started in batches of 2.
    new_replicas = 1
    old_replicas = 9
    while old_replicas > 3:
        # Replicas starting up.
        deployment_state_update_result = deployment_state.update()
        deployment_state._deployment_scheduler.schedule(
            {deployment_state._id: deployment_state_update_result.upscale}, {}
        )
        check_counts(deployment_state, total=10)
        check_counts(
            deployment_state,
            version=b_version_1,
            total=old_replicas - 2,
            by_state=[(ReplicaState.RUNNING, old_replicas - 2)],
        )
        check_counts(
            deployment_state,
            version=b_version_2,
            total=new_replicas + 2,
            by_state=[(ReplicaState.RUNNING, new_replicas), (ReplicaState.STARTING, 2)],
        )

        # Set both ready.
        deployment_state._replicas.get(states=[ReplicaState.STARTING])[
            0
        ]._actor.set_ready()
        deployment_state._replicas.get(states=[ReplicaState.STARTING])[
            1
        ]._actor.set_ready()
        new_replicas += 2

        # Two more old replicas should be stopped.
        old_replicas -= 2
        deployment_state.update()
        check_counts(deployment_state, total=10)
        check_counts(
            deployment_state,
            version=b_version_1,
            total=old_replicas,
            by_state=[
                (ReplicaState.RUNNING, old_replicas - 2),
                (ReplicaState.STOPPING, 2),
            ],
        )
        check_counts(
            deployment_state,
            version=b_version_2,
            total=new_replicas,
            by_state=[(ReplicaState.RUNNING, new_replicas)],
        )

        deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
            0
        ]._actor.set_done_stopping()
        deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
            1
        ]._actor.set_done_stopping()

        assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # 2 left to update.
    # Replicas starting up.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=10)
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=9,
        by_state=[(ReplicaState.RUNNING, 7), (ReplicaState.STARTING, 2)],
    )

    # Set both ready.
    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    deployment_state._replicas.get(states=[ReplicaState.STARTING])[1]._actor.set_ready()

    # The last replica should be stopped.
    deployment_state.update()
    check_counts(deployment_state, total=10)
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=9,
        by_state=[(ReplicaState.RUNNING, 9)],
    )

    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()

    # The last replica should start up.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=10)
    check_counts(
        deployment_state,
        version=b_version_2,
        total=10,
        by_state=[(ReplicaState.RUNNING, 9), (ReplicaState.STARTING, 1)],
    )

    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Set both ready.
    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    deployment_state.update()
    check_counts(deployment_state, total=10)
    check_counts(
        deployment_state,
        version=b_version_2,
        total=10,
        by_state=[(ReplicaState.RUNNING, 10)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_reconfigure_throttling(mock_deployment_state):
    # All replicas should be started at once for a new deployment.
    # When the version is updated, it should be throttled.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {str(i) for i in range(2)}

    b_info_1, b_version_1 = deployment_info(
        num_replicas=2, version="1", user_config="1"
    )
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    for replica in deployment_state._replicas.get():
        replica._actor.set_ready()

    # Check that the new replicas have started.
    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    # Now deploy a new user_config. One replica should be updated.
    b_info_2, b_version_2 = deployment_info(
        num_replicas=2, version="1", user_config="2"
    )
    updating = deployment_state.deploy(b_info_2)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.UPDATING, 1)],
    )

    # Mark the updating replica as ready.
    deployment_state._replicas.get(states=[ReplicaState.UPDATING])[0]._actor.set_ready()

    # The updated replica should now be RUNNING.
    # The second replica should now be updated.
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_2,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.UPDATING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Mark the updating replica as ready.
    deployment_state._replicas.get(states=[ReplicaState.UPDATING])[0]._actor.set_ready()

    # Both replicas should now be RUNNING.
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_2,
        total=2,
        by_state=[(ReplicaState.RUNNING, 2)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [False], indirect=True)
def test_new_version_and_scale_down(mock_deployment_state):
    # Test the case when we reduce the number of replicas and change the
    # version at the same time. First the number of replicas should be
    # turned down, then the rolling update should happen.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    b_info_1, b_version_1 = deployment_info(num_replicas=10, version="1")
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=10, by_state=[(ReplicaState.STARTING, 10)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    for replica in deployment_state._replicas.get():
        replica._actor.set_ready()

    # Check that the new replicas have started.
    deployment_state.update()
    check_counts(deployment_state, total=10, by_state=[(ReplicaState.RUNNING, 10)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    # Now deploy a new version and scale down the number of replicas to 2.
    # First, 8 old replicas should be stopped to bring it down to the target.
    b_info_2, b_version_2 = deployment_info(num_replicas=2, version="2")
    updating = deployment_state.deploy(b_info_2)
    assert updating
    deployment_state_update_result = deployment_state.update()
    replicas_to_stop = deployment_state._deployment_scheduler.schedule(
        {}, {deployment_state._id: deployment_state_update_result.downscale}
    )[deployment_state._id]
    deployment_state.stop_replicas(replicas_to_stop)
    check_counts(
        deployment_state,
        version=b_version_1,
        total=10,
        by_state=[(ReplicaState.RUNNING, 2), (ReplicaState.STOPPING, 8)],
    )

    # Mark only one of the replicas as done stopping.
    # This should not yet trigger the rolling update because there are still
    # stopping replicas.
    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()

    deployment_state.update()
    check_counts(deployment_state, total=9)
    check_counts(
        deployment_state,
        version=b_version_1,
        total=9,
        by_state=[(ReplicaState.RUNNING, 2), (ReplicaState.STOPPING, 7)],
    )

    # Stop the remaining replicas.
    for replica in deployment_state._replicas.get(states=[ReplicaState.STOPPING]):
        replica._actor.set_done_stopping()

    # Now the rolling update should trigger, stopping one of the old replicas.
    deployment_state.update()
    check_counts(deployment_state, total=2)
    check_counts(
        deployment_state,
        version=b_version_1,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STOPPING, 1)],
    )

    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()

    # Old version stopped, new version should start up.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2)
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )

    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    # New version is started, final old version replica should be stopped.
    deployment_state.update()
    check_counts(deployment_state, total=2)
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )

    deployment_state._replicas.get(states=[ReplicaState.STOPPING])[
        0
    ]._actor.set_done_stopping()

    # Final old version replica is stopped, final new version replica
    # should be started.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2)
    check_counts(
        deployment_state,
        version=b_version_2,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STARTING, 1)],
    )

    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    deployment_state.update()
    check_counts(deployment_state, total=2)
    check_counts(
        deployment_state,
        version=b_version_2,
        total=2,
        by_state=[(ReplicaState.RUNNING, 2)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [False], indirect=True)
def test_new_version_and_scale_up(mock_deployment_state):
    # Test the case when we increase the number of replicas and change the
    # version at the same time. The new replicas should all immediately be
    # turned up. When they're up, rolling update should trigger.
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state

    b_info_1, b_version_1 = deployment_info(num_replicas=2, version="1")
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    for replica in deployment_state._replicas.get():
        replica._actor.set_ready()

    # Check that the new replicas have started.
    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    # Now deploy a new version and scale up the number of replicas to 10.
    # 8 new replicas should be started.
    b_info_2, b_version_2 = deployment_info(num_replicas=10, version="2")
    updating = deployment_state.deploy(b_info_2)
    assert updating
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        version=b_version_1,
        total=2,
        by_state=[(ReplicaState.RUNNING, 2)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=8,
        by_state=[(ReplicaState.STARTING, 8)],
    )

    # Mark the new replicas as ready.
    for replica in deployment_state._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()

    # Now that the new version replicas are up, rolling update should start.
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_1,
        total=2,
        by_state=[(ReplicaState.RUNNING, 0), (ReplicaState.STOPPING, 2)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=8,
        by_state=[(ReplicaState.RUNNING, 8)],
    )

    # Mark the replicas as done stopping.
    for replica in deployment_state._replicas.get(states=[ReplicaState.STOPPING]):
        replica._actor.set_done_stopping()

    # The remaining replicas should be started.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=10)
    check_counts(
        deployment_state,
        version=b_version_2,
        total=10,
        by_state=[(ReplicaState.RUNNING, 8), (ReplicaState.STARTING, 2)],
    )

    # Mark the remaining replicas as ready.
    for replica in deployment_state._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()

    # All new replicas should be up and running.
    deployment_state.update()
    check_counts(deployment_state, total=10)
    check_counts(
        deployment_state,
        version=b_version_2,
        total=10,
        by_state=[(ReplicaState.RUNNING, 10)],
    )

    deployment_state.update()
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_health_check(mock_deployment_state):
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {str(i) for i in range(2)}

    b_info_1, b_version_1 = deployment_info(num_replicas=2, version="1")
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    for replica in deployment_state._replicas.get():
        replica._actor.set_ready()
        # Health check shouldn't be called until it's ready.
        assert not replica._actor.health_check_called

    # Check that the new replicas have started.
    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    deployment_state.update()
    for replica in deployment_state._replicas.get():
        # Health check shouldn't be called until it's ready.
        assert replica._actor.health_check_called

    # Mark one replica unhealthy. It should be stopped.
    deployment_state._replicas.get()[0]._actor.set_unhealthy()
    deployment_state.update()
    check_counts(
        deployment_state,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STOPPING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.UNHEALTHY

    replica = deployment_state._replicas.get(states=[ReplicaState.STOPPING])[0]
    replica._actor.set_done_stopping()

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STARTING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.UNHEALTHY

    replica = deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]
    replica._actor.set_ready()
    assert deployment_state.curr_status_info.status == DeploymentStatus.UNHEALTHY

    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_update_while_unhealthy(mock_deployment_state):
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {str(i) for i in range(2)}

    b_info_1, b_version_1 = deployment_info(num_replicas=2, version="1")
    updating = deployment_state.deploy(b_info_1)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    for replica in deployment_state._replicas.get():
        replica._actor.set_ready()
        # Health check shouldn't be called until it's ready.
        assert not replica._actor.health_check_called

    # Check that the new replicas have started.
    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY

    deployment_state.update()
    for replica in deployment_state._replicas.get():
        # Health check shouldn't be called until it's ready.
        assert replica._actor.health_check_called

    # Mark one replica unhealthy. It should be stopped.
    deployment_state._replicas.get()[0]._actor.set_unhealthy()
    deployment_state.update()
    check_counts(
        deployment_state,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STOPPING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.UNHEALTHY

    replica = deployment_state._replicas.get(states=[ReplicaState.STOPPING])[0]
    replica._actor.set_done_stopping()

    # Now deploy a new version (e.g., a rollback). This should update the status
    # to UPDATING and then it should eventually become healthy.
    b_info_2, b_version_2 = deployment_info(num_replicas=2, version="2")
    updating = deployment_state.deploy(b_info_2)
    assert updating

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Check that a failure in the old version replica does not mark the
    # deployment as UNHEALTHY.
    deployment_state._replicas.get(states=[ReplicaState.RUNNING])[
        0
    ]._actor.set_unhealthy()
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_1,
        total=1,
        by_state=[(ReplicaState.STOPPING, 1)],
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=1,
        by_state=[(ReplicaState.STARTING, 1)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    replica = deployment_state._replicas.get(states=[ReplicaState.STOPPING])[0]
    replica._actor.set_done_stopping()

    # Another replica of the new version should get started.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        version=b_version_2,
        total=2,
        by_state=[(ReplicaState.STARTING, 2)],
    )

    # Mark new version replicas as ready.
    for replica in deployment_state._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()

    # Both replicas should be RUNNING, deployment should be HEALTHY.
    deployment_state.update()
    check_counts(
        deployment_state,
        version=b_version_2,
        total=2,
        by_state=[(ReplicaState.RUNNING, 2)],
    )
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


def _constructor_failure_loop_two_replica(deployment_state, num_loops):
    """Helper function to exact constructor failure loops."""
    for i in range(num_loops):
        # Two replicas should be created.
        deployment_state_update_result = deployment_state.update()
        deployment_state._deployment_scheduler.schedule(
            {deployment_state._id: deployment_state_update_result.upscale}, {}
        )
        check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])

        assert deployment_state._replica_constructor_retry_counter == i * 2

        replica_1 = deployment_state._replicas.get()[0]
        replica_2 = deployment_state._replicas.get()[1]

        replica_1._actor.set_failed_to_start()
        replica_2._actor.set_failed_to_start()
        # Now the replica should be marked SHOULD_STOP after failure.
        deployment_state.update()
        check_counts(deployment_state, total=2, by_state=[(ReplicaState.STOPPING, 2)])

        # Once it's done stopping, replica should be removed.
        replica_1._actor.set_done_stopping()
        replica_2._actor.set_done_stopping()


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_deploy_with_consistent_constructor_failure(mock_deployment_state):
    """
    Test deploy() multiple replicas with consistent constructor failure.

    The deployment should get marked FAILED.
    """
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {str(i) for i in range(2)}

    b_info_1, b_version_1 = deployment_info(num_replicas=2)
    updating = deployment_state.deploy(b_info_1)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING
    _constructor_failure_loop_two_replica(deployment_state, 3)

    assert deployment_state._replica_constructor_retry_counter == 6
    assert deployment_state.curr_status_info.status == DeploymentStatus.UNHEALTHY
    check_counts(deployment_state, total=2)
    assert deployment_state.curr_status_info.message != ""


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_deploy_with_partial_constructor_failure(mock_deployment_state):
    """
    Test deploy() multiple replicas with constructor failure exceedining
    pre-set limit but achieved partial success with at least 1 running replica.

    Ensures:
        1) Deployment status doesn't get marked FAILED.
        2) There should be expected # of RUNNING replicas eventually that
            matches user intent
        3) Replica counter set as -1 to stop tracking current goal as it's
            already completed

    Same testing for same test case in test_deploy.py.
    """
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {str(i) for i in range(2)}

    b_info_1, b_version_1 = deployment_info(num_replicas=2)
    updating = deployment_state.deploy(b_info_1)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    _constructor_failure_loop_two_replica(deployment_state, 2)
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert deployment_state._replica_constructor_retry_counter == 4
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Let one replica reach RUNNING state while the other still fails
    replica_1 = deployment_state._replicas.get()[0]
    replica_2 = deployment_state._replicas.get()[1]
    replica_1._actor.set_ready()
    replica_2._actor.set_failed_to_start()

    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STOPPING, 1)])

    # Ensure failed to start replica is removed
    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STOPPING, 1)])

    replica_2._actor.set_done_stopping()
    # New update cycle should spawn new replica after previous one is removed
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 1)])

    # Set the starting one to fail again and trigger retry limit
    starting_replica = deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]
    starting_replica._actor.set_failed_to_start()

    deployment_state.update()
    # Ensure our goal returned with construtor start counter reset
    assert deployment_state._replica_constructor_retry_counter == -1
    # Deployment should NOT be considered complete yet
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STOPPING, 1)])

    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STOPPING, 1)])
    starting_replica = deployment_state._replicas.get(states=[ReplicaState.STOPPING])[0]
    starting_replica._actor.set_done_stopping()

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 1)])
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 1)])

    starting_replica = deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]
    starting_replica._actor.set_ready()

    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])

    # Deployment should be considered complete
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [True, False], indirect=True)
def test_deploy_with_transient_constructor_failure(mock_deployment_state):
    """
    Test deploy() multiple replicas with transient constructor failure.
    Ensures:
        1) Deployment status gets marked as RUNNING.
        2) There should be expected # of RUNNING replicas eventually that
            matches user intent.
        3) Replica counter set as -1 to stop tracking current goal as it's
            already completed.

    Same testing for same test case in test_deploy.py.
    """
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {str(i) for i in range(2)}

    b_info_1, b_version_1 = deployment_info(num_replicas=2)
    updating = deployment_state.deploy(b_info_1)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Burn 4 retries from both replicas.
    _constructor_failure_loop_two_replica(deployment_state, 2)
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    # Let both replicas succeed in last try.
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    assert deployment_state._replica_constructor_retry_counter == 4
    replica_1 = deployment_state._replicas.get()[0]
    replica_2 = deployment_state._replicas.get()[1]

    replica_1._actor.set_ready()
    replica_2._actor.set_ready()
    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])

    assert deployment_state._replica_constructor_retry_counter == 4
    assert deployment_state.curr_status_info.status == DeploymentStatus.HEALTHY


@pytest.mark.parametrize("mock_deployment_state", [False], indirect=True)
def test_exponential_backoff(mock_deployment_state):
    """Test exponential backoff."""
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {str(i) for i in range(2)}

    b_info_1, b_version_1 = deployment_info(num_replicas=2)
    updating = deployment_state.deploy(b_info_1)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    _constructor_failure_loop_two_replica(deployment_state, 3)
    assert deployment_state._replica_constructor_retry_counter == 6
    last_retry = timer.time()

    for i in range(7):
        while timer.time() - last_retry < 2**i:
            deployment_state.update()
            assert deployment_state._replica_constructor_retry_counter == 6 + 2 * i
            # Check that during backoff time, no replicas are created
            check_counts(deployment_state, total=0)
            timer.advance(0.1)  # simulate time passing between each call to udpate

        # Skip past random additional backoff time used to avoid synchronization
        timer.advance(5)

        # Set new replicas to fail consecutively
        check_counts(deployment_state, total=0)  # No replicas
        deployment_state_update_result = deployment_state.update()
        deployment_state._deployment_scheduler.schedule(
            {deployment_state._id: deployment_state_update_result.upscale}, {}
        )
        last_retry = timer.time()  # This should be time at which replicas were retried
        check_counts(deployment_state, total=2)  # Two new replicas
        replica_1 = deployment_state._replicas.get()[0]
        replica_2 = deployment_state._replicas.get()[1]
        replica_1._actor.set_failed_to_start()
        replica_2._actor.set_failed_to_start()
        timer.advance(0.1)  # simulate time passing between each call to udpate

        # Now the replica should be marked STOPPING after failure.
        deployment_state.update()
        check_counts(deployment_state, total=2, by_state=[(ReplicaState.STOPPING, 2)])
        timer.advance(0.1)  # simulate time passing between each call to udpate

        # Once it's done stopping, replica should be removed.
        replica_1._actor.set_done_stopping()
        replica_2._actor.set_done_stopping()
        deployment_state.update()
        check_counts(deployment_state, total=0)
        timer.advance(0.1)  # simulate time passing between each call to udpate


@pytest.fixture
def mock_deployment_state_manager_full(
    request,
) -> Tuple[DeploymentStateManager, Mock, Mock]:
    """Fully mocked deployment state manager.

    i.e kv store and gcs client is mocked so we don't need to initialize
    ray. Also, since this is used for some recovery tests, this yields a
    method for creating a new mocked deployment state manager.
    """

    timer = MockTimer()
    with patch(
        "ray.serve._private.deployment_state.ActorReplicaWrapper",
        new=MockReplicaActorWrapper,
    ), patch(
        "ray.serve._private.default_impl.create_deployment_scheduler",
    ) as mock_create_deployment_scheduler, patch(
        "time.time", new=timer.time
    ), patch(
        "ray.serve._private.long_poll.LongPollHost"
    ) as mock_long_poll, patch(
        "ray.get_runtime_context"
    ):

        kv_store = MockKVStore()
        cluster_node_info_cache = MockClusterNodeInfoCache()

        def create_deployment_state_manager(
            actor_names=None, placement_group_names=None, is_driver_deployment=False
        ):
            if actor_names is None:
                actor_names = []

            if placement_group_names is None:
                placement_group_names = []

            mock_create_deployment_scheduler.return_value = MockDeploymentScheduler(
                cluster_node_info_cache
            )

            return DeploymentStateManager(
                "name",
                True,
                kv_store,
                mock_long_poll,
                actor_names,
                placement_group_names,
                cluster_node_info_cache,
            )

        yield create_deployment_state_manager, timer, cluster_node_info_cache


@pytest.mark.parametrize("is_driver_deployment", [True, False])
def test_recover_state_from_replica_names(
    mock_deployment_state_manager_full, is_driver_deployment
):
    """Test recover deployment state."""
    deployment_id = DeploymentID("test_deployment", "test_app")
    (
        create_deployment_state_manager,
        _,
        cluster_node_info_cache,
    ) = mock_deployment_state_manager_full
    deployment_state_manager = create_deployment_state_manager()
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    # Deploy deployment with version "1" and one replica
    info1, version1 = deployment_info(
        version="1", is_driver_deployment=is_driver_deployment
    )
    updating = deployment_state_manager.deploy(deployment_id, info1)
    deployment_state = deployment_state_manager._deployment_states[deployment_id]
    assert updating

    # Single replica of version `version1` should be created and in STARTING state
    deployment_state_manager.update()
    check_counts(
        deployment_state,
        total=1,
        version=version1,
        by_state=[(ReplicaState.STARTING, 1)],
    )
    mocked_replica = deployment_state._replicas.get()[0]

    # The same replica should transition to RUNNING
    mocked_replica._actor.set_ready()
    deployment_state_manager.update()
    check_counts(
        deployment_state,
        total=1,
        version=version1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )

    # (simulate controller crashed!) Create a new deployment state
    # manager, and it should call _recover_from_checkpoint
    new_deployment_state_manager = create_deployment_state_manager(
        [ReplicaName.prefix + mocked_replica.replica_tag]
    )

    # New deployment state should be created and one replica should
    # be RECOVERING with last-checkpointed target version `version1`
    new_deployment_state = new_deployment_state_manager._deployment_states[
        deployment_id
    ]
    check_counts(
        new_deployment_state,
        total=1,
        version=version1,
        by_state=[(ReplicaState.RECOVERING, 1)],
    )

    # Get the new mocked replica. Note that this represents a newly
    # instantiated class keeping track of the state of the replica,
    # but pointing to the same replica actor
    new_mocked_replica = new_deployment_state._replicas.get()[0]
    new_mocked_replica._actor.set_ready(version1)
    any_recovering = new_deployment_state_manager.update()
    check_counts(
        new_deployment_state,
        total=1,
        version=version1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    assert not any_recovering
    # Make sure replica name is the same, meaning the actor is the same
    assert mocked_replica.replica_tag == new_mocked_replica.replica_tag


@pytest.mark.parametrize("is_driver_deployment", [True, False])
def test_recover_during_rolling_update(
    mock_deployment_state_manager_full, is_driver_deployment
):
    """Test controller crashes before a replica is updated to new version.

    During recovery, the controller should wait for the version to be fetched from
    the replica actor. Once it is fetched and the controller realizes the replica
    has an outdated version, it should be stopped and a new replica should be started
    with the target version.
    """
    deployment_id = DeploymentID("test_deployment", "test_app")
    (
        create_deployment_state_manager,
        _,
        cluster_node_info_cache,
    ) = mock_deployment_state_manager_full
    deployment_state_manager = create_deployment_state_manager()
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    # Step 1: Create some deployment info with actors in running state
    info1, version1 = deployment_info(
        version="1", is_driver_deployment=is_driver_deployment
    )
    updating = deployment_state_manager.deploy(deployment_id, info1)
    deployment_state = deployment_state_manager._deployment_states[deployment_id]
    assert updating

    # Single replica of version `version1` should be created and in STARTING state
    deployment_state_manager.update()
    check_counts(
        deployment_state,
        total=1,
        version=version1,
        by_state=[(ReplicaState.STARTING, 1)],
    )
    mocked_replica = deployment_state._replicas.get()[0]

    # The same replica should transition to RUNNING
    mocked_replica._actor.set_ready()
    deployment_state_manager.update()
    check_counts(
        deployment_state,
        total=1,
        version=version1,
        by_state=[(ReplicaState.RUNNING, 1)],
    )

    # Now execute a rollout: upgrade the version to "2".
    info2, version2 = deployment_info(
        version="2", is_driver_deployment=is_driver_deployment
    )
    updating = deployment_state_manager.deploy(deployment_id, info2)
    assert updating

    # Before the replica could be stopped and restarted, simulate
    # controller crashed! A new deployment state manager should be
    # created, and it should call _recover_from_checkpoint
    new_deployment_state_manager = create_deployment_state_manager(
        [ReplicaName.prefix + mocked_replica.replica_tag]
    )
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    # New deployment state should be created and one replica should
    # be RECOVERING with last-checkpointed target version "2"
    new_deployment_state = new_deployment_state_manager._deployment_states[
        deployment_id
    ]
    check_counts(
        new_deployment_state,
        total=1,
        version=version2,
        by_state=[(ReplicaState.RECOVERING, 1)],
    )
    # Replica should remain recovering and remain labeled as version "2"
    # before it recovers the real version from the actor
    if not is_driver_deployment:
        # NOTE(zcin): for is_driver_deployment=True, since the node id is not available
        # when recovering, if we perform an update before marking the replica as ready,
        # it will think it needs to bring up a new replica on the node(s) in the cluster
        for _ in range(3):
            new_deployment_state_manager.update()
            check_counts(
                new_deployment_state,
                total=1,
                version=version2,
                by_state=[(ReplicaState.RECOVERING, 1)],
            )

    # Get the new mocked replica. Note that this represents a newly
    # instantiated class keeping track of the state of the replica,
    # but pointing to the same replica actor
    new_mocked_replica = new_deployment_state._replicas.get()[0]
    # Recover real version "1" (simulate previous actor not yet stopped)
    new_mocked_replica._actor.set_ready(version1)
    # At this point the replica is running
    new_deployment_state_manager.update()
    # Then deployment state manager notices the replica has outdated version -> stops it
    new_deployment_state_manager.update()
    check_counts(
        new_deployment_state,
        total=1,
        version=version1,
        by_state=[(ReplicaState.STOPPING, 1)],
    )
    new_mocked_replica._actor.set_done_stopping()

    # Now that the replica of version "1" has been stopped, a new
    # replica of version "2" should be started
    new_deployment_state_manager.update()
    check_counts(
        new_deployment_state,
        total=1,
        version=version2,
        by_state=[(ReplicaState.STARTING, 1)],
    )
    new_mocked_replica_version2 = new_deployment_state._replicas.get()[0]
    new_mocked_replica_version2._actor.set_ready()
    new_deployment_state_manager.update()
    check_counts(
        new_deployment_state,
        total=1,
        version=version2,
        by_state=[(ReplicaState.RUNNING, 1)],
    )
    # Make sure replica name is different, meaning a different "actor" was started
    assert mocked_replica.replica_tag != new_mocked_replica_version2.replica_tag


@pytest.fixture
def mock_deployment_state_manager(request) -> Tuple[DeploymentStateManager, Mock, Mock]:
    ray.init()
    timer = MockTimer()
    with patch(
        "ray.serve._private.deployment_state.ActorReplicaWrapper",
        new=MockReplicaActorWrapper,
    ), patch(
        "ray.serve._private.default_impl.create_deployment_scheduler",
    ) as mock_create_deployment_scheduler, patch(
        "time.time", new=timer.time
    ), patch(
        "ray.serve._private.long_poll.LongPollHost"
    ) as mock_long_poll:

        kv_store = RayInternalKVStore("test")
        cluster_node_info_cache = MockClusterNodeInfoCache()
        mock_create_deployment_scheduler.return_value = MockDeploymentScheduler(
            cluster_node_info_cache
        )
        all_current_actor_names = []
        all_current_placement_group_names = []
        deployment_state_manager = DeploymentStateManager(
            DeploymentID("name", "my_app"),
            True,
            kv_store,
            mock_long_poll,
            all_current_actor_names,
            all_current_placement_group_names,
            cluster_node_info_cache,
        )

        yield deployment_state_manager, timer, cluster_node_info_cache
    ray.shutdown()


@pytest.mark.parametrize("is_driver_deployment", [False, True])
def test_shutdown(mock_deployment_state_manager, is_driver_deployment):
    """
    Test that shutdown waits for all deployments to be deleted and they
    are force-killed without a grace period.
    """
    (
        deployment_state_manager,
        timer,
        cluster_node_info_cache,
    ) = mock_deployment_state_manager
    cluster_node_info_cache.alive_node_ids = {"node-id"}

    deployment_id = DeploymentID("test_deployment", "test_app")

    grace_period_s = 10
    b_info_1, _ = deployment_info(
        graceful_shutdown_timeout_s=grace_period_s,
        is_driver_deployment=is_driver_deployment,
    )
    updating = deployment_state_manager.deploy(deployment_id, b_info_1)
    assert updating

    deployment_state = deployment_state_manager._deployment_states[deployment_id]

    # Single replica should be created.
    deployment_state_manager.update()
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STARTING, 1)])
    deployment_state._replicas.get()[0]._actor.set_ready()

    # Now the replica should be marked running.
    deployment_state_manager.update()
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.RUNNING, 1)])

    # Test shutdown flow
    assert not deployment_state._replicas.get()[0]._actor.stopped

    # Before shutdown, `is_ready_for_shutdown()` should return False
    assert not deployment_state_manager.is_ready_for_shutdown()

    deployment_state_manager.shutdown()

    timer.advance(grace_period_s + 0.1)
    deployment_state_manager.update()

    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STOPPING, 1)])
    assert deployment_state._replicas.get()[0]._actor.stopped
    assert len(deployment_state_manager.get_deployment_statuses()) > 0

    # Once it's done stopping, replica should be removed.
    replica = deployment_state._replicas.get()[0]
    replica._actor.set_done_stopping()
    deployment_state_manager.update()
    check_counts(deployment_state, total=0)
    assert len(deployment_state_manager.get_deployment_statuses()) == 0

    # After all deployments shutdown, `is_ready_for_shutdown()` should return True
    assert deployment_state_manager.is_ready_for_shutdown()


def test_resource_requirements_none():
    """Ensure resource_requirements doesn't break if a requirement is None"""

    class FakeActor:

        actor_resources = {"num_cpus": 2, "fake": None}
        placement_group_bundles = None
        available_resources = {}

    # Make a DeploymentReplica just to accesss its resource_requirement function
    replica = DeploymentReplica(None, None, "random_tag", None, None)
    replica._actor = FakeActor()

    # resource_requirements() should not error
    replica.resource_requirements()


@pytest.mark.parametrize("mock_deployment_state", [True], indirect=True)
def test_cancel_extra_replicas_for_driver_deployment(mock_deployment_state):
    """Test to make sure the driver deployment state
    can cancel extra starting replicas.
    """

    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"0", "1"}

    b_info_1, b_version_1 = deployment_info()
    updating = deployment_state.deploy(b_info_1)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    deployment_state_update_result = deployment_state.update()
    # 1 node dies, now the cluster only has 1 node
    cluster_node_info_cache.alive_node_ids = {"0"}
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    # only 1 replica is scheduled successfully, the other is PENDING_ALLOCATION
    deployment_state._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    # the other replica should be cancelled
    deployment_state.update()
    check_counts(
        deployment_state,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1), (ReplicaState.STOPPING, 1)],
    )


@pytest.mark.parametrize("mock_deployment_state", [True], indirect=True)
def test_add_and_remove_nodes_for_driver_deployment(mock_deployment_state):
    deployment_state, timer, cluster_node_info_cache = mock_deployment_state
    cluster_node_info_cache.alive_node_ids = {"0"}

    b_info_1, b_version_1 = deployment_info()
    updating = deployment_state.deploy(b_info_1)
    assert updating
    assert deployment_state.curr_status_info.status == DeploymentStatus.UPDATING

    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=1, by_state=[(ReplicaState.STARTING, 1)])

    # Add a node when previous one is in STARTING state
    cluster_node_info_cache.alive_node_ids = {"0", "1"}
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.STARTING, 2)])
    for replica in deployment_state._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()
    deployment_state.update()
    check_counts(deployment_state, total=2, by_state=[(ReplicaState.RUNNING, 2)])

    # Add another two nodes
    cluster_node_info_cache.alive_node_ids = {"0", "1", "2", "3"}
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        total=4,
        by_state=[(ReplicaState.RUNNING, 2), (ReplicaState.STARTING, 2)],
    )
    for replica in deployment_state._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()
    deployment_state.update()
    check_counts(deployment_state, total=4, by_state=[(ReplicaState.RUNNING, 4)])

    # Remove one node and add another node, node3 is removed, node4 added
    cluster_node_info_cache.alive_node_ids = {"0", "1", "2", "4"}
    deployment_state._replicas.get(states=[ReplicaState.RUNNING])[
        3
    ]._actor.set_unhealthy()
    deployment_state_update_result = deployment_state.update()
    deployment_state._deployment_scheduler.schedule(
        {deployment_state._id: deployment_state_update_result.upscale}, {}
    )
    check_counts(
        deployment_state,
        total=4,
        by_state=[
            (ReplicaState.RUNNING, 3),
            (ReplicaState.STOPPING, 1),
        ],
    )

    # Mark stopped replica finish stopping step.
    for replica in deployment_state._replicas.get(states=[ReplicaState.STOPPING]):
        replica._actor.set_done_stopping()
    deployment_state.update()

    # Make starting replica finish starting step.
    for replica in deployment_state._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()
    deployment_state.update()
    check_counts(deployment_state, total=4, by_state=[(ReplicaState.RUNNING, 4)])


class TestActorReplicaWrapper:
    def test_default_value(self):
        actor_replica = ActorReplicaWrapper(
            version=deployment_version("1"),
            actor_name="test",
            detached=False,
            controller_name="test_controller",
            replica_tag="test_tag",
            deployment_id=DeploymentID("test_deployment", "test_app"),
        )
        assert (
            actor_replica.graceful_shutdown_timeout_s
            == DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S
        )
        assert actor_replica.max_concurrent_queries == DEFAULT_MAX_CONCURRENT_QUERIES
        assert actor_replica.health_check_period_s == DEFAULT_HEALTH_CHECK_PERIOD_S
        assert actor_replica.health_check_timeout_s == DEFAULT_HEALTH_CHECK_TIMEOUT_S

    def test_recover(self):
        actor_replica = ActorReplicaWrapper(
            version=deployment_version("1"),
            actor_name="test",
            detached=False,
            controller_name="test_controller",
            replica_tag="test_tag",
            deployment_id=DeploymentID("test_deployment", "test_app"),
        )
        actor_replica._actor_handle = MockActorHandle()
        actor_replica.recover()
        assert actor_replica._actor_handle.initialize_and_get_metadata_called
        assert actor_replica._actor_handle.is_allocated_called


def test_get_active_node_ids(mock_deployment_state_manager_full):
    """Test get_active_node_ids() are collecting the correct node ids

    When there are no running replicas, both methods should return empty results. When
    the replicas are in the RUNNING state, get_running_replica_node_ids() should return
    a list of all node ids. `get_active_node_ids()` should return a set
    of all node ids.
    """
    node_ids = ("node1", "node2", "node2")

    deployment_id = DeploymentID("test_deployment", "test_app")
    (
        create_deployment_state_manager,
        _,
        cluster_node_info_cache,
    ) = mock_deployment_state_manager_full
    deployment_state_manager = create_deployment_state_manager()
    cluster_node_info_cache.alive_node_ids = set(node_ids)

    # Deploy deployment with version "1" and 3 replicas
    info1, version1 = deployment_info(version="1", num_replicas=3)
    updating = deployment_state_manager.deploy(deployment_id, info1)
    deployment_state = deployment_state_manager._deployment_states[deployment_id]
    assert updating

    # When the replicas are in the STARTING state, `get_active_node_ids()` should
    # return a set of node ids.
    deployment_state_manager.update()
    check_counts(
        deployment_state,
        total=3,
        version=version1,
        by_state=[(ReplicaState.STARTING, 3)],
    )
    mocked_replicas = deployment_state._replicas.get()
    for idx, mocked_replica in enumerate(mocked_replicas):
        mocked_replica._actor.set_node_id(node_ids[idx])
    assert deployment_state.get_active_node_ids() == set(node_ids)
    assert deployment_state_manager.get_active_node_ids() == set(node_ids)

    # When the replicas are in RUNNING state, `get_active_node_ids()` should
    # return a set of `node_ids`.
    for mocked_replica in mocked_replicas:
        mocked_replica._actor.set_ready()
    deployment_state_manager.update()
    check_counts(
        deployment_state,
        total=3,
        version=version1,
        by_state=[(ReplicaState.RUNNING, 3)],
    )
    assert deployment_state.get_active_node_ids() == set(node_ids)
    assert deployment_state_manager.get_active_node_ids() == set(node_ids)

    # When the replicas are in the STOPPING state, `get_active_node_ids()` should
    # return empty set.
    for _ in mocked_replicas:
        deployment_state._stop_one_running_replica_for_testing()
    deployment_state_manager.update()
    check_counts(
        deployment_state,
        total=3,
        version=version1,
        by_state=[(ReplicaState.STOPPING, 3)],
    )
    assert deployment_state.get_active_node_ids() == set()
    assert deployment_state_manager.get_active_node_ids() == set()


def test_get_active_node_ids_none(mock_deployment_state_manager_full):
    """Test get_active_node_ids() are not collecting none node ids.

    When the running replicas has None as the node id, `get_active_node_ids()` should
    not include it in the set.
    """
    node_ids = ("node1", "node2", "node2")

    deployment_id = DeploymentID("test_deployment", "test_app")
    (
        create_deployment_state_manager,
        _,
        cluster_node_info_cache,
    ) = mock_deployment_state_manager_full
    deployment_state_manager = create_deployment_state_manager()
    cluster_node_info_cache.alive_node_ids = set(node_ids)

    # Deploy deployment with version "1" and 3 replicas
    info1, version1 = deployment_info(version="1", num_replicas=3)
    updating = deployment_state_manager.deploy(deployment_id, info1)
    deployment_state = deployment_state_manager._deployment_states[deployment_id]
    assert updating

    # When the replicas are in the STARTING state, `get_active_node_ids()` should
    # return a set of node ids.
    deployment_state_manager.update()
    check_counts(
        deployment_state,
        total=3,
        version=version1,
        by_state=[(ReplicaState.STARTING, 3)],
    )
    mocked_replicas = deployment_state._replicas.get()
    for idx, mocked_replica in enumerate(mocked_replicas):
        mocked_replica._actor.set_node_id(node_ids[idx])
    assert deployment_state.get_active_node_ids() == set(node_ids)
    assert deployment_state_manager.get_active_node_ids() == set(node_ids)

    # When the replicas are in the RUNNING state and are having None node id,
    # `get_active_node_ids()` should return empty set.
    for mocked_replica in mocked_replicas:
        mocked_replica._actor.set_node_id(None)
        mocked_replica._actor.set_ready()
    deployment_state_manager.update()
    check_counts(
        deployment_state,
        total=3,
        version=version1,
        by_state=[(ReplicaState.RUNNING, 3)],
    )
    assert None not in deployment_state.get_active_node_ids()
    assert None not in deployment_state_manager.get_active_node_ids()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
