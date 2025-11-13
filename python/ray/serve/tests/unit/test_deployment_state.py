import sys
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

import pytest

from ray._common.ray_constants import DEFAULT_MAX_CONCURRENCY_ASYNC
from ray._raylet import NodeID
from ray.serve._private.autoscaling_state import AutoscalingStateManager
from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    DeploymentHandleSource,
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusTrigger,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    ReplicaState,
    TargetCapacityDirection,
    TimeStampedValue,
)
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve._private.constants import (
    DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S,
    DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S,
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_MAX_ONGOING_REQUESTS,
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.deployment_scheduler import ReplicaSchedulingRequest
from ray.serve._private.deployment_state import (
    ALL_REPLICA_STATES,
    SLOW_STARTUP_WARNING_S,
    ActorReplicaWrapper,
    DeploymentReplica,
    DeploymentState,
    DeploymentStateManager,
    DeploymentVersion,
    ReplicaStartupStatus,
    ReplicaStateContainer,
)
from ray.serve._private.exceptions import DeploymentIsBeingDeletedError
from ray.serve._private.test_utils import (
    MockActorHandle,
    MockClusterNodeInfoCache,
    MockKVStore,
    MockTimer,
)
from ray.serve._private.utils import (
    get_capacity_adjusted_num_replicas,
    get_random_string,
)
from ray.util.placement_group import validate_placement_group

# Global variable that is fetched during controller recovery that
# marks (simulates) which replicas have died since controller first
# recovered a list of live replica names.
# NOTE(zcin): This is necessary because the replica's `recover()` method
# is called in the controller's init function, instead of in the control
# loop, so we can't "mark" a replica dead through a method. This global
# state is cleared after each test that uses the fixtures in this file.
dead_replicas_context = set()
replica_rank_context = {}
TEST_DEPLOYMENT_ID = DeploymentID(name="test_deployment", app_name="test_app")
TEST_DEPLOYMENT_ID_2 = DeploymentID(name="test_deployment_2", app_name="test_app")


class MockReplicaActorWrapper:
    def __init__(
        self,
        replica_id: ReplicaID,
        version: DeploymentVersion,
    ):
        self._replica_id = replica_id
        self._actor_name = replica_id.to_full_id_str()
        # Will be set when `start()` is called.
        self.started = False
        # Will be set when `recover()` is called.
        self.recovering = False
        # Will be set when `start()` is called.
        self.version = version
        # Initial state for a replica is PENDING_ALLOCATION.
        self.status = ReplicaStartupStatus.PENDING_ALLOCATION
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
        self._node_ip = None
        self._node_instance_id = None
        self._node_id_is_set = False
        self._actor_id = None
        self._internal_grpc_port = None
        self._pg_bundles = None
        self._initialization_latency_s = -1
        self._docs_path = None
        self._rank = replica_rank_context.get(replica_id.unique_id, None)

    @property
    def is_cross_language(self) -> bool:
        return self._is_cross_language

    @property
    def replica_id(self) -> ReplicaID:
        return self._replica_id

    @property
    def deployment_name(self) -> str:
        return self._replica_id.deployment_id.name

    @property
    def actor_handle(self) -> MockActorHandle:
        return self._actor_handle

    @property
    def max_ongoing_requests(self) -> int:
        return self.version.deployment_config.max_ongoing_requests

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
        return self._actor_id

    @property
    def worker_id(self) -> Optional[str]:
        return None

    @property
    def node_id(self) -> Optional[str]:
        if self._node_id_is_set:
            return self._node_id
        if self.status == ReplicaStartupStatus.SUCCEEDED or self.started:
            return "node-id"
        return None

    @property
    def availability_zone(self) -> Optional[str]:
        return None

    @property
    def node_ip(self) -> Optional[str]:
        return None

    @property
    def node_instance_id(self) -> Optional[str]:
        return None

    @property
    def log_file_path(self) -> Optional[str]:
        return None

    @property
    def grpc_port(self) -> Optional[int]:
        return None

    @property
    def placement_group_bundles(self) -> Optional[List[Dict[str, float]]]:
        return None

    @property
    def initialization_latency_s(self) -> float:
        return self._initialization_latency_s

    def set_docs_path(self, docs_path: str):
        self._docs_path = docs_path

    @property
    def docs_path(self) -> Optional[str]:
        return self._docs_path

    def set_status(self, status: ReplicaStartupStatus):
        self.status = status

    def set_ready(self, version: DeploymentVersion = None):
        self.status = ReplicaStartupStatus.SUCCEEDED
        if version:
            self.version_to_be_fetched_from_actor = version
        else:
            self.version_to_be_fetched_from_actor = self.version

    def set_failed_to_start(self):
        self.status = ReplicaStartupStatus.FAILED

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

    def set_actor_id(self, actor_id: str):
        self._actor_id = actor_id

    def start(self, deployment_info: DeploymentInfo, rank: int):
        self.started = True
        self._rank = rank
        replica_rank_context[self._replica_id.unique_id] = rank

        def _on_scheduled_stub(*args, **kwargs):
            pass

        return ReplicaSchedulingRequest(
            replica_id=self._replica_id,
            actor_def=Mock(),
            actor_resources={},
            actor_options={"name": "placeholder"},
            actor_init_args=(),
            placement_group_bundles=(
                deployment_info.replica_config.placement_group_bundles
            ),
            on_scheduled=_on_scheduled_stub,
        )

    @property
    def rank(self) -> Optional[int]:
        return self._rank

    def reconfigure(
        self,
        version: DeploymentVersion,
        rank: int = None,
    ):
        self.started = True
        updating = self.version.requires_actor_reconfigure(version)
        self.version = version
        self._rank = rank
        replica_rank_context[self._replica_id.unique_id] = rank
        return updating

    def recover(self):
        if self.replica_id in dead_replicas_context:
            return False

        self.recovering = True
        self.started = False
        self._rank = replica_rank_context.get(self._replica_id.unique_id, None)
        return True

    def check_ready(self) -> ReplicaStartupStatus:
        ready = self.status
        self.status = ReplicaStartupStatus.PENDING_INITIALIZATION
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

    def force_stop(self, log_shutdown_message: bool = False):
        self.force_stopped_counter += 1

    def check_health(self):
        self.health_check_called = True
        return self.healthy

    def get_routing_stats(self) -> Dict[str, Any]:
        return {}

    @property
    def route_patterns(self) -> Optional[List[str]]:
        return None


def deployment_info(
    version: Optional[str] = None,
    num_replicas: Optional[int] = 1,
    user_config: Optional[Any] = None,
    replica_config: Optional[ReplicaConfig] = None,
    **config_opts,
) -> Tuple[DeploymentInfo, DeploymentVersion]:
    info = DeploymentInfo(
        version=version,
        start_time_ms=0,
        actor_name="abc",
        deployment_config=DeploymentConfig(
            num_replicas=num_replicas, user_config=user_config, **config_opts
        ),
        replica_config=replica_config or ReplicaConfig.create(lambda x: x),
        deployer_job_id="",
    )

    if version is not None:
        code_version = version
    else:
        code_version = get_random_string()

    version = DeploymentVersion(
        code_version, info.deployment_config, info.replica_config.ray_actor_options
    )

    return info, version


def deployment_version(code_version) -> DeploymentVersion:
    return DeploymentVersion(code_version, DeploymentConfig(), {})


@pytest.fixture
def mock_deployment_state_manager(
    request,
) -> Tuple[DeploymentStateManager, MockTimer, Mock, Mock]:
    """Fully mocked deployment state manager.

    i.e kv store and gcs client is mocked so we don't need to initialize
    ray. Also, since this is used for some recovery tests, this yields a
    method for creating a new mocked deployment state manager.
    """

    timer = MockTimer()
    with patch(
        "ray.serve._private.deployment_state.ActorReplicaWrapper",
        new=MockReplicaActorWrapper,
    ), patch("time.time", new=timer.time), patch(
        "ray.serve._private.long_poll.LongPollHost"
    ) as mock_long_poll, patch(
        "ray.get_runtime_context"
    ):
        kv_store = MockKVStore()
        cluster_node_info_cache = MockClusterNodeInfoCache()
        cluster_node_info_cache.add_node(NodeID.from_random().hex())
        autoscaling_state_manager = AutoscalingStateManager()

        def create_deployment_state_manager(
            actor_names=None,
            placement_group_names=None,
            create_placement_group_fn_override=None,
        ):
            if actor_names is None:
                actor_names = []

            if placement_group_names is None:
                placement_group_names = []

            return DeploymentStateManager(
                kv_store,
                mock_long_poll,
                actor_names,
                placement_group_names,
                cluster_node_info_cache,
                autoscaling_state_manager,
                head_node_id_override="fake-head-node-id",
                create_placement_group_fn_override=create_placement_group_fn_override,
            )

        yield (
            create_deployment_state_manager,
            timer,
            cluster_node_info_cache,
            autoscaling_state_manager,
        )

        dead_replicas_context.clear()
        replica_rank_context.clear()


@pytest.fixture
def mock_max_per_replica_retry_count():
    with patch("ray.serve._private.deployment_state.MAX_PER_REPLICA_RETRY_COUNT", 2):
        yield 2


class FakeDeploymentReplica:
    """Fakes the DeploymentReplica class."""

    def __init__(self, version: DeploymentVersion):
        self._version = version

    @property
    def version(self):
        return self._version

    def update_state(self, state):
        pass


def replica(version: Optional[DeploymentVersion] = None) -> FakeDeploymentReplica:
    version = version or DeploymentVersion(get_random_string(), DeploymentConfig(), {})
    return FakeDeploymentReplica(version)


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
    by_state: Optional[List[Tuple[ReplicaState, int]]] = None,
):
    replicas = {
        state: deployment_state._replicas.count(states=[state])
        for state in ALL_REPLICA_STATES
    }
    if total is not None:
        assert deployment_state._replicas.count() == total, f"Replicas: {replicas}"

    if by_state is not None:
        for state, count, version in by_state:
            assert isinstance(state, ReplicaState)
            assert isinstance(count, int) and count >= 0
            curr_count = deployment_state._replicas.count(
                version=version, states=[state]
            )
            msg = (
                f"Expected {count} for state {state} but got {curr_count}. Current "
                f"replicas: {replicas}"
            )
            assert curr_count == count, msg


def test_create_delete_single_replica(mock_deployment_state_manager):
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    info_1, v1 = deployment_info()
    dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    # Single replica should be created.
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])

    # update() should not transition the state if the replica isn't ready.
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
    ds._replicas.get()[0]._actor.set_ready()
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Now the replica should be marked running.
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Removing the replica should transition it to stopping.
    ds.delete()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STOPPING, 1, None)])
    assert ds._replicas.get()[0]._actor.stopped
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert ds.curr_status_info.status_trigger == DeploymentStatusTrigger.DELETING

    # Once it's done stopping, replica should be removed.
    replica = ds._replicas.get()[0]
    replica._actor.set_done_stopping()
    dsm.update()
    check_counts(ds, total=0)


def test_force_kill(mock_deployment_state_manager):
    create_dsm, timer, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    grace_period_s = 10
    info_1, _ = deployment_info(graceful_shutdown_timeout_s=grace_period_s)
    dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]
    dsm.update()

    # Create deployment.
    ds._replicas.get()[0]._actor.set_ready()
    dsm.update()

    # Delete deployment.
    ds.delete()

    # Replica should remain in STOPPING until it finishes.
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STOPPING, 1, None)])
    assert ds._replicas.get()[0]._actor.stopped

    for _ in range(10):
        dsm.update()

    # force_stop shouldn't be called until after the timer.
    assert not ds._replicas.get()[0]._actor.force_stopped_counter
    print(ds._replicas)
    check_counts(ds, total=1, by_state=[(ReplicaState.STOPPING, 1, None)])

    # Advance the timer, now the replica should be force stopped.
    timer.advance(grace_period_s + 0.1)
    dsm.update()
    assert ds._replicas.get()[0]._actor.force_stopped_counter == 1
    check_counts(ds, total=1, by_state=[(ReplicaState.STOPPING, 1, None)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert ds.curr_status_info.status_trigger == DeploymentStatusTrigger.DELETING

    # Force stop should be called repeatedly until the replica stops.
    dsm.update()
    assert ds._replicas.get()[0]._actor.force_stopped_counter == 2
    check_counts(ds, total=1, by_state=[(ReplicaState.STOPPING, 1, None)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert ds.curr_status_info.status_trigger == DeploymentStatusTrigger.DELETING

    # Once the replica is done stopping, it should be removed.
    replica = ds._replicas.get()[0]
    replica._actor.set_done_stopping()
    dsm.update()
    check_counts(ds, total=0)


def test_redeploy_same_version(mock_deployment_state_manager):
    # Redeploying with the same version and code should do nothing.
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    info_1, v1 = deployment_info(version="1")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]
    dsm.update()

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Test redeploying while the initial deployment is still pending.
    updating = dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
    assert not updating
    # Redeploying the exact same info shouldn't cause any change in status
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])

    # Mark the replica ready. After this, the initial goal should be complete.
    ds._replicas.get()[0]._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Test redeploying after the initial deployment has finished.
    updating = dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
    assert not updating
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_redeploy_no_version(mock_deployment_state_manager):
    """Redeploying with no version specified (`None`) should always
    redeploy the replicas.
    """

    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(version=None)
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Test redeploying while the initial deployment is still pending.
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    # The initial replica should be stopping. The new replica should
    # start without waiting for the old one to stop completely.
    check_counts(
        ds,
        total=2,
        by_state=[
            (ReplicaState.STOPPING, 1, None),
            (ReplicaState.STARTING, 1, None),
        ],
    )

    # Mark old replica as completely stopped.
    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Check that the new replica has started.
    ds._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Now deploy a third version after the transition has finished.
    b_info_3, v3 = deployment_info(version="3")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_3)
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    # The initial replica should be stopping. The new replica should
    # start without waiting for the old one to stop completely.
    check_counts(
        ds,
        total=2,
        by_state=[
            (ReplicaState.STOPPING, 1, None),
            (ReplicaState.STARTING, 1, v3),
        ],
    )
    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()

    dsm.update()
    ds._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_redeploy_new_version(mock_deployment_state_manager):
    """Redeploying with a new version should start a new replica."""
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(version="1")
    dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Test redeploying while the initial deployment is still pending.
    b_info_2, v2 = deployment_info(version="2")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    # The new replica should start without waiting for the old one
    # to stop.
    check_counts(
        ds,
        total=2,
        by_state=[(ReplicaState.STOPPING, 1, v1), (ReplicaState.STARTING, 1, v2)],
    )

    # Mark old replica as stopped.
    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v2)])

    # Mark new replica as ready
    ds._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v2)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Now deploy a third version after the transition has finished.
    b_info_3, v3 = deployment_info(version="3")
    dsm.deploy(TEST_DEPLOYMENT_ID, b_info_3)
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    # New replica should start without waiting for old one to stop
    check_counts(
        ds,
        total=2,
        by_state=[(ReplicaState.STOPPING, 1, v2), (ReplicaState.STARTING, 1, v3)],
    )

    # Mark old replica as stopped and mark new replica as ready
    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v3)])

    ds._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v3)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_redeploy_different_num_replicas(mock_deployment_state_manager):
    """Tests status changes when redeploying with different num_replicas.

    1. Deploys a deployment -> checks if it's UPDATING.
    2. Redeploys deployment -> checks that it's still UPDATING.
    3. Makes deployment HEALTHY, and then redeploys with more replicas ->
       check that is becomes UPSCALING.
    4. Makes deployment HEALTHY, and then redeploys with more replicas ->
       check that is becomes DOWNSCALING.
    """
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    version = "1"
    b_info_1, v1 = deployment_info(version=version, num_replicas=5)
    dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, by_state=[(ReplicaState.STARTING, 5, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Test redeploying with a higher num_replicas while the deployment is UPDATING.
    b_info_2, v1 = deployment_info(version=version, num_replicas=10)
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)
    # Redeploying while the deployment is UPDATING shouldn't change status.
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    check_counts(ds, by_state=[(ReplicaState.STARTING, 10, v1)])

    for replica in ds._replicas.get():
        replica._actor.set_ready()

    dsm.update()
    check_counts(ds, by_state=[(ReplicaState.RUNNING, 10, v1)])

    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Redeploy with a higher number of replicas. The status should be UPSCALING.
    b_info_3, v1 = deployment_info(version=version, num_replicas=20)
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_3)

    assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    check_counts(ds, by_state=[(ReplicaState.STARTING, 10, v1)])

    for replica in ds._replicas.get():
        replica._actor.set_ready()

    dsm.update()
    check_counts(ds, by_state=[(ReplicaState.RUNNING, 20, v1)])

    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger == DeploymentStatusTrigger.UPSCALE_COMPLETED
    )

    # Redeploy with lower number of replicas. The status should be DOWNSCALING.
    b_info_4, v1 = deployment_info(version=version, num_replicas=5)
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_4)

    assert ds.curr_status_info.status == DeploymentStatus.DOWNSCALING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    check_counts(
        ds, by_state=[(ReplicaState.STOPPING, 15, v1), (ReplicaState.RUNNING, 5, v1)]
    )

    for replica in ds._replicas.get(states=[ReplicaState.STOPPING]):
        replica._actor.set_done_stopping()

    dsm.update()
    check_counts(ds, total=5, by_state=[(ReplicaState.RUNNING, 5, v1)])

    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.DOWNSCALE_COMPLETED
    )


@pytest.mark.parametrize(
    "option,value",
    [
        ("user_config", {"hello": "world"}),
        ("max_ongoing_requests", 10),
        ("graceful_shutdown_timeout_s", DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S + 1),
        ("graceful_shutdown_wait_loop_s", DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S + 1),
        ("health_check_period_s", DEFAULT_HEALTH_CHECK_PERIOD_S + 1),
        ("health_check_timeout_s", DEFAULT_HEALTH_CHECK_TIMEOUT_S + 1),
    ],
)
def test_deploy_new_config_same_code_version(
    mock_deployment_state_manager, option, value
):
    """Deploying a new config with the same version should not deploy a new replica."""

    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(version="1")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)

    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Create the replica initially.
    dsm.update()
    ds._replicas.get()[0]._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Update to a new config without changing the code version.
    b_info_2, v2 = deployment_info(version="1", **{option: value})
    updated = dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)
    assert updated
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v1)])

    if option in [
        "user_config",
        "graceful_shutdown_wait_loop_s",
        "max_ongoing_requests",
    ]:
        dsm.update()
        check_counts(ds, total=1)
        check_counts(
            ds,
            total=1,
            by_state=[(ReplicaState.UPDATING, 1, v2)],
        )
        # Mark the replica as ready.
        ds._replicas.get()[0]._actor.set_ready()

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v2)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_deploy_new_config_same_code_version_2(mock_deployment_state_manager):
    """Make sure we don't transition from STARTING to UPDATING directly."""

    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(version="1")
    updated = dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    assert updated
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Create the replica initially.
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])

    # Update to a new config without changing the code version.
    b_info_2, v2 = deployment_info(version="1", user_config={"hello": "world"})
    updated = dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)
    assert updated
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    # Since it's STARTING, we cannot transition to UPDATING
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])

    ds._replicas.get()[0]._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.UPDATING, 1, v2)])

    # Mark the replica as ready.
    ds._replicas.get()[0]._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1)
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v2)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_deploy_new_config_new_version(mock_deployment_state_manager):
    # Deploying a new config with a new version should deploy a new replica.

    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(version="1")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    # Create the replica initially.
    dsm.update()
    ds._replicas.get()[0]._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Update to a new config and a new version.
    b_info_2, v2 = deployment_info(version="2", user_config={"hello": "world"})
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)

    dsm.update()
    # New version should start immediately without waiting for
    # replicas of old version to completely stop
    check_counts(
        ds,
        total=2,
        by_state=[
            (ReplicaState.STOPPING, 1, v1),
            (ReplicaState.STARTING, 1, v2),
        ],
    )

    # Mark replica of old version as stopped
    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v2)])

    # Mark new replica as ready
    ds._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Check that the new version is now running.
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v2)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_initial_deploy_no_throttling(mock_deployment_state_manager):
    # All replicas should be started at once for a new deployment.
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(num_replicas=10, version="1")
    updated = dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    assert updated
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=10, by_state=[(ReplicaState.STARTING, 10, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    for replica in ds._replicas.get():
        replica._actor.set_ready()

    # Check that the new replicas have started.
    dsm.update()
    check_counts(ds, total=10, by_state=[(ReplicaState.RUNNING, 10, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_new_version_deploy_throttling_new(mock_deployment_state_manager):
    """All replicas should be started at once for a new deployment.

    When the version is updated, it should be throttled. The throttling
    should apply to both code version and user config updates.
    """

    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(num_replicas=10, version="1", user_config="1")
    updated = dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    assert updated
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=10, by_state=[(ReplicaState.STARTING, 10, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    for replica in ds._replicas.get():
        replica._actor.set_ready()

    # Check that the new replicas have started.
    dsm.update()
    check_counts(ds, total=10, by_state=[(ReplicaState.RUNNING, 10, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Now deploy a new version. Two old replicas should be stopped.
    b_info_2, v2 = deployment_info(num_replicas=10, version="2", user_config="2")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)
    dsm.update()
    check_counts(
        ds,
        total=12,
        by_state=[
            (ReplicaState.RUNNING, 8, v1),
            (ReplicaState.STOPPING, 2, v1),
            (ReplicaState.STARTING, 2, v2),
        ],
    )

    # Mark only one of the replicas as done stopping.
    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    dsm.update()
    check_counts(
        ds,
        total=11,
        by_state=[
            # Old version running
            (ReplicaState.RUNNING, 8, v1),
            # Replicas being "rolled out"
            (ReplicaState.STARTING, 2, v2),
            # Out of the picture
            (ReplicaState.STOPPING, 1, v1),
        ],
    )

    # Mark one new replica as ready. Then the rollout should continue,
    # stopping another old-version-replica and starting another
    # new-version-replica
    ds._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    dsm.update()
    check_counts(
        ds,
        total=12,
        by_state=[
            # Old version running
            (ReplicaState.RUNNING, 7, v1),
            # New version running
            (ReplicaState.RUNNING, 1, v2),
            # Replicas being "rolled out"
            (ReplicaState.STARTING, 2, v2),
            # Out of the picture
            (ReplicaState.STOPPING, 2, v1),
        ],
    )

    # Mark the old replicas as done stopping.
    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    ds._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()

    # Old replicas should be stopped and new versions started in batches of 2.
    new_replicas = 2
    old_replicas = 8
    while old_replicas:
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        # 2 replicas should be stopping, and simultaneously 2 replicas
        # should start to fill the gap.
        old_replicas -= 2
        dsm.update()
        check_counts(
            ds,
            total=12,
            by_state=[
                # Old version running
                (ReplicaState.RUNNING, old_replicas, v1),
                # New version running
                (ReplicaState.RUNNING, new_replicas, v2),
                # New replicas being "rolled out"
                (ReplicaState.STARTING, 2, v2),
                # Out of the picture
                (ReplicaState.STOPPING, 2, v1),
            ],
        )

        ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
        ds._replicas.get(states=[ReplicaState.STOPPING])[1]._actor.set_done_stopping()

        dsm.update()
        check_counts(
            ds,
            total=10,
            by_state=[
                # Old version running
                (ReplicaState.RUNNING, old_replicas, v1),
                # New version running
                (ReplicaState.RUNNING, new_replicas, v2),
                # Replicas being "rolled out"
                (ReplicaState.STARTING, 2, v2),
            ],
        )

        # Set both ready.
        ds._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
        ds._replicas.get(states=[ReplicaState.STARTING])[1]._actor.set_ready()
        new_replicas += 2

    # All new replicas should be up and running.
    dsm.update()
    check_counts(ds, total=10, by_state=[(ReplicaState.RUNNING, 10, v2)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_reconfigure_throttling(mock_deployment_state_manager):
    """All replicas should be started at once for a new deployment.

    When the version is updated, it should be throttled.
    """

    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(num_replicas=2, version="1", user_config="1")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    for replica in ds._replicas.get():
        replica._actor.set_ready()

    # Check that the new replicas have started.
    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Now deploy a new user_config. One replica should be updated.
    b_info_2, v2 = deployment_info(num_replicas=2, version="1", user_config="2")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    check_counts(
        ds,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1, v1), (ReplicaState.UPDATING, 1, v2)],
    )

    # Mark the updating replica as ready.
    ds._replicas.get(states=[ReplicaState.UPDATING])[0]._actor.set_ready()

    # The updated replica should now be RUNNING.
    # The second replica should now be updated.
    dsm.update()
    check_counts(
        ds,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1, v2), (ReplicaState.UPDATING, 1, v2)],
    )
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Mark the updating replica as ready.
    ds._replicas.get(states=[ReplicaState.UPDATING])[0]._actor.set_ready()

    # Both replicas should now be RUNNING.
    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v2)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_new_version_and_scale_down(mock_deployment_state_manager):
    # Test the case when we reduce the number of replicas and change the
    # version at the same time. First the number of replicas should be
    # turned down, then the rolling update should happen.
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(num_replicas=10, version="1")
    updated = dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    assert updated
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=10, by_state=[(ReplicaState.STARTING, 10, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    for replica in ds._replicas.get():
        replica._actor.set_ready()

    # Check that the new replicas have started.
    dsm.update()
    check_counts(ds, total=10, by_state=[(ReplicaState.RUNNING, 10, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Now deploy a new version and scale down the number of replicas to 2.
    # First, 8 old replicas should be stopped to bring it down to the target.
    b_info_2, v2 = deployment_info(num_replicas=2, version="2")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)
    dsm.update()
    check_counts(
        ds,
        total=10,
        by_state=[(ReplicaState.RUNNING, 2, v1), (ReplicaState.STOPPING, 8, v1)],
    )

    # Mark only one of the replicas as done stopping.
    # This should not yet trigger the rolling update because there are still
    # stopping replicas.
    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()

    dsm.update()
    check_counts(
        ds,
        total=9,
        by_state=[(ReplicaState.RUNNING, 2, v1), (ReplicaState.STOPPING, 7, v1)],
    )

    # Stop the remaining replicas.
    for replica in ds._replicas.get(states=[ReplicaState.STOPPING]):
        replica._actor.set_done_stopping()

    # Now the rolling update should trigger, stopping one of the old
    # replicas, simultaneously starting replica of new version.
    dsm.update()
    check_counts(
        ds,
        total=3,
        by_state=[
            (ReplicaState.RUNNING, 1, v1),
            (ReplicaState.STOPPING, 1, v1),
            (ReplicaState.STARTING, 1, v2),
        ],
    )

    # Mark old replica as stopped
    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    dsm.update()
    check_counts(
        ds,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1, v1), (ReplicaState.STARTING, 1, v2)],
    )

    # New version is started, final old version replica should be stopped.
    ds._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    dsm.update()
    check_counts(
        ds,
        total=3,
        by_state=[
            (ReplicaState.RUNNING, 1, v2),
            (ReplicaState.STOPPING, 1, v1),
            (ReplicaState.STARTING, 1, v2),
        ],
    )

    # Old replica finishes stopping and new replica is ready
    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    dsm.update()
    ds._replicas.get(states=[ReplicaState.STARTING])[0]._actor.set_ready()
    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v2)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_new_version_and_scale_up(mock_deployment_state_manager):
    # Test the case when we increase the number of replicas and change the
    # version at the same time. The new replicas should all immediately be
    # turned up. When they're up, rolling update should trigger.
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(num_replicas=2, version="1")
    updated = dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    assert updated
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    for replica in ds._replicas.get():
        replica._actor.set_ready()

    # Check that the new replicas have started.
    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # Now deploy a new version and scale up the number of replicas to 10.
    # 8 new replicas should be started.
    b_info_2, v2 = deployment_info(num_replicas=10, version="2")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)
    dsm.update()
    check_counts(
        ds,
        total=10,
        by_state=[(ReplicaState.RUNNING, 2, v1), (ReplicaState.STARTING, 8, v2)],
    )

    # Mark the new replicas as ready.
    for replica in ds._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()

    # Now that the new version replicas are up, rolling update should start.
    dsm.update()
    check_counts(
        ds,
        total=12,
        by_state=[
            (ReplicaState.RUNNING, 0, v1),
            (ReplicaState.STOPPING, 2, v1),
            (ReplicaState.STARTING, 2, v2),
            (ReplicaState.RUNNING, 8, v2),
        ],
    )

    # Mark the replicas as done stopping.
    for replica in ds._replicas.get(states=[ReplicaState.STOPPING]):
        replica._actor.set_done_stopping()
    dsm.update()
    check_counts(
        ds,
        total=10,
        by_state=[(ReplicaState.RUNNING, 8, v2), (ReplicaState.STARTING, 2, v2)],
    )

    # Mark the remaining replicas as ready.
    for replica in ds._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()

    # All new replicas should be up and running.
    dsm.update()
    check_counts(ds, total=10, by_state=[(ReplicaState.RUNNING, 10, v2)])

    dsm.update()
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


@pytest.mark.parametrize("target_capacity_direction", ["up", "down"])
def test_scale_num_replicas(mock_deployment_state_manager, target_capacity_direction):
    """Test upscaling and downscaling the number of replicas manually.

    Upscaling version:
    1. Deploy deployment with num_replicas=3.
    2. 3 replicas starting, status=UPDATING, trigger=DEPLOY.
    3. It becomes healthy with 3 running replicas.
    4. Update deployment to num_replicas=5.
    5. 2 replicas starting, status=UPSCALING, trigger=CONFIG_UPDATE.
    6. It becomes healthy with 5 running replicas, status=HEALTHY, trigger=CONFIG_UPDATE
    """

    # State
    version = get_random_string()

    # Create deployment state manager
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    # Deploy deployment with 3 replicas
    info_1, v1 = deployment_info(num_replicas=3, version=version)
    dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
    ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    # status=UPDATING, status_trigger=DEPLOY
    dsm.update()
    check_counts(ds, total=3, by_state=[(ReplicaState.STARTING, 3, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Set replicas ready and check statuses
    for replica in ds._replicas.get():
        replica._actor.set_ready()

    # status=HEALTHY, status_trigger=DEPLOY
    dsm.update()
    check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    # upscale or downscale the number of replicas manually
    new_num_replicas = 5 if target_capacity_direction == "up" else 1
    info_2, _ = deployment_info(num_replicas=new_num_replicas, version=version)
    dsm.deploy(TEST_DEPLOYMENT_ID, info_2)
    dsm.update()

    # status=UPSCALING/DOWNSCALING, status_trigger=CONFIG_UPDATE
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )
    if target_capacity_direction == "up":
        check_counts(
            ds,
            total=5,
            by_state=[(ReplicaState.RUNNING, 3, v1), (ReplicaState.STARTING, 2, v1)],
        )
        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        for replica in ds._replicas.get():
            replica._actor.set_ready()
    else:
        check_counts(
            ds,
            total=3,
            by_state=[(ReplicaState.RUNNING, 1, v1), (ReplicaState.STOPPING, 2, v1)],
        )
        assert ds.curr_status_info.status == DeploymentStatus.DOWNSCALING
        for replica in ds._replicas.get():
            replica._actor.set_done_stopping()

    # After the upscaling/downscaling finishes
    # status=HEALTHY, status_trigger=UPSCALING_COMPLETED/DOWNSCALE_COMPLETED
    dsm.update()
    check_counts(
        ds,
        total=new_num_replicas,
        by_state=[(ReplicaState.RUNNING, new_num_replicas, v1)],
    )
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert ds.curr_status_info.status_trigger == (
        DeploymentStatusTrigger.UPSCALE_COMPLETED
        if target_capacity_direction == "up"
        else DeploymentStatusTrigger.DOWNSCALE_COMPLETED
    )


@pytest.mark.parametrize("force_stop_unhealthy_replicas", [False, True])
def test_health_check(
    mock_deployment_state_manager, force_stop_unhealthy_replicas: bool
):
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(num_replicas=2, version="1")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    ds.FORCE_STOP_UNHEALTHY_REPLICAS = force_stop_unhealthy_replicas

    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    for replica in ds._replicas.get():
        replica._actor.set_ready()
        # Health check shouldn't be called until it's ready.
        assert not replica._actor.health_check_called

    # Check that the new replicas have started.
    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    dsm.update()
    for replica in ds._replicas.get():
        # Health check shouldn't be called until it's ready.
        assert replica._actor.health_check_called

    # Mark one replica unhealthy; it should be stopped.
    ds._replicas.get()[0]._actor.set_unhealthy()
    dsm.update()
    # SIMULTANEOUSLY a new replica should be started to try to reach
    # the target number of healthy replicas.
    check_counts(
        ds,
        total=3,
        by_state=[
            (ReplicaState.RUNNING, 1, v1),
            (ReplicaState.STOPPING, 1, v1),
            (ReplicaState.STARTING, 1, v1),
        ],
    )

    stopping_replicas = ds._replicas.get(states=[ReplicaState.STOPPING])
    assert len(stopping_replicas) == 1
    stopping_replica = stopping_replicas[0]
    if force_stop_unhealthy_replicas:
        assert stopping_replica._actor.force_stopped_counter == 1
    else:
        assert stopping_replica._actor.force_stopped_counter == 0

    assert ds.curr_status_info.status == DeploymentStatus.UNHEALTHY
    # If state transitioned from healthy -> unhealthy, status driver should be none
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.HEALTH_CHECK_FAILED
    )

    stopping_replica._actor.set_done_stopping()

    dsm.update()
    check_counts(
        ds,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1, v1), (ReplicaState.STARTING, 1, v1)],
    )
    assert ds.curr_status_info.status == DeploymentStatus.UNHEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.HEALTH_CHECK_FAILED
    )

    replica = ds._replicas.get(states=[ReplicaState.STARTING])[0]
    replica._actor.set_ready()
    assert ds.curr_status_info.status == DeploymentStatus.UNHEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.HEALTH_CHECK_FAILED
    )

    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert ds.curr_status_info.status_trigger == DeploymentStatusTrigger.UNSPECIFIED


def test_update_while_unhealthy(mock_deployment_state_manager):
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, v1 = deployment_info(num_replicas=2, version="1")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    for replica in ds._replicas.get():
        replica._actor.set_ready()
        # Health check shouldn't be called until it's ready.
        assert not replica._actor.health_check_called

    # Check that the new replicas have started.
    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v1)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )

    dsm.update()
    for replica in ds._replicas.get():
        # Health check shouldn't be called until it's ready.
        assert replica._actor.health_check_called

    # Mark one replica unhealthy. It should be stopped.
    ds._replicas.get()[0]._actor.set_unhealthy()
    dsm.update()
    check_counts(
        ds,
        total=3,
        by_state=[
            (ReplicaState.RUNNING, 1, v1),
            (ReplicaState.STOPPING, 1, v1),
            (ReplicaState.STARTING, 1, v1),
        ],
    )
    assert ds.curr_status_info.status == DeploymentStatus.UNHEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.HEALTH_CHECK_FAILED
    )

    replica = ds._replicas.get(states=[ReplicaState.STOPPING])[0]
    replica._actor.set_done_stopping()
    dsm.update()
    check_counts(
        ds,
        total=2,
        by_state=[
            (ReplicaState.RUNNING, 1, v1),
            (ReplicaState.STARTING, 1, v1),
        ],
    )

    # Now deploy a new version (e.g., a rollback). This should update the status
    # to UPDATING and then it should eventually become healthy.
    b_info_2, v2 = deployment_info(num_replicas=2, version="2")
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)

    # The replica that was still starting should be stopped (over the
    # running replica).
    dsm.update()
    # Simultaneously, a replica with the new version should be started
    check_counts(
        ds,
        total=3,
        by_state=[
            (ReplicaState.RUNNING, 1, v1),
            (ReplicaState.STOPPING, 1, v1),
            (ReplicaState.STARTING, 1, v2),
        ],
    )
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Mark the remaining running replica of the old version as unhealthy
    ds._replicas.get(states=[ReplicaState.RUNNING])[0]._actor.set_unhealthy()
    dsm.update()
    # A replica of the new version should get started to try to reach
    # the target number of healthy replicas
    check_counts(
        ds,
        total=4,
        by_state=[(ReplicaState.STOPPING, 2, v1), (ReplicaState.STARTING, 2, v2)],
    )
    # Check that a failure in the old version replica does not mark the
    # deployment as UNHEALTHY.
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    ds._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    ds._replicas.get(states=[ReplicaState.STOPPING])[1]._actor.set_done_stopping()

    # Another replica of the new version should get started.
    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v2)])

    # Mark new version replicas as ready.
    for replica in ds._replicas.get(states=[ReplicaState.STARTING]):
        replica._actor.set_ready()

    # Both replicas should be RUNNING, deployment should be HEALTHY.
    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v2)])
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def _constructor_failure_loop_two_replica(
    dsm, ds, num_loops, replica_retry_multiplier=3
):
    """Helper function to exact constructor failure loops."""

    for i in range(num_loops):
        # Two replicas should be created.
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, None)])

        assert ds._replica_constructor_retry_counter == i * 2

        replica_1 = ds._replicas.get()[0]
        replica_2 = ds._replicas.get()[1]

        replica_1._actor.set_failed_to_start()
        replica_2._actor.set_failed_to_start()
        # Now the replica should be marked STOPPING after failure.
        dsm.update()
        if ds._replica_constructor_retry_counter >= replica_retry_multiplier * 2:
            check_counts(
                ds,
                total=2,
                by_state=[(ReplicaState.STOPPING, 2, None)],
            )
        else:
            check_counts(
                ds,
                total=4,
                by_state=[
                    (ReplicaState.STOPPING, 2, None),
                    (ReplicaState.STARTING, 2, None),
                ],
            )

        # Once it's done stopping, replica should be removed.
        replica_1._actor.set_done_stopping()
        replica_2._actor.set_done_stopping()


def test_deploy_with_consistent_constructor_failure(
    mock_deployment_state_manager, mock_max_per_replica_retry_count
):
    """
    Test deploy() multiple replicas with consistent constructor failure.

    The deployment should get marked FAILED.
    """
    create_dsm, timer, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, _ = deployment_info(num_replicas=2)
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )
    loop_count = mock_max_per_replica_retry_count
    _constructor_failure_loop_two_replica(
        dsm, ds, loop_count, mock_max_per_replica_retry_count
    )

    assert ds._replica_constructor_retry_counter == 2 * mock_max_per_replica_retry_count
    assert ds.curr_status_info.status == DeploymentStatus.DEPLOY_FAILED
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.REPLICA_STARTUP_FAILED
    )
    check_counts(ds, total=2)
    assert ds.curr_status_info.message != ""

    # No more replicas should be retried.
    for _ in range(20):
        dsm.update()
        assert ds._replica_constructor_retry_counter == 4
        check_counts(ds, total=0)
        timer.advance(10)  # simulate time passing between each call to update


def test_deploy_with_partial_constructor_failure(
    mock_deployment_state_manager, mock_max_per_replica_retry_count
):
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
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, _ = deployment_info(num_replicas=2)
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    _constructor_failure_loop_two_replica(dsm, ds, 1, mock_max_per_replica_retry_count)
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, None)])
    assert ds._replica_constructor_retry_counter == 2
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Let one replica reach RUNNING state while the other still fails
    replica_1 = ds._replicas.get()[0]
    replica_2 = ds._replicas.get()[1]
    replica_1._actor.set_ready()
    replica_2._actor.set_failed_to_start()

    # Failed to start replica should be removed
    dsm.update()
    # A new replica should be brought up to take its place
    check_counts(
        ds,
        total=3,
        by_state=[
            (ReplicaState.RUNNING, 1, None),
            (ReplicaState.STOPPING, 1, None),
            (ReplicaState.STARTING, 1, None),
        ],
    )

    # Mark old replica as done stopping
    replica_2._actor.set_done_stopping()
    dsm.update()
    check_counts(
        ds,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1, None), (ReplicaState.STARTING, 1, None)],
    )

    # Set the starting one to fail again and trigger retry limit
    starting_replica = ds._replicas.get(states=[ReplicaState.STARTING])[0]
    starting_replica._actor.set_failed_to_start()

    dsm.update()
    # Ensure our goal returned with replica_has_started flag set
    assert ds._replica_has_started
    # Deployment should NOT be considered complete yet
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # A new replica should be brought up to take its place
    check_counts(
        ds,
        total=3,
        by_state=[
            (ReplicaState.RUNNING, 1, None),
            (ReplicaState.STOPPING, 1, None),
            (ReplicaState.STARTING, 1, None),
        ],
    )
    starting_replica = ds._replicas.get(states=[ReplicaState.STOPPING])[0]
    starting_replica._actor.set_done_stopping()

    dsm.update()
    check_counts(
        ds,
        total=2,
        by_state=[(ReplicaState.RUNNING, 1, None), (ReplicaState.STARTING, 1, None)],
    )
    starting_replica = ds._replicas.get(states=[ReplicaState.STARTING])[0]
    starting_replica._actor.set_ready()

    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])

    # Deployment should be considered complete
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_deploy_with_placement_group_failure(mock_deployment_state_manager):
    """
    Test deploy with a placement group failure.
    """

    def fake_create_placement_group_fn(placement_group_bundles, *args, **kwargs):
        """Fakes the placement_group_fn used by the scheduler.

        Lets the test to run without starting Ray. Raises an exception if the
        bundles are invalid.
        """

        validate_placement_group(bundles=placement_group_bundles)

    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm(
        create_placement_group_fn_override=fake_create_placement_group_fn,
    )

    def create_deployment_state(
        deployment_id: DeploymentID, pg_bundles=None
    ) -> List[DeploymentState]:
        b_info, _ = deployment_info(num_replicas=3)
        b_info.replica_config.placement_group_bundles = pg_bundles
        assert dsm.deploy(deployment_id, b_info)
        ds = dsm._deployment_states[deployment_id]
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )
        return ds

    # Make all of ds1's replica's placement groups invalid.
    invalid_bundle = [{"GPU": 0}]
    with pytest.raises(ValueError):
        validate_placement_group(invalid_bundle)

    ds1 = create_deployment_state(TEST_DEPLOYMENT_ID, pg_bundles=invalid_bundle)
    ds2 = create_deployment_state(TEST_DEPLOYMENT_ID_2)

    # Now ds1's replicas should all fail, while ds2's replicas should run.
    dsm.update()

    check_counts(ds1, total=3, by_state=[(ReplicaState.STOPPING, 3, None)])
    assert ds1._replica_constructor_retry_counter == 3
    assert "Retrying 6 more time(s)" in ds1.curr_status_info.message

    # Set all of ds1's replicas to stopped.
    for replica in ds1._replicas.get():
        replica._actor.set_done_stopping()

    check_counts(ds2, total=3, by_state=[(ReplicaState.STARTING, 3, None)])
    assert ds2._replica_constructor_retry_counter == 0

    # Set all of ds2's replicas to ready.
    for replica in ds2._replicas.get():
        replica._actor.set_ready()

    dsm.update()

    assert ds1.curr_status_info.status == DeploymentStatus.UPDATING
    check_counts(ds1, total=3, by_state=[(ReplicaState.STOPPING, 3, None)])
    assert ds1._replica_constructor_retry_counter == 6
    assert "Retrying 3 more time(s)" in ds1.curr_status_info.message

    # Set all of ds1's replicas to stopped.
    for replica in ds1._replicas.get():
        replica._actor.set_done_stopping()

    assert ds2.curr_status_info.status == DeploymentStatus.HEALTHY
    check_counts(ds2, total=3, by_state=[(ReplicaState.RUNNING, 3, None)])
    assert ds2._replica_constructor_retry_counter == 0

    dsm.update()

    assert ds1.curr_status_info.status == DeploymentStatus.UPDATING
    check_counts(ds1, total=3, by_state=[(ReplicaState.STOPPING, 3, None)])
    assert ds1._replica_constructor_retry_counter == 9
    assert "Retrying 0 more time(s)" in ds1.curr_status_info.message

    # Set all of ds1's replicas to stopped.
    for replica in ds1._replicas.get():
        replica._actor.set_done_stopping()

    dsm.update()

    # All replicas have failed to initialize 3 times. The deployment should
    # stop trying to initialize replicas.
    assert ds1.curr_status_info.status == DeploymentStatus.DEPLOY_FAILED
    check_counts(ds1, total=0)
    assert ds1._replica_constructor_retry_counter == 9
    assert "The deployment failed to start" in ds1.curr_status_info.message


def test_deploy_with_transient_constructor_failure(mock_deployment_state_manager):
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

    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    b_info_1, _ = deployment_info(num_replicas=2)
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )

    # Burn 4 retries from both replicas.
    _constructor_failure_loop_two_replica(dsm, ds, 2)
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )
    assert ds._replica_constructor_retry_counter == 4

    # Let both replicas succeed in last try.
    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, None)])
    assert ds.curr_status_info.status == DeploymentStatus.UPDATING
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
    )
    ds._replicas.get()[0]._actor.set_ready()
    ds._replicas.get()[1]._actor.set_ready()

    # Everything should be running and healthy now
    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])

    assert ds._replica_constructor_retry_counter == 0
    assert ds._replica_has_started
    assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
    assert (
        ds.curr_status_info.status_trigger
        == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
    )


def test_recover_state_from_replica_names(mock_deployment_state_manager):
    """Test recover deployment state."""
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    # Deploy deployment with version "1" and one replica
    info1, v1 = deployment_info(version="1")
    target_state_changed = dsm.deploy(TEST_DEPLOYMENT_ID, info1)
    assert target_state_changed
    dsm.save_checkpoint()
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    # Single replica of version `version1` should be created and in STARTING state
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])
    mocked_replica = ds._replicas.get()[0]

    # The same replica should transition to RUNNING
    mocked_replica._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v1)])

    # (simulate controller crashed!) Create a new deployment state
    # manager, and it should call _recover_from_checkpoint
    new_dsm: DeploymentStateManager = create_dsm(
        [mocked_replica.replica_id.to_full_id_str()]
    )

    # New deployment state should be created and one replica should
    # be RECOVERING with last-checkpointed target version `version1`
    new_ds = new_dsm._deployment_states[TEST_DEPLOYMENT_ID]
    check_counts(new_ds, total=1, by_state=[(ReplicaState.RECOVERING, 1, v1)])

    # Get the new mocked replica. Note that this represents a newly
    # instantiated class keeping track of the state of the replica,
    # but pointing to the same replica actor
    new_mocked_replica = new_ds._replicas.get()[0]
    new_mocked_replica._actor.set_ready(v1)
    any_recovering = new_dsm.update()
    check_counts(new_ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v1)])
    assert not any_recovering
    # Make sure replica ID is the same, meaning the actor is the same
    assert mocked_replica.replica_id == new_mocked_replica.replica_id


def test_recover_during_rolling_update(mock_deployment_state_manager):
    """Test controller crashes before a replica is updated to new version.

    During recovery, the controller should wait for the version to be fetched from
    the replica actor. Once it is fetched and the controller realizes the replica
    has an outdated version, it should be stopped and a new replica should be started
    with the target version.
    """
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm = create_dsm()

    # Step 1: Create some deployment info with actors in running state
    info1, v1 = deployment_info(version="1")
    target_state_changed = dsm.deploy(TEST_DEPLOYMENT_ID, info1)
    assert target_state_changed
    dsm.save_checkpoint()
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    # Single replica of version `version1` should be created and in STARTING state
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])
    mocked_replica = ds._replicas.get()[0]

    # The same replica should transition to RUNNING
    mocked_replica._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v1)])

    # Now execute a rollout: upgrade the version to "2".
    info2, v2 = deployment_info(version="2")
    target_state_changed = dsm.deploy(TEST_DEPLOYMENT_ID, info2)
    assert target_state_changed
    # In real code this checkpoint would be done by the caller of .deploy()
    dsm.save_checkpoint()

    # Before the replica could be stopped and restarted, simulate
    # controller crashed! A new deployment state manager should be
    # created, and it should call _recover_from_checkpoint
    new_dsm = create_dsm([mocked_replica.replica_id.to_full_id_str()])

    # New deployment state should be created and one replica should
    # be RECOVERING with last-checkpointed target version "2"
    new_ds = new_dsm._deployment_states[TEST_DEPLOYMENT_ID]
    check_counts(new_ds, total=1, by_state=[(ReplicaState.RECOVERING, 1, v2)])

    for _ in range(3):
        new_dsm.update()
        check_counts(new_ds, total=1, by_state=[(ReplicaState.RECOVERING, 1, v2)])

    # Get the new mocked replica. Note that this represents a newly
    # instantiated class keeping track of the state of the replica,
    # but pointing to the same replica actor
    new_mocked_replica = new_ds._replicas.get()[0]
    # Recover real version "1" (simulate previous actor not yet stopped)
    new_mocked_replica._actor.set_ready(v1)
    # At this point the replica is running
    new_dsm.update()
    # Then deployment state manager notices the replica has outdated version -> stops it
    new_dsm.update()
    # Also, a replica of version "2" should be started
    check_counts(
        new_ds,
        total=2,
        by_state=[(ReplicaState.STOPPING, 1, v1), (ReplicaState.STARTING, 1, v2)],
    )
    new_mocked_replica._actor.set_done_stopping()

    # Mark old replica as stopped.
    new_dsm.update()
    check_counts(new_ds, total=1, by_state=[(ReplicaState.STARTING, 1, v2)])
    new_mocked_replica_version2 = new_ds._replicas.get()[0]
    new_mocked_replica_version2._actor.set_ready()
    new_dsm.update()
    check_counts(new_ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v2)])
    # Make sure replica name is different, meaning a different "actor" was started
    assert mocked_replica.replica_id != new_mocked_replica_version2.replica_id


def test_actor_died_before_recover(mock_deployment_state_manager):
    """Test replica actor died before controller could recover it.

    * Deploy app / 1 deployment / 1 replica
    * (Simulated) Controller crashes.
    * Controller recovers, and tries to recover replicas from actor names.
    * (Simulated) The single replica from before has died before
      controller could recover it.
    * There should be 0 replicas in the deployment.
    * In the following control loop update cycle, the controller adds a
      new replica to match target state.
    """

    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm = create_dsm()

    # Create some deployment info with actors in running state
    info1, v1 = deployment_info(version="1")
    target_state_changed = dsm.deploy(TEST_DEPLOYMENT_ID, info1)
    assert target_state_changed
    dsm.save_checkpoint()
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    # Single replica of version `version1` should be created and in STARTING state
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])
    mocked_replica = ds._replicas.get()[0]
    replica_id = mocked_replica.replica_id

    # The same replica should transition to RUNNING
    mocked_replica._actor.set_ready()
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v1)])

    # Set dead replicas context. When the controller recovers and tries
    # to recover replicas from actor names, the replica actor wrapper
    # will fail to recover.
    dead_replicas_context.add(replica_id)

    # Simulate controller crashed! A new deployment state manager should
    # be created, and it should call _recover_from_checkpoint
    new_dsm = create_dsm([replica_id.to_full_id_str()])

    # Replica should fail to recover (simulate failed to get handle to
    # actor), meaning replica has died.
    new_ds = new_dsm._deployment_states[TEST_DEPLOYMENT_ID]
    check_counts(new_ds, total=0)

    # Since the previous replica is now marked dead (because controller
    # failed to recover it), a new replica should be added to meet
    # target state.
    new_dsm.update()
    check_counts(new_ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])
    dead_replicas_context.remove(replica_id)


def test_shutdown(mock_deployment_state_manager):
    """
    Test that shutdown waits for all deployments to be deleted and they
    are force-killed without a grace period.
    """
    create_dsm, timer, _, _ = mock_deployment_state_manager
    dsm = create_dsm()

    grace_period_s = 10
    b_info_1, _ = deployment_info(
        graceful_shutdown_timeout_s=grace_period_s,
    )
    assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)

    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    # Single replica should be created.
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
    ds._replicas.get()[0]._actor.set_ready()

    # Now the replica should be marked running.
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])

    # Test shutdown flow
    assert not ds._replicas.get()[0]._actor.stopped

    # Before shutdown, `is_ready_for_shutdown()` should return False
    assert not dsm.is_ready_for_shutdown()

    dsm.shutdown()

    timer.advance(grace_period_s + 0.1)
    dsm.update()

    check_counts(ds, total=1, by_state=[(ReplicaState.STOPPING, 1, None)])
    assert ds._replicas.get()[0]._actor.stopped
    assert len(dsm.get_deployment_statuses()) > 0

    # Once it's done stopping, replica should be removed.
    replica = ds._replicas.get()[0]
    replica._actor.set_done_stopping()
    dsm.update()
    check_counts(ds, total=0)
    assert len(dsm.get_deployment_statuses()) == 0

    # After all deployments shutdown, `is_ready_for_shutdown()` should return True
    assert dsm.is_ready_for_shutdown()


def test_resource_requirements_none():
    """Ensure resource_requirements doesn't break if a requirement is None"""

    class FakeActor:
        actor_resources = {"num_cpus": 2, "fake": None}
        placement_group_bundles = None
        available_resources = {}

    # Make a DeploymentReplica just to accesss its resource_requirement function
    replica_id = ReplicaID("asdf123", DeploymentID(name="test"))
    replica = DeploymentReplica(replica_id, None)
    replica._actor = FakeActor()

    # resource_requirements() should not error
    replica.resource_requirements()


class TestActorReplicaWrapper:
    def test_default_value(self):
        actor_replica = ActorReplicaWrapper(
            version=deployment_version("1"),
            replica_id=ReplicaID(
                "abc123",
                deployment_id=DeploymentID(name="test_deployment", app_name="test_app"),
            ),
        )
        assert (
            actor_replica.graceful_shutdown_timeout_s
            == DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S
        )
        assert actor_replica.max_ongoing_requests == DEFAULT_MAX_ONGOING_REQUESTS
        assert actor_replica.health_check_period_s == DEFAULT_HEALTH_CHECK_PERIOD_S
        assert actor_replica.health_check_timeout_s == DEFAULT_HEALTH_CHECK_TIMEOUT_S

    def test_max_concurrency_override(self):
        actor_replica = ActorReplicaWrapper(
            version=deployment_version("1"),
            replica_id=ReplicaID(
                "abc123",
                deployment_id=DeploymentID(name="test_deployment", app_name="test_app"),
            ),
        )
        max_ongoing_requests = DEFAULT_MAX_CONCURRENCY_ASYNC + 1
        d_info, _ = deployment_info(max_ongoing_requests=max_ongoing_requests)
        replica_scheduling_request = actor_replica.start(d_info, rank=0)
        assert (
            "max_concurrency" in replica_scheduling_request.actor_options
            and replica_scheduling_request.actor_options["max_concurrency"]
            == max_ongoing_requests
        )


def test_get_active_node_ids(mock_deployment_state_manager):
    """Test get_active_node_ids() are collecting the correct node ids

    When there are no running replicas, both methods should return empty results. When
    the replicas are in the RUNNING state, get_running_replica_node_ids() should return
    a list of all node ids. `get_active_node_ids()` should return a set
    of all node ids.
    """
    node1 = NodeID.from_random().hex()
    node2 = NodeID.from_random().hex()
    node_ids = (node1, node2, node2)

    create_dsm, _, cluster_node_info_cache, _ = mock_deployment_state_manager
    dsm = create_dsm()
    cluster_node_info_cache.add_node(node1)
    cluster_node_info_cache.add_node(node2)

    # Deploy deployment with version "1" and 3 replicas
    info1, v1 = deployment_info(version="1", num_replicas=3)
    assert dsm.deploy(TEST_DEPLOYMENT_ID, info1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    # When the replicas are in the STARTING state, `get_active_node_ids()` should
    # return a set of node ids.
    dsm.update()
    check_counts(ds, total=3, by_state=[(ReplicaState.STARTING, 3, v1)])
    mocked_replicas = ds._replicas.get()
    for idx, mocked_replica in enumerate(mocked_replicas):
        mocked_replica._actor.set_node_id(node_ids[idx])
    assert ds.get_active_node_ids() == set(node_ids)
    assert dsm.get_active_node_ids() == set(node_ids)

    # When the replicas are in RUNNING state, `get_active_node_ids()` should
    # return a set of `node_ids`.
    for mocked_replica in mocked_replicas:
        mocked_replica._actor.set_ready()
    dsm.update()
    check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, v1)])
    assert ds.get_active_node_ids() == set(node_ids)
    assert dsm.get_active_node_ids() == set(node_ids)

    for _ in mocked_replicas:
        ds._stop_one_running_replica_for_testing()
    dsm.update()
    check_counts(
        ds,
        total=6,
        by_state=[(ReplicaState.STOPPING, 3, v1), (ReplicaState.STARTING, 3, v1)],
    )


def test_get_active_node_ids_none(mock_deployment_state_manager):
    """Test get_active_node_ids() are not collecting none node ids.

    When the running replicas has None as the node id, `get_active_node_ids()` should
    not include it in the set.
    """
    node1 = NodeID.from_random().hex()
    node2 = NodeID.from_random().hex()
    node_ids = (node1, node2, node2)

    create_dsm, _, cluster_node_info_cache, _ = mock_deployment_state_manager
    dsm = create_dsm()
    cluster_node_info_cache.add_node(node1)
    cluster_node_info_cache.add_node(node2)

    # Deploy deployment with version "1" and 3 replicas
    info1, v1 = deployment_info(version="1", num_replicas=3)
    assert dsm.deploy(TEST_DEPLOYMENT_ID, info1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    # When the replicas are in the STARTING state, `get_active_node_ids()` should
    # return a set of node ids.
    dsm.update()
    check_counts(ds, total=3, by_state=[(ReplicaState.STARTING, 3, v1)])
    mocked_replicas = ds._replicas.get()
    for idx, mocked_replica in enumerate(mocked_replicas):
        mocked_replica._actor.set_node_id(node_ids[idx])
    assert ds.get_active_node_ids() == set(node_ids)
    assert dsm.get_active_node_ids() == set(node_ids)

    # When the replicas are in the RUNNING state and are having None node id,
    # `get_active_node_ids()` should return empty set.
    for mocked_replica in mocked_replicas:
        mocked_replica._actor.set_node_id(None)
        mocked_replica._actor.set_ready()
    dsm.update()
    check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, v1)])
    assert None not in ds.get_active_node_ids()
    assert None not in dsm.get_active_node_ids()


class TestAutoscaling:
    def scale(
        self,
        dsm: DeploymentStateManager,
        asm: AutoscalingStateManager,
        deployment_ids: List[DeploymentID],
    ):
        if not deployment_ids:
            return

        app_name = deployment_ids[0].app_name
        assert all(dep_id.app_name == app_name for dep_id in deployment_ids)

        deployment_to_target_num_replicas = {
            dep_id: dsm.get_deployment_details(dep_id).target_num_replicas
            for dep_id in deployment_ids
        }
        decisions = asm.get_decision_num_replicas(
            app_name, deployment_to_target_num_replicas
        )
        for deployment_id, decision_num_replicas in decisions.items():
            dsm.autoscale(deployment_id, decision_num_replicas)

    @pytest.mark.parametrize("target_capacity_direction", ["up", "down"])
    def test_basic_autoscaling(
        self, mock_deployment_state_manager, target_capacity_direction
    ):
        """Test autoscaling up and down.

        Upscaling version:
        1. Deploy deployment with autoscaling limits [0,6],
           initial_replicas=3, target=1.
        2. It becomes healthy with 3 running replicas.
        3. Set average request metrics to 2 (compare to target=1).
        4. Deployment autoscales, 3 replicas starting, status=UPSCALING,
           trigger=AUTOSCALE.
        5. It becomes healthy with 6 running replicas, status=HEALTHY,
           trigger=UPSCALE.
        """

        # Create deployment state manager
        create_dsm, timer, _, asm = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()
        asm: AutoscalingStateManager = asm

        # Deploy deployment with 3 replicas
        info, _ = deployment_info(
            autoscaling_config={
                "target_ongoing_requests": 1,
                "min_replicas": 0,
                "max_replicas": 6,
                "initial_replicas": 3,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
                "metrics_interval_s": 100,
            }
        )
        dsm.deploy(TEST_DEPLOYMENT_ID, info)
        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # status=UPDATING, status_trigger=DEPLOY
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.STARTING, 3, None)])
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        # Set replicas ready and check statuses
        for replica in ds._replicas.get():
            replica._actor.set_ready()

        # status=HEALTHY, status_trigger=DEPLOY
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
        )

        req_per_replica = 2 if target_capacity_direction == "up" else 0
        replicas = ds._replicas.get()
        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
            handle_metric_report = HandleMetricReport(
                deployment_id=TEST_DEPLOYMENT_ID,
                handle_id="random",
                actor_id="actor_id",
                handle_source=DeploymentHandleSource.UNKNOWN,
                queued_requests=[TimeStampedValue(timer.time() - 0.1, 0)],
                aggregated_queued_requests=0,
                aggregated_metrics={
                    RUNNING_REQUESTS_KEY: {
                        replica._actor.replica_id: req_per_replica
                        for replica in replicas
                    }
                },
                metrics={
                    RUNNING_REQUESTS_KEY: {
                        replica._actor.replica_id: [
                            TimeStampedValue(timer.time() - 0.1, req_per_replica)
                        ]
                        for replica in replicas
                    }
                },
                timestamp=timer.time(),
            )
            asm.record_request_metrics_for_handle(handle_metric_report)
        else:
            for replica in replicas:
                replica_metric_report = ReplicaMetricReport(
                    replica_id=replica._actor.replica_id,
                    aggregated_metrics={RUNNING_REQUESTS_KEY: req_per_replica},
                    metrics={
                        RUNNING_REQUESTS_KEY: [
                            TimeStampedValue(timer.time() - 0.1, req_per_replica)
                        ]
                    },
                    timestamp=timer.time(),
                )
                asm.record_request_metrics_for_replica(replica_metric_report)

        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])

        # status=UPSCALING/DOWNSCALING, status_trigger=AUTOSCALE
        dsm.update()
        if target_capacity_direction == "up":
            check_counts(
                ds,
                total=6,
                by_state=[
                    (ReplicaState.RUNNING, 3, None),
                    (ReplicaState.STARTING, 3, None),
                ],
            )
            assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
            assert (
                ds.curr_status_info.status_trigger
                == DeploymentStatusTrigger.AUTOSCALING
            )

            # Advance timer by 60 seconds; this should exceed the slow startup
            # warning threshold. The message should be updated, but the status
            # should remain upscaling/autoscaling
            timer.advance(60)
            dsm.update()
            check_counts(
                ds,
                total=6,
                by_state=[
                    (ReplicaState.RUNNING, 3, None),
                    (ReplicaState.STARTING, 3, None),
                ],
            )
            assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
            assert (
                ds.curr_status_info.status_trigger
                == DeploymentStatusTrigger.AUTOSCALING
            )
            assert "have taken more than" in ds.curr_status_info.message

            # Set replicas ready
            for replica in ds._replicas.get():
                replica._actor.set_ready()
        else:
            # Due to two-stage downscaling one of the replicas will still be running
            check_counts(
                ds,
                total=3,
                by_state=[
                    (ReplicaState.STOPPING, 2, None),
                    (ReplicaState.RUNNING, 1, None),
                ],
            )
            # Trigger the second stage of downscaling
            self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
            dsm.update()
            check_counts(ds, total=3, by_state=[(ReplicaState.STOPPING, 3, None)])

            assert ds.curr_status_info.status == DeploymentStatus.DOWNSCALING
            assert (
                ds.curr_status_info.status_trigger
                == DeploymentStatusTrigger.AUTOSCALING
            )
            for replica in ds._replicas.get():
                replica._actor.set_done_stopping()

            dsm.update()
            astate = asm._app_autoscaling_states[
                TEST_DEPLOYMENT_ID.app_name
            ]._deployment_autoscaling_states[TEST_DEPLOYMENT_ID]
            assert len(astate._replica_metrics) == 0

        # status=HEALTHY, status_trigger=UPSCALE/DOWNSCALE
        dsm.update()
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
        assert ds.curr_status_info.status_trigger == (
            DeploymentStatusTrigger.UPSCALE_COMPLETED
            if target_capacity_direction == "up"
            else DeploymentStatusTrigger.DOWNSCALE_COMPLETED
        )

        # Make sure autoscaling state is removed when deployment is deleted
        dsm.delete_deployment(TEST_DEPLOYMENT_ID)
        dsm.update()
        for replica in ds._replicas.get():
            replica._actor.set_done_stopping()
        dsm.update()
        assert TEST_DEPLOYMENT_ID not in dsm._deployment_states

    @pytest.mark.parametrize(
        "target_startup_status",
        [
            ReplicaStartupStatus.PENDING_ALLOCATION,
            ReplicaStartupStatus.PENDING_INITIALIZATION,
        ],
    )
    def test_downscaling_reclaiming_starting_replicas_first(
        self,
        target_startup_status,
        mock_deployment_state_manager,
    ):
        """This test asserts that when downscaling first any non-running replicas are
        scavenged, before stopping fully running replicas

        More context on the issue could be found in:
        https://github.com/ray-project/ray/issues/43034
        """

        # Create deployment state manager
        create_dsm, timer, _, asm = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()
        asm: AutoscalingStateManager = asm

        # Deploy deployment with 3 replicas
        info, _ = deployment_info(
            autoscaling_config={
                "target_ongoing_requests": 1,
                "min_replicas": 0,
                "max_replicas": 6,
                "initial_replicas": 3,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
                "metrics_interval_s": 100,
            }
        )

        dsm.deploy(TEST_DEPLOYMENT_ID, info)

        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # status=UPDATING, status_trigger=DEPLOY
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.STARTING, 3, None)])
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        # Set replicas as SUCCESSFUL and check statuses
        for replica in ds._replicas.get():
            replica._actor.set_ready()

        # status=HEALTHY, status_trigger=DEPLOY
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
        )

        # Fetch all currently running replicas
        running_replicas = ds._replicas.get(states=[ReplicaState.RUNNING])
        replicas = ds._replicas.get()
        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
            handle_metric_report = HandleMetricReport(
                deployment_id=TEST_DEPLOYMENT_ID,
                handle_id="random",
                actor_id="actor_id",
                handle_source=DeploymentHandleSource.UNKNOWN,
                queued_requests=[TimeStampedValue(timer.time() - 0.1, 0)],
                aggregated_queued_requests=0,
                aggregated_metrics={
                    RUNNING_REQUESTS_KEY: {
                        replica._actor.replica_id: 2 for replica in replicas
                    }
                },
                metrics={
                    RUNNING_REQUESTS_KEY: {
                        replica._actor.replica_id: [
                            TimeStampedValue(timer.time() - 0.1, 2)
                        ]
                        for replica in replicas
                    }
                },
                timestamp=timer.time(),
            )
            asm.record_request_metrics_for_handle(handle_metric_report)
        else:
            for replica in replicas:
                replica_metric_report = ReplicaMetricReport(
                    replica_id=replica._actor.replica_id,
                    aggregated_metrics={RUNNING_REQUESTS_KEY: 2},
                    metrics={
                        RUNNING_REQUESTS_KEY: [TimeStampedValue(timer.time() - 0.1, 2)]
                    },
                    timestamp=timer.time(),
                )
                asm.record_request_metrics_for_replica(replica_metric_report)

        # status=UPSCALING, status_trigger=AUTOSCALE
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
        dsm.update()
        check_counts(
            ds,
            total=6,
            by_state=[
                (ReplicaState.RUNNING, 3, None),
                (ReplicaState.STARTING, 3, None),
            ],
        )
        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        assert ds.curr_status_info.status_trigger == DeploymentStatusTrigger.AUTOSCALING

        # Set replicas as PENDING_INITIALIZATION: actors have been
        # successfully allocated, but replicas are still pending
        # successful initialization
        for replica in ds._replicas.get():
            replica._actor.set_status(target_startup_status)

        # Advance timer by 60 seconds; this should exceed the slow startup
        # warning threshold. The message should be updated, but the status
        # should remain upscaling/autoscaling
        timer.advance(60)
        dsm.update()
        check_counts(
            ds,
            total=6,
            by_state=[
                (ReplicaState.RUNNING, 3, None),
                (ReplicaState.STARTING, 3, None),
            ],
        )

        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        assert ds.curr_status_info.status_trigger == DeploymentStatusTrigger.AUTOSCALING

        if target_startup_status == ReplicaStartupStatus.PENDING_INITIALIZATION:
            expected_message = (
                "Deployment 'test_deployment' in application 'test_app' has 3 replicas "
                f"that have taken more than {SLOW_STARTUP_WARNING_S}s to initialize.\n"
                "This may be caused by a slow __init__ or reconfigure method."
            )
        elif target_startup_status == ReplicaStartupStatus.PENDING_ALLOCATION:
            expected_message = (
                "Deployment 'test_deployment' in application 'test_app' "
                "has 3 replicas that have taken more than 30s to be scheduled. "
                "This may be due to waiting for the cluster to auto-scale or for "
                "a runtime environment to be installed. "
                "Resources required for each replica: "
                '{"CPU": 0.1}, '
                "total resources available: "
                "{}. "
                "Use `ray status` for more details."
            )
        else:
            raise RuntimeError(f"Got unexpected status: {target_startup_status}")

        assert expected_message == ds.curr_status_info.message

        # Now, trigger downscaling attempting to reclaim half (3) of the replicas
        replicas = ds._replicas.get(states=[ReplicaState.RUNNING])
        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
            handle_metric_report = HandleMetricReport(
                deployment_id=TEST_DEPLOYMENT_ID,
                handle_id="random",
                actor_id="actor_id",
                handle_source=DeploymentHandleSource.UNKNOWN,
                queued_requests=[TimeStampedValue(timer.time() - 0.1, 0)],
                aggregated_queued_requests=0,
                aggregated_metrics={
                    RUNNING_REQUESTS_KEY: {
                        replica._actor.replica_id: 1 for replica in replicas
                    }
                },
                metrics={
                    RUNNING_REQUESTS_KEY: {
                        replica._actor.replica_id: [
                            TimeStampedValue(timer.time() - 0.1, 1)
                        ]
                        for replica in replicas
                    }
                },
                timestamp=timer.time(),
            )
            asm.record_request_metrics_for_handle(handle_metric_report)
        else:
            for replica in replicas:
                replica_metric_report = ReplicaMetricReport(
                    replica_id=replica._actor.replica_id,
                    aggregated_metrics={RUNNING_REQUESTS_KEY: 1},
                    metrics={
                        RUNNING_REQUESTS_KEY: [TimeStampedValue(timer.time() - 0.1, 1)]
                    },
                    timestamp=timer.time(),
                )
                asm.record_request_metrics_for_replica(replica_metric_report)

        # status=DOWNSCALING, status_trigger=AUTOSCALE
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
        dsm.update()
        check_counts(
            ds,
            total=6,
            by_state=[
                (ReplicaState.RUNNING, 3, None),
                (ReplicaState.STOPPING, 3, None),
            ],
        )

        # Assert that no RUNNING replicas are being stopped
        assert running_replicas == ds._replicas.get(states=[ReplicaState.RUNNING])

        assert ds.curr_status_info.status == DeploymentStatus.DOWNSCALING
        assert ds.curr_status_info.status_trigger == DeploymentStatusTrigger.AUTOSCALING

        for replica in ds._replicas.get():
            replica._actor.set_done_stopping()

        # status=HEALTHY, status_trigger=UPSCALE/DOWNSCALE
        dsm.update()
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.DOWNSCALE_COMPLETED
        )

    def test_update_autoscaling_config(self, mock_deployment_state_manager):
        """Test updating the autoscaling config.

        1. Deploy deployment with autoscaling limits [0,6] and initial replicas = 3.
        2. It becomes healthy with 3 running replicas.
        3. Update autoscaling config to limits [6,10].
        4. 3 new replicas should be STARTING, and deployment status should be UPDATING.
        5. It becomes healthy with 6 running replicas.
        """

        # Create deployment state manager
        create_dsm, timer, _, asm = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()
        asm: AutoscalingStateManager = asm

        # Deploy deployment with 3 replicas
        info1, _ = deployment_info(
            autoscaling_config={
                "target_ongoing_requests": 1,
                "min_replicas": 0,
                "max_replicas": 6,
                "initial_replicas": 3,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
            },
            version="1",
        )
        dsm.deploy(TEST_DEPLOYMENT_ID, info1)
        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Set replicas ready
        dsm.update()
        for replica in ds._replicas.get():
            replica._actor.set_ready()

        # status=HEALTHY, status_trigger=DEPLOY
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
        )

        # Num ongoing requests = 1, status should remain HEALTHY
        replicas = ds._replicas.get()
        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
            handle_metric_report = HandleMetricReport(
                deployment_id=TEST_DEPLOYMENT_ID,
                handle_id="random",
                actor_id="actor_id",
                handle_source=DeploymentHandleSource.UNKNOWN,
                queued_requests=[TimeStampedValue(timer.time() - 0.1, 0)],
                aggregated_queued_requests=0,
                aggregated_metrics={
                    RUNNING_REQUESTS_KEY: {
                        replica._actor.replica_id: 1 for replica in replicas
                    }
                },
                metrics={
                    RUNNING_REQUESTS_KEY: {
                        replica._actor.replica_id: [
                            TimeStampedValue(timer.time() - 0.1, 1)
                        ]
                        for replica in replicas
                    }
                },
                timestamp=timer.time(),
            )
            asm.record_request_metrics_for_handle(handle_metric_report)
        else:
            for replica in replicas:
                replica_metric_report = ReplicaMetricReport(
                    replica_id=replica._actor.replica_id,
                    aggregated_metrics={RUNNING_REQUESTS_KEY: 1},
                    metrics={
                        RUNNING_REQUESTS_KEY: [TimeStampedValue(timer.time() - 0.1, 1)]
                    },
                    timestamp=timer.time(),
                )
                asm.record_request_metrics_for_replica(replica_metric_report)

        check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
        )

        # Update autoscaling config
        info2, _ = deployment_info(
            autoscaling_config={
                "target_ongoing_requests": 1,
                "min_replicas": 6,
                "max_replicas": 10,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
            },
            version="1",
        )
        dsm.deploy(TEST_DEPLOYMENT_ID, info2)

        # 3 new replicas should be starting, status should be UPDATING (not upscaling)
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
        dsm.update()
        check_counts(
            ds,
            total=6,
            by_state=[
                (ReplicaState.RUNNING, 3, None),
                (ReplicaState.STARTING, 3, None),
            ],
        )
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        # Set replicas ready
        dsm.update()
        for replica in ds._replicas.get():
            replica._actor.set_ready()
        dsm.update()
        check_counts(ds, total=6, by_state=[(ReplicaState.RUNNING, 6, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
        )

    def test_replicas_fail_during_initial_scale_from_zero(
        self, mock_deployment_state_manager
    ):
        """Test the following case:

        - An "erroneous" deployment (w/ autoscaling enabled) is deployed
          with initial replicas set to 0. Since no replicas are started,
          no errors have occurred from trying to start the replicas yet.
        - A request is sent, triggering an upscale.
        - The controller tries to start new replicas, but fails because
          of a constructor error.

        In this case, the deployment should transition to UNHEALTHY and
        stop retrying after a threshold.
        """
        create_dsm, timer, _, asm = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()
        asm: AutoscalingStateManager = asm

        # Deploy deployment with 1 initial replica
        info, _ = deployment_info(
            autoscaling_config={
                "target_ongoing_requests": 1,
                "min_replicas": 0,
                "max_replicas": 2,
                "initial_replicas": 0,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
                "metrics_interval_s": 100,
            }
        )
        dsm.deploy(TEST_DEPLOYMENT_ID, info)
        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Send request metrics to controller to make the deployment upscale
        handle_metric_report = HandleMetricReport(
            deployment_id=TEST_DEPLOYMENT_ID,
            handle_id="random",
            actor_id="actor_id",
            handle_source=DeploymentHandleSource.UNKNOWN,
            queued_requests=[TimeStampedValue(timer.time() - 0.1, 1)],
            aggregated_queued_requests=1,
            aggregated_metrics={},
            metrics={},
            timestamp=timer.time(),
        )
        asm.record_request_metrics_for_handle(handle_metric_report)
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])

        # The controller should try to start a new replica. If that replica repeatedly
        # fails to start, the deployment should transition to UNHEALTHY and NOT retry
        # replicas anymore
        for i in range(10):
            dsm.update()
            if i < 3:
                check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
                assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
                assert (
                    ds.curr_status_info.status_trigger
                    == DeploymentStatusTrigger.AUTOSCALING
                )
                # Set replica failed to start
                replica = ds._replicas.get()[0]
                replica._actor.set_failed_to_start()
                dsm.update()
                if i < 2:
                    check_counts(
                        ds,
                        total=2,
                        by_state=[
                            (ReplicaState.STOPPING, 1, None),
                            (ReplicaState.STARTING, 1, None),
                        ],
                    )
                else:
                    check_counts(
                        ds, total=1, by_state=[(ReplicaState.STOPPING, 1, None)]
                    )

                # Set replica finished stopping
                replica._actor.set_done_stopping()
            else:
                check_counts(ds, total=0)
                assert ds.curr_status_info.status == DeploymentStatus.UNHEALTHY
                assert (
                    ds.curr_status_info.status_trigger
                    == DeploymentStatusTrigger.REPLICA_STARTUP_FAILED
                )

    def test_replicas_fail_during_subsequent_scale_from_zero(
        self, mock_deployment_state_manager
    ):
        """Test the following case:

        - An autoscaling deployment is deployed and it reaches HEALTHY
          with a non-zero number of replicas.
        - After a period of no traffic, the deployment scales down to 0.
        - New traffic is sent, triggering an upscale.
        - The controller tries to start new replicas, but for some
          reason some replicas fail to start because of transient errors

        In this case, the deployment should transition to UNHEALTHY and
        keep retrying, since at least one replica of this version has
        successfully started in the past, meaning we don't know if it is
        an unrecoverable user code error.
        """
        create_dsm, timer, _, asm = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()
        asm: AutoscalingStateManager = asm

        # Deploy deployment with 1 initial replica
        info, _ = deployment_info(
            autoscaling_config={
                "target_ongoing_requests": 1,
                "min_replicas": 0,
                "max_replicas": 2,
                "initial_replicas": 1,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
                "metrics_interval_s": 100,
            }
        )
        dsm.deploy(TEST_DEPLOYMENT_ID, info)
        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Expected: status=UPDATING, status_trigger=CONFIG_UPDATED_STARTED
        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        # Set replicas ready and check statuses
        # Expected: status=HEALTHY, status_trigger=CONFIG_UPDATED_COMPLETED
        ds._replicas.get()[0]._actor.set_ready()
        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
        )

        # There are no requests, so the deployment should be downscaled to zero.
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.STOPPING, 1, None)])
        ds._replicas.get()[0]._actor.set_done_stopping()
        dsm.update()
        check_counts(ds, total=0)

        # Send request metrics to controller to make the deployment upscale
        handle_metric_report = HandleMetricReport(
            deployment_id=TEST_DEPLOYMENT_ID,
            handle_id="random",
            actor_id="actor_id",
            handle_source=DeploymentHandleSource.UNKNOWN,
            queued_requests=[TimeStampedValue(timer.time() - 0.1, 1)],
            aggregated_queued_requests=1,
            aggregated_metrics={},
            metrics={},
            timestamp=timer.time(),
        )
        asm.record_request_metrics_for_handle(handle_metric_report)
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
        # The controller should try to start a new replica. If that replica repeatedly
        # fails to start, the deployment should transition to UNHEALTHY. Meanwhile
        # the controller should continue retrying after 3 times.
        for i in range(10):
            dsm.update()
            check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
            if i < 3:
                assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
                assert (
                    ds.curr_status_info.status_trigger
                    == DeploymentStatusTrigger.AUTOSCALING
                )
            else:
                assert ds.curr_status_info.status == DeploymentStatus.UNHEALTHY
                assert (
                    ds.curr_status_info.status_trigger
                    == DeploymentStatusTrigger.REPLICA_STARTUP_FAILED
                )

            # Set replica failed to start
            replica = ds._replicas.get()[0]
            replica._actor.set_failed_to_start()
            dsm.update()
            check_counts(
                ds,
                total=2,
                by_state=[
                    (ReplicaState.STOPPING, 1, None),
                    (ReplicaState.STARTING, 1, None),
                ],
            )

            # Set replica finished stopping
            replica._actor.set_done_stopping()

    @pytest.mark.skipif(
        not RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
        reason="Testing handle metrics behavior.",
    )
    def test_handle_metrics_timeout(self, mock_deployment_state_manager):
        create_dsm, timer, _, asm = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()
        asm: AutoscalingStateManager = asm

        # Deploy, start with 1 replica
        info, _ = deployment_info(
            autoscaling_config={
                "target_ongoing_requests": 1,
                "min_replicas": 0,
                "max_replicas": 6,
                "initial_replicas": 1,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
            }
        )
        dsm.deploy(TEST_DEPLOYMENT_ID, info)
        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]
        dsm.update()
        ds._replicas.get()[0]._actor.set_ready()
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])

        # Record 2 requests/replica -> trigger upscale
        handle_metric_report = HandleMetricReport(
            deployment_id=TEST_DEPLOYMENT_ID,
            handle_id="random",
            actor_id="actor_id",
            handle_source=DeploymentHandleSource.UNKNOWN,
            queued_requests=[TimeStampedValue(timer.time() - 0.1, 0)],
            aggregated_queued_requests=0,
            aggregated_metrics={
                RUNNING_REQUESTS_KEY: {ds._replicas.get()[0]._actor.replica_id: 2}
            },
            metrics={
                RUNNING_REQUESTS_KEY: {
                    ds._replicas.get()[0]._actor.replica_id: [
                        TimeStampedValue(timer.time() - 0.1, 2)
                    ]
                }
            },
            timestamp=timer.time(),
        )
        asm.record_request_metrics_for_handle(handle_metric_report)
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
        dsm.update()
        check_counts(
            ds,
            total=2,
            by_state=[
                (ReplicaState.RUNNING, 1, None),
                (ReplicaState.STARTING, 1, None),
            ],
        )
        assert asm.get_total_num_requests_for_deployment(TEST_DEPLOYMENT_ID) == 2
        ds._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
        assert asm.get_total_num_requests_for_deployment(TEST_DEPLOYMENT_ID) == 2

        # Simulate handle was on an actor that died. 10 seconds later
        # the handle fails to push metrics
        timer.advance(10)
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
        assert asm.get_total_num_requests_for_deployment(TEST_DEPLOYMENT_ID) == 2

        # Another 10 seconds later handle still fails to push metrics. At
        # this point the data from the handle should be invalidated. As a
        # result, the replicas should scale back down to 0.
        timer.advance(10)
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        # The first update will trigger the first stage of downscaling to 1
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
        dsm.update()
        check_counts(
            ds,
            total=2,
            by_state=[
                (ReplicaState.STOPPING, 1, None),
                (ReplicaState.RUNNING, 1, None),
            ],
        )
        # The second update will trigger the second stage of downscaling from 1 to 0
        self.scale(dsm, asm, [TEST_DEPLOYMENT_ID])
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.STOPPING, 2, None)])
        assert asm.get_total_num_requests_for_deployment(TEST_DEPLOYMENT_ID) == 0

    @pytest.mark.skipif(
        not RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
        reason="Testing handle metrics behavior.",
    )
    def test_handle_metrics_on_dead_serve_actor(self, mock_deployment_state_manager):
        """Metrics for handles on dead serve actors should be dropped."""

        create_dsm, timer, _, asm = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()
        asm: AutoscalingStateManager = asm
        d_id1 = DeploymentID("d1", "app")
        d_id2 = DeploymentID("d2", "app")

        # Deploy, start with 1 replica
        info1, _ = deployment_info(
            autoscaling_config={
                "target_ongoing_requests": 1,
                "min_replicas": 0,
                "max_replicas": 6,
                "initial_replicas": 1,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
            },
        )
        info2, _ = deployment_info(health_check_period_s=0.1)
        dsm.deploy(d_id1, info1)
        dsm.deploy(d_id2, info2)

        ds1: DeploymentState = dsm._deployment_states[d_id1]
        ds2: DeploymentState = dsm._deployment_states[d_id2]

        # One replica each
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        dsm.update()
        ds1._replicas.get()[0]._actor.set_ready()
        ds2._replicas.get()[0]._actor.set_ready()
        ds2._replicas.get()[0]._actor.set_actor_id("d2_replica_actor_id")
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        dsm.update()
        check_counts(ds1, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
        check_counts(ds2, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])

        # Record 2 requests/replica (sent from d2 replica) -> trigger upscale
        handle_metric_report = HandleMetricReport(
            deployment_id=d_id1,
            handle_id="random",
            actor_id="d2_replica_actor_id",
            handle_source=DeploymentHandleSource.REPLICA,
            queued_requests=[TimeStampedValue(timer.time() - 0.1, 0)],
            aggregated_queued_requests=0,
            aggregated_metrics={
                RUNNING_REQUESTS_KEY: {ds1._replicas.get()[0]._actor.replica_id: 2}
            },
            metrics={
                RUNNING_REQUESTS_KEY: {
                    ds1._replicas.get()[0]._actor.replica_id: [
                        TimeStampedValue(timer.time() - 0.1, 2)
                    ]
                }
            },
            timestamp=timer.time(),
        )
        asm.record_request_metrics_for_handle(handle_metric_report)
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        self.scale(dsm, asm, [d_id1, d_id2])
        dsm.update()
        check_counts(
            ds1,
            total=2,
            by_state=[
                (ReplicaState.RUNNING, 1, None),
                (ReplicaState.STARTING, 1, None),
            ],
        )
        assert asm.get_total_num_requests_for_deployment(d_id1) == 2
        ds1._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        self.scale(dsm, asm, [d_id1, d_id2])
        dsm.update()
        check_counts(ds1, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
        assert asm.get_total_num_requests_for_deployment(d_id1) == 2

        # d2 replica died
        ds2._replicas.get()[0]._actor.set_unhealthy()
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        self.scale(dsm, asm, [d_id1, d_id2])
        dsm.update()
        check_counts(
            ds2,
            total=2,
            by_state=[
                (ReplicaState.STARTING, 1, None),
                (ReplicaState.STOPPING, 1, None),
            ],
        )
        ds2._replicas.get(states=[ReplicaState.STOPPING])[0]._actor.set_done_stopping()
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        self.scale(dsm, asm, [d_id1, d_id2])
        dsm.update()
        check_counts(ds2, total=1, by_state=[(ReplicaState.STARTING, 1, None)])

        # Now that the d2 replica is dead, its metrics should be dropped.
        # Consequently d1 should scale down to 0 replicas
        asm.drop_stale_handle_metrics(dsm.get_alive_replica_actor_ids())
        self.scale(dsm, asm, [d_id1, d_id2])
        dsm.update()
        # Due to two-stage downscaling one of the replicas will still be running
        check_counts(
            ds1,
            total=2,
            by_state=[
                (ReplicaState.STOPPING, 1, None),
                (ReplicaState.RUNNING, 1, None),
            ],
        )
        # Trigger the second stage of downscaling
        self.scale(dsm, asm, [d_id1, d_id2])
        dsm.update()
        check_counts(ds1, total=2, by_state=[(ReplicaState.STOPPING, 2, None)])


class TestTargetCapacity:
    """
    Tests related to the `target_capacity` field that adjusts the target num_replicas.
    """

    def update_target_capacity(
        self,
        deployment_state: DeploymentState,
        curr_deployment_info: DeploymentInfo,
        target_capacity: Optional[float],
        target_capacity_direction: Optional[TargetCapacityDirection],
    ):
        new_deployment_info = deepcopy(curr_deployment_info)
        new_deployment_info.set_target_capacity(
            new_target_capacity=target_capacity,
            new_target_capacity_direction=target_capacity_direction,
        )
        updating = deployment_state.deploy(new_deployment_info)
        assert updating

    @pytest.mark.parametrize(
        "num_replicas,target_capacity,expected_output",
        [
            (10, None, 10),
            (10, 100, 10),
            (10, 99, 10),
            (10, 50, 5),
            (10, 1, 1),
            (10, 0, 0),
            (10, 25, 3),
            (1, None, 1),
            (1, 100, 1),
            (1, 1, 1),
            (1, 0, 0),
            (1, 23, 1),
            (3, 20, 1),
            (3, 40, 1),
            (3, 70, 2),
            (3, 90, 3),
            (0, None, 0),
            (0, 1, 0),
            (0, 99, 0),
            (0, 100, 0),
        ],
    )
    def test_get_capacity_adjusted_num_replicas(
        self, num_replicas: int, target_capacity: Optional[float], expected_output: int
    ):
        result = get_capacity_adjusted_num_replicas(num_replicas, target_capacity)
        assert isinstance(result, int)
        assert result == expected_output

    def test_initial_deploy(self, mock_deployment_state_manager):
        """Deploy with target_capacity set, should apply immediately."""

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        b_info_1, _ = deployment_info(num_replicas=2)
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=50,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING

        for replica in ds._replicas.get():
            replica._actor.set_ready()
        dsm.update()

        check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    def test_target_capacity_100_no_effect(self, mock_deployment_state_manager):
        """
        Deploy with no target_capacity set, then set to 100. Should take no effect.

        Then go back to no target_capacity, should still have no effect.
        """

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        code_version = "arbitrary_version"
        b_info_1, _ = deployment_info(num_replicas=2, version=code_version)
        # Initially deploy with no target_capacity set.
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, None)])
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Now update target_capacity to 100, should have no effect.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=100,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Now update target_capacity back to None, should have no effect.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=None,
            target_capacity_direction=None,
        )

        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    def test_target_capacity_0(self, mock_deployment_state_manager):
        """Deploy with target_capacity set to 0. Should have no replicas."""

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        b_info_1, _ = deployment_info(num_replicas=100)
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=0,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    def test_reduce_target_capacity(self, mock_deployment_state_manager):
        """
        Deploy with target capacity set to 100, then reduce to 50, then reduce to 0.
        """

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        code_version = "arbitrary_version"
        b_info_1, _ = deployment_info(num_replicas=10, version=code_version)
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Start with target_capacity 100.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=100,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(ds, total=10, by_state=[(ReplicaState.STARTING, 10, None)])
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=10, by_state=[(ReplicaState.RUNNING, 10, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Reduce target_capacity to 50, half the replicas should be stopped.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=50,
            target_capacity_direction=TargetCapacityDirection.DOWN,
        )

        dsm.update()
        check_counts(
            ds,
            total=10,
            by_state=[
                (ReplicaState.RUNNING, 5, None),
                (ReplicaState.STOPPING, 5, None),
            ],
        )

        assert ds.curr_status_info.status == DeploymentStatus.DOWNSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get([ReplicaState.STOPPING]):
            replica._actor.set_done_stopping()

        dsm.update()
        check_counts(ds, total=5, by_state=[(ReplicaState.RUNNING, 5, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Reduce target_capacity to 1, all but 1 of the replicas should be stopped.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=1,
            target_capacity_direction=TargetCapacityDirection.DOWN,
        )

        dsm.update()
        check_counts(
            ds,
            total=5,
            by_state=[
                (ReplicaState.RUNNING, 1, None),
                (ReplicaState.STOPPING, 4, None),
            ],
        )

        assert ds.curr_status_info.status == DeploymentStatus.DOWNSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get([ReplicaState.STOPPING]):
            replica._actor.set_done_stopping()

        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])

        # Reduce target_capacity to 0, all replicas should be stopped.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=0,
            target_capacity_direction=TargetCapacityDirection.DOWN,
        )

        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.STOPPING, 1, None)])

        assert ds.curr_status_info.status == DeploymentStatus.DOWNSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get([ReplicaState.STOPPING]):
            replica._actor.set_done_stopping()

        dsm.update()
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    def test_increase_target_capacity(self, mock_deployment_state_manager):
        """
        Deploy with target_capacity set to 0, then increase to 1, then increase to 50,
        then increase to 100.
        """

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        code_version = "arbitrary_version"
        b_info_1, _ = deployment_info(num_replicas=10, version=code_version)
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Start with target_capacity set to 0, should have no replicas start up.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=0,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Increase target_capacity to 1, should have 1 replica start up.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=1,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Set target_capacity to 50, should have 4 more replicas start up.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=50,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(
            ds,
            total=5,
            by_state=[
                (ReplicaState.RUNNING, 1, None),
                (ReplicaState.STARTING, 4, None),
            ],
        )
        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=5, by_state=[(ReplicaState.RUNNING, 5, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Set target_capacity to 100, should have 5 more replicas start up.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=100,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(
            ds,
            total=10,
            by_state=[
                (ReplicaState.RUNNING, 5, None),
                (ReplicaState.STARTING, 5, None),
            ],
        )
        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=10, by_state=[(ReplicaState.RUNNING, 10, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    def test_clear_target_capacity(self, mock_deployment_state_manager):
        """Deploy with target_capacity set, should apply immediately."""

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        code_version = "arbitrary_version"
        b_info_1, _ = deployment_info(num_replicas=10, version=code_version)
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Start with target_capacity set to 50, should have 5 replicas start up.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=50,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(ds, total=5, by_state=[(ReplicaState.STARTING, 5, None)])
        assert ds.curr_status_info.status == DeploymentStatus.UPDATING

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=5, by_state=[(ReplicaState.RUNNING, 5, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Clear target_capacity, should have 5 more replicas start up.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=None,
            target_capacity_direction=None,
        )

        dsm.update()
        check_counts(
            ds,
            total=10,
            by_state=[
                (ReplicaState.RUNNING, 5, None),
                (ReplicaState.STARTING, 5, None),
            ],
        )
        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=10, by_state=[(ReplicaState.RUNNING, 10, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    def test_target_num_replicas_is_zero(self, mock_deployment_state_manager):
        """
        If the target `num_replicas` is zero (i.e., scale-to-zero is enabled and it's
        autoscaled down), then replicas should remain at zero regardless of
        target_capacity.
        """

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        # Set num_replicas to 0.
        code_version = "arbitrary_version"
        b_info_1, _ = deployment_info(num_replicas=0, version=code_version)
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Start with target_capacity of 50.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=50,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        dsm.update()
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Regardless of target_capacity, should stay at 0 replicas.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=None,
            target_capacity_direction=None,
        )

        dsm.update()
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=0,
            target_capacity_direction=TargetCapacityDirection.UP,
        )
        dsm.update()
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=50,
            target_capacity_direction=TargetCapacityDirection.UP,
        )
        dsm.update()
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=100,
            target_capacity_direction=TargetCapacityDirection.UP,
        )
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Now scale back up to 1 replica.
        b_info_2, _ = deployment_info(num_replicas=1, version=code_version)
        self.update_target_capacity(
            ds,
            b_info_2,
            target_capacity=100,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        ds._target_state.num_replicas = 1
        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    # TODO(edoakes): this test should be updated to go through the autoscaling policy.
    def test_target_capacity_with_changing_num_replicas(
        self, mock_deployment_state_manager
    ):
        """
        Test that target_capacity works with changing num_replicas (emulating
        autoscaling).
        """

        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        # Set num_replicas to 0.
        code_version = "arbitrary_version"
        b_info_1, _ = deployment_info(num_replicas=2, version=code_version)
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Start with target_capacity set to 0, should have 0 replica start up
        # regardless of the autoscaling decision.
        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=0,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(ds, total=0)
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        self.update_target_capacity(
            ds,
            b_info_1,
            target_capacity=1,
            target_capacity_direction=TargetCapacityDirection.UP,
        )
        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        # TODO (shrekris): once this test uses the autoscaling logic, this
        # status trigger should be DeploymentStatusTrigger.AUTOSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Increase the target number of replicas. Should still only have 1.
        b_info_2, _ = deployment_info(num_replicas=10, version=code_version)
        self.update_target_capacity(
            ds,
            b_info_2,
            target_capacity=1,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Increase target_capacity to 50, should have 4 more replicas start up.
        self.update_target_capacity(
            ds,
            b_info_2,
            target_capacity=50,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(
            ds,
            total=5,
            by_state=[
                (ReplicaState.RUNNING, 1, None),
                (ReplicaState.STARTING, 4, None),
            ],
        )

        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        # TODO (shrekris): once this test uses the autoscaling logic, this
        # status trigger should be DeploymentStatusTrigger.AUTOSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=5, by_state=[(ReplicaState.RUNNING, 5, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Reduce num_replicas and remove target_capacity, should stay the same.
        b_info_3, _ = deployment_info(num_replicas=5, version=code_version)
        self.update_target_capacity(
            ds,
            b_info_3,
            target_capacity=None,
            target_capacity_direction=None,
        )

        dsm.update()
        check_counts(
            ds,
            total=5,
            by_state=[(ReplicaState.RUNNING, 5, None)],
        )

        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.UPSCALE_COMPLETED
        )

        dsm.update()
        check_counts(ds, total=5, by_state=[(ReplicaState.RUNNING, 5, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Set target_capacity to 50 and increase num_replicas to 6, should have 2 stop.
        b_info_4, _ = deployment_info(num_replicas=6, version=code_version)
        self.update_target_capacity(
            ds,
            b_info_4,
            target_capacity=50,
            target_capacity_direction=TargetCapacityDirection.UP,
        )

        dsm.update()
        check_counts(
            ds,
            total=5,
            by_state=[
                (ReplicaState.RUNNING, 3, None),
                (ReplicaState.STOPPING, 2, None),
            ],
        )

        assert ds.curr_status_info.status == DeploymentStatus.DOWNSCALING
        # TODO (shrekris): once this test uses the autoscaling logic, this
        # status trigger should be DeploymentStatusTrigger.AUTOSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get([ReplicaState.STOPPING]):
            replica._actor.set_done_stopping()

        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Unset target capacity, should scale back up to 6.
        self.update_target_capacity(
            ds,
            b_info_4,
            target_capacity=None,
            target_capacity_direction=None,
        )

        dsm.update()
        check_counts(
            ds,
            total=6,
            by_state=[
                (ReplicaState.RUNNING, 3, None),
                (ReplicaState.STARTING, 3, None),
            ],
        )
        assert ds.curr_status_info.status == DeploymentStatus.UPSCALING
        # TODO (shrekris): once this test uses the autoscaling logic, this
        # status trigger should be DeploymentStatusTrigger.AUTOSCALING
        assert (
            ds.curr_status_info.status_trigger
            == DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        )

        for replica in ds._replicas.get():
            replica._actor.set_ready()

        dsm.update()
        check_counts(ds, total=6, by_state=[(ReplicaState.RUNNING, 6, None)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY


class TestStopReplicasOnDrainingNodes:
    """Test the behavior when draining node(s)."""

    def test_draining_start_then_stop_replica(self, mock_deployment_state_manager):
        """A new replica should be started before stopping old replica.

        If the new replica starts quickly, the replica on the draining
        node should then be gracefully stopped after the new replica
        transitions to RUNNING.
        """

        create_dsm, timer, cluster_node_info_cache, _ = mock_deployment_state_manager
        node_1 = NodeID.from_random().hex()
        node_2 = NodeID.from_random().hex()
        cluster_node_info_cache.add_node(node_1)
        cluster_node_info_cache.add_node(node_2)
        dsm: DeploymentStateManager = create_dsm()
        timer.reset(0)

        b_info_1, v1 = deployment_info(
            num_replicas=2, graceful_shutdown_timeout_s=20, version="1"
        )
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])

        # Drain node-2 with deadline 60. Since the replicas are still
        # starting and we don't know the actor node id yet nothing happens
        cluster_node_info_cache.draining_nodes = {node_2: 60 * 1000}
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])

        one_replica, another_replica = ds._replicas.get()

        one_replica._actor.set_node_id(node_1)
        one_replica._actor.set_ready()

        another_replica._actor.set_node_id(node_2)
        another_replica._actor.set_ready()

        # Try to start a new replica before initiating the graceful stop
        # process for the replica on the draining node
        dsm.update()
        check_counts(
            ds,
            total=3,
            by_state=[
                (ReplicaState.RUNNING, 1, v1),
                (ReplicaState.PENDING_MIGRATION, 1, v1),
                (ReplicaState.STARTING, 1, v1),
            ],
        )

        # 5 seconds later, the replica hasn't started yet. The replica on
        # the draining node should not start graceful termination yet.
        timer.advance(5)
        dsm.update()
        check_counts(
            ds,
            total=3,
            by_state=[
                (ReplicaState.RUNNING, 1, v1),
                (ReplicaState.PENDING_MIGRATION, 1, v1),
                (ReplicaState.STARTING, 1, v1),
            ],
        )

        # Simulate it took 5 more seconds for the new replica to be started
        timer.advance(5)
        ds._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
        dsm.update()
        check_counts(
            ds,
            total=3,
            by_state=[
                (ReplicaState.RUNNING, 2, v1),
                (ReplicaState.STOPPING, 1, v1),
            ],
        )

        # After replica on draining node stops, deployment is healthy with 2
        # running replicas.
        another_replica._actor.set_done_stopping()
        dsm.update()
        check_counts(
            ds,
            total=2,
            by_state=[(ReplicaState.RUNNING, 2, v1)],
        )
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    def test_draining_stop_replica_before_deadline(self, mock_deployment_state_manager):
        """If the new replacement replica takes a long time to start,
        the replica on the draining node should start gracefully
        terminating ahead of time.

        The graceful termination should be initiated `graceful_shutdown_timeout_s`
        seconds before the draining node's deadline, even if the new
        replica hasn't transitioned to RUNNING yet.
        """

        create_dsm, timer, cluster_node_info_cache, _ = mock_deployment_state_manager
        node_1 = NodeID.from_random().hex()
        node_2 = NodeID.from_random().hex()
        cluster_node_info_cache.add_node(node_1)
        cluster_node_info_cache.add_node(node_2)
        dsm: DeploymentStateManager = create_dsm()
        timer.reset(0)

        b_info_1, v1 = deployment_info(
            num_replicas=2, graceful_shutdown_timeout_s=20, version="1"
        )
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])

        # Drain node-2 with deadline 60. Since the replicas are still
        # starting and we don't know the actor node id yet nothing happens
        cluster_node_info_cache.draining_nodes = {node_2: 60 * 1000}
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])

        one_replica, another_replica = ds._replicas.get()

        one_replica._actor.set_node_id(node_1)
        one_replica._actor.set_ready()

        another_replica._actor.set_node_id(node_2)
        another_replica._actor.set_ready()

        # Try to start a new replica before initiating the graceful stop
        # process for the replica on the draining node
        dsm.update()
        check_counts(
            ds,
            total=3,
            by_state=[
                (ReplicaState.RUNNING, 1, v1),
                (ReplicaState.PENDING_MIGRATION, 1, v1),
                (ReplicaState.STARTING, 1, v1),
            ],
        )

        # Simulate the replica is not yet started after 40 seconds. The
        # replica on node-2 should start graceful termination even though
        # a new replica hasn't come up yet.
        timer.advance(40)
        dsm.update()
        check_counts(
            ds,
            total=3,
            by_state=[
                (ReplicaState.RUNNING, 1, v1),
                (ReplicaState.STOPPING, 1, v1),
                (ReplicaState.STARTING, 1, v1),
            ],
        )

        # Mark replica as finished stopping.
        another_replica._actor.set_done_stopping()
        dsm.update()
        check_counts(
            ds,
            total=2,
            by_state=[(ReplicaState.STARTING, 1, v1), (ReplicaState.RUNNING, 1, v1)],
        )

        # 5 seconds later, the replica finally starts.
        timer.advance(5)
        ds._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v1)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    def test_draining_multiple_nodes(self, mock_deployment_state_manager):
        """Test multiple nodes draining at the same time.

        We should choose to stop replicas on nodes with the earliest
        deadlines when new replicas are started.
        """

        create_dsm, timer, cluster_node_info_cache, _ = mock_deployment_state_manager
        node_1 = NodeID.from_random().hex()
        node_2 = NodeID.from_random().hex()
        node_3 = NodeID.from_random().hex()
        node_4 = NodeID.from_random().hex()
        cluster_node_info_cache.add_node(node_1)
        cluster_node_info_cache.add_node(node_2)
        cluster_node_info_cache.add_node(node_3)
        cluster_node_info_cache.add_node(node_4)
        dsm: DeploymentStateManager = create_dsm()
        timer.reset(0)

        b_info_1, v1 = deployment_info(
            num_replicas=4, graceful_shutdown_timeout_s=20, version="1"
        )
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        dsm.update()
        check_counts(ds, total=4, by_state=[(ReplicaState.STARTING, 4, v1)])

        # Drain node-2 with deadline 60. Since the replicas are still
        # starting and we don't know the actor node id yet nothing happens
        cluster_node_info_cache.draining_nodes = {
            node_2: 60 * 1000,
            node_3: 100 * 1000,
            node_4: 40 * 1000,
        }
        dsm.update()
        check_counts(ds, total=4, by_state=[(ReplicaState.STARTING, 4, v1)])

        for i, replica in enumerate(ds._replicas.get()):
            replica._actor.set_node_id([node_1, node_2, node_3, node_4][i])
            replica._actor.set_ready()

        # Try to start new replicas before initiating the graceful stop
        # process for the replica on the draining node
        dsm.update()
        check_counts(
            ds,
            total=7,
            by_state=[
                (ReplicaState.RUNNING, 1, v1),
                (ReplicaState.PENDING_MIGRATION, 3, v1),
                (ReplicaState.STARTING, 3, v1),
            ],
        )

        # First new replica transitions to RUNNING.
        timer.advance(5)
        ds._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
        dsm.update()
        check_counts(
            ds,
            total=7,
            by_state=[
                (ReplicaState.RUNNING, 2, v1),
                (ReplicaState.STOPPING, 1, v1),
                (ReplicaState.PENDING_MIGRATION, 2, v1),
                (ReplicaState.STARTING, 2, v1),
            ],
        )
        # The replica on node_4 should be selected for graceful termination,
        # because node_4 has the earliest deadline.
        stopping_replica = ds._replicas.get([ReplicaState.STOPPING])[0]
        assert stopping_replica.actor_node_id == node_4
        stopping_replica._actor.set_done_stopping()
        dsm.update()

        # Second new replica transitions to RUNNING.
        timer.advance(5)
        ds._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
        dsm.update()
        check_counts(
            ds,
            total=6,
            by_state=[
                (ReplicaState.RUNNING, 3, v1),
                (ReplicaState.STOPPING, 1, v1),
                (ReplicaState.PENDING_MIGRATION, 1, v1),
                (ReplicaState.STARTING, 1, v1),
            ],
        )
        # The replica on node_2 should be selected for graceful termination,
        # because node_2 has the second earliest deadline.
        stopping_replica = ds._replicas.get([ReplicaState.STOPPING])[0]
        assert stopping_replica.actor_node_id == node_2
        stopping_replica._actor.set_done_stopping()
        dsm.update()

        # Third new replica transitions to RUNNING.
        timer.advance(5)
        ds._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
        dsm.update()
        check_counts(
            ds,
            total=5,
            by_state=[
                (ReplicaState.RUNNING, 4, v1),
                (ReplicaState.STOPPING, 1, v1),
            ],
        )

        # The replica on node_3 should be selected for graceful termination
        # last because node_3 has the latest deadline.
        stopping_replica = ds._replicas.get([ReplicaState.STOPPING])[0]
        assert stopping_replica.actor_node_id == node_3
        stopping_replica._actor.set_done_stopping()
        dsm.update()

        # Finally all 4 replicas are running.
        check_counts(ds, total=4, by_state=[(ReplicaState.RUNNING, 4, v1)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    def test_replicas_unhealthy_on_draining_node(self, mock_deployment_state_manager):
        """Replicas pending migration should be stopped if unhealthy."""

        create_dsm, timer, cluster_node_info_cache, _ = mock_deployment_state_manager
        node_1 = NodeID.from_random().hex()
        node_2 = NodeID.from_random().hex()
        cluster_node_info_cache.add_node(node_1)
        cluster_node_info_cache.add_node(node_2)
        dsm: DeploymentStateManager = create_dsm()
        timer.reset(0)

        b_info_1, v1 = deployment_info(
            num_replicas=2, graceful_shutdown_timeout_s=20, version="1"
        )
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])

        # Drain node-2 with deadline 60.
        cluster_node_info_cache.draining_nodes = {node_2: 60 * 1000}
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])

        one_replica, another_replica = ds._replicas.get()

        one_replica._actor.set_node_id(node_1)
        another_replica._actor.set_node_id(node_2)
        one_replica._actor.set_ready()
        another_replica._actor.set_ready()

        # Try to start a new replica before initiating the graceful stop
        # process for the replica on the draining node
        dsm.update()
        check_counts(
            ds,
            total=3,
            by_state=[
                (ReplicaState.RUNNING, 1, v1),
                (ReplicaState.PENDING_MIGRATION, 1, v1),
                (ReplicaState.STARTING, 1, v1),
            ],
        )

        # 5 seconds later, the new replica hasn't started but the
        # replica on the draining node has become unhealthy. It should
        # be stopped.
        timer.advance(5)
        ds._replicas.get([ReplicaState.PENDING_MIGRATION])[0]._actor.set_unhealthy()
        dsm.update()
        check_counts(
            ds,
            total=3,
            by_state=[
                (ReplicaState.RUNNING, 1, v1),
                (ReplicaState.STOPPING, 1, v1),
                (ReplicaState.STARTING, 1, v1),
            ],
        )

        # Unhealthy replica is stopped.
        ds._replicas.get([ReplicaState.STOPPING])[0]._actor.set_done_stopping()
        check_counts(
            ds,
            total=3,
            by_state=[(ReplicaState.RUNNING, 1, v1), (ReplicaState.STARTING, 1, v1)],
        )

        # New replica starts.
        ds._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v1)])

    def test_starting_replica_on_draining_node(self, mock_deployment_state_manager):
        """When a node gets drained, replicas in STARTING state should be stopped."""

        create_dsm, timer, cluster_node_info_cache, _ = mock_deployment_state_manager
        node_1 = NodeID.from_random().hex()
        node_2 = NodeID.from_random().hex()
        cluster_node_info_cache.add_node(node_1)
        cluster_node_info_cache.add_node(node_2)
        dsm: DeploymentStateManager = create_dsm()
        timer.reset(0)

        b_info_1, v1 = deployment_info(
            num_replicas=2, graceful_shutdown_timeout_s=20, version="1"
        )
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])

        # Mark replica on node_1 as ready, but replica on node_2 is
        # still starting
        one_replica, another_replica = ds._replicas.get()
        one_replica._actor.set_node_id(node_1)
        another_replica._actor.set_node_id(node_2)
        one_replica._actor.set_ready()
        dsm.update()
        check_counts(
            ds,
            total=2,
            by_state=[(ReplicaState.RUNNING, 1, v1), (ReplicaState.STARTING, 1, v1)],
        )

        # Drain node-2. The starting replica should be stopped immediately
        # without waiting for the replica to start.
        cluster_node_info_cache.draining_nodes = {node_2: 60 * 1000}
        dsm.update()
        check_counts(
            ds,
            total=3,
            by_state=[
                (ReplicaState.RUNNING, 1, v1),
                (ReplicaState.STOPPING, 1, v1),
                (ReplicaState.STARTING, 1, v1),
            ],
        )
        stopping_replica = ds._replicas.get([ReplicaState.STOPPING])[0]
        assert stopping_replica.actor_node_id == node_2

        # Finish stopping old replica
        stopping_replica._actor.set_done_stopping()
        dsm.update()
        starting_replica = ds._replicas.get([ReplicaState.STARTING])[0]
        assert starting_replica.actor_node_id != node_2

        # Finish starting new replica
        starting_replica._actor.set_ready()
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v1)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

    def test_in_place_update_during_draining(self, mock_deployment_state_manager):
        """Test that pending migration replicas of old versions are updated."""

        create_dsm, timer, cluster_node_info_cache, _ = mock_deployment_state_manager
        node_1 = NodeID.from_random().hex()
        node_2 = NodeID.from_random().hex()
        cluster_node_info_cache.add_node(node_1)
        cluster_node_info_cache.add_node(node_2)
        dsm: DeploymentStateManager = create_dsm()
        timer.reset(0)

        b_info_1, v1 = deployment_info(
            num_replicas=10, graceful_shutdown_timeout_s=20, version="1"
        )
        assert dsm.deploy(TEST_DEPLOYMENT_ID, b_info_1)
        ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        dsm.update()
        check_counts(ds, total=10, by_state=[(ReplicaState.STARTING, 10, v1)])

        replicas = ds._replicas.get()
        replicas[0]._actor.set_node_id(node_2)
        replicas[0]._actor.set_ready()
        for r in replicas[1:]:
            r._actor.set_node_id(node_1)
            r._actor.set_ready()
        dsm.update()
        check_counts(ds, total=10, by_state=[(ReplicaState.RUNNING, 10, v1)])

        # Drain node-2 with deadline 60.
        cluster_node_info_cache.draining_nodes = {node_2: 60 * 1000}
        dsm.update()
        check_counts(
            ds,
            total=11,
            by_state=[
                (ReplicaState.RUNNING, 9, v1),
                (ReplicaState.PENDING_MIGRATION, 1, v1),
                (ReplicaState.STARTING, 1, v1),
            ],
        )

        # Deploy a new version. The STARTING and PENDING_MIGRATION
        # replicas of the old version should be stopped.
        migrating_replica = ds._replicas.get([ReplicaState.PENDING_MIGRATION])[0]
        b_info_2, v2 = deployment_info(
            num_replicas=10, graceful_shutdown_timeout_s=20, version="2"
        )
        dsm.deploy(TEST_DEPLOYMENT_ID, b_info_2)
        dsm.update()
        check_counts(
            ds,
            total=12,
            by_state=[
                (ReplicaState.RUNNING, 9, v1),
                (ReplicaState.STOPPING, 2, v1),
                (ReplicaState.STARTING, 1, v2),
            ],
        )
        assert migrating_replica.actor_details.state == ReplicaState.STOPPING

        # Rolling update should continue
        ds._replicas.get([ReplicaState.STOPPING])[0]._actor.set_done_stopping()
        ds._replicas.get([ReplicaState.STOPPING])[1]._actor.set_done_stopping()
        dsm.update()

        ds._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
        dsm.update()
        check_counts(
            ds,
            total=12,
            by_state=[
                # Old and new running replicas
                (ReplicaState.RUNNING, 7, v1),
                (ReplicaState.RUNNING, 1, v2),
                # Being rolling updated
                (ReplicaState.STOPPING, 2, v1),
                (ReplicaState.STARTING, 2, v2),
            ],
        )


def test_docs_path_not_updated_for_different_version(mock_deployment_state_manager):
    # Create deployment state manager
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    info_1, v1 = deployment_info(version="1")
    dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
    ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])

    test_docs_path = "/test/docs/path"

    # Set replicas ready and check statuses
    for replica in ds._replicas.get([ReplicaState.STARTING]):
        replica._actor.set_ready()
        replica._actor.set_docs_path(test_docs_path)

    assert ds.docs_path is None

    # status=HEALTHY, status_trigger=DEPLOY
    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v1)])
    assert ds.docs_path == test_docs_path

    # Deploy a new version
    info_2, v2 = deployment_info(version="2")
    dsm.deploy(TEST_DEPLOYMENT_ID, info_2)

    dsm.update()
    check_counts(
        ds,
        total=2,
        by_state=[(ReplicaState.STOPPING, 1, v1), (ReplicaState.STARTING, 1, v2)],
    )
    assert ds.docs_path == test_docs_path

    test_docs_path_new = "/test/docs/path/2"
    # Set done stopping replicas ready and check statuses
    for replica in ds._replicas.get([ReplicaState.STOPPING]):
        replica._actor.set_done_stopping()

    # status=HEALTHY, status_trigger=DEPLOY
    dsm.update()

    for replica in ds._replicas.get([ReplicaState.STARTING]):
        replica._actor.set_ready()
        replica._actor.set_docs_path(test_docs_path_new)
    assert ds.docs_path == test_docs_path

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v2)])
    assert ds.docs_path == test_docs_path_new

    # Deploy a new version with None docs path
    info_3, v3 = deployment_info(version="3")
    dsm.deploy(TEST_DEPLOYMENT_ID, info_3)

    dsm.update()
    check_counts(
        ds,
        total=2,
        by_state=[(ReplicaState.STOPPING, 1, v2), (ReplicaState.STARTING, 1, v3)],
    )
    assert ds.docs_path == test_docs_path_new

    for replica in ds._replicas.get([ReplicaState.STOPPING]):
        replica._actor.set_done_stopping()

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v3)])
    assert ds.docs_path == test_docs_path_new

    for replica in ds._replicas.get([ReplicaState.STARTING]):
        replica._actor.set_ready()
        replica._actor.set_docs_path(None)

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.RUNNING, 1, v3)])
    assert ds.docs_path is None


def test_set_target_num_replicas_api(mock_deployment_state_manager):
    """Test the new set_target_num_replicas API for scaling deployments."""
    # Create deployment state manager
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    # Deploy initial deployment with 1 replica
    info_1, v1 = deployment_info(version="1", num_replicas=1)
    dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
    ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=1, by_state=[(ReplicaState.STARTING, 1, v1)])
    assert ds.target_num_replicas == 1

    # Test scaling up using the new API
    dsm.set_target_num_replicas(TEST_DEPLOYMENT_ID, 3)
    assert ds.target_num_replicas == 3

    dsm.update()
    check_counts(ds, total=3, by_state=[(ReplicaState.STARTING, 3, v1)])


def test_set_target_num_replicas_nonexistent_deployment(mock_deployment_state_manager):
    """Test that scaling nonexistent deployment raises KeyError."""
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    nonexistent_id = DeploymentID("nonexistent", "test_app")

    with pytest.raises(ValueError, match="Deployment.*not found"):
        dsm.set_target_num_replicas(nonexistent_id, 3)


def test_set_target_num_replicas_during_upgrade(mock_deployment_state_manager):
    """Test setting target replicas while an upgrade is ongoing."""

    # Create deployment state manager
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    # Deploy initial deployment (v1) with 2 replicas
    info_1, v1 = deployment_info(version="1", num_replicas=2)
    dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
    ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])
    assert ds.target_num_replicas == 2

    # Get replicas to RUNNING state
    for replica in ds._replicas.get([ReplicaState.STARTING]):
        replica._actor.set_ready()

    dsm.update()
    check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v1)])

    # Start an upgrade to v2 with 2 replicas
    info_2, v2 = deployment_info(version="2", num_replicas=2)
    dsm.deploy(TEST_DEPLOYMENT_ID, info_2)
    dsm.update()

    check_counts(
        ds,
        total=3,
        by_state=[
            (ReplicaState.STARTING, 1, v2),
            (ReplicaState.RUNNING, 1, v1),
            (ReplicaState.STOPPING, 1, v1),
        ],
    )
    assert ds.target_num_replicas == 2

    # Scale up to 5 replicas in the middle of the upgrade.
    dsm.set_target_num_replicas(TEST_DEPLOYMENT_ID, 5)
    assert ds.target_num_replicas == 5

    def dsm_update():
        for replica in ds._replicas.get([ReplicaState.STOPPING]):
            replica._actor.set_done_stopping()
        for replica in ds._replicas.get([ReplicaState.STARTING]):
            replica._actor.set_ready()
        dsm.update()

    dsm_update()
    check_counts(
        ds,
        total=5,
        by_state=[
            (ReplicaState.STARTING, 3, v2),
            (ReplicaState.RUNNING, 1, v1),
            (ReplicaState.RUNNING, 1, v2),
        ],
    )

    dsm_update()
    check_counts(
        ds,
        total=6,
        by_state=[
            (ReplicaState.STARTING, 1, v2),
            (ReplicaState.RUNNING, 4, v2),
            (ReplicaState.STOPPING, 1, v1),
        ],
    )

    dsm_update()
    check_counts(ds, total=5, by_state=[(ReplicaState.RUNNING, 5, v2)])

    assert ds.target_num_replicas == 5


def test_set_target_num_replicas_deleting_deployment(mock_deployment_state_manager):
    """Test scaling deployment that is being deleted."""
    create_dsm, _, _, _ = mock_deployment_state_manager
    dsm: DeploymentStateManager = create_dsm()

    # Deploy an initial deployment
    info, v1 = deployment_info(num_replicas=2, version="v1")
    dsm.deploy(TEST_DEPLOYMENT_ID, info)

    ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]
    dsm.update()

    check_counts(ds, total=2, by_state=[(ReplicaState.STARTING, 2, v1)])

    # Delete the deployment
    dsm.delete_deployment(TEST_DEPLOYMENT_ID)

    # The deployment status should be DELETING
    statuses = dsm.get_deployment_statuses([TEST_DEPLOYMENT_ID])
    assert statuses[0].status_trigger == DeploymentStatusTrigger.DELETING

    # Scaling should fail
    with pytest.raises(DeploymentIsBeingDeletedError):
        dsm.set_target_num_replicas(TEST_DEPLOYMENT_ID, 3)


class TestDeploymentRankManagerIntegrationE2E:
    """End-to-end integration tests for rank functionality through deployment state manager."""

    def _set_replicas_ready(
        self, ds: DeploymentState, replica_states: List[ReplicaState]
    ):
        """Helper to set replicas in given states to ready."""
        for replica in ds._replicas.get(replica_states):
            replica._actor.set_ready()

    def _set_replicas_done_stopping(self, ds: DeploymentState):
        """Helper to set stopping replicas as done stopping."""
        for replica in ds._replicas.get([ReplicaState.STOPPING]):
            replica._actor.set_done_stopping()

    def test_scaling_up_and_down_scenario(self, mock_deployment_state_manager):
        """Test a realistic scaling scenario through deployment state manager."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        # Start with 3 replicas
        info_1, v1 = deployment_info(num_replicas=3, version="1")
        dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Create initial replicas
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.STARTING, 3, v1)])

        # Set replicas ready
        self._set_replicas_ready(ds, [ReplicaState.STARTING])
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, v1)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Check initial ranks are 0, 1, 2
        ranks_mapping = ds._get_replica_ranks_mapping()
        ranks = sorted(ranks_mapping.values())
        assert ranks == [0, 1, 2], f"Expected ranks [0, 1, 2], got {ranks}"

        # Scale down to 2 replicas - this should trigger rank reassignment
        info_2, _ = deployment_info(num_replicas=2, version="1")
        dsm.deploy(TEST_DEPLOYMENT_ID, info_2)
        dsm.update()

        # One replica should be stopping
        check_counts(
            ds,
            total=3,
            by_state=[(ReplicaState.RUNNING, 2, v1), (ReplicaState.STOPPING, 1, v1)],
        )

        # Complete the scale down
        self._set_replicas_done_stopping(ds)
        dsm.update()
        check_counts(ds, total=2, by_state=[(ReplicaState.RUNNING, 2, v1)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Trigger rank consistency check with one more update
        dsm.update()

        # After scaling down and reaching healthy status, ranks should be contiguous [0, 1]
        ranks_mapping = ds._get_replica_ranks_mapping()
        ranks = sorted(ranks_mapping.values())
        assert ranks == [0, 1], f"Expected ranks [0, 1] after scale down, got {ranks}"

        # Scale back up to 3 replicas - new replica should reuse available rank
        info_3, _ = deployment_info(num_replicas=3, version="1")
        dsm.deploy(TEST_DEPLOYMENT_ID, info_3)
        dsm.update()

        # Should have one new starting replica
        check_counts(
            ds,
            total=3,
            by_state=[(ReplicaState.RUNNING, 2, v1), (ReplicaState.STARTING, 1, v1)],
        )

        # Set new replica ready
        self._set_replicas_ready(ds, [ReplicaState.STARTING])
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, v1)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Trigger rank consistency check with one more update
        dsm.update()

        # Final ranks should be contiguous [0, 1, 2]
        ranks_mapping = ds._get_replica_ranks_mapping()
        ranks = sorted(ranks_mapping.values())
        assert ranks == [0, 1, 2], f"Expected final ranks [0, 1, 2], got {ranks}"

    def test_controller_recovery_with_scattered_ranks(
        self, mock_deployment_state_manager
    ):
        """Test controller recovery with existing replica ranks through deployment state manager."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        # Deploy with 3 replicas
        info_1, v1 = deployment_info(num_replicas=3, version="1")
        target_state_changed = dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
        assert target_state_changed
        dsm.save_checkpoint()
        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Create replicas and get them running
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.STARTING, 3, v1)])
        self._set_replicas_ready(ds, [ReplicaState.STARTING])
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, v1)])

        # Get the actual replica objects (not just IDs)
        replicas = ds._replicas.get([ReplicaState.RUNNING])
        replica_ids = [replica.replica_id for replica in replicas]

        # Simulate controller crashed! Create a new deployment state manager
        # with the existing replica IDs to trigger recovery
        new_dsm: DeploymentStateManager = create_dsm(
            [replica_id.to_full_id_str() for replica_id in replica_ids]
        )

        # New deployment state should be created and replicas should be RECOVERING
        new_ds = new_dsm._deployment_states[TEST_DEPLOYMENT_ID]
        check_counts(new_ds, total=3, by_state=[(ReplicaState.RECOVERING, 3, v1)])

        # Complete recovery - set replicas ready
        self._set_replicas_ready(new_ds, [ReplicaState.RECOVERING])
        new_dsm.update()
        check_counts(new_ds, total=3, by_state=[(ReplicaState.RUNNING, 3, v1)])
        assert new_ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # At this point ranks should be scattered but all values [0, 1, 2] should be present
        ranks_mapping = new_ds._get_replica_ranks_mapping()
        ranks = sorted(ranks_mapping.values())
        assert ranks == [0, 1, 2], "Should have recovered scattered ranks"

        # Trigger rank consistency check with one more update - this should reorder if needed
        new_dsm.update()

        # After rank consistency check, ranks should still be [0, 1, 2]
        final_ranks_mapping = new_ds._get_replica_ranks_mapping()
        final_ranks = sorted(final_ranks_mapping.values())
        assert final_ranks == [
            0,
            1,
            2,
        ], f"Expected contiguous ranks [0, 1, 2] after consistency check, got {final_ranks}"

        # Clean up
        replica_rank_context.clear()

    def test_complex_reassignment_scenario(self, mock_deployment_state_manager):
        """Test complex reassignment with many gaps through deployment state manager."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        # Deploy with 4 replicas
        info_1, v1 = deployment_info(num_replicas=4, version="1")
        target_state_changed = dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
        assert target_state_changed
        dsm.save_checkpoint()
        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Create replicas and get them running
        dsm.update()
        check_counts(ds, total=4, by_state=[(ReplicaState.STARTING, 4, v1)])
        self._set_replicas_ready(ds, [ReplicaState.STARTING])
        dsm.update()
        check_counts(ds, total=4, by_state=[(ReplicaState.RUNNING, 4, v1)])

        # Get the actual replica objects
        replicas = ds._replicas.get([ReplicaState.RUNNING])
        replica_ids = [replica.replica_id for replica in replicas]

        # Simulate very scattered ranks in global context: 0, 3, 7, 10
        global replica_rank_context
        replica_rank_context.clear()
        replica_rank_context[replica_ids[0].unique_id] = 0
        replica_rank_context[replica_ids[1].unique_id] = 3
        replica_rank_context[replica_ids[2].unique_id] = 7
        replica_rank_context[replica_ids[3].unique_id] = 10

        # Simulate controller crashed! Create a new deployment state manager
        # with the existing replica IDs to trigger recovery
        new_dsm: DeploymentStateManager = create_dsm(
            [replica_id.to_full_id_str() for replica_id in replica_ids]
        )

        # New deployment state should be created and replicas should be RECOVERING
        new_ds = new_dsm._deployment_states[TEST_DEPLOYMENT_ID]
        check_counts(new_ds, total=4, by_state=[(ReplicaState.RECOVERING, 4, v1)])

        # Complete recovery - set replicas ready
        self._set_replicas_ready(new_ds, [ReplicaState.RECOVERING])
        new_dsm.update()
        check_counts(new_ds, total=4, by_state=[(ReplicaState.RUNNING, 4, v1)])
        assert new_ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Trigger rank consistency check with one more update
        new_dsm.update()

        # After reassignment, ranks should be contiguous [0, 1, 2, 3]
        ranks_mapping = new_ds._get_replica_ranks_mapping()
        ranks = sorted(ranks_mapping.values())
        assert ranks == [
            0,
            1,
            2,
            3,
        ], f"Expected reassigned ranks [0, 1, 2, 3], got {ranks}"

    def test_rank_consistency_during_version_rollout(
        self, mock_deployment_state_manager
    ):
        """Test that rank consistency is maintained during version rollouts."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        # Start with 3 replicas of version 1
        info_1, v1 = deployment_info(num_replicas=3, version="1")
        dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Create and ready initial replicas
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.STARTING, 3, v1)])
        self._set_replicas_ready(ds, [ReplicaState.STARTING])
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.RUNNING, 3, v1)])
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Verify initial ranks are contiguous
        ranks_mapping = ds._get_replica_ranks_mapping()
        initial_ranks = sorted(ranks_mapping.values())
        assert initial_ranks == [0, 1, 2]

        # Deploy version 2 - this should trigger rolling update
        info_2, v2 = deployment_info(num_replicas=3, version="2")
        dsm.deploy(TEST_DEPLOYMENT_ID, info_2)
        dsm.update()

        # Complete the rolling update step by step
        while True:
            # Set any new starting replicas ready
            starting_replicas = ds._replicas.get([ReplicaState.STARTING])
            if starting_replicas:
                self._set_replicas_ready(ds, [ReplicaState.STARTING])

            # Complete any stopping replicas
            stopping_replicas = ds._replicas.get([ReplicaState.STOPPING])
            if stopping_replicas:
                self._set_replicas_done_stopping(ds)

            dsm.update()

            # Check if rolling update is complete
            running_replicas = ds._replicas.get([ReplicaState.RUNNING])
            if len(running_replicas) == 3 and all(
                r.version == v2 for r in running_replicas
            ):
                break

        # After rolling update is complete, deployment should be healthy
        assert ds.curr_status_info.status == DeploymentStatus.HEALTHY

        # Trigger rank consistency check with one more update
        dsm.update()

        # After rolling update, verify ranks are still contiguous
        final_ranks_mapping = ds._get_replica_ranks_mapping()
        final_ranks = sorted(final_ranks_mapping.values())
        assert final_ranks == [
            0,
            1,
            2,
        ], f"Expected contiguous ranks [0, 1, 2] after rollout, got {final_ranks}"

    def test_rank_assignment_with_replica_failures(self, mock_deployment_state_manager):
        """Test rank handling when replicas fail during startup."""
        create_dsm, _, _, _ = mock_deployment_state_manager
        dsm: DeploymentStateManager = create_dsm()

        # Deploy with 3 replicas
        info_1, v1 = deployment_info(num_replicas=3, version="1")
        dsm.deploy(TEST_DEPLOYMENT_ID, info_1)
        ds: DeploymentState = dsm._deployment_states[TEST_DEPLOYMENT_ID]

        # Create initial replicas
        dsm.update()
        check_counts(ds, total=3, by_state=[(ReplicaState.STARTING, 3, v1)])

        # Make first two replicas ready, but let the third fail
        starting_replicas = ds._replicas.get([ReplicaState.STARTING])
        starting_replicas[0]._actor.set_ready()
        starting_replicas[1]._actor.set_ready()
        starting_replicas[2]._actor.set_failed_to_start()

        dsm.update()

        running_count = ds._replicas.count(states=[ReplicaState.RUNNING])
        stopping_count = ds._replicas.count(states=[ReplicaState.STOPPING])
        assert running_count == 2, "Should have 2 running replicas"
        assert stopping_count == 1, "Should have 1 stopping replica"

        self._set_replicas_done_stopping(ds)
        dsm.update()

        starting_count = ds._replicas.count(states=[ReplicaState.STARTING])
        assert starting_count == 1, "Should have 1 starting replica"

        self._set_replicas_ready(ds, [ReplicaState.STARTING])

        dsm.update()
        # second update to reassign ranks
        dsm.update()

        # Final verification - should have 3 running replicas (ignore failed/stopping replicas)
        running_replicas = ds._replicas.get([ReplicaState.RUNNING])
        assert (
            len(running_replicas) == 3
        ), f"Expected 3 running replicas, got {len(running_replicas)}"

        # Verify that ranks are properly assigned and unique for running replicas
        ranks_mapping = ds._get_replica_ranks_mapping()

        # Filter ranks to only include those for running replicas
        running_replica_ids = [
            replica.replica_id.unique_id for replica in running_replicas
        ]
        running_replica_ranks = [
            ranks_mapping[replica_id]
            for replica_id in running_replica_ids
            if replica_id in ranks_mapping
        ]

        # The ranks should be assigned to all running replicas
        assert set(running_replica_ranks) == {
            0,
            1,
            2,
        }, f"Expected ranks [0, 1, 2], got {ranks_mapping.values()}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
