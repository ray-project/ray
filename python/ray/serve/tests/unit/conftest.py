import asyncio
from typing import Optional, Set, Tuple
from unittest.mock import Mock, patch

import pytest

import ray
from ray._raylet import NodeID
from ray.actor import ActorHandle
from ray.serve._private.autoscaling_state import AutoscalingStateManager
from ray.serve._private.common import DeploymentID, ReplicaID, RunningReplicaInfo
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.request_router import PendingRequest, RunningReplica
from ray.serve._private.test_utils import (
    MockClusterNodeInfoCache,
    MockDeploymentActorWrapper,
    MockKVStore,
    MockReplicaActorWrapper,
    MockTimer,
    dead_replicas_context,
    replica_rank_context,
)

# Default value used by FakeRunningReplica when the test doesn't override.
FAKE_REPLICA_DEFAULT_MAX_ONGOING_REQUESTS = 10
# Every FakeRunningReplica is stamped with this deployment id. Router
# tests never care about app/name specifically; cross-test consistency
# matters more than differentiation.
FAKE_REPLICA_DEPLOYMENT_ID = DeploymentID(name="TEST_DEPLOYMENT")


class FakeRunningReplica(RunningReplica):
    """In-memory ``RunningReplica`` stand-in for request-router unit tests.

    Shared between ``test_pow_2_request_router.py`` and
    ``test_consistent_hash_router.py``. Supports enough knobs for pow-2's
    locality / cancellation / re-probe tests; consistent-hash only uses
    the queue-length response hooks.
    """

    def __init__(
        self,
        replica_unique_id: str,
        *,
        node_id: str = "",
        availability_zone: Optional[str] = None,
        reset_after_response: bool = False,
        model_ids: Optional[Set[str]] = None,
        sleep_time_s: float = 0.0,
        max_ongoing_requests: int = FAKE_REPLICA_DEFAULT_MAX_ONGOING_REQUESTS,
    ):
        self._replica_id = ReplicaID(
            unique_id=replica_unique_id,
            deployment_id=FAKE_REPLICA_DEPLOYMENT_ID,
        )
        self._node_id = node_id
        self._availability_zone = availability_zone
        self._queue_len = 0
        self._max_ongoing_requests = max_ongoing_requests
        self._has_queue_len_response = asyncio.Event()
        self._reset_after_response = reset_after_response
        self._model_ids = model_ids or set()
        self._sleep_time_s = sleep_time_s
        self._exception: Optional[Exception] = None

        self.get_queue_len_was_cancelled = False
        self.queue_len_deadline_history = list()
        self.num_get_queue_len_calls = 0

    @property
    def replica_id(self) -> ReplicaID:
        return self._replica_id

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def availability_zone(self) -> Optional[str]:
        return self._availability_zone

    @property
    def multiplexed_model_ids(self) -> Set[str]:
        return self._model_ids

    def update_replica_info(self, replica_info: RunningReplicaInfo) -> None:
        self._model_ids = set(replica_info.multiplexed_model_ids)

    @property
    def max_ongoing_requests(self) -> int:
        return self._max_ongoing_requests

    def set_queue_len_response(
        self,
        queue_len: int,
        exception: Optional[Exception] = None,
    ):
        self._queue_len = queue_len
        self._exception = exception
        self._has_queue_len_response.set()

    def push_proxy_handle(self, handle: ActorHandle):
        pass

    async def get_queue_len(self, *, deadline_s: float) -> int:
        self.num_get_queue_len_calls += 1
        self.queue_len_deadline_history.append(deadline_s)
        try:
            while not self._has_queue_len_response.is_set():
                await self._has_queue_len_response.wait()

            if self._sleep_time_s > 0:
                await asyncio.sleep(self._sleep_time_s)

            if self._reset_after_response:
                self._has_queue_len_response.clear()

            if self._exception is not None:
                raise self._exception

            return self._queue_len
        except asyncio.CancelledError:
            self.get_queue_len_was_cancelled = True
            raise

    def try_send_request(
        self, pr: PendingRequest, with_rejection: bool
    ) -> ReplicaResult:
        raise NotImplementedError()

    def send_request_with_rejection(self, pr: PendingRequest) -> ReplicaResult:
        raise NotImplementedError()


@pytest.fixture(autouse=True)
def disallow_ray_init(monkeypatch):
    def raise_on_init():
        raise RuntimeError("Unit tests should not depend on Ray being initialized.")

    monkeypatch.setattr(ray, "init", raise_on_init)


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
    ), patch(
        "ray.serve._private.deployment_state.DeploymentActorWrapper",
        new=MockDeploymentActorWrapper,
    ), patch(
        "time.time", new=timer.time
    ), patch(
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
