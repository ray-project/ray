import asyncio
import importlib
import os
from typing import Optional, Set
from unittest.mock import patch

import pytest

import ray
from ray._common.utils import get_or_create_event_loop
from ray.actor import ActorHandle
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    ReplicaID,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.request_router import (
    PendingRequest,
    RandomRoundRobinRouter,
    RunningReplica,
)
from ray.serve._private.test_utils import MockTimer
from ray.serve._private.utils import generate_request_id

TIMER = MockTimer()

DEFAULT_MAX_ONGOING_REQUESTS = 10
ROUTER_NODE_ID = "router_node_id"
ROUTER_AZ = "router_az"


class FakeRunningReplica(RunningReplica):
    def __init__(
        self,
        replica_unique_id: str,
        *,
        node_id: str = "",
        availability_zone: Optional[str] = None,
        reset_after_response: bool = False,
        model_ids: Optional[Set[str]] = None,
        sleep_time_s: float = 0.0,
        max_ongoing_requests: int = DEFAULT_MAX_ONGOING_REQUESTS,
    ):
        self._replica_id = ReplicaID(
            unique_id=replica_unique_id,
            deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
        )
        self._node_id = node_id
        self._availability_zone = availability_zone
        self._queue_len = 0
        self._max_ongoing_requests = max_ongoing_requests
        self._has_queue_len_response = asyncio.Event()
        self._reset_after_response = reset_after_response
        self._model_ids = model_ids or set()
        self._sleep_time_s = sleep_time_s

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


def fake_pending_request(*, model_id: str = "") -> PendingRequest:
    return PendingRequest(
        args=list(),
        kwargs=dict(),
        metadata=RequestMetadata(
            request_id=generate_request_id(),
            internal_request_id=generate_request_id(),
            multiplexed_model_id=model_id,
        ),
    )


@pytest.fixture
def rr_router(request) -> RandomRoundRobinRouter:
    if not hasattr(request, "param"):
        request.param = {}

    async def construct_request_router(loop: asyncio.AbstractEventLoop):
        router = RandomRoundRobinRouter(
            deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
            handle_source=request.param.get(
                "handle_source", DeploymentHandleSource.REPLICA
            ),
            prefer_local_node_routing=request.param.get("prefer_local_node", False),
            prefer_local_az_routing=request.param.get("prefer_local_az", False),
            self_node_id=ROUTER_NODE_ID,
            self_actor_id="fake-actor-id",
            self_actor_handle=None,
            self_availability_zone=request.param.get("az", None),
            use_replica_queue_len_cache=False,
            get_curr_time_s=TIMER.time,
        )
        router.initial_backoff_s = 0.001
        router.backoff_multiplier = 1
        router.max_backoff_s = 0.001
        return router

    s = asyncio.new_event_loop().run_until_complete(
        construct_request_router(get_or_create_event_loop())
    )

    os.environ.update({"RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S": "0.01"})
    importlib.reload(ray.serve._private.constants)
    importlib.reload(ray.serve._private.request_router.request_router)

    TIMER.reset()

    yield s

    assert s.curr_num_routing_tasks == 0
    assert s.num_pending_requests == 0


@pytest.mark.asyncio
async def test_round_robin_ordering(rr_router):
    """Consecutive choose_replicas calls cycle through replicas in sorted order."""
    s = rr_router

    replicas = [FakeRunningReplica(f"r{i}") for i in range(4)]
    for r in replicas:
        r.set_queue_len_response(0)
    s.update_replicas(replicas)

    # Force counter to a known value.
    s._counter = 0

    sorted_ids = sorted([r.replica_id for r in replicas], key=lambda rid: rid.unique_id)

    for i in range(4):
        result = await s.choose_replicas(replicas, fake_pending_request())
        assert len(result) == 1  # one rank
        chosen = result[0]
        assert len(chosen) == 2
        assert chosen[0].replica_id == sorted_ids[i % 4]
        assert chosen[1].replica_id == sorted_ids[(i + 1) % 4]


@pytest.mark.asyncio
async def test_round_robin_wraps_around(rr_router):
    """Counter wraps around after cycling through all replicas."""
    s = rr_router

    replicas = [FakeRunningReplica(f"r{i}") for i in range(3)]
    for r in replicas:
        r.set_queue_len_response(0)
    s.update_replicas(replicas)

    s._counter = 0
    sorted_ids = sorted([r.replica_id for r in replicas], key=lambda rid: rid.unique_id)

    # Cycle through 3 replicas, then verify wrap-around.
    for _ in range(3):
        await s.choose_replicas(replicas, fake_pending_request())

    # Counter is now 3, so idx = 3 % 3 = 0 -> back to start.
    result = await s.choose_replicas(replicas, fake_pending_request())
    chosen = result[0]
    assert chosen[0].replica_id == sorted_ids[0]
    assert chosen[1].replica_id == sorted_ids[1]


@pytest.mark.asyncio
async def test_single_replica(rr_router):
    """With a single replica, it always returns that replica."""
    s = rr_router

    r1 = FakeRunningReplica("r1")
    r1.set_queue_len_response(0)
    s.update_replicas([r1])

    s._counter = 0

    for _ in range(3):
        result = await s.choose_replicas([r1], fake_pending_request())
        assert len(result) == 1
        assert len(result[0]) == 1
        assert result[0][0].replica_id == r1.replica_id


@pytest.mark.asyncio
async def test_random_initial_offset(rr_router):
    """initialize_state sets counter to a random value."""
    s = rr_router

    with patch(
        "ray.serve._private.request_router" ".random_round_robin_router.random.randint",
        return_value=42,
    ):
        s.initialize_state()
    assert s._counter == 42

    with patch(
        "ray.serve._private.request_router" ".random_round_robin_router.random.randint",
        return_value=9999,
    ):
        s.initialize_state()
    assert s._counter == 9999


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rr_router",
    [{"prefer_local_node": True}],
    indirect=True,
)
async def test_locality_routing_applied(rr_router):
    """Locality mixin filters candidates before round-robin selection."""
    s = rr_router

    # r1 is on the same node as the router, r2 is not.
    r1 = FakeRunningReplica("r1", node_id=ROUTER_NODE_ID)
    r2 = FakeRunningReplica("r2", node_id="other_node")
    for r in [r1, r2]:
        r.set_queue_len_response(0)
    s.update_replicas([r1, r2])

    s._counter = 0

    # With locality preference, only the local replica should be a candidate.
    result = await s.choose_replicas([r1, r2], fake_pending_request())
    assert len(result) == 1
    assert len(result[0]) == 1
    assert result[0][0].replica_id == r1.replica_id


@pytest.mark.asyncio
async def test_no_replicas_available(rr_router):
    """Returns empty list when no candidates are available."""
    s = rr_router
    s._counter = 0

    # No replicas updated, so no candidates.
    result = await s.choose_replicas([], fake_pending_request())
    assert result == []


@pytest.mark.asyncio
async def test_replicas_updated(rr_router):
    """Round-robin adapts when the replica set changes."""
    s = rr_router

    replicas_v1 = [FakeRunningReplica(f"r{i}") for i in range(3)]
    for r in replicas_v1:
        r.set_queue_len_response(0)
    s.update_replicas(replicas_v1)

    s._counter = 0
    sorted_v1 = sorted(
        [r.replica_id for r in replicas_v1], key=lambda rid: rid.unique_id
    )

    # Make one call to advance counter.
    result = await s.choose_replicas(replicas_v1, fake_pending_request())
    assert result[0][0].replica_id == sorted_v1[0]

    # Now update replicas (add a new one, remove one).
    replicas_v2 = [FakeRunningReplica(f"r{i}") for i in range(1, 5)]
    for r in replicas_v2:
        r.set_queue_len_response(0)
    s.update_replicas(replicas_v2)

    sorted_v2 = sorted(
        [r.replica_id for r in replicas_v2], key=lambda rid: rid.unique_id
    )

    # Counter is 1. With 4 replicas, idx = 1 % 4 = 1.
    result = await s.choose_replicas(replicas_v2, fake_pending_request())
    assert result[0][0].replica_id == sorted_v2[1]
    assert result[0][1].replica_id == sorted_v2[2]


@pytest.mark.asyncio
async def test_end_to_end_routing(rr_router):
    """Test full routing flow via _choose_replica_for_request."""
    s = rr_router
    loop = get_or_create_event_loop()

    r1 = FakeRunningReplica("r1")
    r2 = FakeRunningReplica("r2")
    r3 = FakeRunningReplica("r3")
    for r in [r1, r2, r3]:
        r.set_queue_len_response(0)
    s.update_replicas([r1, r2, r3])
    s.initialize_state()
    s._counter = 0

    task = loop.create_task(s._choose_replica_for_request(fake_pending_request()))
    replica = await task

    # The request should have been routed to one of the replicas.
    sorted_ids = sorted(
        [r.replica_id for r in [r1, r2, r3]], key=lambda rid: rid.unique_id
    )
    assert replica.replica_id in sorted_ids


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
