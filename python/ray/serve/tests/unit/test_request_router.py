import asyncio
import sys

import pytest

from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    RequestMetadata,
)
from ray.serve._private.request_router import PendingRequest, RequestRouter
from ray.serve._private.test_utils import FakeRunningReplica
from ray.serve._private.utils import generate_request_id


def fake_pending_request() -> PendingRequest:
    return PendingRequest(
        args=[],
        kwargs={},
        metadata=RequestMetadata(
            request_id=generate_request_id(),
            internal_request_id=generate_request_id(),
        ),
    )


@pytest.mark.asyncio
async def test_inflight_selection_completes():
    """Finishing one request must not strand another task's owned request."""
    second_started = asyncio.Event()
    release_second = asyncio.Event()
    release_idle = asyncio.Event()
    first_request = fake_pending_request()

    # Use exact request matching from RequestRouter; the power-of-two router's
    # FIFO mixin can fulfill requests whose original routing task has exited.
    class Router(RequestRouter):
        async def choose_replicas(self, candidate_replicas, pending_request=None):
            if pending_request is None:
                # Keep an idle selector alive so the second task sees an excess
                # routing task after the first request completes.
                await release_idle.wait()
            elif pending_request is first_request:
                await second_started.wait()
            else:
                second_started.set()
                await release_second.wait()
            return [candidate_replicas]

    s = Router(
        deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
        handle_source=DeploymentHandleSource.REPLICA,
        self_actor_id="fake-actor-id",
        self_actor_handle=None,
        use_replica_queue_len_cache=True,
    )
    replica = FakeRunningReplica("replica")
    replica.set_queue_len_response(0)
    s.update_replicas([replica])
    first = asyncio.create_task(s._choose_replica_for_request(first_request))
    second = asyncio.create_task(s._choose_replica_for_request(fake_pending_request()))
    try:
        assert await asyncio.wait_for(first, timeout=2) == replica
        release_second.set()
        assert await asyncio.wait_for(second, timeout=2) == replica
    finally:
        release_idle.set()
        release_second.set()
        first.cancel()
        second.cancel()
        await asyncio.gather(first, second, return_exceptions=True)
        tasks = list(s._routing_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_inflight_selections_complete_after_replica_removal():
    """Lowering the task cap must not abandon requests already being routed."""
    all_started = asyncio.Event()
    release_selections = asyncio.Event()
    started = 0

    class Router(RequestRouter):
        async def choose_replicas(self, candidate_replicas, pending_request=None):
            nonlocal started
            started += 1
            if started == 4:
                all_started.set()
            await release_selections.wait()
            # Select the surviving replica so replica removal/retry behavior does
            # not mask whether a task abandons its request when the cap drops.
            return [[r1]]

    s = Router(
        deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
        handle_source=DeploymentHandleSource.REPLICA,
        self_actor_id="fake-actor-id",
        self_actor_handle=None,
        use_replica_queue_len_cache=True,
    )
    r1, r2 = FakeRunningReplica("r1"), FakeRunningReplica("r2")
    r1.set_queue_len_response(0)
    r2.set_queue_len_response(0)
    s.update_replicas([r1, r2])
    tasks = [
        asyncio.create_task(s._choose_replica_for_request(fake_pending_request()))
        for _ in range(4)
    ]
    try:
        await asyncio.wait_for(all_started.wait(), timeout=2)
        s.update_replicas([r1])
        assert s.curr_num_routing_tasks == 4
        assert s.target_num_routing_tasks == 2
        release_selections.set()
        assert await asyncio.wait_for(asyncio.gather(*tasks), timeout=2) == [r1] * 4
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        routing_tasks = list(s._routing_tasks)
        for task in routing_tasks:
            task.cancel()
        await asyncio.gather(*routing_tasks, return_exceptions=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
