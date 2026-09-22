import asyncio
import sys
from unittest.mock import AsyncMock

import pytest

from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    RequestMetadata,
)
from ray.serve._private.request_router import PendingRequest, RequestRouter
from ray.serve._private.request_router.request_router import FIFOMixin
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
async def test_inflight_request_completes():
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
async def test_downscale_drains_tasks():
    """Finish active requests above a reduced cap before picking up queued work."""
    all_started = asyncio.Event()
    queued_started = asyncio.Event()
    requests = [fake_pending_request() for _ in range(5)]
    releases = {request.metadata.request_id: asyncio.Event() for request in requests}
    started = 0

    class Router(RequestRouter):
        async def choose_replicas(self, candidate_replicas, pending_request=None):
            nonlocal started
            started += 1
            if started == 4:
                all_started.set()
            if pending_request is requests[4]:
                queued_started.set()
            await releases[pending_request.metadata.request_id].wait()
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
        asyncio.create_task(s._choose_replica_for_request(request))
        for request in requests[:4]
    ]
    try:
        await asyncio.wait_for(all_started.wait(), timeout=2)
        s.update_replicas([r1])
        assert s.curr_num_routing_tasks == 4
        assert s.target_num_routing_tasks == 2
        tasks.append(asyncio.create_task(s._choose_replica_for_request(requests[4])))
        await asyncio.sleep(0)
        assert s.curr_num_routing_tasks == 4
        assert not queued_started.is_set()

        # Excess tasks retire as they finish, without picking up queued work.
        for i, expected_tasks in enumerate((3, 2)):
            releases[requests[i].metadata.request_id].set()
            assert await asyncio.wait_for(tasks[i], timeout=2) == r1
            assert s.curr_num_routing_tasks == expected_tasks
            assert not queued_started.is_set()

        # Once within the cap, a finishing task may pick up the queued request.
        releases[requests[2].metadata.request_id].set()
        assert await asyncio.wait_for(tasks[2], timeout=2) == r1
        await asyncio.wait_for(queued_started.wait(), timeout=2)
        assert s.curr_num_routing_tasks == 2
        for release in releases.values():
            release.set()
        assert await asyncio.wait_for(asyncio.gather(*tasks), timeout=2) == [r1] * 5
        assert s.curr_num_routing_tasks == 0
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        routing_tasks = list(s._routing_tasks)
        for task in routing_tasks:
            task.cancel()
        await asyncio.gather(*routing_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_request_fulfilled_by_another_task():
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    first_request = fake_pending_request()

    class Router(FIFOMixin, RequestRouter):
        async def choose_replicas(self, candidate_replicas, pending_request=None):
            if pending_request is first_request:
                first_started.set()
                await release_first.wait()
            else:
                await first_started.wait()
            return [candidate_replicas]

    router = Router(
        deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
        handle_source=DeploymentHandleSource.REPLICA,
        self_actor_id="fake-actor-id",
        self_actor_handle=None,
        use_replica_queue_len_cache=True,
    )
    replica = FakeRunningReplica("replica")
    replica.set_queue_len_response(0)
    router.update_replicas([replica])
    tasks = [
        asyncio.create_task(router._choose_replica_for_request(request))
        for request in (first_request, fake_pending_request())
    ]
    try:
        # The second task selects first, but fulfills request 1.
        assert await asyncio.wait_for(tasks[0], timeout=2) == replica
        # Request 2 completes without waiting for request 1's slow selector.
        assert await asyncio.wait_for(tasks[1], timeout=2) == replica
        assert router.curr_num_routing_tasks == 0
    finally:
        release_first.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        routing_tasks = list(router._routing_tasks)
        for task in routing_tasks:
            task.cancel()
        await asyncio.gather(*routing_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_request_skips_probe(monkeypatch):
    selection_started = asyncio.Event()
    release_selection = asyncio.Event()
    cancelled_request = fake_pending_request()

    class Router(RequestRouter):
        async def choose_replicas(self, candidate_replicas, pending_request=None):
            if pending_request is cancelled_request:
                selection_started.set()
                await release_selection.wait()
            return [candidate_replicas]

    router = Router(
        deployment_id=DeploymentID(name="TEST_DEPLOYMENT"),
        handle_source=DeploymentHandleSource.REPLICA,
        self_actor_id="fake-actor-id",
        self_actor_handle=None,
        use_replica_queue_len_cache=False,
    )
    router.max_num_routing_tasks_cap = 1
    replica = FakeRunningReplica("replica")
    replica.set_queue_len_response(0)
    router.update_replicas([replica])
    select = AsyncMock(wraps=router._select_from_candidate_replicas)
    monkeypatch.setattr(router, "_select_from_candidate_replicas", select)
    tasks = [
        asyncio.create_task(router._choose_replica_for_request(request))
        for request in (cancelled_request, fake_pending_request())
    ]
    try:
        await asyncio.wait_for(selection_started.wait(), timeout=2)
        tasks[0].cancel()
        await asyncio.gather(tasks[0], return_exceptions=True)
        release_selection.set()
        assert await asyncio.wait_for(tasks[1], timeout=2) == replica
        # Only the live request should probe; queued work keeps the task target at 1.
        assert select.await_count == 1
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        routing_tasks = list(router._routing_tasks)
        for task in routing_tasks:
            task.cancel()
        await asyncio.gather(*routing_tasks, return_exceptions=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
