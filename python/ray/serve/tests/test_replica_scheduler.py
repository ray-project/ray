import asyncio
import time
from typing import Tuple, Union

import pytest

import ray
from ray._private.utils import get_or_create_event_loop

from ray.serve._private.router import (
    PowerOfTwoChoicesReplicaScheduler,
    Query,
    ReplicaWrapper,
    RequestMetadata,
)


class FakeReplicaWrapper(ReplicaWrapper):
    def __init__(self, replica_id: str, *, reset_after_response: bool = False):
        self._replica_id = replica_id
        self._queue_len = 0
        self._accepted = False
        self._has_queue_len_response = asyncio.Event()
        self._reset_after_response = reset_after_response

    @property
    def replica_id(self) -> str:
        return self._replica_id

    def set_queue_state_response(self, queue_len: int, accepted: bool = True):
        self._queue_len = queue_len
        self._accepted = accepted
        self._has_queue_len_response.set()

    async def get_queue_state(self) -> Tuple[str, int, bool]:
        while not self._has_queue_len_response.is_set():
            await self._has_queue_len_response.wait()

        if self._reset_after_response:
            self._has_queue_len_response.clear()

        return self._replica_id, self._queue_len, self._accepted

    def send_query(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        raise NotImplementedError()


@pytest.fixture
def pow_2_scheduler() -> PowerOfTwoChoicesReplicaScheduler:
    s = PowerOfTwoChoicesReplicaScheduler(
        get_or_create_event_loop(),
        "TEST_DEPLOYMENT",
    )

    yield s

    # Always verify that all scheduling tasks exit once all queries are satisfied.
    assert s.curr_num_scheduling_tasks == 0
    assert s.num_pending_assignments == 0


@pytest.fixture
def fake_query() -> Query:
    meta = RequestMetadata(request_id="req_id", endpoint="endpoint")
    return Query([], {}, meta)


@pytest.mark.asyncio
async def test_no_replicas_available_then_one_available(pow_2_scheduler, fake_query):
    """
    If there are replicas available, we should wait until one is added. Once a
    replica is added via `update_replicas`, the pending assignment should be fulfilled.
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    task = loop.create_task(s.choose_replica_for_query(fake_query))
    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0

    r1 = FakeReplicaWrapper("r1")
    r1.set_queue_state_response(0)
    s.update_replicas([r1])

    assert (await task) == r1


@pytest.mark.asyncio
async def test_no_replicas_accept_then_one_accepts(pow_2_scheduler, fake_query):
    """
    If none of the replicas accept the request, we should repeatedly try with backoff.
    Once one accepts, the pending assignment should be fulfilled.
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    task = loop.create_task(s.choose_replica_for_query(fake_query))
    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0

    r1 = FakeReplicaWrapper("r1")
    r1.set_queue_state_response(0, accepted=False)
    s.update_replicas([r1])

    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0

    r1.set_queue_state_response(0, accepted=True)
    assert (await task) == r1


@pytest.mark.asyncio
async def test_one_replica_available_then_none_then_one(pow_2_scheduler, fake_query):
    """
    If a replica stops accepting requests, it should stop being scheduled. When it then
    accepts, pending assingments should be scheduled on it.
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    r1 = FakeReplicaWrapper("r1")
    r1.set_queue_state_response(0, accepted=False)
    s.update_replicas([r1])

    task = loop.create_task(s.choose_replica_for_query(fake_query))
    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0

    s.update_replicas([])
    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0

    r1.set_queue_state_response(0, accepted=True)
    s.update_replicas([r1])

    assert (await task) == r1


@pytest.mark.asyncio
async def test_two_replicas_available_then_one(pow_2_scheduler, fake_query):
    """
    If two replicas are available and accepting requests, they should both get
    scheduled. If one is removed, only the other should be scheduled.
    """
    s = pow_2_scheduler

    r1 = FakeReplicaWrapper("r1")
    r1.set_queue_state_response(0)

    r2 = FakeReplicaWrapper("r2")
    r2.set_queue_state_response(0)

    s.update_replicas([r1, r2])

    for _ in range(100):
        assert (await s.choose_replica_for_query(fake_query)) in {r1, r2}

    s.update_replicas([r1])

    for _ in range(100):
        assert (await s.choose_replica_for_query(fake_query)) == r1


@pytest.mark.asyncio
async def test_two_replicas_one_accepts(pow_2_scheduler, fake_query):
    """
    If two replicas are available but only one accepts, only it should be scheduled.
    """
    s = pow_2_scheduler

    r1 = FakeReplicaWrapper("r1")
    r1.set_queue_state_response(0)

    r2 = FakeReplicaWrapper("r2")
    r2.set_queue_state_response(0, accepted=False)

    s.update_replicas([r1, r2])

    for _ in range(100):
        assert (await s.choose_replica_for_query(fake_query)) == r1


@pytest.mark.asyncio
async def test_three_replicas_two_accept(pow_2_scheduler, fake_query):
    """
    If three replicas are available but only two accept, only those should be scheduled.
    """
    s = pow_2_scheduler

    r1 = FakeReplicaWrapper("r1")
    r1.set_queue_state_response(0)

    r2 = FakeReplicaWrapper("r2")
    r2.set_queue_state_response(0, accepted=False)

    r3 = FakeReplicaWrapper("r3")
    r3.set_queue_state_response(0)

    s.update_replicas([r1, r2, r3])

    for _ in range(100):
        assert (await s.choose_replica_for_query(fake_query)) in {r1, r3}


@pytest.mark.asyncio
async def test_two_replicas_choose_shorter_queue(pow_2_scheduler, fake_query):
    """
    If two replicas are available and accept requests, the one with the shorter
    queue should be scheduled.
    """
    s = pow_2_scheduler

    r1 = FakeReplicaWrapper("r1")
    r1.set_queue_state_response(1)

    r2 = FakeReplicaWrapper("r2")
    r2.set_queue_state_response(0)

    s.update_replicas([r1, r2])

    for _ in range(100):
        assert (await s.choose_replica_for_query(fake_query)) == r2


@pytest.mark.asyncio
async def test_tasks_scheduled_fifo(pow_2_scheduler, fake_query):
    """
    Verify that requests are always scheduled in FIFO order, even if many are being
    assigned concurrently.
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    # Schedule many requests in parallel; they cannot be fulfilled yet.
    tasks = []
    for _ in range(100):
        tasks.append(loop.create_task(s.choose_replica_for_query(fake_query)))

    done, _ = await asyncio.wait(tasks, timeout=0.1)
    assert len(done) == 0

    # Only a single request will be accepted at a time due to
    # `reset_after_response=True`.
    r1 = FakeReplicaWrapper("r1", reset_after_response=True)
    s.update_replicas([r1])

    for i in range(len(tasks)):
        r1.set_queue_state_response(0)
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        # If the order was not FIFO, the fulfilled assignment may not be the front of
        # the list.
        assert done.pop() == tasks[0]
        tasks = tasks[1:]


@pytest.mark.asyncio
async def test_cancellation(pow_2_scheduler, fake_query):
    """
    If a pending assignment is cancelled, it shouldn't get fulfilled and the next
    request in the queue should be.
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    task1 = loop.create_task(s.choose_replica_for_query(fake_query))
    task2 = loop.create_task(s.choose_replica_for_query(fake_query))

    done, _ = await asyncio.wait([task1, task2], timeout=0.1)
    assert len(done) == 0

    task1.cancel()

    r1 = FakeReplicaWrapper("r1")
    r1.set_queue_state_response(0)
    s.update_replicas([r1])

    assert (await task2) == r1

    # Verify that the scheduling tasks exit and there are no assignments left.
    assert s.curr_num_scheduling_tasks == 0
    assert s.num_pending_assignments == 0


@pytest.mark.asyncio
async def test_only_task_cancelled(pow_2_scheduler, fake_query):
    """
    If a pending assignment is cancelled and it's the only one in the queue, it should
    be passed over and the scheduling task should exit.
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    task = loop.create_task(s.choose_replica_for_query(fake_query))

    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0

    task.cancel()

    r1 = FakeReplicaWrapper("r1")
    r1.set_queue_state_response(0)
    s.update_replicas([r1])

    start = time.time()
    while time.time() - start < 10:
        # Verify that the scheduling task exits and there are no assignments left.
        if s.curr_num_scheduling_tasks == 0 and s.num_pending_assignments == 0:
            break
        await asyncio.sleep(0.1)
    else:
        raise TimeoutError(
            "Scheduling task and pending assignment still around after 10s."
        )


@pytest.mark.asyncio
async def test_scheduling_task_cap(pow_2_scheduler, fake_query):
    """
    Verify that the number of scheduling tasks never exceeds the cap (2 * num_replicas).
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    tasks = []
    for _ in range(100):
        tasks.append(loop.create_task(s.choose_replica_for_query(fake_query)))

    done, _ = await asyncio.wait(tasks, timeout=0.1)
    assert len(done) == 0

    # There should be zero scheduling tasks while there are no replicas.
    assert s.curr_num_scheduling_tasks == 0

    r1 = FakeReplicaWrapper("r1", reset_after_response=True)
    r1.set_queue_state_response(0, accepted=False)
    s.update_replicas([r1])

    done, _ = await asyncio.wait(tasks, timeout=0.1)
    assert len(done) == 0

    # Now that there is at least one replica available, there should be nonzero
    # number of tasks running.
    assert s.curr_num_scheduling_tasks > 0
    assert s.curr_num_scheduling_tasks == s.max_num_scheduling_tasks

    # Number of tasks should increase when more replicas are available.
    scheduling_tasks_one_replica = s.curr_num_scheduling_tasks
    r2 = FakeReplicaWrapper("r2")
    r2.set_queue_state_response(0, accepted=False)
    s.update_replicas([r1, r2])
    assert s.curr_num_scheduling_tasks > scheduling_tasks_one_replica
    assert s.curr_num_scheduling_tasks == s.max_num_scheduling_tasks

    # Number of tasks should decrease as the number of pending queries decreases.
    for i in range(len(tasks)):
        r1.set_queue_state_response(0, accepted=True)
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        assert done.pop() == tasks[0]
        tasks = tasks[1:]

        assert s.curr_num_scheduling_tasks == min(
            len(tasks), s.max_num_scheduling_tasks
        )


@pytest.mark.asyncio
async def test_replica_responds_after_being_removed(pow_2_scheduler, fake_query):
    """
    Verify that if a replica is removed from the active set while the queue length
    message is in flight, it won't be scheduled and a new replica will be.
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    # Set a very high response deadline to ensure we can have the replica respond after
    # calling `update_replicas`.
    s.queue_len_response_deadline_s = 100

    r1 = FakeReplicaWrapper("r1")
    s.update_replicas([r1])

    # Start the scheduling task, which will hang waiting for the queue length response.
    task = loop.create_task(s.choose_replica_for_query(fake_query))

    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0
    assert s.curr_num_scheduling_tasks == 1

    # Update the replicas to remove the existing replica and add a new one.
    # Also set the queue length response on the existing replica.
    r2 = FakeReplicaWrapper("r2")
    s.update_replicas([r2])
    r1.set_queue_state_response(0, accepted=True)

    # The original replica should *not* be scheduled.
    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0
    assert s.curr_num_scheduling_tasks == 1

    # Set the new replica to accept, it should be scheduled.
    r2.set_queue_state_response(0, accepted=True)
    assert (await task) == r2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
