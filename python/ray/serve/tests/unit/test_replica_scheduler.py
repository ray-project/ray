import asyncio
import importlib
import os
import sys
import time
from typing import Optional, Set, Tuple, Union

import pytest

import ray
from ray._private.utils import get_or_create_event_loop
from ray.serve._private.common import DeploymentID
from ray.serve._private.router import (
    PowerOfTwoChoicesReplicaScheduler,
    Query,
    ReplicaWrapper,
    RequestMetadata,
)

SCHEDULER_NODE_ID = "scheduler_node_id"
SCHEDULER_AZ = "scheduler_az"


class FakeReplicaWrapper(ReplicaWrapper):
    def __init__(
        self,
        replica_id: str,
        *,
        node_id: str = "",
        availability_zone: Optional[str] = None,
        reset_after_response: bool = False,
        model_ids: Optional[Set[str]] = None,
        sleep_time_s: float = 0.0
    ):
        self._replica_id = replica_id
        self._node_id = node_id
        self._availability_zone = availability_zone
        self._queue_len = 0
        self._accepted = False
        self._has_queue_len_response = asyncio.Event()
        self._reset_after_response = reset_after_response
        self._model_ids = model_ids or set()
        self._sleep_time_s = sleep_time_s

        self.get_queue_state_was_cancelled = False

    @property
    def replica_id(self) -> str:
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

    def set_queue_state_response(
        self,
        queue_len: int,
        accepted: bool = True,
        exception: Optional[Exception] = None,
    ):
        self._queue_len = queue_len
        self._accepted = accepted
        self._exception = exception
        self._has_queue_len_response.set()

    async def get_queue_state(self) -> Tuple[int, bool]:
        try:
            while not self._has_queue_len_response.is_set():
                await self._has_queue_len_response.wait()

            if self._sleep_time_s > 0:
                await asyncio.sleep(self._sleep_time_s)

            if self._reset_after_response:
                self._has_queue_len_response.clear()

            if self._exception is not None:
                raise self._exception

            return self._queue_len, self._accepted
        except asyncio.CancelledError:
            self.get_queue_state_was_cancelled = True
            raise

    def send_query(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"]:
        raise NotImplementedError()


@pytest.fixture
def pow_2_scheduler(request) -> PowerOfTwoChoicesReplicaScheduler:
    if not hasattr(request, "param"):
        request.param = {}

    # In order to prevent issues like https://github.com/ray-project/ray/issues/40631,
    # construct the scheduler on a different loop to mimic the deployment handle path.
    async def construct_scheduler(loop: asyncio.AbstractEventLoop):
        return PowerOfTwoChoicesReplicaScheduler(
            loop,
            DeploymentID("TEST_DEPLOYMENT", "TEST_APP"),
            prefer_local_node_routing=request.param.get("prefer_local_node", False),
            prefer_local_az_routing=request.param.get("prefer_local_az", False),
            self_node_id=SCHEDULER_NODE_ID,
            self_actor_id="fake-actor-id",
            self_availability_zone=request.param.get("az", None),
        )

    s = asyncio.new_event_loop().run_until_complete(
        construct_scheduler(get_or_create_event_loop())
    )

    # Update the RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S
    # to 0.01s to speed up the test.
    os.environ.update({"RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S": "0.01"})
    importlib.reload(ray.serve._private.constants)
    importlib.reload(ray.serve._private.router)

    yield s

    # Always verify that all scheduling tasks exit once all queries are satisfied.
    assert s.curr_num_scheduling_tasks == 0
    assert s.num_pending_requests == 0


@pytest.fixture
def fake_query() -> Query:
    meta = RequestMetadata(request_id="req_id", endpoint="endpoint")
    return Query([], {}, meta)


def query_with_model_id(model_id: str):
    meta = RequestMetadata(
        request_id="req_id",
        endpoint="endpoint",
        multiplexed_model_id=model_id,
    )
    return Query([], {}, meta)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
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
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
async def test_replica_does_not_accept_then_accepts(pow_2_scheduler, fake_query):
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
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
async def test_no_replicas_accept_then_new_one_accepts(pow_2_scheduler, fake_query):
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

    r2 = FakeReplicaWrapper("r2")
    r2.set_queue_state_response(0)
    s.update_replicas([r1, r2])

    assert (await task) == r2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
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
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
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

    for _ in range(10):
        assert (await s.choose_replica_for_query(fake_query)) in {r1, r2}

    s.update_replicas([r1])

    for _ in range(10):
        assert (await s.choose_replica_for_query(fake_query)) == r1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
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

    for _ in range(10):
        assert (await s.choose_replica_for_query(fake_query)) == r1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
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

    for _ in range(10):
        assert (await s.choose_replica_for_query(fake_query)) in {r1, r3}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
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

    for _ in range(10):
        assert (await s.choose_replica_for_query(fake_query)) == r2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
async def test_tasks_scheduled_fifo(pow_2_scheduler, fake_query):
    """
    Verify that requests are always scheduled in FIFO order, even if many are being
    assigned concurrently.
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    # Schedule many requests in parallel; they cannot be fulfilled yet.
    tasks = []
    for _ in range(10):
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
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
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
    assert s.num_pending_requests == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
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
        if s.curr_num_scheduling_tasks == 0 and s.num_pending_requests == 0:
            break
        await asyncio.sleep(0.1)
    else:
        raise TimeoutError(
            "Scheduling task and pending assignment still around after 10s."
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
async def test_scheduling_task_cap(pow_2_scheduler, fake_query):
    """
    Verify that the number of scheduling tasks never exceeds the cap (2 * num_replicas).
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    tasks = []
    for _ in range(10):
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
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
async def test_scheduling_task_cap_hard_limit(pow_2_scheduler, fake_query):
    """
    Verify that the number of scheduling tasks never exceeds the hard limit if set.
    """
    s = pow_2_scheduler
    hard_limit = 2
    s.max_num_scheduling_tasks_cap = hard_limit

    loop = get_or_create_event_loop()

    tasks = []
    for _ in range(10):
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
    assert s.curr_num_scheduling_tasks == 2

    # Number of tasks should not increase when adding another replica due to the limit.
    r2 = FakeReplicaWrapper("r2")
    r2.set_queue_state_response(0, accepted=False)
    s.update_replicas([r1, r2])
    assert s.curr_num_scheduling_tasks == hard_limit

    # Number of tasks should decrease as the number of pending queries decreases.
    for i in range(len(tasks)):
        r1.set_queue_state_response(0, accepted=True)
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        assert done.pop() == tasks[0]
        tasks = tasks[1:]

        assert s.curr_num_scheduling_tasks == min(len(tasks), hard_limit)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
    ],
    indirect=True,
)
async def test_prefer_replica_on_same_node(pow_2_scheduler, fake_query):
    """
    Verify that the scheduler prefers replicas that are colocated on the same node ID
    as itself. If the first candidate replicas on the same node reject the request,
    it should fall back to all replicas.
    """
    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    r1 = FakeReplicaWrapper("r1", node_id=SCHEDULER_NODE_ID)
    print(r1.node_id)
    r1.set_queue_state_response(0, accepted=True)
    r2 = FakeReplicaWrapper("r2", node_id="some_other_node_in_the_stratosphere")
    print(r2.node_id)
    r2.set_queue_state_response(0, accepted=True)
    s.update_replicas([r1, r2])

    tasks = []
    for _ in range(10):
        tasks.append(loop.create_task(s.choose_replica_for_query(fake_query)))

    # All requests should be scheduled to the replica on the same node if it accepts.
    assert all(replica == r1 for replica in await asyncio.gather(*tasks))

    # Update the replica on the same node to reject requests -- now requests should
    # fall back to the other replica..
    r1.set_queue_state_response(0, accepted=False)

    tasks = []
    for _ in range(10):
        tasks.append(loop.create_task(s.choose_replica_for_query(fake_query)))

    # All requests should be scheduled to the other replica.
    assert all(replica == r2 for replica in await asyncio.gather(*tasks))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [{"prefer_local_node": True, "prefer_local_az": True, "az": SCHEDULER_AZ}],
    indirect=True,
)
async def test_prefer_replica_in_same_az(pow_2_scheduler, fake_query):
    """
    When prefer routing on same node and prefer routing to same AZ is
    on, verify that the scheduler prefers
    * replicas that are colocated on the same node
    * then replicas that are colocated in the same AZ
    * lastly fall back to all replicas
    """

    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    r1 = FakeReplicaWrapper(
        "r1", node_id=SCHEDULER_NODE_ID, availability_zone=SCHEDULER_AZ
    )
    r2 = FakeReplicaWrapper(
        "r2",
        node_id="some_other_node_in_the_stratosphere",
        availability_zone=SCHEDULER_AZ,
    )
    r3 = FakeReplicaWrapper(
        "r3",
        node_id="some_other_node_in_the_stratosphere",
        availability_zone="some_other_az_in_the_solar_system",
    )
    r1.set_queue_state_response(0, accepted=True)
    r2.set_queue_state_response(0, accepted=True)
    r3.set_queue_state_response(0, accepted=True)
    s.update_replicas([r1, r2, r3])

    async def choose_replicas():
        tasks = []
        for _ in range(10):
            tasks.append(loop.create_task(s.choose_replica_for_query(fake_query)))
        return await asyncio.gather(*tasks)

    # All requests should be scheduled to the replica on the same node if it accepts.
    assert all(replica == r1 for replica in await choose_replicas())

    # Update the replica on the same node to reject requests -- now requests should
    # fall back to replica in the same az.
    r1.set_queue_state_response(0, accepted=False)
    assert all(replica == r2 for replica in await choose_replicas())

    # Update the replica on the same az to reject requests -- now requests should
    # fall back to the last replica.
    r2.set_queue_state_response(0, accepted=False)
    assert all(replica == r3 for replica in await choose_replicas())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [{"prefer_local_az": False, "az": SCHEDULER_AZ}],
    indirect=True,
)
async def test_prefer_az_off(pow_2_scheduler, fake_query):
    """
    When prefer routing to same AZ is OFF, verify that requests are
    spread to replicas across AZs
    """

    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    r1 = FakeReplicaWrapper("r1", availability_zone=SCHEDULER_AZ)
    r2 = FakeReplicaWrapper("r2", availability_zone=SCHEDULER_AZ)
    r3 = FakeReplicaWrapper("r3", availability_zone="western-hemisphere")
    r1.set_queue_state_response(0, accepted=True)
    r2.set_queue_state_response(0, accepted=True)
    r3.set_queue_state_response(0, accepted=True)
    s.update_replicas([r1, r2, r3])

    async def choose_replicas():
        tasks = []
        for _ in range(10):
            tasks.append(loop.create_task(s.choose_replica_for_query(fake_query)))
        replicas = await asyncio.gather(*tasks)
        return {r.replica_id for r in replicas}

    async def verify_replicas_batched(expected_replicas: Set[str]):
        chosen_replicas = set()
        for _ in range(100):
            chosen_replicas = chosen_replicas.union(await choose_replicas())
            print("Replicas chosen after batch of 10:", chosen_replicas)
            if chosen_replicas == expected_replicas:
                break
        assert chosen_replicas == expected_replicas

    # Requests should be spread across all nodes
    # NOTE(zcin): Choose up to 1000 replicas in batches of 10 at a time.
    # This deflakes the test, but also makes sure the test runs fast on average
    await verify_replicas_batched({r1.replica_id, r2.replica_id, r3.replica_id})

    r1.set_queue_state_response(0, accepted=False)
    await verify_replicas_batched({r2.replica_id, r3.replica_id})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [{"prefer_local_node": False, "prefer_local_az": True, "az": SCHEDULER_AZ}],
    indirect=True,
)
async def test_prefer_replica_in_same_az_without_prefer_node(
    pow_2_scheduler, fake_query
):
    """
    When prefer routing on same node is OFF and prefer routing to same
    AZ is ON, verify that the scheduler prefers
    * replicas that are colocated in the same AZ
    * then fall back to all replicas
    """

    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    r1 = FakeReplicaWrapper(
        "r1", node_id=SCHEDULER_NODE_ID, availability_zone=SCHEDULER_AZ
    )
    r2 = FakeReplicaWrapper("r2", node_id="node-alpha", availability_zone=SCHEDULER_AZ)
    r3 = FakeReplicaWrapper("r3", node_id="node-beta", availability_zone="some_zone")
    r1.set_queue_state_response(0, accepted=True)
    r2.set_queue_state_response(0, accepted=True)
    r3.set_queue_state_response(0, accepted=True)
    s.update_replicas([r1, r2, r3])

    async def choose_replicas():
        tasks = []
        for _ in range(10):
            tasks.append(loop.create_task(s.choose_replica_for_query(fake_query)))
        return await asyncio.gather(*tasks)

    # All requests should be scheduled to the two nodes in the same AZ
    # (r1 and r2). Without node preference in routing, requests should
    # be scheduled to BOTH r1 and r2
    assert set(await choose_replicas()) == {r1, r2}

    # Update replica on one of the nodes in the same AZ to reject
    # requests. Now requests should only go to the remaining node in the
    # same AZ
    r2.set_queue_state_response(0, accepted=False)
    assert all(replica == r1 for replica in await choose_replicas())

    # Update the replica on last node in the same AZ to reject requests.
    # Now requests should fall back to the last replica.
    r1.set_queue_state_response(0, accepted=False)
    assert all(replica == r3 for replica in await choose_replicas())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [{"prefer_local_node": True, "prefer_local_az": False, "az": SCHEDULER_AZ}],
    indirect=True,
)
async def test_prefer_replica_on_same_node_without_prefer_az(
    pow_2_scheduler, fake_query
):
    """
    When prefer routing to same node is ON and prefer routing to same AZ
    is OFF, verify that requests are first scheduled to same-node
    replicas, then spread across all availability zones.
    """

    s = pow_2_scheduler
    loop = get_or_create_event_loop()

    r1 = FakeReplicaWrapper(
        "r1", node_id=SCHEDULER_NODE_ID, availability_zone=SCHEDULER_AZ
    )  # noqa
    r2 = FakeReplicaWrapper("r2", node_id="node-alpha", availability_zone=SCHEDULER_AZ)
    r3 = FakeReplicaWrapper("r3", node_id="node-beta", availability_zone="west")
    r1.set_queue_state_response(0, accepted=True)
    r2.set_queue_state_response(0, accepted=True)
    r3.set_queue_state_response(0, accepted=True)
    s.update_replicas([r1, r2, r3])

    async def choose_replicas():
        tasks = []
        for _ in range(10):
            tasks.append(loop.create_task(s.choose_replica_for_query(fake_query)))
        return await asyncio.gather(*tasks)

    # Requests should be sent to replica on same node
    assert all(replica == r1 for replica in await choose_replicas())

    # If replica on same node is blocked, there should be no preference between
    # remaining replicas even if the availability zones are different.
    r1.set_queue_state_response(0, accepted=False)
    assert set(await choose_replicas()) == {r2, r3}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pow_2_scheduler",
    [
        {"prefer_local_node": True, "prefer_local_az": True},
        {"prefer_local_node": True, "prefer_local_az": False},
        {"prefer_local_node": False, "prefer_local_az": True},
        {"prefer_local_node": False, "prefer_local_az": False},
    ],
    indirect=True,
)
class TestModelMultiplexing:
    async def test_replicas_with_model_id_always_chosen(self, pow_2_scheduler):
        """
        Verify that if accepted, only replicas with a given model ID will be chosen.
        This should be independent of queue length.
        """
        s = pow_2_scheduler
        loop = get_or_create_event_loop()

        r1 = FakeReplicaWrapper("r1", model_ids={"m1", "m2"})
        r1.set_queue_state_response(100, accepted=True)
        r2 = FakeReplicaWrapper("r2", model_ids={"m2", "m3"})
        r2.set_queue_state_response(100, accepted=True)
        r3 = FakeReplicaWrapper("r3", model_ids={})
        r3.set_queue_state_response(0, accepted=True)
        s.update_replicas([r1, r2, r3])

        for _ in range(10):
            query = query_with_model_id("m2")
            task = loop.create_task(s.choose_replica_for_query(query))
            assert (await task) in {r1, r2}

    async def test_choose_least_number_of_models_replicas(self, pow_2_scheduler):
        """
        If no replica has the model_id, choose the least number of models replicas.
        """
        s = pow_2_scheduler
        loop = get_or_create_event_loop()
        r1 = FakeReplicaWrapper("r1", model_ids={"m1", "m2"})
        r2 = FakeReplicaWrapper("r2", model_ids={"m2"})
        r1.set_queue_state_response(0, accepted=True)
        r2.set_queue_state_response(0, accepted=True)
        s.update_replicas([r1, r2])
        for _ in range(10):
            query = query_with_model_id("m3")
            task = loop.create_task(s.choose_replica_for_query(query))
            assert (await task) == r2

    async def test_no_replica_has_model_id(self, pow_2_scheduler):
        """
        If no replica has the model_id, we should fall back to normal procedure.
        """
        s = pow_2_scheduler
        loop = get_or_create_event_loop()

        r1 = FakeReplicaWrapper("r1", model_ids={})
        r1.set_queue_state_response(0, accepted=True)
        s.update_replicas([r1])

        for _ in range(10):
            query = query_with_model_id("m1")
            task = loop.create_task(s.choose_replica_for_query(query))
            assert (await task) == r1

    async def test_fall_back_to_replica_without_model_id(self, pow_2_scheduler):
        """
        Verify that we'll fall back to a replica that doesn't have the model ID if
        none of the replicas with it can accept the request.
        """
        s = pow_2_scheduler
        loop = get_or_create_event_loop()

        r1 = FakeReplicaWrapper("r1", model_ids={"m1", "m2"})
        r1.set_queue_state_response(0, accepted=False)
        r2 = FakeReplicaWrapper("r2", model_ids={"m2", "m3"})
        r2.set_queue_state_response(100, accepted=False)
        r3 = FakeReplicaWrapper("r3", model_ids={})
        r3.set_queue_state_response(0, accepted=True)
        s.update_replicas([r1, r2, r3])

        for _ in range(10):
            query = query_with_model_id("m2")
            task = loop.create_task(s.choose_replica_for_query(query))
            assert (await task) == r3

    async def test_multiple_queries_with_different_model_ids(self, pow_2_scheduler):
        """
        Verify that multiple queries with different model_ids will be mapped to the
        appropriate replicas.
        """
        s = pow_2_scheduler
        loop = get_or_create_event_loop()

        r1 = FakeReplicaWrapper("r1", model_ids={"m1"})
        r1.set_queue_state_response(0, accepted=True)
        r2 = FakeReplicaWrapper("r2", model_ids={"m2"})
        r2.set_queue_state_response(0, accepted=True)
        r3 = FakeReplicaWrapper("r3", model_ids={"m3"})
        r3.set_queue_state_response(0, accepted=True)
        s.update_replicas([r1, r2, r3])

        for _ in range(10):
            tasks = [
                loop.create_task(s.choose_replica_for_query(query_with_model_id("m1"))),
                loop.create_task(s.choose_replica_for_query(query_with_model_id("m2"))),
                loop.create_task(s.choose_replica_for_query(query_with_model_id("m3"))),
                loop.create_task(s.choose_replica_for_query(query_with_model_id("m1"))),
                loop.create_task(s.choose_replica_for_query(query_with_model_id("m2"))),
                loop.create_task(s.choose_replica_for_query(query_with_model_id("m3"))),
            ]

            done, _ = await asyncio.wait(tasks, timeout=0.1)
            assert len(done) == len(tasks)

            assert all(
                [
                    tasks[0].result() == r1,
                    tasks[1].result() == r2,
                    tasks[2].result() == r3,
                    tasks[3].result() == r1,
                    tasks[4].result() == r2,
                    tasks[5].result() == r3,
                ]
            )

    async def test_no_replicas_available_then_choose_one_with_id(self, pow_2_scheduler):
        """
        Verify that if new replicas are added while the scheduling task is in backoff,
        it will prioritize those with the model ID.
        """
        s = pow_2_scheduler
        loop = get_or_create_event_loop()

        r1 = FakeReplicaWrapper("r1")
        r1.set_queue_state_response(0, accepted=False)

        tasks = [
            loop.create_task(s.choose_replica_for_query(query_with_model_id("m1")))
            for _ in range(100)
        ]

        # Scheduling tasks should be in backoff.
        done, _ = await asyncio.wait(tasks, timeout=0.1)
        assert len(done) == 0

        # Now add two more replicas, one of which has the model ID.
        # That one should be chosen for all of the tasks.
        r2 = FakeReplicaWrapper("r2")
        r2.set_queue_state_response(0, accepted=False)
        r3 = FakeReplicaWrapper("r3", model_ids={"m1"})
        r3.set_queue_state_response(100, accepted=True)

        s.update_replicas([r1, r2, r3])

        assert all(replica == r3 for replica in await asyncio.gather(*tasks))

    @pytest.mark.asyncio
    async def test_tasks_scheduled_fifo_among_model_ids(
        self, pow_2_scheduler, fake_query
    ):
        """
        Verify that requests are scheduled FIFO based on model ID.
        """
        s = pow_2_scheduler
        loop = get_or_create_event_loop()

        # Schedule many requests to each model ID in parallel
        # that cannot be fulfilled yet.
        m1_tasks = []
        m2_tasks = []
        for _ in range(10):
            m1_tasks.append(
                loop.create_task(s.choose_replica_for_query(query_with_model_id("m1")))
            )
            m2_tasks.append(
                loop.create_task(s.choose_replica_for_query(query_with_model_id("m2")))
            )

        done, _ = await asyncio.wait(m1_tasks + m2_tasks, timeout=0.1)
        assert len(done) == 0

        r1 = FakeReplicaWrapper("r1", model_ids={"m1"}, reset_after_response=True)
        r1.set_queue_state_response(0, accepted=True)
        r2 = FakeReplicaWrapper("r2", model_ids={"m2"}, reset_after_response=True)
        r2.set_queue_state_response(0, accepted=True)
        s.update_replicas([r1, r2])

        # In each iteration, allow one replica of w/ each model ID to be scheduled.
        # The tasks for each model ID should be scheduled in FIFO order.
        for i in range(10):
            r1.set_queue_state_response(0, accepted=True)
            r2.set_queue_state_response(0, accepted=True)

            done, pending = await asyncio.wait(
                m1_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            assert done.pop() == m1_tasks[0]
            m1_tasks = m1_tasks[1:]

            done, pending = await asyncio.wait(
                m2_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            assert done.pop() == m2_tasks[0]
            m2_tasks = m2_tasks[1:]


@pytest.mark.asyncio
async def test_get_queue_state_cancelled_on_timeout(pow_2_scheduler, fake_query):
    """
    Verify that `get_queue_state` is cancelled if the `queue_len_response_deadline_s`
    is reached.
    """
    s = pow_2_scheduler
    s.queue_len_response_deadline_s = 0.001
    loop = get_or_create_event_loop()

    r1 = FakeReplicaWrapper("r1")
    s.update_replicas([r1])

    # Attempt to schedule; the replica will be attempted and a timeout will occur
    # due to the short timeout set above.
    task = loop.create_task(s.choose_replica_for_query(fake_query))
    done, _ = await asyncio.wait([task], timeout=0.1)
    assert len(done) == 0

    # The `get_queue_state` method should be cancelled.
    assert r1.get_queue_state_was_cancelled

    r1.set_queue_state_response(0, accepted=True)
    assert (await task) == r1


@pytest.mark.asyncio
async def test_replicas_updated_event_on_correct_loop(pow_2_scheduler):
    """See https://github.com/ray-project/ray/issues/40631.

    The `await` statements below would fail with
    "RuntimeError: ... got Future <Future pending> attached to a different loop."
    """
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            pow_2_scheduler._replicas_updated_event.wait(), timeout=0.001
        )

    pow_2_scheduler._replicas_updated_event.set()
    await pow_2_scheduler._replicas_updated_event.wait()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
