import asyncio

import pytest
import ray

from ray.serve.policy import (
    RandomPolicyQueue, RandomPolicyQueueActor, RoundRobinPolicyQueueActor,
    PowerOfTwoPolicyQueueActor, FixedPackingPolicyQueueActor)
from ray.serve.request_params import RequestMetadata

pytestmark = pytest.mark.asyncio


def make_task_runner_mock():
    @ray.remote(num_cpus=0)
    class TaskRunnerMock:
        def __init__(self):
            self.query = None
            self.queries = []

        async def _ray_serve_call(self, request_item):
            self.query = request_item
            self.queries.append(request_item)
            return "DONE"

        def get_recent_call(self):
            return self.query

        def get_all_calls(self):
            return self.queries

    return TaskRunnerMock.remote()


@pytest.fixture(scope="session")
def task_runner_mock_actor():
    yield make_task_runner_mock()


async def test_single_prod_cons_queue(serve_instance, task_runner_mock_actor):
    q = RandomPolicyQueueActor.remote()
    q.link.remote("svc", "backend")
    q.dequeue_request.remote("backend", task_runner_mock_actor)

    # Make sure we get the request result back
    result = await q.enqueue_request.remote(RequestMetadata("svc", None), 1)
    assert result == "DONE"

    # Make sure it's the right request
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.request_args[0] == 1
    assert got_work.request_kwargs == {}


async def test_slo(serve_instance, task_runner_mock_actor):
    q = RandomPolicyQueueActor.remote()
    await q.link.remote("svc", "backend")

    all_request_sent = []
    for i in range(10):
        slo_ms = 1000 - 100 * i
        all_request_sent.append(
            q.enqueue_request.remote(
                RequestMetadata("svc", None, relative_slo_ms=slo_ms), i))

    for i in range(10):
        await q.dequeue_request.remote("backend", task_runner_mock_actor)

    await asyncio.gather(*all_request_sent)

    i_should_be = 9
    all_calls = await task_runner_mock_actor.get_all_calls.remote()
    all_calls = all_calls[-10:]
    for call in all_calls:
        assert call.request_args[0] == i_should_be
        i_should_be -= 1


async def test_alter_backend(serve_instance, task_runner_mock_actor):
    q = RandomPolicyQueueActor.remote()

    await q.set_traffic.remote("svc", {"backend-1": 1})
    await q.dequeue_request.remote("backend-1", task_runner_mock_actor)
    await q.enqueue_request.remote(RequestMetadata("svc", None), 1)
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.request_args[0] == 1

    await q.set_traffic.remote("svc", {"backend-2": 1})
    await q.dequeue_request.remote("backend-2", task_runner_mock_actor)
    await q.enqueue_request.remote(RequestMetadata("svc", None), 2)
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.request_args[0] == 2


async def test_split_traffic_random(serve_instance, task_runner_mock_actor):
    q = RandomPolicyQueueActor.remote()

    await q.set_traffic.remote("svc", {"backend-1": 0.5, "backend-2": 0.5})
    runner_1, runner_2 = [make_task_runner_mock() for _ in range(2)]
    for _ in range(20):
        await q.dequeue_request.remote("backend-1", runner_1)
        await q.dequeue_request.remote("backend-2", runner_2)

    # assume 50% split, the probability of all 20 requests goes to a
    # single queue is 0.5^20 ~ 1-6
    for _ in range(20):
        await q.enqueue_request.remote(RequestMetadata("svc", None), 1)

    got_work = [
        await runner.get_recent_call.remote()
        for runner in (runner_1, runner_2)
    ]
    assert [g.request_args[0] for g in got_work] == [1, 1]


async def test_round_robin(serve_instance, task_runner_mock_actor):
    q = RoundRobinPolicyQueueActor.remote()

    await q.set_traffic.remote("svc", {"backend-1": 0.5, "backend-2": 0.5})
    runner_1, runner_2 = [make_task_runner_mock() for _ in range(2)]

    # NOTE: this is the only difference between the
    # test_split_traffic_random and test_round_robin
    for _ in range(10):
        await q.dequeue_request.remote("backend-1", runner_1)
        await q.dequeue_request.remote("backend-2", runner_2)

    for _ in range(20):
        await q.enqueue_request.remote(RequestMetadata("svc", None), 1)

    got_work = [
        await runner.get_recent_call.remote()
        for runner in (runner_1, runner_2)
    ]
    assert [g.request_args[0] for g in got_work] == [1, 1]


async def test_fixed_packing(serve_instance):
    packing_num = 4
    q = FixedPackingPolicyQueueActor.remote(packing_num=packing_num)
    await q.set_traffic.remote("svc", {"backend-1": 0.5, "backend-2": 0.5})

    runner_1, runner_2 = (make_task_runner_mock() for _ in range(2))
    # both the backends will get equal number of queries
    # as it is packed round robin
    for _ in range(packing_num):
        await q.dequeue_request.remote("backend-1", runner_1)
        await q.dequeue_request.remote("backend-2", runner_2)

    for backend, runner in zip(["1", "2"], [runner_1, runner_2]):
        for _ in range(packing_num):
            input_value = "should-go-to-backend-{}".format(backend)
            await q.enqueue_request.remote(
                RequestMetadata("svc", None), input_value)
            all_calls = await runner.get_all_calls.remote()
            for call in all_calls:
                assert call.request_args[0] == input_value


async def test_power_of_two_choices(serve_instance):
    q = PowerOfTwoPolicyQueueActor.remote()
    enqueue_futures = []

    # First, fill the queue for backend-1 with 3 requests
    await q.set_traffic.remote("svc", {"backend-1": 1.0})
    for _ in range(3):
        future = q.enqueue_request.remote(RequestMetadata("svc", None), "1")
        enqueue_futures.append(future)

    # Then, add a new backend, this backend should be filled next
    await q.set_traffic.remote("svc", {"backend-1": 0.5, "backend-2": 0.5})
    for _ in range(2):
        future = q.enqueue_request.remote(RequestMetadata("svc", None), "2")
        enqueue_futures.append(future)

    runner_1, runner_2 = (make_task_runner_mock() for _ in range(2))
    for _ in range(3):
        await q.dequeue_request.remote("backend-1", runner_1)
        await q.dequeue_request.remote("backend-2", runner_2)

    await asyncio.gather(*enqueue_futures)

    assert len(await runner_1.get_all_calls.remote()) == 3
    assert len(await runner_2.get_all_calls.remote()) == 2


async def test_queue_remove_replicas(serve_instance):
    temp_actor = make_task_runner_mock()
    q = RandomPolicyQueue()
    await q.dequeue_request("backend", temp_actor)
    await q.remove_and_destory_replica("backend", temp_actor)
    assert q.worker_queues["backend"].qsize() == 0
