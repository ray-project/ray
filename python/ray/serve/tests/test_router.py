"""
Unit tests for the router class. Please don't add any test that will involve
controller or the backend worker, use mock if necessary.
"""
import asyncio
from collections import defaultdict
import os

import pytest

import ray
from ray.serve.config import BackendConfig
from ray.serve.controller import TrafficPolicy
from ray.serve.router import Query, ReplicaSet, RequestMetadata, Router
from ray.serve.utils import get_random_letters
from ray.test_utils import SignalActor

pytestmark = pytest.mark.asyncio


@pytest.fixture
def ray_instance():
    os.environ["SERVE_LOG_DEBUG"] = "1"  # Turns on debug log for tests
    ray.init(num_cpus=16)
    yield
    ray.shutdown()


def mock_task_runner():
    @ray.remote(num_cpus=0)
    class TaskRunnerMock:
        def __init__(self):
            self.query = None
            self.queries = []

        @ray.method(num_returns=2)
        async def handle_request(self, request_metadata, *args, **kwargs):
            self.query = Query(args, kwargs, request_metadata)
            self.queries.append(self.query)
            return b"", "DONE"

        def get_recent_call(self):
            return self.query

        def get_all_calls(self):
            return self.queries

        def clear_calls(self):
            self.queries = []

        def ready(self):
            pass

    return TaskRunnerMock.remote()


@pytest.fixture
def task_runner_mock_actor():
    yield mock_task_runner()


async def test_simple_endpoint_backend_pair(ray_instance, mock_controller,
                                            task_runner_mock_actor):
    q = ray.remote(Router).remote(mock_controller, "svc")

    # Propogate configs
    await mock_controller.set_traffic.remote(
        "svc", TrafficPolicy({
            "backend-single-prod": 1.0
        }))
    await mock_controller.add_new_replica.remote("backend-single-prod",
                                                 task_runner_mock_actor)

    # Make sure we get the request result back
    ref = await q.assign_request.remote(
        RequestMetadata(get_random_letters(10), "svc"), 1)
    result = await ref
    assert result == "DONE"

    # Make sure it's the right request
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.args[0] == 1
    assert got_work.kwargs == {}


async def test_changing_backend(ray_instance, mock_controller,
                                task_runner_mock_actor):
    q = ray.remote(Router).remote(mock_controller, "svc")

    await mock_controller.set_traffic.remote(
        "svc", TrafficPolicy({
            "backend-alter": 1
        }))
    await mock_controller.add_new_replica.remote("backend-alter",
                                                 task_runner_mock_actor)

    await (await q.assign_request.remote(
        RequestMetadata(get_random_letters(10), "svc"), 1))
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.args[0] == 1

    await mock_controller.set_traffic.remote(
        "svc", TrafficPolicy({
            "backend-alter-2": 1
        }))
    await mock_controller.add_new_replica.remote("backend-alter-2",
                                                 task_runner_mock_actor)
    await (await q.assign_request.remote(
        RequestMetadata(get_random_letters(10), "svc"), 2))
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.args[0] == 2


async def test_split_traffic_random(ray_instance, mock_controller,
                                    task_runner_mock_actor):
    q = ray.remote(Router).remote(mock_controller, "svc")

    await mock_controller.set_traffic.remote(
        "svc", TrafficPolicy({
            "backend-split": 0.5,
            "backend-split-2": 0.5
        }))
    runner_1, runner_2 = [mock_task_runner() for _ in range(2)]
    await mock_controller.add_new_replica.remote("backend-split", runner_1)
    await mock_controller.add_new_replica.remote("backend-split-2", runner_2)

    # assume 50% split, the probability of all 20 requests goes to a
    # single queue is 0.5^20 ~ 1-6
    object_refs = []
    for _ in range(20):
        ref = await q.assign_request.remote(
            RequestMetadata(get_random_letters(10), "svc"), 1)
        object_refs.append(ref)
    ray.get(object_refs)

    got_work = [
        await runner.get_recent_call.remote()
        for runner in (runner_1, runner_2)
    ]
    assert [g.args[0] for g in got_work] == [1, 1]


async def test_shard_key(ray_instance, mock_controller,
                         task_runner_mock_actor):
    q = ray.remote(Router).remote(mock_controller, "svc")

    num_backends = 5
    traffic_dict = {}
    runners = [mock_task_runner() for _ in range(num_backends)]
    for i, runner in enumerate(runners):
        backend_name = "backend-split-" + str(i)
        traffic_dict[backend_name] = 1.0 / num_backends
        await mock_controller.add_new_replica.remote(backend_name, runner)
    await mock_controller.set_traffic.remote("svc",
                                             TrafficPolicy(traffic_dict))

    # Generate random shard keys and send one request for each.
    shard_keys = [get_random_letters() for _ in range(100)]
    for shard_key in shard_keys:
        await (await q.assign_request.remote(
            RequestMetadata(
                get_random_letters(10), "svc", shard_key=shard_key),
            shard_key))

    # Log the shard keys that were assigned to each backend.
    runner_shard_keys = defaultdict(set)
    for i, runner in enumerate(runners):
        calls = await runner.get_all_calls.remote()
        for call in calls:
            runner_shard_keys[i].add(call.args[0])
        await runner.clear_calls.remote()

    # Send queries with the same shard keys a second time.
    for shard_key in shard_keys:
        await (await q.assign_request.remote(
            RequestMetadata(
                get_random_letters(10), "svc", shard_key=shard_key),
            shard_key))

    # Check that the requests were all mapped to the same backends.
    for i, runner in enumerate(runners):
        calls = await runner.get_all_calls.remote()
        for call in calls:
            assert call.args[0] in runner_shard_keys[i]


async def test_replica_set(ray_instance, mock_controller_with_name):
    signal = SignalActor.remote()

    @ray.remote(num_cpus=0)
    class MockWorker:
        _num_queries = 0

        @ray.method(num_returns=2)
        async def handle_request(self, request):
            self._num_queries += 1
            await signal.wait.remote()
            return b"", "DONE"

        async def num_queries(self):
            return self._num_queries

    # We will test a scenario with two replicas in the replica set.
    rs = ReplicaSet(
        mock_controller_with_name[1],
        "my_backend",
        asyncio.get_event_loop(),
    )
    workers = [MockWorker.remote() for _ in range(2)]
    rs.set_max_concurrent_queries(BackendConfig(max_concurrent_queries=1))
    rs.update_worker_replicas(workers)

    # Send two queries. They should go through the router but blocked by signal
    # actors.
    query = Query([], {}, RequestMetadata("request-id", "endpoint"))
    first_ref = await rs.assign_replica(query)
    second_ref = await rs.assign_replica(query)

    # These should be blocked by signal actor.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get([first_ref, second_ref], timeout=1)

    # Each replica should have exactly one inflight query. Let make sure the
    # queries arrived there.
    for worker in workers:
        while await worker.num_queries.remote() != 1:
            await asyncio.sleep(1)

    # Let's try to send another query.
    third_ref_pending_task = asyncio.get_event_loop().create_task(
        rs.assign_replica(query))
    # We should fail to assign a replica, so this coroutine should still be
    # pending after some time.
    await asyncio.sleep(0.2)
    assert not third_ref_pending_task.done()

    # Let's unblock the two workers
    await signal.send.remote()
    assert await first_ref == "DONE"
    assert await second_ref == "DONE"

    # The third request should be unblocked and sent to first worker.
    # This meas we should be able to get the object ref.
    third_ref = await third_ref_pending_task

    # Now we got the object ref, let's get it result.
    await signal.send.remote()
    assert await third_ref == "DONE"

    # Finally, make sure that one of the replica processed the third query.
    num_queries_set = {(await worker.num_queries.remote())
                       for worker in workers}
    assert num_queries_set == {2, 1}


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
