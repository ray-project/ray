import asyncio
from collections import defaultdict

import pytest
import ray

from ray.serve.controller import TrafficPolicy
from ray.serve.router import Router, Query
from ray.serve.request_params import RequestMetadata
from ray.serve.utils import get_random_letters
from ray.test_utils import SignalActor
from ray.serve.config import BackendConfig

pytestmark = pytest.mark.asyncio


def mock_task_runner():
    @ray.remote(num_cpus=0)
    class TaskRunnerMock:
        def __init__(self):
            self.query = None
            self.queries = []

        async def handle_request(self, request):
            if isinstance(request, bytes):
                request = Query.ray_deserialize(request)
            self.query = request
            self.queries.append(request)
            return "DONE"

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


async def test_single_prod_cons_queue(serve_instance, task_runner_mock_actor):
    q = ray.remote(Router).remote()
    await q.setup.remote()

    q.set_traffic.remote("svc", TrafficPolicy({"backend-single-prod": 1.0}))
    q.add_new_worker.remote("backend-single-prod", "replica-1",
                            task_runner_mock_actor)

    # Make sure we get the request result back
    result = await q.enqueue_request.remote(RequestMetadata("svc", None), 1)
    assert result == "DONE"

    # Make sure it's the right request
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.request_args[0] == 1
    assert got_work.request_kwargs == {}


async def test_slo(serve_instance, task_runner_mock_actor):
    q = ray.remote(Router).remote()
    await q.setup.remote()
    await q.set_traffic.remote("svc", TrafficPolicy({"backend-slo": 1.0}))

    all_request_sent = []
    for i in range(10):
        slo_ms = 1000 - 100 * i
        all_request_sent.append(
            q.enqueue_request.remote(
                RequestMetadata("svc", None, relative_slo_ms=slo_ms), i))

    await q.add_new_worker.remote("backend-slo", "replica-1",
                                  task_runner_mock_actor)

    await asyncio.gather(*all_request_sent)

    i_should_be = 9
    all_calls = await task_runner_mock_actor.get_all_calls.remote()
    all_calls = all_calls[-10:]
    for call in all_calls:
        assert call.request_args[0] == i_should_be
        i_should_be -= 1


async def test_alter_backend(serve_instance, task_runner_mock_actor):
    q = ray.remote(Router).remote()
    await q.setup.remote()

    await q.set_traffic.remote("svc", TrafficPolicy({"backend-alter": 1}))
    await q.add_new_worker.remote("backend-alter", "replica-1",
                                  task_runner_mock_actor)
    await q.enqueue_request.remote(RequestMetadata("svc", None), 1)
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.request_args[0] == 1

    await q.set_traffic.remote("svc", TrafficPolicy({"backend-alter-2": 1}))
    await q.add_new_worker.remote("backend-alter-2", "replica-1",
                                  task_runner_mock_actor)
    await q.enqueue_request.remote(RequestMetadata("svc", None), 2)
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.request_args[0] == 2


async def test_split_traffic_random(serve_instance, task_runner_mock_actor):
    q = ray.remote(Router).remote()
    await q.setup.remote()

    await q.set_traffic.remote(
        "svc", TrafficPolicy({
            "backend-split": 0.5,
            "backend-split-2": 0.5
        }))
    runner_1, runner_2 = [mock_task_runner() for _ in range(2)]
    await q.add_new_worker.remote("backend-split", "replica-1", runner_1)
    await q.add_new_worker.remote("backend-split-2", "replica-1", runner_2)

    # assume 50% split, the probability of all 20 requests goes to a
    # single queue is 0.5^20 ~ 1-6
    for _ in range(20):
        await q.enqueue_request.remote(RequestMetadata("svc", None), 1)

    got_work = [
        await runner.get_recent_call.remote()
        for runner in (runner_1, runner_2)
    ]
    assert [g.request_args[0] for g in got_work] == [1, 1]


async def test_queue_remove_replicas(serve_instance):
    class TestRouter(Router):
        def worker_queue_size(self, backend):
            return len(self.worker_queues["backend-remove"])

    temp_actor = mock_task_runner()
    q = ray.remote(TestRouter).remote()
    await q.setup.remote()
    await q.add_new_worker.remote("backend-remove", "replica-1", temp_actor)
    await q.remove_worker.remote("backend-remove", "replica-1")
    assert ray.get(q.worker_queue_size.remote("backend")) == 0


async def test_shard_key(serve_instance, task_runner_mock_actor):
    q = ray.remote(Router).remote()
    await q.setup.remote()

    num_backends = 5
    traffic_dict = {}
    runners = [mock_task_runner() for _ in range(num_backends)]
    for i, runner in enumerate(runners):
        backend_name = "backend-split-" + str(i)
        traffic_dict[backend_name] = 1.0 / num_backends
        await q.add_new_worker.remote(backend_name, "replica-1", runner)
    await q.set_traffic.remote("svc", TrafficPolicy(traffic_dict))

    # Generate random shard keys and send one request for each.
    shard_keys = [get_random_letters() for _ in range(100)]
    for shard_key in shard_keys:
        await q.enqueue_request.remote(
            RequestMetadata("svc", None, shard_key=shard_key), shard_key)

    # Log the shard keys that were assigned to each backend.
    runner_shard_keys = defaultdict(set)
    for i, runner in enumerate(runners):
        calls = await runner.get_all_calls.remote()
        for call in calls:
            runner_shard_keys[i].add(call.request_args[0])
        await runner.clear_calls.remote()

    # Send queries with the same shard keys a second time.
    for shard_key in shard_keys:
        await q.enqueue_request.remote(
            RequestMetadata("svc", None, shard_key=shard_key), shard_key)

    # Check that the requests were all mapped to the same backends.
    for i, runner in enumerate(runners):
        calls = await runner.get_all_calls.remote()
        for call in calls:
            assert call.request_args[0] in runner_shard_keys[i]


async def test_router_use_max_concurrency(serve_instance):
    # The VisibleRouter::get_queues method needs to pickle queries
    # so we register serializer here. In regular code path, query
    # serialization is done by Serve manually for performance.
    ray.register_custom_serializer(Query, Query.ray_serialize,
                                   Query.ray_deserialize)

    signal = SignalActor.remote()

    @ray.remote
    class MockWorker:
        async def handle_request(self, request):
            await signal.wait.remote()
            return "DONE"

        def ready(self):
            pass

    class VisibleRouter(Router):
        def get_queues(self):
            return self.queries_counter, self.backend_queues

    worker = MockWorker.remote()
    q = ray.remote(VisibleRouter).remote()
    await q.setup.remote()
    backend_name = "max-concurrent-test"
    config = BackendConfig({"max_concurrent_queries": 1})
    await q.set_traffic.remote("svc", TrafficPolicy({backend_name: 1.0}))
    await q.add_new_worker.remote(backend_name, "replica-tag", worker)
    await q.set_backend_config.remote(backend_name, config)

    # We send over two queries
    first_query = q.enqueue_request.remote(RequestMetadata("svc", None), 1)
    second_query = q.enqueue_request.remote(RequestMetadata("svc", None), 1)

    # Neither queries should be available
    with pytest.raises(ray.exceptions.RayTimeoutError):
        ray.get([first_query, second_query], timeout=0.2)

    # Let's retrieve the router internal state
    queries_counter, backend_queues = await q.get_queues.remote()
    # There should be just one inflight request
    assert queries_counter["max-concurrent-test:replica-tag"] == 1
    # The second query is buffered
    assert len(backend_queues["max-concurrent-test"]) == 1

    # Let's unblock the first query
    await signal.send.remote(clear=True)
    assert await first_query == "DONE"

    # The internal state of router should have changed.
    queries_counter, backend_queues = await q.get_queues.remote()
    # There should still be one inflight request
    assert queries_counter["max-concurrent-test:replica-tag"] == 1
    # But there shouldn't be any queries in the queue
    assert len(backend_queues["max-concurrent-test"]) == 0

    # Unblocking the second query
    await signal.send.remote(clear=True)
    assert await second_query == "DONE"

    # Checking the internal state of the router one more time
    queries_counter, backend_queues = await q.get_queues.remote()
    assert queries_counter["max-concurrent-test:replica-tag"] == 0
    assert len(backend_queues["max-concurrent-test"]) == 0


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
