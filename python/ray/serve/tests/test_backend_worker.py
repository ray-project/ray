import asyncio

import pytest

import ray
from ray import serve
import ray.serve.context as context
from ray.serve.policy import RoundRobinPolicyQueueActor
from ray.serve.backend_worker import create_backend_worker, wrap_to_ray_error
from ray.serve.request_params import RequestMetadata
from ray.serve.config import BackendConfig

pytestmark = pytest.mark.asyncio


def setup_worker(name, func_or_class, init_args=None):
    if init_args is None:
        init_args = ()

    @ray.remote
    class WorkerActor:
        def __init__(self):
            self.worker = create_backend_worker(func_or_class)(
                name, name + ":tag", init_args)

        def ready(self):
            pass

        def get_metrics(self):
            return self.worker.get_metrics()

        async def handle_request(self, *args, **kwargs):
            return await self.worker.handle_request(*args, **kwargs)

    worker = WorkerActor.remote()
    ray.get(worker.ready.remote())
    return worker


async def test_runner_wraps_error():
    wrapped = wrap_to_ray_error(Exception())
    assert isinstance(wrapped, ray.exceptions.RayTaskError)


async def test_runner_actor(serve_instance):
    q = RoundRobinPolicyQueueActor.remote()

    def echo(flask_request, i=None):
        return i

    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "prod"

    worker = setup_worker(CONSUMER_NAME, echo)
    await q.add_new_worker.remote(CONSUMER_NAME, "replica1", worker)

    q.set_traffic.remote(PRODUCER_NAME, {CONSUMER_NAME: 1.0})

    for query in [333, 444, 555]:
        query_param = RequestMetadata(PRODUCER_NAME,
                                      context.TaskContext.Python)
        result = await q.enqueue_request.remote(query_param, i=query)
        assert result == query


async def test_ray_serve_mixin(serve_instance):
    q = RoundRobinPolicyQueueActor.remote()

    CONSUMER_NAME = "runner-cls"
    PRODUCER_NAME = "prod-cls"

    class MyAdder:
        def __init__(self, inc):
            self.increment = inc

        def __call__(self, flask_request, i=None):
            return i + self.increment

    worker = setup_worker(CONSUMER_NAME, MyAdder, init_args=(3, ))
    await q.add_new_worker.remote(CONSUMER_NAME, "replica1", worker)

    q.set_traffic.remote(PRODUCER_NAME, {CONSUMER_NAME: 1.0})

    for query in [333, 444, 555]:
        query_param = RequestMetadata(PRODUCER_NAME,
                                      context.TaskContext.Python)
        result = await q.enqueue_request.remote(query_param, i=query)
        assert result == query + 3


async def test_task_runner_check_context(serve_instance):
    q = RoundRobinPolicyQueueActor.remote()

    def echo(flask_request, i=None):
        # Accessing the flask_request without web context should throw.
        return flask_request.args["i"]

    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "producer"

    worker = setup_worker(CONSUMER_NAME, echo)
    await q.add_new_worker.remote(CONSUMER_NAME, "replica1", worker)

    q.set_traffic.remote(PRODUCER_NAME, {CONSUMER_NAME: 1.0})
    query_param = RequestMetadata(PRODUCER_NAME, context.TaskContext.Python)
    result_oid = q.enqueue_request.remote(query_param, i=42)

    with pytest.raises(ray.exceptions.RayTaskError):
        await result_oid


async def test_task_runner_custom_method_single(serve_instance):
    q = RoundRobinPolicyQueueActor.remote()

    class NonBatcher:
        def a(self, _):
            return "a"

        def b(self, _):
            return "b"

    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "producer"

    worker = setup_worker(CONSUMER_NAME, NonBatcher)
    await q.add_new_worker.remote(CONSUMER_NAME, "replica1", worker)

    q.set_traffic.remote(PRODUCER_NAME, {CONSUMER_NAME: 1.0})

    query_param = RequestMetadata(
        PRODUCER_NAME, context.TaskContext.Python, call_method="a")
    a_result = await q.enqueue_request.remote(query_param)
    assert a_result == "a"

    query_param = RequestMetadata(
        PRODUCER_NAME, context.TaskContext.Python, call_method="b")
    b_result = await q.enqueue_request.remote(query_param)
    assert b_result == "b"

    query_param = RequestMetadata(
        PRODUCER_NAME, context.TaskContext.Python, call_method="non_exist")
    with pytest.raises(ray.exceptions.RayTaskError):
        await q.enqueue_request.remote(query_param)


async def test_task_runner_custom_method_batch(serve_instance):
    q = RoundRobinPolicyQueueActor.remote()

    @serve.accept_batch
    class Batcher:
        def a(self, _):
            return ["a-{}".format(i) for i in range(serve.context.batch_size)]

        def b(self, _):
            return ["b-{}".format(i) for i in range(serve.context.batch_size)]

    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "producer"

    worker = setup_worker(CONSUMER_NAME, Batcher)

    await q.set_traffic.remote(PRODUCER_NAME, {CONSUMER_NAME: 1.0})
    await q.set_backend_config.remote(
        CONSUMER_NAME,
        BackendConfig({
            "max_batch_size": 10
        }, accepts_batches=True))

    a_query_param = RequestMetadata(
        PRODUCER_NAME, context.TaskContext.Python, call_method="a")
    b_query_param = RequestMetadata(
        PRODUCER_NAME, context.TaskContext.Python, call_method="b")

    futures = [q.enqueue_request.remote(a_query_param) for _ in range(2)]
    futures += [q.enqueue_request.remote(b_query_param) for _ in range(2)]

    await q.add_new_worker.remote(CONSUMER_NAME, "replica1", worker)

    gathered = await asyncio.gather(*futures)
    assert set(gathered) == {"a-0", "a-1", "b-0", "b-1"}
