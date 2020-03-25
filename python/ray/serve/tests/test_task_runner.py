import asyncio

import pytest

import ray
from ray import serve
import ray.serve.context as context
from ray.serve.policy import RoundRobinPolicyQueueActor
from ray.serve.task_runner import (RayServeMixin, TaskRunner, TaskRunnerActor,
                                   wrap_to_ray_error)
from ray.serve.request_params import RequestMetadata
from ray.serve.backend_config import BackendConfig

pytestmark = pytest.mark.asyncio


async def test_runner_basic():
    def echo(i):
        return i

    r = TaskRunner(echo)
    assert r(1) == 1


async def test_runner_wraps_error():
    wrapped = wrap_to_ray_error(Exception())
    assert isinstance(wrapped, ray.exceptions.RayTaskError)


async def test_runner_actor(serve_instance):
    q = RoundRobinPolicyQueueActor.remote()

    def echo(flask_request, i=None):
        return i

    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "prod"

    runner = TaskRunnerActor.remote(echo)
    runner._ray_serve_setup.remote(CONSUMER_NAME, q, runner)
    runner._ray_serve_fetch.remote()

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)

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

    @ray.remote
    class CustomActor(MyAdder, RayServeMixin):
        pass

    runner = CustomActor.remote(3)

    runner._ray_serve_setup.remote(CONSUMER_NAME, q, runner)
    runner._ray_serve_fetch.remote()

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)

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

    runner = TaskRunnerActor.remote(echo)

    runner._ray_serve_setup.remote(CONSUMER_NAME, q, runner)
    runner._ray_serve_fetch.remote()

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)
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

    @ray.remote
    class CustomActor(NonBatcher, RayServeMixin):
        pass

    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "producer"

    runner = CustomActor.remote()

    runner._ray_serve_setup.remote(CONSUMER_NAME, q, runner)
    runner._ray_serve_fetch.remote()

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)

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

    @ray.remote
    class CustomActor(Batcher, RayServeMixin):
        pass

    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "producer"

    runner = CustomActor.remote()

    runner._ray_serve_setup.remote(CONSUMER_NAME, q, runner)

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)
    q.set_backend_config.remote(
        CONSUMER_NAME, BackendConfig(max_batch_size=2).__dict__)

    a_query_param = RequestMetadata(
        PRODUCER_NAME, context.TaskContext.Python, call_method="a")
    b_query_param = RequestMetadata(
        PRODUCER_NAME, context.TaskContext.Python, call_method="b")

    futures = [q.enqueue_request.remote(a_query_param) for _ in range(2)]
    futures += [q.enqueue_request.remote(b_query_param) for _ in range(2)]

    runner._ray_serve_fetch.remote()

    gathered = await asyncio.gather(*futures)
    assert gathered == ["a-0", "a-1", "b-0", "b-1"]
