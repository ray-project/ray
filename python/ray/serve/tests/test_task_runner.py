import pytest

import ray
import ray.serve.context as context
from ray.serve.policy import RoundRobinPolicyQueueActor
from ray.serve.task_runner import (RayServeMixin, TaskRunner, TaskRunnerActor,
                                   wrap_to_ray_error)
from ray.serve.request_params import RequestMetadata

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
