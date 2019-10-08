import pytest
import ray
from ray.experimental.serve.queues import CentralizedQueuesActor
from ray.experimental.serve.task_runner import (
    RayServeMixin,
    TaskRunner,
    TaskRunnerActor,
    wrap_to_ray_error,
)
import ray.experimental.serve.context as context


def test_runner_basic():
    def echo(i):
        return i

    r = TaskRunner(echo)
    assert r(1) == 1


def test_runner_wraps_error():
    wrapped = wrap_to_ray_error(Exception())
    assert isinstance(wrapped, ray.exceptions.RayTaskError)


def test_runner_actor(serve_instance):
    q = CentralizedQueuesActor.remote()

    def echo(flask_request, i=None):
        return i

    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "prod"

    runner = TaskRunnerActor.remote(echo)

    runner._ray_serve_setup.remote(CONSUMER_NAME, q)
    runner._ray_serve_main_loop.remote(runner)

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)

    for query in [333, 444, 555]:
        result_token = ray.ObjectID(
            ray.get(
                q.enqueue_request.remote(
                    PRODUCER_NAME,
                    request_args=None,
                    request_kwargs={"i": query},
                    request_context=context.TaskContext.Python)))
        assert ray.get(result_token) == query


def test_ray_serve_mixin(serve_instance):
    q = CentralizedQueuesActor.remote()

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

    runner._ray_serve_setup.remote(CONSUMER_NAME, q)
    runner._ray_serve_main_loop.remote(runner)

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)

    for query in [333, 444, 555]:
        result_token = ray.ObjectID(
            ray.get(
                q.enqueue_request.remote(
                    PRODUCER_NAME,
                    request_args=None,
                    request_kwargs={"i": query},
                    request_context=context.TaskContext.Python)))
        assert ray.get(result_token) == query + 3


def test_task_runner_check_context(serve_instance):
    q = CentralizedQueuesActor.remote()

    def echo(flask_request, i=None):
        # Accessing the flask_request without web context should throw.
        return flask_request.args["i"]

    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "producer"

    runner = TaskRunnerActor.remote(echo)

    runner._ray_serve_setup.remote(CONSUMER_NAME, q)
    runner._ray_serve_main_loop.remote(runner)

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)
    result_token = ray.ObjectID(
        ray.get(
            q.enqueue_request.remote(
                PRODUCER_NAME,
                request_args=None,
                request_kwargs={"i": 42},
                request_context=context.TaskContext.Python)))

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(result_token)
