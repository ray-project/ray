import ray
from ray.experimental.serve.queues import CentralizedQueuesActor
from ray.experimental.serve.task_runner import (
    RayServeMixin,
    TaskRunner,
    TaskRunnerActor,
    wrap_to_ray_error,
)


def test_runner_basic():
    def echo(i):
        return i

    r = TaskRunner(echo)
    assert r(1) == 1


def test_runner_wraps_error():
    def echo(i):
        return i

    assert wrap_to_ray_error(echo, 2) == 2

    def error(_):
        return 1 / 0

    assert isinstance(wrap_to_ray_error(error, 1), ray.exceptions.RayTaskError)


def test_runner_actor(serve_instance):
    q = CentralizedQueuesActor.remote()

    def echo(i):
        return i

    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "prod"

    runner = TaskRunnerActor.remote(echo)

    runner._ray_serve_setup.remote(CONSUMER_NAME, q)
    runner._ray_serve_main_loop.remote(runner)

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)

    for query in [333, 444, 555]:
        result_token = ray.ObjectID(
            ray.get(q.enqueue_request.remote(PRODUCER_NAME, query)))
        assert ray.get(result_token) == query


def test_ray_serve_mixin(serve_instance):
    q = CentralizedQueuesActor.remote()

    CONSUMER_NAME = "runner-cls"
    PRODUCER_NAME = "prod-cls"

    class MyAdder:
        def __init__(self, inc):
            self.increment = inc

        def __call__(self, context):
            return context + self.increment

    @ray.remote
    class CustomActor(MyAdder, RayServeMixin):
        pass

    runner = CustomActor.remote(3)

    runner._ray_serve_setup.remote(CONSUMER_NAME, q)
    runner._ray_serve_main_loop.remote(runner)

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)

    for query in [333, 444, 555]:
        result_token = ray.ObjectID(
            ray.get(q.enqueue_request.remote(PRODUCER_NAME, query)))
        assert ray.get(result_token) == query + 3
