import asyncio

import pytest
import numpy as np

import ray
from ray import serve
from ray.serve.backend_worker import create_backend_replica, wrap_to_ray_error
from ray.serve.controller import TrafficPolicy
from ray.serve.router import RequestMetadata, Router
from ray.serve.config import BackendConfig, BackendMetadata
from ray.serve.exceptions import RayServeException
from ray.serve.utils import get_random_letters

pytestmark = pytest.mark.asyncio


def setup_worker(name,
                 backend_def,
                 init_args=None,
                 backend_config=BackendConfig(),
                 controller_name=""):
    if init_args is None:
        init_args = ()

    @ray.remote
    class WorkerActor:
        def __init__(self):
            self.worker = create_backend_replica(backend_def)(
                name, name + ":tag", init_args, backend_config,
                controller_name)

        def ready(self):
            pass

        @ray.method(num_returns=2)
        async def handle_request(self, *args, **kwargs):
            return await self.worker.handle_request(*args, **kwargs)

        def update_config(self, new_config):
            return self.worker.update_config(new_config)

        async def drain_pending_queries(self):
            return await self.worker.drain_pending_queries()

    worker = WorkerActor.remote()
    ray.get(worker.ready.remote())
    return worker


async def add_servable_to_router(
        servable,
        controller_name,
        controller_actor,
        **kwargs,
):
    worker = setup_worker(
        "backend", servable, controller_name=controller_name, **kwargs)
    await controller_actor.set_traffic.remote(
        "endpoint",
        TrafficPolicy({
            "backend": 1.0
        }),
    )
    await controller_actor.add_new_replica.remote(
        "backend", worker, kwargs.get("backend_config", BackendConfig()))

    router = Router(
        controller_actor,
        "endpoint",
        asyncio.get_event_loop(),
    )
    return worker, router


def make_request_param(call_method="__call__"):
    return RequestMetadata(
        get_random_letters(10), "endpoint", call_method=call_method)


async def test_runner_wraps_error():
    wrapped = wrap_to_ray_error("test_function", Exception())
    assert isinstance(wrapped, ray.exceptions.RayTaskError)


async def test_servable_function(serve_instance, mock_controller_with_name):
    def echo(request):
        return request.query_params["i"]

    worker, router = await add_servable_to_router(echo,
                                                  *mock_controller_with_name)

    for query in [333, 444, 555]:
        query_param = make_request_param()
        result = await (await router.assign_request(query_param, i=query))
        assert result == query


async def test_servable_class(serve_instance, mock_controller_with_name):
    class MyAdder:
        def __init__(self, inc):
            self.increment = inc

        def __call__(self, request):
            return request.query_params["i"] + self.increment

    worker, router = await add_servable_to_router(
        MyAdder, *mock_controller_with_name, init_args=(3, ))

    for query in [333, 444, 555]:
        query_param = make_request_param()
        result = await (await router.assign_request(query_param, i=query))
        assert result == query + 3


async def test_task_runner_custom_method_single(serve_instance,
                                                mock_controller_with_name):
    class NonBatcher:
        def a(self, _):
            return "a"

        def b(self, _):
            return "b"

    worker, router = await add_servable_to_router(NonBatcher,
                                                  *mock_controller_with_name)

    query_param = make_request_param("a")
    a_result = await (await router.assign_request(query_param))
    assert a_result == "a"

    query_param = make_request_param("b")
    b_result = await (await router.assign_request(query_param))
    assert b_result == "b"

    query_param = make_request_param("non_exist")
    with pytest.raises(ray.exceptions.RayTaskError):
        await (await router.assign_request(query_param))


async def test_task_runner_custom_method_batch(serve_instance,
                                               mock_controller_with_name):
    @serve.accept_batch
    class Batcher:
        def a(self, requests):
            return ["a-{}".format(i) for i in range(len(requests))]

        def b(self, requests):
            return ["b-{}".format(i) for i in range(len(requests))]

    backend_config = BackendConfig(
        max_batch_size=4,
        batch_wait_timeout=10,
        internal_metadata=BackendMetadata(accepts_batches=True))
    worker, router = await add_servable_to_router(
        Batcher, *mock_controller_with_name, backend_config=backend_config)

    a_query_param = make_request_param("a")
    b_query_param = make_request_param("b")

    futures = [(await router.assign_request(a_query_param)) for _ in range(2)]
    futures += [(await router.assign_request(b_query_param)) for _ in range(2)]

    gathered = await asyncio.gather(*futures)
    assert set(gathered) == {"a-0", "a-1", "b-0", "b-1"}


async def test_servable_batch_error(serve_instance, mock_controller_with_name):
    @serve.accept_batch
    class ErrorBatcher:
        def error_different_size(self, requests):
            return [""] * (len(requests) + 10)

        def error_non_iterable(self, _):
            return 42

        def return_np_array(self, requests):
            return np.array([1] * len(requests)).astype(np.int32)

    backend_config = BackendConfig(
        max_batch_size=4,
        internal_metadata=BackendMetadata(accepts_batches=True))
    worker, router = await add_servable_to_router(
        ErrorBatcher,
        *mock_controller_with_name,
        backend_config=backend_config)

    with pytest.raises(RayServeException, match="doesn't preserve batch size"):
        different_size = make_request_param("error_different_size")
        await (await router.assign_request(different_size))

    with pytest.raises(RayServeException, match="iterable"):
        non_iterable = make_request_param("error_non_iterable")
        await (await router.assign_request(non_iterable))

    np_array = make_request_param("return_np_array")
    result_np_value = await (await router.assign_request(np_array))
    assert isinstance(result_np_value, np.int32)


async def test_task_runner_perform_batch(serve_instance,
                                         mock_controller_with_name):
    def batcher(requests):
        batch_size = len(requests)
        return [batch_size] * batch_size

    config = BackendConfig(
        max_batch_size=2,
        batch_wait_timeout=10,
        internal_metadata=BackendMetadata(accepts_batches=True))

    worker, router = await add_servable_to_router(
        batcher, *mock_controller_with_name, backend_config=config)

    query_param = make_request_param()
    my_batch_sizes = await asyncio.gather(*[(
        await router.assign_request(query_param)) for _ in range(3)])
    assert my_batch_sizes == [2, 2, 1]


async def test_task_runner_perform_async(serve_instance,
                                         mock_controller_with_name):
    @ray.remote
    class Barrier:
        def __init__(self, release_on):
            self.release_on = release_on
            self.current_waiters = 0
            self.event = asyncio.Event()

        async def wait(self):
            self.current_waiters += 1
            if self.current_waiters == self.release_on:
                self.event.set()
            else:
                await self.event.wait()

    barrier = Barrier.remote(release_on=10)

    async def wait_and_go(*args, **kwargs):
        await barrier.wait.remote()
        return "done!"

    config = BackendConfig(
        max_concurrent_queries=10,
        internal_metadata=BackendMetadata(is_blocking=False))

    worker, router = await add_servable_to_router(
        wait_and_go, *mock_controller_with_name, backend_config=config)

    query_param = make_request_param()

    done, not_done = await asyncio.wait(
        [(await router.assign_request(query_param)) for _ in range(10)],
        timeout=10)
    assert len(done) == 10
    for item in done:
        assert await item == "done!"


async def test_user_config_update(serve_instance, mock_controller_with_name):
    class Customizable:
        def __init__(self):
            self.reval = ""

        def __call__(self, starlette_request):
            return self.retval

        def reconfigure(self, config):
            self.retval = config["return_val"]

    config = BackendConfig(
        num_replicas=2, user_config={
            "return_val": "original",
            "b": 2
        })
    worker, router = await add_servable_to_router(
        Customizable, *mock_controller_with_name, backend_config=config)

    query_param = make_request_param()

    done = [(await router.assign_request(query_param)) for _ in range(10)]
    for i in done:
        assert await i == "original"

    config = BackendConfig()
    config.user_config = {"return_val": "new_val"}
    await mock_controller_with_name[1].update_backend.remote("backend", config)

    async def new_val_returned():
        result = await (await router.assign_request(query_param))
        assert "new_val" == result

    for _ in range(10):
        try:
            await new_val_returned()
        except AssertionError:
            # Wait for config to propogate
            await asyncio.sleep(0.5)
    new_val_returned()


async def test_graceful_shutdown(serve_instance, mock_controller_with_name):
    class KeepInflight:
        def __init__(self):
            self.events = []

        def reconfigure(self, config):
            if config["release"]:
                [event.set() for event in self.events]

        async def __call__(self, _):
            e = asyncio.Event()
            self.events.append(e)
            await e.wait()

    backend_worker, router = await add_servable_to_router(
        KeepInflight,
        *mock_controller_with_name,
        backend_config=BackendConfig(
            num_replicas=1,
            internal_metadata=BackendMetadata(is_blocking=False),
            user_config={"release": False}))

    query_param = make_request_param()

    refs = [(await router.assign_request(query_param)) for _ in range(6)]

    shutdown_ref = backend_worker.drain_pending_queries.remote()

    with pytest.raises(ray.exceptions.GetTimeoutError):
        # Shutdown should block because there are still inflight queries.
        ray.get(shutdown_ref, timeout=2)

    config = BackendConfig()
    config.user_config = {"release": True}
    await mock_controller_with_name[1].update_backend.remote("backend", config)

    # All queries should complete successfully
    ray.get(refs)
    # The draining operation should be completed.
    ray.get(shutdown_ref)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
