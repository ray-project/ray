import asyncio
import concurrent.futures
import threading

import pytest
import requests

import ray

from ray import serve
from ray.serve.exceptions import RayServeException
from ray.serve.handle import HandleOptions, RayServeHandle, RayServeSyncHandle
from ray.serve._private.router import PowerOfTwoChoicesReplicaScheduler
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve._private.common import RequestProtocol


def test_handle_options():
    default_options = HandleOptions()
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.stream is False
    assert default_options._request_protocol == RequestProtocol.UNDEFINED

    # Test setting method name.
    only_set_method = default_options.copy_and_update(method_name="hi")
    assert only_set_method.method_name == "hi"
    assert only_set_method.multiplexed_model_id == ""
    assert only_set_method.stream is False

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.stream is False
    assert default_options._request_protocol == RequestProtocol.UNDEFINED

    # Test setting model ID.
    only_set_model_id = default_options.copy_and_update(multiplexed_model_id="hi")
    assert only_set_model_id.method_name == "__call__"
    assert only_set_model_id.multiplexed_model_id == "hi"
    assert only_set_model_id.stream is False

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.stream is False
    assert default_options._request_protocol == RequestProtocol.UNDEFINED

    # Test setting stream.
    only_set_stream = default_options.copy_and_update(stream=True)
    assert only_set_stream.method_name == "__call__"
    assert only_set_stream.multiplexed_model_id == ""
    assert only_set_stream.stream is True

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.stream is False
    assert default_options._request_protocol == RequestProtocol.UNDEFINED

    # Test setting multiple.
    set_multiple = default_options.copy_and_update(method_name="hi", stream=True)
    assert set_multiple.method_name == "hi"
    assert set_multiple.multiplexed_model_id == ""
    assert set_multiple.stream is True
    assert default_options._request_protocol == RequestProtocol.UNDEFINED


@pytest.mark.asyncio
async def test_async_handle_serializable(serve_instance):
    @serve.deployment
    def f():
        return "hello"

    f.deploy()

    @ray.remote
    class TaskActor:
        async def task(self, handle):
            ref = await handle.remote()
            output = await ref
            return output

    # Test pickling via ray.remote()
    handle = f.get_handle(sync=False)

    task_actor = TaskActor.remote()
    result = await task_actor.task.remote(handle)
    assert result == "hello"


def test_sync_handle_serializable(serve_instance):
    @serve.deployment
    def f():
        return "hello"

    handle = serve.run(f.bind())

    @ray.remote
    def task(handle):
        return ray.get(handle.remote())

    # Test pickling via ray.remote()
    result_ref = task.remote(handle)
    assert ray.get(result_ref) == "hello"


def test_handle_serializable_in_deployment_init(serve_instance):
    """Test that a handle can be passed into a constructor (#22110)"""

    @serve.deployment
    class RayServer1:
        def __init__(self):
            pass

        def __call__(self, *args):
            return {"count": self.count}

    @serve.deployment
    class RayServer2:
        def __init__(self, handle):
            self.handle = handle

        def __call__(self, *args):
            return {"count": self.count}

    rs1 = RayServer1.bind()
    rs2 = RayServer2.bind(rs1)
    serve.run(rs2)


def test_sync_handle_in_thread(serve_instance):
    @serve.deployment
    def f():
        return "hello"

    handle = serve.run(f.bind())

    def thread_get_handle(deploy):
        handle = serve.get_deployment_handle(
            deploy._name, SERVE_DEFAULT_APP_NAME, sync=True
        )
        return handle

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        fut = executor.submit(thread_get_handle, f)
        handle = fut.result()
        assert ray.get(handle.remote()) == "hello"


def test_handle_in_endpoint(serve_instance):
    @serve.deployment
    class Endpoint1:
        def __call__(self, *args):
            return "hello"

    @serve.deployment
    class Endpoint2:
        def __init__(self, handle):
            self.handle = handle

        async def __call__(self, _):
            return await (await self.handle.remote())

    end_p1 = Endpoint1.bind()
    end_p2 = Endpoint2.bind(end_p1)
    serve.run(end_p2)

    assert requests.get("http://127.0.0.1:8000/Endpoint2").text == "hello"


@pytest.mark.skipif(
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING, reason="Not supported w/ streaming."
)
def test_handle_inject_starlette_request(serve_instance):
    @serve.deployment(name="echo")
    def echo_request_type(request):
        return str(type(request))

    echo_request_type.deploy()

    @serve.deployment(name="wrapper")
    def wrapper_model(web_request):
        handle = echo_request_type.get_handle()
        return ray.get(handle.remote(web_request))

    wrapper_model.deploy()

    for route in ["echo", "wrapper"]:
        resp = requests.get(f"http://127.0.0.1:8000/{route}")
        request_type = resp.text
        assert request_type == "<class 'starlette.requests.Request'>"


def test_handle_option_chaining(serve_instance):
    # https://github.com/ray-project/ray/issues/12802
    # https://github.com/ray-project/ray/issues/12798

    @serve.deployment
    class MultiMethod:
        def method_a(self):
            return "method_a"

        def method_b(self):
            return "method_b"

        def __call__(self):
            return "__call__"

    handle1 = serve.run(MultiMethod.bind())
    metrics = handle1.request_counter
    assert ray.get(handle1.remote()) == "__call__"

    handle2 = handle1.options(method_name="method_a")
    assert ray.get(handle2.remote()) == "method_a"
    assert handle2.request_counter == metrics

    handle3 = handle1.options(method_name="method_b")
    assert ray.get(handle3.remote()) == "method_b"
    assert handle3.request_counter == metrics


def test_repeated_get_handle_cached(serve_instance):
    @serve.deployment
    def f(_):
        return ""

    f.deploy()

    handle_sets = {f.get_handle() for _ in range(100)}
    assert len(handle_sets) == 1

    handle_sets = {serve.get_deployment("f").get_handle() for _ in range(100)}
    assert len(handle_sets) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [True, False])
async def test_args_kwargs(serve_instance, sync):
    @serve.deployment
    async def f(*args, **kwargs):
        assert args[0] == "hi"
        assert kwargs["kwarg1"] == 1
        assert kwargs["kwarg2"] == "2"

    f.deploy()

    handle = f.get_handle(sync=sync)

    def call():
        return handle.remote("hi", kwarg1=1, kwarg2="2")

    if sync:
        obj_ref = call()
    else:
        obj_ref = await call()

    ray.get(obj_ref)


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [True, False])
async def test_nonexistent_method(serve_instance, sync):
    @serve.deployment
    class A:
        def exists(self):
            pass

    A.deploy()
    handle = A.get_handle(sync=sync)

    if sync:
        obj_ref = handle.does_not_exist.remote()
    else:
        obj_ref = await handle.does_not_exist.remote()

    with pytest.raises(RayServeException) as excinfo:
        ray.get(obj_ref)

    exception_string = str(excinfo.value)
    assert "'does_not_exist'" in exception_string
    assert "Available methods: ['exists']" in exception_string


def _get_asyncio_loop_running_in_thread() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    threading.Thread(
        daemon=True,
        target=loop.run_forever,
    ).start()
    return loop


@pytest.mark.asyncio
async def test_handle_across_loops(serve_instance):
    @serve.deployment
    class A:
        def exists(self):
            return True

    A.deploy()

    async def refresh_get():
        handle = A.get_handle(sync=False)
        assert await (await handle.exists.remote())

    for _ in range(10):
        loop = _get_asyncio_loop_running_in_thread()
        asyncio.run_coroutine_threadsafe(refresh_get(), loop).result()

    handle = A.get_handle(sync=False)

    async def cache_get():
        assert await (await handle.exists.remote())

    for _ in range(10):
        loop = _get_asyncio_loop_running_in_thread()
        asyncio.run_coroutine_threadsafe(refresh_get(), loop).result()


def test_handle_typing(serve_instance):
    @serve.deployment
    class DeploymentClass:
        pass

    @serve.deployment
    def deployment_func():
        pass

    @serve.deployment
    class Ingress:
        def __init__(
            self, class_downstream: RayServeHandle, func_downstream: RayServeHandle
        ):
            # serve.run()'ing this deployment fails if these assertions fail.
            assert isinstance(class_downstream, RayServeHandle)
            assert isinstance(func_downstream, RayServeHandle)

    h = serve.run(Ingress.bind(DeploymentClass.bind(), deployment_func.bind()))
    assert isinstance(h, RayServeSyncHandle)


def test_call_function_with_argument(serve_instance):
    @serve.deployment
    def echo(name: str):
        return f"Hi {name}"

    @serve.deployment
    class Ingress:
        def __init__(self, h: RayServeHandle):
            self._h = h

        async def __call__(self, name: str):
            return await (await self._h.remote(name))

    h = serve.run(Ingress.bind(echo.bind()))
    assert ray.get(h.remote("sned")) == "Hi sned"


def test_handle_options_with_same_router(serve_instance):
    """Make sure that multiple handles share same router object."""

    @serve.deployment
    def echo(name: str):
        return f"Hi {name}"

    handle = serve.run(echo.bind())
    handle2 = handle.options(multiplexed_model_id="model2")
    assert handle._router
    assert id(handle2._router) == id(handle._router)


class MyRouter(PowerOfTwoChoicesReplicaScheduler):
    pass


def test_handle_options_custom_router(serve_instance):
    @serve.deployment
    def echo(name: str):
        return f"Hi {name}"

    handle = serve.run(echo.bind())
    handle2 = handle.options(_router_cls="ray.serve.tests.test_handle.MyRouter")
    ray.get(handle2.remote("HI"))
    print("Router class used", handle2._router._replica_scheduler)
    assert (
        "MyRouter" in handle2._router._replica_scheduler.__class__.__name__
    ), handle2._router._replica_scheduler


def test_set_request_protocol(serve_instance):
    """Test setting request protocol for a handle.

    When a handle is created, it's _request_protocol is undefined. When calling
    `_set_request_protocol()`, _request_protocol is set to the specified protocol.
    When chaining options, the _request_protocol on the new handle is copied over.
    When calling `_set_request_protocol()` on the new handle, _request_protocol
    on the new handle is changed accordingly, while _request_protocol on the
    original handle remains unchanged.
    """

    @serve.deployment
    def echo(name: str):
        return f"Hi {name}"

    handle = serve.run(echo.bind())
    assert handle.handle_options._request_protocol == RequestProtocol.UNDEFINED

    handle._set_request_protocol(RequestProtocol.HTTP)
    assert handle.handle_options._request_protocol == RequestProtocol.HTTP

    multiplexed_model_id = "fake-multiplexed_model_id"
    new_handle = handle.options(multiplexed_model_id=multiplexed_model_id)
    assert new_handle.handle_options.multiplexed_model_id == multiplexed_model_id
    assert new_handle.handle_options._request_protocol == RequestProtocol.HTTP

    new_handle._set_request_protocol(RequestProtocol.GRPC)
    assert new_handle.handle_options._request_protocol == RequestProtocol.GRPC
    assert handle.handle_options._request_protocol == RequestProtocol.HTTP


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
