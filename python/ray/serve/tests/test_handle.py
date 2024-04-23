import asyncio
import concurrent.futures
import threading

import pytest
import requests

import ray
from ray import serve
from ray.serve._private.common import RequestProtocol
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.exceptions import RayServeException
from ray.serve.handle import DeploymentHandle, _HandleOptions


def test_handle_options():
    default_options = _HandleOptions()
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.stream is False
    assert default_options._request_protocol == RequestProtocol.UNDEFINED

    # Test setting method name.
    only_set_method = default_options.copy_and_update(method_name="hi")
    assert only_set_method.method_name == "hi"
    assert only_set_method.multiplexed_model_id == ""
    assert only_set_method.stream is False
    assert default_options._request_protocol == RequestProtocol.UNDEFINED

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
    assert default_options._request_protocol == RequestProtocol.UNDEFINED

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
    assert default_options._request_protocol == RequestProtocol.UNDEFINED

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


def test_async_handle_serializable(serve_instance):
    @serve.deployment
    def f():
        return "hello"

    @ray.remote
    class DelegateActor:
        async def call_handle(self, handle):
            return await handle.remote()

    @serve.deployment
    class Ingress:
        def __init__(self, handle):
            self._handle = handle

        async def __call__(self):
            # Test pickling handle via `actor.method.remote()`.
            a = DelegateActor.remote()
            return await a.call_handle.remote(self._handle)

    app_handle = serve.run(Ingress.bind(f.bind()))
    assert app_handle.remote().result() == "hello"


def test_sync_handle_serializable(serve_instance):
    @serve.deployment
    def f():
        return "hello"

    handle = serve.run(f.bind())

    @ray.remote
    def task(handle):
        return handle.remote().result()

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
        handle = serve.get_deployment_handle(deploy._name, SERVE_DEFAULT_APP_NAME)
        return handle

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        fut = executor.submit(thread_get_handle, f)
        handle = fut.result()
        assert handle.remote().result() == "hello"


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
            return await self.handle.remote()

    end_p1 = Endpoint1.bind()
    end_p2 = Endpoint2.bind(end_p1)
    serve.run(end_p2)

    assert requests.get("http://127.0.0.1:8000/Endpoint2").text == "hello"


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
    counter = handle1.request_counter
    counter_info = counter.info
    assert handle1.remote().result() == "__call__"

    handle2 = handle1.options(method_name="method_a")

    assert handle2.remote().result() == "method_a"
    assert handle2.request_counter == counter
    assert handle2.request_counter.info == counter_info

    handle3 = handle1.options(method_name="method_b")

    assert handle3.remote().result() == "method_b"
    assert handle3.request_counter == counter
    assert handle2.request_counter.info == counter_info


def test_repeated_get_handle_cached(serve_instance):
    @serve.deployment
    def f(_):
        return ""

    serve.run(f.bind())

    handle_sets = {serve.get_app_handle("default") for _ in range(100)}
    assert len(handle_sets) == 1

    handle_sets = {serve.get_deployment_handle("f", "default") for _ in range(100)}
    assert len(handle_sets) == 1


def test_args_kwargs_sync(serve_instance):
    @serve.deployment
    async def f(*args, **kwargs):
        assert args[0] == "hi"
        assert kwargs["kwarg1"] == 1
        assert kwargs["kwarg2"] == "2"

    handle = serve.run(f.bind())
    handle.remote("hi", kwarg1=1, kwarg2="2").result()


@pytest.mark.asyncio
async def test_args_kwargs_async(serve_instance):
    @serve.deployment
    async def f(*args, **kwargs):
        assert args[0] == "hi"
        assert kwargs["kwarg1"] == 1
        assert kwargs["kwarg2"] == "2"

    handle = serve.run(f.bind())
    await handle.remote("hi", kwarg1=1, kwarg2="2")


def test_nonexistent_method_sync(serve_instance):
    @serve.deployment
    class A:
        def exists(self):
            pass

    handle = serve.run(A.bind())
    with pytest.raises(RayServeException) as excinfo:
        handle.does_not_exist.remote().result()

    exception_string = str(excinfo.value)
    assert "'does_not_exist'" in exception_string
    assert "Available methods: ['exists']" in exception_string


@pytest.mark.asyncio
async def test_nonexistent_method_async(serve_instance):
    @serve.deployment
    class A:
        def exists(self):
            pass

    handle = serve.run(A.bind())
    with pytest.raises(RayServeException) as excinfo:
        await handle.does_not_exist.remote()

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

    serve.run(A.bind())

    async def refresh_get():
        handle = serve.get_app_handle("default")
        assert await handle.exists.remote()

    for _ in range(10):
        loop = _get_asyncio_loop_running_in_thread()
        asyncio.run_coroutine_threadsafe(refresh_get(), loop).result()

    handle = serve.get_app_handle("default")

    async def cache_get():
        assert await handle.exists.remote()

    for _ in range(10):
        loop = _get_asyncio_loop_running_in_thread()
        asyncio.run_coroutine_threadsafe(cache_get(), loop).result()


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
            self, class_downstream: DeploymentHandle, func_downstream: DeploymentHandle
        ):
            # serve.run()'ing this deployment fails if these assertions fail.
            assert isinstance(class_downstream, DeploymentHandle)
            assert isinstance(func_downstream, DeploymentHandle)

    h = serve.run(Ingress.bind(DeploymentClass.bind(), deployment_func.bind()))
    assert isinstance(h, DeploymentHandle)


def test_call_function_with_argument(serve_instance):
    @serve.deployment
    def echo(name: str):
        return f"Hi {name}"

    @serve.deployment
    class Ingress:
        def __init__(self, h: DeploymentHandle):
            self._h = h

        async def __call__(self, name: str):
            return await self._h.remote(name)

    h = serve.run(Ingress.bind(echo.bind()))
    assert h.remote("sned").result() == "Hi sned"


def test_handle_options_with_same_router(serve_instance):
    """Make sure that multiple handles share same router object."""

    @serve.deployment
    def echo(name: str):
        return f"Hi {name}"

    handle = serve.run(echo.bind())
    handle2 = handle.options(multiplexed_model_id="model2")
    assert handle._router
    assert id(handle2._router) == id(handle._router)


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
