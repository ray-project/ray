import concurrent.futures
import asyncio
import pytest
from ray._private.utils import get_or_create_event_loop
import requests

import ray
from ray import serve
from ray.serve.exceptions import RayServeException


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
        handle = deploy.get_handle(sync=True)
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
    assert ray.get(handle1.remote()) == "__call__"

    handle2 = handle1.options(method_name="method_a")
    assert ray.get(handle2.remote()) == "method_a"

    handle3 = handle1.options(method_name="method_b")
    assert ray.get(handle3.remote()) == "method_b"


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


def test_handle_across_loops(serve_instance):
    @serve.deployment
    class A:
        def exists(self):
            return True

    A.deploy()

    async def refresh_get():
        handle = A.get_handle(sync=False)
        assert await (await handle.exists.remote())

    for _ in range(10):
        asyncio.set_event_loop(asyncio.new_event_loop())
        get_or_create_event_loop().run_until_complete(refresh_get())

    handle = A.get_handle(sync=False)

    async def cache_get():
        assert await (await handle.exists.remote())

    for _ in range(10):
        asyncio.set_event_loop(asyncio.new_event_loop())
        get_or_create_event_loop().run_until_complete(cache_get())


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
