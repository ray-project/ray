import pytest
import requests

import ray
from ray import serve


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

    handle = f.get_handle(sync=False)

    task_actor = TaskActor.remote()
    result = await task_actor.task.remote(handle)
    assert result == "hello"


def test_sync_handle_serializable(serve_instance):
    @serve.deployment
    def f():
        return "hello"

    f.deploy()

    @ray.remote
    def task(handle):
        return ray.get(handle.remote())

    handle = f.get_handle(sync=True)
    result_ref = task.remote(handle)
    assert ray.get(result_ref) == "hello"


def test_handle_in_endpoint(serve_instance):
    @serve.deployment
    class Endpoint1:
        def __call__(self, *args):
            return "hello"

    @serve.deployment
    class Endpoint2:
        def __init__(self):
            self.handle = Endpoint1.get_handle()

        def __call__(self, _):
            return ray.get(self.handle.remote())

    Endpoint1.deploy()
    Endpoint2.deploy()

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

    MultiMethod.deploy()

    # get_handle should give you a clean handle
    handle1 = MultiMethod.get_handle().options(method_name="method_a")
    handle2 = MultiMethod.get_handle()
    # options().options() override should work
    handle3 = handle1.options(method_name="method_b")

    assert ray.get(handle1.remote()) == "method_a"
    assert ray.get(handle2.remote()) == "__call__"
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
async def test_args_kwargs(sync):
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


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
