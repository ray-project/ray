import requests
import pytest
import ray
from ray import serve


@pytest.mark.asyncio
async def test_async_handle_serializable(serve_instance):
    @serve.deployment
    def f(_):
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
    def f(_):
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
        def __call__(self, starlette_request):
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


def test_handle_http_args(serve_instance):
    @serve.deployment
    class Endpoint:
        async def __call__(self, request):
            return {
                "args": dict(request.query_params),
                "headers": dict(request.headers),
                "method": request.method,
                "json": await request.json()
            }

    Endpoint.deploy()

    ground_truth = {
        "args": {
            "arg1": "1",
            "arg2": "2"
        },
        "headers": {
            "x-custom-header": "value"
        },
        "method": "POST",
        "json": {
            "json_key": "json_val"
        }
    }

    resp_web = requests.post(
        "http://127.0.0.1:8000/Endpoint?arg1=1&arg2=2",
        headers=ground_truth["headers"],
        json=ground_truth["json"]).json()

    handle = Endpoint.get_handle()
    resp_handle = ray.get(
        handle.options(
            http_method=ground_truth["method"],
            http_headers=ground_truth["headers"]).remote(
                ground_truth["json"], **ground_truth["args"]))

    for resp in [resp_web, resp_handle]:
        for field in ["args", "method", "json"]:
            assert resp[field] == ground_truth[field]
        resp["headers"]["x-custom-header"] == "value"


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

    for route in ["/echo", "/wrapper"]:
        resp = requests.get(f"http://127.0.0.1:8000{route}")
        request_type = resp.text
        assert request_type == "<class 'starlette.requests.Request'>"


def test_handle_option_chaining(serve_instance):
    # https://github.com/ray-project/ray/issues/12802
    # https://github.com/ray-project/ray/issues/12798

    @serve.deployment
    class MultiMethod:
        def method_a(self, _):
            return "method_a"

        def method_b(self, _):
            return "method_b"

        def __call__(self, _):
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


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
