import requests
import pytest
import ray
from ray import serve


@pytest.mark.asyncio
async def test_async_handle_serializable(serve_instance):
    def f(_):
        return "hello"

    serve.create_backend("f", f)
    serve.create_endpoint("f", backend="f")

    @ray.remote
    class TaskActor:
        async def task(self, handle):
            ref = await handle.remote()
            output = await ref
            return output

    handle = serve.get_handle("f", sync=False)

    task_actor = TaskActor.remote()
    result = await task_actor.task.remote(handle)
    assert result == "hello"


def test_sync_handle_serializable(serve_instance):
    def f(_):
        return "hello"

    serve.create_backend("f", f)
    serve.create_endpoint("f", backend="f")

    @ray.remote
    def task(handle):
        return ray.get(handle.remote())

    handle = serve.get_handle("f", sync=True)
    result_ref = task.remote(handle)
    assert ray.get(result_ref) == "hello"


def test_handle_in_endpoint(serve_instance):
    class Endpoint1:
        def __call__(self, starlette_request):
            return "hello"

    class Endpoint2:
        def __init__(self):
            self.handle = serve.get_handle("endpoint1")

        def __call__(self, _):
            return ray.get(self.handle.remote())

    serve.create_backend("endpoint1:v0", Endpoint1)
    serve.create_endpoint(
        "endpoint1",
        backend="endpoint1:v0",
        route="/endpoint1",
        methods=["GET", "POST"])

    serve.create_backend("endpoint2:v0", Endpoint2)
    serve.create_endpoint(
        "endpoint2",
        backend="endpoint2:v0",
        route="/endpoint2",
        methods=["GET", "POST"])

    assert requests.get("http://127.0.0.1:8000/endpoint2").text == "hello"


def test_handle_http_args(serve_instance):
    class Endpoint:
        async def __call__(self, request):
            return {
                "args": dict(request.query_params),
                "headers": dict(request.headers),
                "method": request.method,
                "json": await request.json()
            }

    serve.create_backend("backend", Endpoint)
    serve.create_endpoint(
        "endpoint", backend="backend", route="/endpoint", methods=["POST"])

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
        "http://127.0.0.1:8000/endpoint?arg1=1&arg2=2",
        headers=ground_truth["headers"],
        json=ground_truth["json"]).json()

    handle = serve.get_handle("endpoint")
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
    def echo_request_type(request):
        return str(type(request))

    serve.create_backend("echo:v0", echo_request_type)
    serve.create_endpoint("echo", backend="echo:v0", route="/echo")

    def wrapper_model(web_request):
        handle = serve.get_handle("echo")
        return ray.get(handle.remote(web_request))

    serve.create_backend("wrapper:v0", wrapper_model)
    serve.create_endpoint("wrapper", backend="wrapper:v0", route="/wrapper")

    for route in ["/echo", "/wrapper"]:
        resp = requests.get(f"http://127.0.0.1:8000{route}")
        request_type = resp.text
        assert request_type == "<class 'starlette.requests.Request'>"


def test_handle_option_chaining(serve_instance):
    # https://github.com/ray-project/ray/issues/12802
    # https://github.com/ray-project/ray/issues/12798

    class MultiMethod:
        def method_a(self, _):
            return "method_a"

        def method_b(self, _):
            return "method_b"

        def __call__(self, _):
            return "__call__"

    serve.create_backend("m", MultiMethod)
    serve.create_endpoint("m", backend="m")

    # get_handle should give you a clean handle
    handle1 = serve.get_handle("m").options(method_name="method_a")
    handle2 = serve.get_handle("m")
    # options().options() override should work
    handle3 = handle1.options(method_name="method_b")

    assert ray.get(handle1.remote()) == "method_a"
    assert ray.get(handle2.remote()) == "__call__"
    assert ray.get(handle3.remote()) == "method_b"


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
