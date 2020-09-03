import requests

import ray
from ray import serve


def test_handle_in_endpoint(serve_instance):
    serve.init()

    class Endpoint1:
        def __call__(self, flask_request):
            return "hello"

    class Endpoint2:
        def __init__(self):
            self.handle = serve.get_handle("endpoint1", missing_ok=True)

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
    serve.init()

    class Endpoint:
        def __init__(self):
            self.handle = serve.get_handle("endpoint1", missing_ok=True)

        def __call__(self, request):
            return {
                "args": dict(request.args),
                "headers": dict(request.headers),
                "method": request.method,
                "json": request.json
            }

    serve.create_backend("backend", Endpoint)
    serve.create_endpoint(
        "endpoint", backend="backend", route="/endpoint", methods=["POST"])

    grond_truth = {
        "args": {
            "arg1": "1",
            "arg2": "2"
        },
        "headers": {
            "X-Custom-Header": "value"
        },
        "method": "POST",
        "json": {
            "json_key": "json_val"
        }
    }

    resp_web = requests.post(
        "http://127.0.0.1:8000/endpoint?arg1=1&arg2=2",
        headers=grond_truth["headers"],
        json=grond_truth["json"]).json()

    handle = serve.get_handle("endpoint")
    resp_handle = ray.get(
        handle.options(
            http_method=grond_truth["method"],
            http_headers=grond_truth["headers"]).remote(
                grond_truth["json"], **grond_truth["args"]))

    for resp in [resp_web, resp_handle]:
        for field in ["args", "method", "json"]:
            assert resp[field] == grond_truth[field]
        resp["headers"]["X-Custom-Header"] == "value"


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
