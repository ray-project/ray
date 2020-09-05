import requests

import ray
from ray import serve


def test_handle_in_endpoint(serve_instance):
    client = serve_instance

    class Endpoint1:
        def __call__(self, flask_request):
            return "hello"

    class Endpoint2:
        def __init__(self):
            client = serve.connect()
            self.handle = client.get_handle("endpoint1")

        def __call__(self, _):
            return ray.get(self.handle.remote())

    client.create_backend("endpoint1:v0", Endpoint1)
    client.create_endpoint(
        "endpoint1",
        backend="endpoint1:v0",
        route="/endpoint1",
        methods=["GET", "POST"])

    client.create_backend("endpoint2:v0", Endpoint2)
    client.create_endpoint(
        "endpoint2",
        backend="endpoint2:v0",
        route="/endpoint2",
        methods=["GET", "POST"])

    assert requests.get("http://127.0.0.1:8000/endpoint2").text == "hello"


def test_handle_http_args(serve_instance):
    client = serve_instance

    class Endpoint:
        def __call__(self, request):
            return {
                "args": dict(request.args),
                "headers": dict(request.headers),
                "method": request.method,
                "json": request.json
            }

    client.create_backend("backend", Endpoint)
    client.create_endpoint(
        "endpoint", backend="backend", route="/endpoint", methods=["POST"])

    ground_truth = {
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
        headers=ground_truth["headers"],
        json=ground_truth["json"]).json()

    handle = client.get_handle("endpoint")
    resp_handle = ray.get(
        handle.options(
            http_method=ground_truth["method"],
            http_headers=ground_truth["headers"]).remote(
                ground_truth["json"], **ground_truth["args"]))

    for resp in [resp_web, resp_handle]:
        for field in ["args", "method", "json"]:
            assert resp[field] == ground_truth[field]
        resp["headers"]["X-Custom-Header"] == "value"


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
