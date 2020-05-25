import ray
from ray import serve

import requests


def test_handle_in_endpoint(serve_instance):
    serve.init()

    class Endpoint1:
        def __call__(self, flask_request):
            return "hello"

    class Endpoint2:
        def __init__(self):
            self.handle = serve.get_handle("endpoint1", missing_ok=True)

        def __call__(self):
            return ray.get(self.handle.remote())

    serve.create_endpoint("endpoint1", "/endpoint1", methods=["GET", "POST"])
    serve.create_backend("endpoint1:v0", Endpoint1)
    serve.set_traffic("endpoint1", {"endpoint1:v0": 1.0})

    serve.create_endpoint("endpoint2", "/endpoint2", methods=["GET", "POST"])
    serve.create_backend("endpoint2:v0", Endpoint2)
    serve.set_traffic("endpoint2", {"endpoint2:v0": 1.0})

    assert requests.get("http://127.0.0.1:8000/endpoint2").text == "hello"
