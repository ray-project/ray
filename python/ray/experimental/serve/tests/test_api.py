import time

import requests

import ray
from ray.experimental import serve


def test_e2e(serve_instance):
    serve.create_endpoint("endpoint", "/api")
    result = ray.get(
        serve.global_state.kv_store_actor_handle.list_service.remote())
    assert result == {"/api": "endpoint"}

    time.sleep(1)
    assert requests.get("http://127.0.0.1:8000/").json() == result

    def function(flask_request):
        return "OK"

    serve.create_backend(function, "echo:v1")
    serve.link("endpoint", "echo:v1")

    resp = requests.get("http://127.0.0.1:8000/api").json()["result"]
    assert resp == "OK"
