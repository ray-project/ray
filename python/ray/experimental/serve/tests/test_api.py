import time

import requests

import ray
from ray.experimental import serve


def test_e2e(serve_instance):
    serve.create_endpoint("endpoint", "/api")
    result = ray.get(
        serve.global_state.kv_store_actor_handle.list_service.remote())
    assert result == {"/api": "endpoint"}

    retry_count = 3
    while True:
        try:
            resp = requests.get("http://127.0.0.1:8000/").json()
            assert resp == result
            break
        except Exception:
            time.sleep(0.5)
            retry_count -= 1
            if retry_count == 0:
                assert False, "Route table hasn't been updated after 3 tries."

    def function(flask_request):
        return "OK"

    serve.create_backend(function, "echo:v1")
    serve.link("endpoint", "echo:v1")

    resp = requests.get("http://127.0.0.1:8000/api").json()["result"]
    assert resp == "OK"
