import time

import requests
from flaky import flaky

import ray
from ray.experimental import serve


def delay_rerun(*_):
    time.sleep(1)
    return True


# flaky test because the routing table might not be populated
@flaky(rerun_filter=delay_rerun)
def test_e2e(serve_instance):
    serve.create_endpoint("endpoint", "/api")
    result = ray.get(
        serve.global_state.kv_store_actor_handle.list_service.remote())
    assert result == {"/api": "endpoint"}

    assert requests.get("http://127.0.0.1:8000/").json() == result

    def echo(i):
        return i

    serve.create_backend(echo, "echo:v1")
    serve.link("endpoint", "echo:v1")

    resp = requests.get("http://127.0.0.1:8000/api").json()["result"]
    assert resp["path"] == "/api"
    assert resp["method"] == "GET"
