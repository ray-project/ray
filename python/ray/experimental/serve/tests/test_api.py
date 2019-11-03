import time

import requests

from ray.experimental import serve


def test_e2e(serve_instance):
    serve.init()  # so we have access to global state
    serve.create_endpoint("endpoint", "/api", blocking=True)
    result = serve.api._get_global_state().route_table.list_service()
    assert result["/api"] == "endpoint"

    retry_count = 5
    timeout_sleep = 0.5
    while True:
        try:
            resp = requests.get("http://127.0.0.1:8000/").json()
            assert resp == result
            break
        except Exception:
            time.sleep(timeout_sleep)
            timeout_sleep *= 2
            retry_count -= 1
            if retry_count == 0:
                assert False, "Route table hasn't been updated after 3 tries."

    def function(flask_request):
        return "OK"

    serve.create_backend(function, "echo:v1")
    serve.link("endpoint", "echo:v1")

    resp = requests.get("http://127.0.0.1:8000/api").json()["result"]
    assert resp == "OK"


def test_scaling_replicas(serve_instance):
    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, _):
            self.count += 1
            return self.count

    serve.create_endpoint("counter", "/increment")

    # Keep checking the routing table until /increment is populated
    while "/increment" not in requests.get("http://127.0.0.1:8000/").json():
        time.sleep(0.2)

    serve.create_backend(Counter, "counter:v1")
    serve.link("counter", "counter:v1")

    serve.scale("counter:v1", 2)

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/increment").json()["result"]
        counter_result.append(resp)

    # If the load is shared among two replicas. The max result cannot be 10.
    assert max(counter_result) < 10

    serve.scale("counter:v1", 1)

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/increment").json()["result"]
        counter_result.append(resp)
    # Give some time for a replica to spin down. But majority of the request
    # should be served by the only remaining replica.
    assert max(counter_result) - min(counter_result) > 6
