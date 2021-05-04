import asyncio
import time
import os

import requests
import pytest
import starlette.responses

import ray
from ray import serve
from ray.test_utils import wait_for_condition


def test_e2e(serve_instance):
    @serve.deployment(name="api")
    def function(starlette_request):
        return {"method": starlette_request.method}

    function.deploy()

    resp = requests.get("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "GET"

    resp = requests.post("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "POST"


def test_starlette_response(serve_instance):
    @serve.deployment(name="basic")
    def basic(_):
        return starlette.responses.Response(
            "Hello, world!", media_type="text/plain")

    basic.deploy()
    assert requests.get("http://127.0.0.1:8000/basic").text == "Hello, world!"

    @serve.deployment(name="html")
    def html(_):
        return starlette.responses.HTMLResponse(
            "<html><body><h1>Hello, world!</h1></body></html>")

    html.deploy()
    assert requests.get(
        "http://127.0.0.1:8000/html"
    ).text == "<html><body><h1>Hello, world!</h1></body></html>"

    @serve.deployment(name="plain_text")
    def plain_text(_):
        return starlette.responses.PlainTextResponse("Hello, world!")

    plain_text.deploy()
    assert requests.get(
        "http://127.0.0.1:8000/plain_text").text == "Hello, world!"

    @serve.deployment(name="json")
    def json(_):
        return starlette.responses.JSONResponse({"hello": "world"})

    json.deploy()
    assert requests.get("http://127.0.0.1:8000/json").json()[
        "hello"] == "world"

    @serve.deployment(name="redirect")
    def redirect(_):
        return starlette.responses.RedirectResponse(
            url="http://127.0.0.1:8000/basic")

    redirect.deploy()
    assert requests.get(
        "http://127.0.0.1:8000/redirect").text == "Hello, world!"

    @serve.deployment(name="streaming")
    def streaming(_):
        async def slow_numbers():
            for number in range(1, 4):
                yield str(number)
                await asyncio.sleep(0.01)

        return starlette.responses.StreamingResponse(
            slow_numbers(), media_type="text/plain", status_code=418)

    streaming.deploy()
    resp = requests.get("http://127.0.0.1:8000/streaming")
    assert resp.text == "123"
    assert resp.status_code == 418


def test_backend_user_config(serve_instance):
    @serve.deployment(
        "counter", num_replicas=2, user_config={
            "count": 123,
            "b": 2
        })
    class Counter:
        def __init__(self):
            self.count = 10

        def __call__(self, *args):
            return self.count, os.getpid()

        def reconfigure(self, config):
            self.count = config["count"]

    Counter.deploy()
    handle = Counter.get_handle()

    def check(val, num_replicas):
        pids_seen = set()
        for i in range(100):
            result = ray.get(handle.remote())
            if str(result[0]) != val:
                return False
            pids_seen.add(result[1])
        return len(pids_seen) == num_replicas

    wait_for_condition(lambda: check("123", 2))

    Counter = Counter.options(num_replicas=3)
    Counter.deploy()
    wait_for_condition(lambda: check("123", 3))

    Counter = Counter.options(user_config={"count": 456})
    Counter.deploy()
    wait_for_condition(lambda: check("456", 3))


def test_call_method(serve_instance):
    @serve.deployment(name="method")
    class CallMethod:
        def method(self, *args):
            return "hello"

    CallMethod.deploy()

    # Test HTTP path.
    resp = requests.get(
        "http://127.0.0.1:8000/method",
        timeout=1,
        headers={"X-SERVE-CALL-METHOD": "method"})
    assert resp.text == "hello"

    # Test serve handle path.
    handle = CallMethod.get_handle()
    assert ray.get(handle.options(method_name="method").remote()) == "hello"


def test_reject_duplicate_route(serve_instance):
    @serve.deployment(name="A", route_prefix="/api")
    class A:
        pass

    A.deploy()
    with pytest.raises(ValueError):
        A.options(name="B").deploy()


def test_scaling_replicas(serve_instance):
    @serve.deployment(name="counter", num_replicas=2)
    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, _):
            self.count += 1
            return self.count

    Counter.deploy()

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/counter").json()
        counter_result.append(resp)

    # If the load is shared among two replicas. The max result cannot be 10.
    assert max(counter_result) < 10

    Counter.options(num_replicas=1).deploy()

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/counter").json()
        counter_result.append(resp)
    # Give some time for a replica to spin down. But majority of the request
    # should be served by the only remaining replica.
    assert max(counter_result) - min(counter_result) > 6


def test_delete_backend(serve_instance):
    @serve.deployment(name="delete")
    def function(_):
        return "hello"

    function.deploy()

    assert requests.get("http://127.0.0.1:8000/delete").text == "hello"

    function.delete()

    @serve.deployment(name="delete")
    def function2(_):
        return "olleh"

    function2.deploy()

    for _ in range(10):
        try:
            assert requests.get("http://127.0.0.1:8000/delete").text == "olleh"
            break
        except AssertionError:
            time.sleep(0.5)  # Wait for the change to propagate.
    else:
        assert requests.get("http://127.0.0.1:8000/delete").text == "olleh"


def test_starlette_request(serve_instance):
    @serve.deployment(name="api")
    async def echo_body(starlette_request):
        data = await starlette_request.body()
        return data

    echo_body.deploy()

    # Long string to test serialization of multiple messages.
    UVICORN_HIGH_WATER_MARK = 65536  # max bytes in one message
    long_string = "x" * 10 * UVICORN_HIGH_WATER_MARK

    resp = requests.post("http://127.0.0.1:8000/api", data=long_string).text
    assert resp == long_string


def test_start_idempotent(serve_instance):
    @serve.deployment(name="start")
    def func(*args):
        pass

    func.deploy()

    assert "start" in serve.list_backends()
    serve.start(detached=True)
    serve.start()
    serve.start(detached=True)
    serve.start()
    assert "start" in serve.list_backends()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
