import asyncio
from collections import defaultdict
import time
import os

import requests
import pytest
import starlette.responses

import ray
from ray import serve
from ray.test_utils import wait_for_condition
from ray.serve.config import BackendConfig
from ray.serve.utils import get_random_letters


def test_e2e(serve_instance):
    def function(starlette_request):
        return {"method": starlette_request.method}

    serve.create_backend("echo:v1", function)
    serve.create_endpoint(
        "endpoint", backend="echo:v1", route="/api", methods=["GET", "POST"])

    resp = requests.get("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "GET"

    resp = requests.post("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "POST"


def test_starlette_response(serve_instance):
    def basic_response(_):
        return starlette.responses.Response(
            "Hello, world!", media_type="text/plain")

    serve.create_backend("basic_response", basic_response)
    serve.create_endpoint(
        "basic_response", backend="basic_response", route="/basic_response")
    assert requests.get(
        "http://127.0.0.1:8000/basic_response").text == "Hello, world!"

    def html_response(_):
        return starlette.responses.HTMLResponse(
            "<html><body><h1>Hello, world!</h1></body></html>")

    serve.create_backend("html_response", html_response)
    serve.create_endpoint(
        "html_response", backend="html_response", route="/html_response")
    assert requests.get(
        "http://127.0.0.1:8000/html_response"
    ).text == "<html><body><h1>Hello, world!</h1></body></html>"

    def plain_text_response(_):
        return starlette.responses.PlainTextResponse("Hello, world!")

    serve.create_backend("plain_text_response", plain_text_response)
    serve.create_endpoint(
        "plain_text_response",
        backend="plain_text_response",
        route="/plain_text_response")
    assert requests.get(
        "http://127.0.0.1:8000/plain_text_response").text == "Hello, world!"

    def json_response(_):
        return starlette.responses.JSONResponse({"hello": "world"})

    serve.create_backend("json_response", json_response)
    serve.create_endpoint(
        "json_response", backend="json_response", route="/json_response")
    assert requests.get("http://127.0.0.1:8000/json_response").json()[
        "hello"] == "world"

    def redirect_response(_):
        return starlette.responses.RedirectResponse(
            url="http://127.0.0.1:8000/basic_response")

    serve.create_backend("redirect_response", redirect_response)
    serve.create_endpoint(
        "redirect_response",
        backend="redirect_response",
        route="/redirect_response")
    assert requests.get(
        "http://127.0.0.1:8000/redirect_response").text == "Hello, world!"

    def streaming_response(_):
        async def slow_numbers():
            for number in range(1, 4):
                yield str(number)
                await asyncio.sleep(0.01)

        return starlette.responses.StreamingResponse(
            slow_numbers(), media_type="text/plain", status_code=418)

    serve.create_backend("streaming_response", streaming_response)
    serve.create_endpoint(
        "streaming_response",
        backend="streaming_response",
        route="/streaming_response")
    resp = requests.get("http://127.0.0.1:8000/streaming_response")
    assert resp.text == "123"
    assert resp.status_code == 418


def test_backend_user_config(serve_instance):
    class Counter:
        def __init__(self):
            self.count = 10

        def __call__(self, starlette_request):
            return self.count, os.getpid()

        def reconfigure(self, config):
            self.count = config["count"]

    config = BackendConfig(num_replicas=2, user_config={"count": 123, "b": 2})
    serve.create_backend("counter", Counter, config=config)
    serve.create_endpoint("counter", backend="counter")
    handle = serve.get_handle("counter")

    def check(val, num_replicas):
        pids_seen = set()
        for i in range(100):
            result = ray.get(handle.remote())
            if str(result[0]) != val:
                return False
            pids_seen.add(result[1])
        return len(pids_seen) == num_replicas

    wait_for_condition(lambda: check("123", 2))

    serve.update_backend_config("counter", BackendConfig(num_replicas=3))
    wait_for_condition(lambda: check("123", 3))

    config = BackendConfig(user_config={"count": 456})
    serve.update_backend_config("counter", config)
    wait_for_condition(lambda: check("456", 3))


def test_call_method(serve_instance):
    class CallMethod:
        def method(self, request):
            return "hello"

    serve.create_backend("backend", CallMethod)
    serve.create_endpoint("endpoint", backend="backend", route="/api")

    # Test HTTP path.
    resp = requests.get(
        "http://127.0.0.1:8000/api",
        timeout=1,
        headers={"X-SERVE-CALL-METHOD": "method"})
    assert resp.text == "hello"

    # Test serve handle path.
    handle = serve.get_handle("endpoint")
    assert ray.get(handle.options(method_name="method").remote()) == "hello"


def test_no_route(serve_instance):
    def func(_, i=1):
        return 1

    serve.create_backend("backend:1", func)
    serve.create_endpoint("noroute-endpoint", backend="backend:1")
    service_handle = serve.get_handle("noroute-endpoint")
    result = ray.get(service_handle.remote(i=1))
    assert result == 1


def test_reject_duplicate_backend(serve_instance):
    def f():
        pass

    def g():
        pass

    serve.create_backend("backend", f)
    with pytest.raises(ValueError):
        serve.create_backend("backend", g)


def test_reject_duplicate_route(serve_instance):
    def f():
        pass

    serve.create_backend("backend", f)

    route = "/foo"
    serve.create_endpoint("bar", backend="backend", route=route)
    with pytest.raises(ValueError):
        serve.create_endpoint("foo", backend="backend", route=route)


def test_reject_duplicate_endpoint(serve_instance):
    def f():
        pass

    serve.create_backend("backend", f)

    endpoint_name = "foo"
    serve.create_endpoint(endpoint_name, backend="backend", route="/ok")
    with pytest.raises(ValueError):
        serve.create_endpoint(
            endpoint_name, backend="backend", route="/different")


def test_reject_duplicate_endpoint_and_route(serve_instance):
    class SimpleBackend(object):
        def __init__(self, message):
            self.message = message

        def __call__(self, *args, **kwargs):
            return {"message": self.message}

    serve.create_backend("backend1", SimpleBackend, "First")
    serve.create_backend("backend2", SimpleBackend, "Second")

    serve.create_endpoint("test", backend="backend1", route="/test")
    with pytest.raises(ValueError):
        serve.create_endpoint("test", backend="backend2", route="/test")


def test_set_traffic_missing_data(serve_instance):
    endpoint_name = "foobar"
    backend_name = "foo_backend"
    serve.create_backend(backend_name, lambda: 5)
    serve.create_endpoint(endpoint_name, backend=backend_name)
    with pytest.raises(ValueError):
        serve.set_traffic(endpoint_name, {"nonexistent_backend": 1.0})
    with pytest.raises(ValueError):
        serve.set_traffic("nonexistent_endpoint_name", {backend_name: 1.0})


def test_scaling_replicas(serve_instance):
    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, _):
            self.count += 1
            return self.count

    config = BackendConfig(num_replicas=2)
    serve.create_backend("counter:v1", Counter, config=config)

    serve.create_endpoint("counter", backend="counter:v1", route="/increment")

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/increment").json()
        counter_result.append(resp)

    # If the load is shared among two replicas. The max result cannot be 10.
    assert max(counter_result) < 10

    update_config = BackendConfig(num_replicas=1)
    serve.update_backend_config("counter:v1", update_config)

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/increment").json()
        counter_result.append(resp)
    # Give some time for a replica to spin down. But majority of the request
    # should be served by the only remaining replica.
    assert max(counter_result) - min(counter_result) > 6


def test_updating_config(serve_instance):
    class BatchSimple:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, request):
            return [1] * len(request)

    config = BackendConfig(max_batch_size=2, num_replicas=3)
    serve.create_backend("bsimple:v1", BatchSimple, config=config)
    serve.create_endpoint("bsimple", backend="bsimple:v1", route="/bsimple")

    controller = serve.api._global_client._controller
    old_replica_tag_list = list(
        ray.get(controller._all_replica_handles.remote())["bsimple:v1"].keys())

    update_config = BackendConfig(max_batch_size=5)
    serve.update_backend_config("bsimple:v1", update_config)
    new_replica_tag_list = list(
        ray.get(controller._all_replica_handles.remote())["bsimple:v1"].keys())
    new_all_tag_list = []
    for worker_dict in ray.get(
            controller._all_replica_handles.remote()).values():
        new_all_tag_list.extend(list(worker_dict.keys()))

    # the old and new replica tag list should be identical
    # and should be subset of all_tag_list
    assert set(old_replica_tag_list) <= set(new_all_tag_list)
    assert set(old_replica_tag_list) == set(new_replica_tag_list)


def test_delete_backend(serve_instance):
    def function(_):
        return "hello"

    serve.create_backend("delete:v1", function)
    serve.create_endpoint(
        "delete_backend", backend="delete:v1", route="/delete-backend")

    assert requests.get("http://127.0.0.1:8000/delete-backend").text == "hello"

    # Check that we can't delete the backend while it's in use.
    with pytest.raises(ValueError):
        serve.delete_backend("delete:v1")

    serve.create_backend("delete:v2", function)
    serve.set_traffic("delete_backend", {"delete:v1": 0.5, "delete:v2": 0.5})

    with pytest.raises(ValueError):
        serve.delete_backend("delete:v1")

    # Check that the backend can be deleted once it's no longer in use.
    serve.set_traffic("delete_backend", {"delete:v2": 1.0})
    serve.delete_backend("delete:v1")

    # Check that we can no longer use the previously deleted backend.
    with pytest.raises(ValueError):
        serve.set_traffic("delete_backend", {"delete:v1": 1.0})

    def function2(_):
        return "olleh"

    # Check that we can now reuse the previously delete backend's tag.
    serve.create_backend("delete:v1", function2)
    serve.set_traffic("delete_backend", {"delete:v1": 1.0})

    for _ in range(10):
        try:
            assert requests.get(
                "http://127.0.0.1:8000/delete-backend").text == "olleh"
            break
        except AssertionError:
            time.sleep(0.5)  # wait for the traffic policy to propogate
    else:
        assert requests.get(
            "http://127.0.0.1:8000/delete-backend").text == "olleh"


@pytest.mark.parametrize("route", [None, "/delete-endpoint"])
def test_delete_endpoint(serve_instance, route):
    def function(_):
        return "hello"

    backend_name = "delete-endpoint:v1"
    serve.create_backend(backend_name, function)

    endpoint_name = "delete_endpoint" + str(route)
    serve.create_endpoint(endpoint_name, backend=backend_name, route=route)
    serve.delete_endpoint(endpoint_name)

    # Check that we can reuse a deleted endpoint name and route.
    serve.create_endpoint(endpoint_name, backend=backend_name, route=route)

    if route is not None:
        assert requests.get(
            "http://127.0.0.1:8000/delete-endpoint").text == "hello"
    else:
        handle = serve.get_handle(endpoint_name)
        assert ray.get(handle.remote()) == "hello"

    # Check that deleting the endpoint doesn't delete the backend.
    serve.delete_endpoint(endpoint_name)
    serve.create_endpoint(endpoint_name, backend=backend_name, route=route)

    if route is not None:
        assert requests.get(
            "http://127.0.0.1:8000/delete-endpoint").text == "hello"
    else:
        handle = serve.get_handle(endpoint_name)
        assert ray.get(handle.remote()) == "hello"


@pytest.mark.parametrize("route", [None, "/shard"])
def test_shard_key(serve_instance, route):
    # Create five backends that return different integers.
    num_backends = 5
    traffic_dict = {}
    for i in range(num_backends):

        def function(_):
            return i

        backend_name = "backend-split-" + str(i)
        traffic_dict[backend_name] = 1.0 / num_backends
        serve.create_backend(backend_name, function)

    serve.create_endpoint(
        "endpoint", backend=list(traffic_dict.keys())[0], route=route)
    serve.set_traffic("endpoint", traffic_dict)

    def do_request(shard_key):
        if route is not None:
            url = "http://127.0.0.1:8000" + route
            headers = {"X-SERVE-SHARD-KEY": shard_key}
            result = requests.get(url, headers=headers).text
        else:
            handle = serve.get_handle("endpoint").options(shard_key=shard_key)
            result = ray.get(handle.options(shard_key=shard_key).remote())
        return result

    # Send requests with different shard keys and log the backends they go to.
    shard_keys = [get_random_letters() for _ in range(20)]
    results = {}
    for shard_key in shard_keys:
        results[shard_key] = do_request(shard_key)

    # Check that the shard keys are mapped to the same backends.
    for shard_key in shard_keys:
        assert do_request(shard_key) == results[shard_key]


def test_list_endpoints(serve_instance):
    def f():
        pass

    serve.create_backend("backend", f)
    serve.create_backend("backend2", f)
    serve.create_backend("backend3", f)
    serve.create_endpoint(
        "endpoint", backend="backend", route="/api", methods=["GET", "POST"])
    serve.create_endpoint("endpoint2", backend="backend2", methods=["POST"])
    serve.shadow_traffic("endpoint", "backend3", 0.5)

    endpoints = serve.list_endpoints()
    assert "endpoint" in endpoints
    assert endpoints["endpoint"] == {
        "route": "/api",
        "methods": ["GET", "POST"],
        "traffic": {
            "backend": 1.0
        },
        "shadows": {
            "backend3": 0.5
        }
    }

    assert "endpoint2" in endpoints
    assert endpoints["endpoint2"] == {
        "route": None,
        "methods": ["POST"],
        "traffic": {
            "backend2": 1.0
        },
        "shadows": {}
    }

    serve.delete_endpoint("endpoint")
    assert "endpoint2" in serve.list_endpoints()

    serve.delete_endpoint("endpoint2")
    assert len(serve.list_endpoints()) == 0


def test_list_backends(serve_instance):
    @serve.accept_batch
    def f():
        pass

    config1 = BackendConfig(max_batch_size=10)
    serve.create_backend("backend", f, config=config1)
    backends = serve.list_backends()
    assert len(backends) == 1
    assert "backend" in backends
    assert backends["backend"].max_batch_size == 10

    config2 = BackendConfig(num_replicas=10)
    serve.create_backend("backend2", f, config=config2)
    backends = serve.list_backends()
    assert len(backends) == 2
    assert backends["backend2"].num_replicas == 10

    serve.delete_backend("backend")
    backends = serve.list_backends()
    assert len(backends) == 1
    assert "backend2" in backends

    serve.delete_backend("backend2")
    assert len(serve.list_backends()) == 0


def test_endpoint_input_validation(serve_instance):
    def f():
        pass

    serve.create_backend("backend", f)
    with pytest.raises(TypeError):
        serve.create_endpoint("endpoint")
    with pytest.raises(TypeError):
        serve.create_endpoint("endpoint", route="/hello")
    with pytest.raises(TypeError):
        serve.create_endpoint("endpoint", backend=2)
    serve.create_endpoint("endpoint", backend="backend")


def test_shadow_traffic(serve_instance):
    @ray.remote
    class RequestCounter:
        def __init__(self):
            self.requests = defaultdict(int)

        def record(self, backend):
            self.requests[backend] += 1

        def get(self, backend):
            return self.requests[backend]

    counter = RequestCounter.remote()

    def f(_):
        ray.get(counter.record.remote("backend1"))
        return "hello"

    def f_shadow_1(_):
        ray.get(counter.record.remote("backend2"))
        return "oops"

    def f_shadow_2(_):
        ray.get(counter.record.remote("backend3"))
        return "oops"

    def f_shadow_3(_):
        ray.get(counter.record.remote("backend4"))
        return "oops"

    serve.create_backend("backend1", f)
    serve.create_backend("backend2", f_shadow_1)
    serve.create_backend("backend3", f_shadow_2)
    serve.create_backend("backend4", f_shadow_3)

    serve.create_endpoint("endpoint", backend="backend1", route="/api")
    serve.shadow_traffic("endpoint", "backend2", 1.0)
    serve.shadow_traffic("endpoint", "backend3", 0.5)
    serve.shadow_traffic("endpoint", "backend4", 0.1)

    start = time.time()
    num_requests = 100
    for _ in range(num_requests):
        assert requests.get("http://127.0.0.1:8000/api").text == "hello"
    print("Finished 100 requests in {}s.".format(time.time() - start))

    def requests_to_backend(backend):
        return ray.get(counter.get.remote(backend))

    def check_requests():
        return all([
            requests_to_backend("backend1") == num_requests,
            requests_to_backend("backend2") == requests_to_backend("backend1"),
            requests_to_backend("backend3") < requests_to_backend("backend2"),
            requests_to_backend("backend4") < requests_to_backend("backend3"),
            requests_to_backend("backend4") > 0,
        ])

    wait_for_condition(check_requests)


def test_starlette_request(serve_instance):
    async def echo_body(starlette_request):
        data = await starlette_request.body()
        return data

    UVICORN_HIGH_WATER_MARK = 65536  # max bytes in one message

    # Long string to test serialization of multiple messages.
    long_string = "x" * 10 * UVICORN_HIGH_WATER_MARK

    serve.create_backend("echo:v1", echo_body)
    serve.create_endpoint(
        "endpoint", backend="echo:v1", route="/api", methods=["GET", "POST"])

    resp = requests.post("http://127.0.0.1:8000/api", data=long_string).text
    assert resp == long_string


def test_variable_routes(serve_instance):
    def f(starlette_request):
        return starlette_request.path_params

    serve.create_backend("f", f)
    serve.create_endpoint("basic", backend="f", route="/api/{username}")

    # Test multiple variables and test type conversion
    serve.create_endpoint(
        "complex", backend="f", route="/api/{user_id:int}/{number:float}")

    assert requests.get("http://127.0.0.1:8000/api/scaly").json() == {
        "username": "scaly"
    }

    assert requests.get("http://127.0.0.1:8000/api/23/12.345").json() == {
        "user_id": 23,
        "number": 12.345
    }


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
