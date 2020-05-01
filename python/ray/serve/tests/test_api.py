import time
import pytest
import requests

from ray import serve
import ray


def test_e2e(serve_instance):
    serve.init()
    serve.create_endpoint("endpoint", "/api", methods=["GET", "POST"])

    retry_count = 5
    timeout_sleep = 0.5
    while True:
        try:
            resp = requests.get(
                "http://127.0.0.1:8000/-/routes", timeout=0.5).json()
            assert resp == {"/api": ["endpoint", ["GET", "POST"]]}
            break
        except Exception as e:
            time.sleep(timeout_sleep)
            timeout_sleep *= 2
            retry_count -= 1
            if retry_count == 0:
                assert False, ("Route table hasn't been updated after 3 tries."
                               "The latest error was {}").format(e)

    def function(flask_request):
        return {"method": flask_request.method}

    serve.create_backend("echo:v1", function)
    serve.set_traffic("endpoint", {"echo:v1": 1.0})

    resp = requests.get("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "GET"

    resp = requests.post("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "POST"


def test_call_method(serve_instance):
    serve.create_endpoint("call-method", "/call-method")

    class CallMethod:
        def method(self, request):
            return "hello"

    serve.create_backend("call-method", CallMethod)
    serve.set_traffic("call-method", {"call-method": 1.0})

    # Test HTTP path.
    resp = requests.get(
        "http://127.0.0.1:8000/call-method",
        timeout=1,
        headers={"X-SERVE-CALL-METHOD": "method"})
    assert resp.text == "hello"

    # Test serve handle path.
    handle = serve.get_handle("call-method")
    assert ray.get(handle.options("method").remote()) == "hello"


def test_no_route(serve_instance):
    serve.create_endpoint("noroute-endpoint")

    def func(_, i=1):
        return 1

    serve.create_backend("backend:1", func)
    serve.set_traffic("noroute-endpoint", {"backend:1": 1.0})
    service_handle = serve.get_handle("noroute-endpoint")
    result = ray.get(service_handle.remote(i=1))
    assert result == 1


def test_reject_duplicate_route(serve_instance):
    route = "/foo"
    serve.create_endpoint("bar", route=route)
    with pytest.raises(ValueError):
        serve.create_endpoint("foo", route=route)


def test_reject_duplicate_endpoint(serve_instance):
    endpoint_name = "foo"
    serve.create_endpoint(endpoint_name, route="/ok")
    with pytest.raises(ValueError):
        serve.create_endpoint(endpoint_name, route="/different")


def test_set_traffic_missing_data(serve_instance):
    endpoint_name = "foobar"
    backend_name = "foo_backend"
    serve.create_endpoint(endpoint_name)
    serve.create_backend(backend_name, lambda: 5)
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

    serve.create_endpoint("counter", "/increment")

    # Keep checking the routing table until /increment is populated
    while "/increment" not in requests.get(
            "http://127.0.0.1:8000/-/routes").json():
        time.sleep(0.2)

    serve.create_backend("counter:v1", Counter, config={"num_replicas": 2})
    serve.set_traffic("counter", {"counter:v1": 1.0})

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/increment").json()
        counter_result.append(resp)

    # If the load is shared among two replicas. The max result cannot be 10.
    assert max(counter_result) < 10

    serve.update_backend_config("counter:v1", {"num_replicas": 1})

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/increment").json()
        counter_result.append(resp)
    # Give some time for a replica to spin down. But majority of the request
    # should be served by the only remaining replica.
    assert max(counter_result) - min(counter_result) > 6


def test_batching(serve_instance):
    class BatchingExample:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, flask_request, temp=None):
            self.count += 1
            batch_size = serve.context.batch_size
            return [self.count] * batch_size

    serve.create_endpoint("counter1", "/increment2")

    # Keep checking the routing table until /increment is populated
    while "/increment2" not in requests.get(
            "http://127.0.0.1:8000/-/routes").json():
        time.sleep(0.2)

    # set the max batch size
    serve.create_backend(
        "counter:v11", BatchingExample, config={"max_batch_size": 5})
    serve.set_traffic("counter1", {"counter:v11": 1.0})

    future_list = []
    handle = serve.get_handle("counter1")
    for _ in range(20):
        f = handle.remote(temp=1)
        future_list.append(f)

    counter_result = ray.get(future_list)
    # since count is only updated per batch of queries
    # If there atleast one __call__ fn call with batch size greater than 1
    # counter result will always be less than 20
    assert max(counter_result) < 20


def test_batching_exception(serve_instance):
    class NoListReturned:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, flask_request, temp=None):
            batch_size = serve.context.batch_size
            return batch_size

    serve.create_endpoint("exception-test", "/noListReturned")
    # set the max batch size
    serve.create_backend(
        "exception:v1", NoListReturned, config={"max_batch_size": 5})
    serve.set_traffic("exception-test", {"exception:v1": 1.0})

    handle = serve.get_handle("exception-test")
    with pytest.raises(ray.exceptions.RayTaskError):
        assert ray.get(handle.remote(temp=1))


def test_updating_config(serve_instance):
    class BatchSimple:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, flask_request, temp=None):
            batch_size = serve.context.batch_size
            return [1] * batch_size

    serve.create_endpoint("bsimple", "/bsimple")
    serve.create_backend(
        "bsimple:v1",
        BatchSimple,
        config={
            "max_batch_size": 2,
            "num_replicas": 3
        })
    master_actor = serve.api._get_master_actor()
    old_replica_tag_list = ray.get(
        master_actor._list_replicas.remote("bsimple:v1"))

    serve.update_backend_config("bsimple:v1", {"max_batch_size": 5})
    new_replica_tag_list = ray.get(
        master_actor._list_replicas.remote("bsimple:v1"))
    new_all_tag_list = []
    for worker_dict in ray.get(
            master_actor.get_all_worker_handles.remote()).values():
        new_all_tag_list.extend(list(worker_dict.keys()))

    # the old and new replica tag list should be identical
    # and should be subset of all_tag_list
    assert set(old_replica_tag_list) <= set(new_all_tag_list)
    assert set(old_replica_tag_list) == set(new_replica_tag_list)


def test_delete_backend(serve_instance):
    serve.create_endpoint("delete_backend", "/delete-backend")

    def function():
        return "hello"

    serve.create_backend("delete:v1", function)
    serve.set_traffic("delete_backend", {"delete:v1": 1.0})

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

    def function2():
        return "olleh"

    # Check that we can now reuse the previously delete backend's tag.
    serve.create_backend("delete:v1", function2)
    serve.set_traffic("delete_backend", {"delete:v1": 1.0})

    assert requests.get("http://127.0.0.1:8000/delete-backend").text == "olleh"


@pytest.mark.parametrize("route", [None, "/delete-endpoint"])
def test_delete_endpoint(serve_instance, route):
    endpoint_name = "delete_endpoint" + str(route)
    serve.create_endpoint(endpoint_name, route=route)
    serve.delete_endpoint(endpoint_name)

    # Check that we can reuse a deleted endpoint name and route.
    serve.create_endpoint(endpoint_name, route=route)

    def function():
        return "hello"

    serve.create_backend("delete-endpoint:v1", function)
    serve.set_traffic(endpoint_name, {"delete-endpoint:v1": 1.0})

    if route is not None:
        assert requests.get(
            "http://127.0.0.1:8000/delete-endpoint").text == "hello"
    else:
        handle = serve.get_handle(endpoint_name)
        assert ray.get(handle.remote()) == "hello"

    # Check that deleting the endpoint doesn't delete the backend.
    serve.delete_endpoint(endpoint_name)
    serve.create_endpoint(endpoint_name, route=route)
    serve.set_traffic(endpoint_name, {"delete-endpoint:v1": 1.0})

    if route is not None:
        assert requests.get(
            "http://127.0.0.1:8000/delete-endpoint").text == "hello"
    else:
        handle = serve.get_handle(endpoint_name)
        assert ray.get(handle.remote()) == "hello"
