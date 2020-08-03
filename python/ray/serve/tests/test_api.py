import time
import asyncio

import pytest
import requests

import ray
from ray import serve
from ray.test_utils import wait_for_condition
from ray.serve import constants
from ray.serve.exceptions import RayServeException
from ray.serve.utils import format_actor_name, get_random_letters


def test_e2e(serve_instance):
    serve.init()

    def function(flask_request):
        return {"method": flask_request.method}

    serve.create_backend("echo:v1", function)
    serve.create_endpoint(
        "endpoint", backend="echo:v1", route="/api", methods=["GET", "POST"])

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

    resp = requests.get("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "GET"

    resp = requests.post("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "POST"


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
    assert ray.get(handle.options("method").remote()) == "hello"


def test_no_route(serve_instance):
    def func(_, i=1):
        return 1

    serve.create_backend("backend:1", func)
    serve.create_endpoint("noroute-endpoint", backend="backend:1")
    service_handle = serve.get_handle("noroute-endpoint")
    result = ray.get(service_handle.remote(i=1))
    assert result == 1


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

    serve.create_backend("counter:v1", Counter, config={"num_replicas": 2})
    serve.create_endpoint("counter", backend="counter:v1", route="/increment")

    # Keep checking the routing table until /increment is populated
    while "/increment" not in requests.get(
            "http://127.0.0.1:8000/-/routes").json():
        time.sleep(0.2)

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

    # set the max batch size
    serve.create_backend(
        "counter:v11",
        BatchingExample,
        config={
            "max_batch_size": 5,
            "batch_wait_timeout": 1
        })
    serve.create_endpoint(
        "counter1", backend="counter:v11", route="/increment2")

    # Keep checking the routing table until /increment is populated
    while "/increment2" not in requests.get(
            "http://127.0.0.1:8000/-/routes").json():
        time.sleep(0.2)

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

    # set the max batch size
    serve.create_backend(
        "exception:v1", NoListReturned, config={"max_batch_size": 5})
    serve.create_endpoint(
        "exception-test", backend="exception:v1", route="/noListReturned")

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

    serve.create_backend(
        "bsimple:v1",
        BatchSimple,
        config={
            "max_batch_size": 2,
            "num_replicas": 3
        })
    serve.create_endpoint("bsimple", backend="bsimple:v1", route="/bsimple")

    controller = serve.api._get_controller()
    old_replica_tag_list = ray.get(
        controller._list_replicas.remote("bsimple:v1"))

    serve.update_backend_config("bsimple:v1", {"max_batch_size": 5})
    new_replica_tag_list = ray.get(
        controller._list_replicas.remote("bsimple:v1"))
    new_all_tag_list = []
    for worker_dict in ray.get(
            controller.get_all_worker_handles.remote()).values():
        new_all_tag_list.extend(list(worker_dict.keys()))

    # the old and new replica tag list should be identical
    # and should be subset of all_tag_list
    assert set(old_replica_tag_list) <= set(new_all_tag_list)
    assert set(old_replica_tag_list) == set(new_replica_tag_list)


def test_delete_backend(serve_instance):
    def function():
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

    def function2():
        return "olleh"

    # Check that we can now reuse the previously delete backend's tag.
    serve.create_backend("delete:v1", function2)
    serve.set_traffic("delete_backend", {"delete:v1": 1.0})

    assert requests.get("http://127.0.0.1:8000/delete-backend").text == "olleh"


@pytest.mark.parametrize("route", [None, "/delete-endpoint"])
def test_delete_endpoint(serve_instance, route):
    def function():
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

        def function():
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


def test_name():
    with pytest.raises(TypeError):
        serve.init(name=1)

    route = "/api"
    backend = "backend"
    endpoint = "endpoint"

    serve.init(name="cluster1", http_port=8001)

    def function():
        return "hello1"

    serve.create_backend(backend, function)
    serve.create_endpoint(endpoint, backend=backend, route=route)

    assert requests.get("http://127.0.0.1:8001" + route).text == "hello1"

    # Create a second cluster on port 8002. Create an endpoint and backend with
    # the same names and check that they don't collide.
    serve.init(name="cluster2", http_port=8002)

    def function():
        return "hello2"

    serve.create_backend(backend, function)
    serve.create_endpoint(endpoint, backend=backend, route=route)

    assert requests.get("http://127.0.0.1:8001" + route).text == "hello1"
    assert requests.get("http://127.0.0.1:8002" + route).text == "hello2"

    # Check that deleting the backend in the current cluster doesn't.
    serve.delete_endpoint(endpoint)
    serve.delete_backend(backend)
    assert requests.get("http://127.0.0.1:8001" + route).text == "hello1"

    # Check that we can re-connect to the first cluster.
    serve.init(name="cluster1")
    serve.delete_endpoint(endpoint)
    serve.delete_backend(backend)


def test_parallel_start(serve_instance):
    # Test the ability to start multiple replicas in parallel.
    # In the past, when Serve scale up a backend, it does so one by one and
    # wait for each replica to initialize. This test avoid this by preventing
    # the first replica to finish initialization unless the second replica is
    # also started.
    @ray.remote
    class Barrier:
        def __init__(self, release_on):
            self.release_on = release_on
            self.current_waiters = 0
            self.event = asyncio.Event()

        async def wait(self):
            self.current_waiters += 1
            if self.current_waiters == self.release_on:
                self.event.set()
            else:
                await self.event.wait()

    barrier = Barrier.remote(release_on=2)

    class LongStartingServable:
        def __init__(self):
            ray.get(barrier.wait.remote(), timeout=10)

        def __call__(self, _):
            return "Ready"

    serve.create_backend(
        "p:v0", LongStartingServable, config={"num_replicas": 2})
    serve.create_endpoint("test-parallel", backend="p:v0")
    handle = serve.get_handle("test-parallel")

    ray.get(handle.remote(), timeout=10)


def test_list_endpoints(serve_instance):
    serve.init()

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
    serve.init()

    @serve.accept_batch
    def f():
        pass

    serve.create_backend("backend", f, config={"max_batch_size": 10})
    backends = serve.list_backends()
    assert len(backends) == 1
    assert "backend" in backends
    assert backends["backend"]["max_batch_size"] == 10

    serve.create_backend("backend2", f, config={"num_replicas": 10})
    backends = serve.list_backends()
    assert len(backends) == 2
    assert backends["backend2"]["num_replicas"] == 10

    serve.delete_backend("backend")
    backends = serve.list_backends()
    assert len(backends) == 1
    assert "backend2" in backends

    serve.delete_backend("backend2")
    assert len(serve.list_backends()) == 0


def test_endpoint_input_validation(serve_instance):
    serve.init()

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


def test_create_infeasible_error(serve_instance):
    serve.init()

    def f():
        pass

    # Non existent resource should be infeasible.
    with pytest.raises(RayServeException, match="Cannot scale backend"):
        serve.create_backend(
            "f:1",
            f,
            ray_actor_options={"resources": {
                "MagicMLResource": 100
            }})

    # Even each replica might be feasible, the total might not be.
    current_cpus = int(ray.nodes()[0]["Resources"]["CPU"])
    with pytest.raises(RayServeException, match="Cannot scale backend"):
        serve.create_backend(
            "f:1",
            f,
            ray_actor_options={"resources": {
                "CPU": 1,
            }},
            config={"num_replicas": current_cpus + 20})

    # No replica should be created!
    replicas = ray.get(serve.api.controller._list_replicas.remote("f1"))
    assert len(replicas) == 0


def test_shutdown(serve_instance):
    def f():
        pass

    instance_name = "shutdown"
    serve.init(name=instance_name, http_port=8003)
    serve.create_backend("backend", f)
    serve.create_endpoint("endpoint", backend="backend")

    serve.shutdown()
    with pytest.raises(RayServeException, match="Please run serve.init"):
        serve.list_backends()

    def check_dead():
        for actor_name in [
                constants.SERVE_CONTROLLER_NAME, constants.SERVE_PROXY_NAME,
                constants.SERVE_METRIC_SINK_NAME
        ]:
            try:
                ray.get_actor(format_actor_name(actor_name, instance_name))
                return False
            except ValueError:
                pass
        return True

    wait_for_condition(check_dead)


def test_shadow_traffic(serve_instance):
    def f():
        return "hello"

    def f_shadow():
        return "oops"

    serve.create_backend("backend1", f)
    serve.create_backend("backend2", f_shadow)
    serve.create_backend("backend3", f_shadow)
    serve.create_backend("backend4", f_shadow)

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
        for entry in serve.stat():
            if entry["info"]["name"] == "backend_request_counter":
                if entry["info"]["backend"] == backend:
                    return entry["value"]

        return 0

    def check_requests():
        return all([
            requests_to_backend("backend1") == num_requests,
            requests_to_backend("backend2") == requests_to_backend("backend1"),
            requests_to_backend("backend3") < requests_to_backend("backend2"),
            requests_to_backend("backend4") < requests_to_backend("backend3"),
            requests_to_backend("backend4") > 0,
        ])

    wait_for_condition(check_requests)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
