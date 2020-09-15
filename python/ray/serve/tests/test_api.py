import asyncio
from collections import defaultdict
import time

import pytest
import requests

import ray
from ray import serve
from ray.test_utils import wait_for_condition
from ray.serve.constants import SERVE_PROXY_NAME
from ray.serve.exceptions import RayServeException
from ray.serve.config import BackendConfig
from ray.serve.utils import (block_until_http_ready, format_actor_name,
                             get_random_letters)


def test_e2e(serve_instance):
    client = serve_instance

    def function(flask_request):
        return {"method": flask_request.method}

    client.create_backend("echo:v1", function)
    client.create_endpoint(
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
    client = serve_instance

    class CallMethod:
        def method(self, request):
            return "hello"

    client.create_backend("backend", CallMethod)
    client.create_endpoint("endpoint", backend="backend", route="/api")

    # Test HTTP path.
    resp = requests.get(
        "http://127.0.0.1:8000/api",
        timeout=1,
        headers={"X-SERVE-CALL-METHOD": "method"})
    assert resp.text == "hello"

    # Test serve handle path.
    handle = client.get_handle("endpoint")
    assert ray.get(handle.options("method").remote()) == "hello"


def test_no_route(serve_instance):
    client = serve_instance

    def func(_, i=1):
        return 1

    client.create_backend("backend:1", func)
    client.create_endpoint("noroute-endpoint", backend="backend:1")
    service_handle = client.get_handle("noroute-endpoint")
    result = ray.get(service_handle.remote(i=1))
    assert result == 1


def test_reject_duplicate_backend(serve_instance):
    client = serve_instance

    def f():
        pass

    def g():
        pass

    client.create_backend("backend", f)
    with pytest.raises(ValueError):
        client.create_backend("backend", g)


def test_reject_duplicate_route(serve_instance):
    client = serve_instance

    def f():
        pass

    client.create_backend("backend", f)

    route = "/foo"
    client.create_endpoint("bar", backend="backend", route=route)
    with pytest.raises(ValueError):
        client.create_endpoint("foo", backend="backend", route=route)


def test_reject_duplicate_endpoint(serve_instance):
    client = serve_instance

    def f():
        pass

    client.create_backend("backend", f)

    endpoint_name = "foo"
    client.create_endpoint(endpoint_name, backend="backend", route="/ok")
    with pytest.raises(ValueError):
        client.create_endpoint(
            endpoint_name, backend="backend", route="/different")


def test_reject_duplicate_endpoint_and_route(serve_instance):
    client = serve_instance

    class SimpleBackend(object):
        def __init__(self, message):
            self.message = message

        def __call__(self, *args, **kwargs):
            return {"message": self.message}

    client.create_backend("backend1", SimpleBackend, "First")
    client.create_backend("backend2", SimpleBackend, "Second")

    client.create_endpoint("test", backend="backend1", route="/test")
    with pytest.raises(ValueError):
        client.create_endpoint("test", backend="backend2", route="/test")


def test_set_traffic_missing_data(serve_instance):
    client = serve_instance

    endpoint_name = "foobar"
    backend_name = "foo_backend"
    client.create_backend(backend_name, lambda: 5)
    client.create_endpoint(endpoint_name, backend=backend_name)
    with pytest.raises(ValueError):
        client.set_traffic(endpoint_name, {"nonexistent_backend": 1.0})
    with pytest.raises(ValueError):
        client.set_traffic("nonexistent_endpoint_name", {backend_name: 1.0})


@pytest.mark.parametrize("use_legacy_config", [False, True])
def test_scaling_replicas(serve_instance, use_legacy_config):
    client = serve_instance

    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, _):
            self.count += 1
            return self.count

    config = {
        "num_replicas": 2
    } if use_legacy_config else BackendConfig(num_replicas=2)
    client.create_backend("counter:v1", Counter, config=config)

    client.create_endpoint("counter", backend="counter:v1", route="/increment")

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

    update_config = {
        "num_replicas": 1
    } if use_legacy_config else BackendConfig(num_replicas=1)
    client.update_backend_config("counter:v1", update_config)

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/increment").json()
        counter_result.append(resp)
    # Give some time for a replica to spin down. But majority of the request
    # should be served by the only remaining replica.
    assert max(counter_result) - min(counter_result) > 6


@pytest.mark.parametrize("use_legacy_config", [False, True])
def test_batching(serve_instance, use_legacy_config):
    client = serve_instance

    class BatchingExample:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, requests):
            self.count += 1
            batch_size = len(requests)
            return [self.count] * batch_size

    # set the max batch size
    config = {
        "max_batch_size": 5,
        "batch_wait_timeout": 1
    } if use_legacy_config else BackendConfig(
        max_batch_size=5, batch_wait_timeout=1)
    client.create_backend("counter:v11", BatchingExample, config=config)
    client.create_endpoint(
        "counter1", backend="counter:v11", route="/increment2")

    # Keep checking the routing table until /increment is populated
    while "/increment2" not in requests.get(
            "http://127.0.0.1:8000/-/routes").json():
        time.sleep(0.2)

    future_list = []
    handle = client.get_handle("counter1")
    for _ in range(20):
        f = handle.remote(temp=1)
        future_list.append(f)

    counter_result = ray.get(future_list)
    # since count is only updated per batch of queries
    # If there atleast one __call__ fn call with batch size greater than 1
    # counter result will always be less than 20
    assert max(counter_result) < 20


@pytest.mark.parametrize("use_legacy_config", [False, True])
def test_batching_exception(serve_instance, use_legacy_config):
    client = serve_instance

    class NoListReturned:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, requests):
            return len(requests)

    # set the max batch size
    config = {
        "max_batch_size": 5
    } if use_legacy_config else BackendConfig(max_batch_size=5)
    client.create_backend("exception:v1", NoListReturned, config=config)
    client.create_endpoint(
        "exception-test", backend="exception:v1", route="/noListReturned")

    handle = client.get_handle("exception-test")
    with pytest.raises(ray.exceptions.RayTaskError):
        assert ray.get(handle.remote(temp=1))


@pytest.mark.parametrize("use_legacy_config", [False, True])
def test_updating_config(serve_instance, use_legacy_config):
    client = serve_instance

    class BatchSimple:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, request):
            return [1] * len(request)

    config = {
        "max_batch_size": 2,
        "num_replicas": 3
    } if use_legacy_config else BackendConfig(
        max_batch_size=2, num_replicas=3)
    client.create_backend("bsimple:v1", BatchSimple, config=config)
    client.create_endpoint("bsimple", backend="bsimple:v1", route="/bsimple")

    controller = client._controller
    old_replica_tag_list = ray.get(
        controller._list_replicas.remote("bsimple:v1"))

    update_config = {
        "max_batch_size": 5
    } if use_legacy_config else BackendConfig(max_batch_size=5)
    client.update_backend_config("bsimple:v1", update_config)
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
    client = serve_instance

    def function(_):
        return "hello"

    client.create_backend("delete:v1", function)
    client.create_endpoint(
        "delete_backend", backend="delete:v1", route="/delete-backend")

    assert requests.get("http://127.0.0.1:8000/delete-backend").text == "hello"

    # Check that we can't delete the backend while it's in use.
    with pytest.raises(ValueError):
        client.delete_backend("delete:v1")

    client.create_backend("delete:v2", function)
    client.set_traffic("delete_backend", {"delete:v1": 0.5, "delete:v2": 0.5})

    with pytest.raises(ValueError):
        client.delete_backend("delete:v1")

    # Check that the backend can be deleted once it's no longer in use.
    client.set_traffic("delete_backend", {"delete:v2": 1.0})
    client.delete_backend("delete:v1")

    # Check that we can no longer use the previously deleted backend.
    with pytest.raises(ValueError):
        client.set_traffic("delete_backend", {"delete:v1": 1.0})

    def function2(_):
        return "olleh"

    # Check that we can now reuse the previously delete backend's tag.
    client.create_backend("delete:v1", function2)
    client.set_traffic("delete_backend", {"delete:v1": 1.0})

    assert requests.get("http://127.0.0.1:8000/delete-backend").text == "olleh"


@pytest.mark.parametrize("route", [None, "/delete-endpoint"])
def test_delete_endpoint(serve_instance, route):
    client = serve_instance

    def function(_):
        return "hello"

    backend_name = "delete-endpoint:v1"
    client.create_backend(backend_name, function)

    endpoint_name = "delete_endpoint" + str(route)
    client.create_endpoint(endpoint_name, backend=backend_name, route=route)
    client.delete_endpoint(endpoint_name)

    # Check that we can reuse a deleted endpoint name and route.
    client.create_endpoint(endpoint_name, backend=backend_name, route=route)

    if route is not None:
        assert requests.get(
            "http://127.0.0.1:8000/delete-endpoint").text == "hello"
    else:
        handle = client.get_handle(endpoint_name)
        assert ray.get(handle.remote()) == "hello"

    # Check that deleting the endpoint doesn't delete the backend.
    client.delete_endpoint(endpoint_name)
    client.create_endpoint(endpoint_name, backend=backend_name, route=route)

    if route is not None:
        assert requests.get(
            "http://127.0.0.1:8000/delete-endpoint").text == "hello"
    else:
        handle = client.get_handle(endpoint_name)
        assert ray.get(handle.remote()) == "hello"


@pytest.mark.parametrize("route", [None, "/shard"])
def test_shard_key(serve_instance, route):
    client = serve_instance

    # Create five backends that return different integers.
    num_backends = 5
    traffic_dict = {}
    for i in range(num_backends):

        def function(_):
            return i

        backend_name = "backend-split-" + str(i)
        traffic_dict[backend_name] = 1.0 / num_backends
        client.create_backend(backend_name, function)

    client.create_endpoint(
        "endpoint", backend=list(traffic_dict.keys())[0], route=route)
    client.set_traffic("endpoint", traffic_dict)

    def do_request(shard_key):
        if route is not None:
            url = "http://127.0.0.1:8000" + route
            headers = {"X-SERVE-SHARD-KEY": shard_key}
            result = requests.get(url, headers=headers).text
        else:
            handle = client.get_handle("endpoint").options(shard_key=shard_key)
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


def test_multiple_instances():
    route = "/api"
    backend = "backend"
    endpoint = "endpoint"

    client1 = serve.start(http_port=8001)

    def function(_):
        return "hello1"

    client1.create_backend(backend, function)
    client1.create_endpoint(endpoint, backend=backend, route=route)

    assert requests.get("http://127.0.0.1:8001" + route).text == "hello1"

    # Create a second cluster on port 8002. Create an endpoint and backend with
    # the same names and check that they don't collide.
    client2 = serve.start(http_port=8002)

    def function(_):
        return "hello2"

    client2.create_backend(backend, function)
    client2.create_endpoint(endpoint, backend=backend, route=route)

    assert requests.get("http://127.0.0.1:8001" + route).text == "hello1"
    assert requests.get("http://127.0.0.1:8002" + route).text == "hello2"

    # Check that deleting the backend in the current cluster doesn't.
    client2.delete_endpoint(endpoint)
    client2.delete_backend(backend)
    assert requests.get("http://127.0.0.1:8001" + route).text == "hello1"

    # Check that the first client still works.
    client1.delete_endpoint(endpoint)
    client1.delete_backend(backend)


@pytest.mark.parametrize("use_legacy_config", [False, True])
def test_parallel_start(serve_instance, use_legacy_config):
    client = serve_instance

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

    config = {
        "num_replicas": 2
    } if use_legacy_config else BackendConfig(num_replicas=2)
    client.create_backend("p:v0", LongStartingServable, config=config)
    client.create_endpoint("test-parallel", backend="p:v0")
    handle = client.get_handle("test-parallel")

    ray.get(handle.remote(), timeout=10)


def test_list_endpoints(serve_instance):
    client = serve_instance

    def f():
        pass

    client.create_backend("backend", f)
    client.create_backend("backend2", f)
    client.create_backend("backend3", f)
    client.create_endpoint(
        "endpoint", backend="backend", route="/api", methods=["GET", "POST"])
    client.create_endpoint("endpoint2", backend="backend2", methods=["POST"])
    client.shadow_traffic("endpoint", "backend3", 0.5)

    endpoints = client.list_endpoints()
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

    client.delete_endpoint("endpoint")
    assert "endpoint2" in client.list_endpoints()

    client.delete_endpoint("endpoint2")
    assert len(client.list_endpoints()) == 0


@pytest.mark.parametrize("use_legacy_config", [False, True])
def test_list_backends(serve_instance, use_legacy_config):
    client = serve_instance

    @serve.accept_batch
    def f():
        pass

    config1 = {
        "max_batch_size": 10
    } if use_legacy_config else BackendConfig(max_batch_size=10)
    client.create_backend("backend", f, config=config1)
    backends = client.list_backends()
    assert len(backends) == 1
    assert "backend" in backends
    assert backends["backend"]["max_batch_size"] == 10

    config2 = {
        "num_replicas": 10
    } if use_legacy_config else BackendConfig(num_replicas=10)
    client.create_backend("backend2", f, config=config2)
    backends = client.list_backends()
    assert len(backends) == 2
    assert backends["backend2"]["num_replicas"] == 10

    client.delete_backend("backend")
    backends = client.list_backends()
    assert len(backends) == 1
    assert "backend2" in backends

    client.delete_backend("backend2")
    assert len(client.list_backends()) == 0


def test_endpoint_input_validation(serve_instance):
    client = serve_instance

    def f():
        pass

    client.create_backend("backend", f)
    with pytest.raises(TypeError):
        client.create_endpoint("endpoint")
    with pytest.raises(TypeError):
        client.create_endpoint("endpoint", route="/hello")
    with pytest.raises(TypeError):
        client.create_endpoint("endpoint", backend=2)
    client.create_endpoint("endpoint", backend="backend")


@pytest.mark.parametrize("use_legacy_config", [False, True])
def test_create_infeasible_error(serve_instance, use_legacy_config):
    client = serve_instance

    def f():
        pass

    # Non existent resource should be infeasible.
    with pytest.raises(RayServeException, match="Cannot scale backend"):
        client.create_backend(
            "f:1",
            f,
            ray_actor_options={"resources": {
                "MagicMLResource": 100
            }})

    # Even each replica might be feasible, the total might not be.
    current_cpus = int(ray.nodes()[0]["Resources"]["CPU"])
    num_replicas = current_cpus + 20
    config = {
        "num_replicas": num_replicas
    } if use_legacy_config else BackendConfig(num_replicas=num_replicas)
    with pytest.raises(RayServeException, match="Cannot scale backend"):
        client.create_backend(
            "f:1",
            f,
            ray_actor_options={"resources": {
                "CPU": 1,
            }},
            config=config)

    # No replica should be created!
    replicas = ray.get(client._controller._list_replicas.remote("f1"))
    assert len(replicas) == 0


def test_shutdown():
    def f():
        pass

    client = serve.start(http_port=8003)
    client.create_backend("backend", f)
    client.create_endpoint("endpoint", backend="backend")

    client.shutdown()
    with pytest.raises(RayServeException):
        client.list_backends()

    def check_dead():
        for actor_name in [
                client._controller_name,
                format_actor_name(SERVE_PROXY_NAME, client._controller_name)
        ]:
            try:
                ray.get_actor(actor_name)
                return False
            except ValueError:
                pass
        return True

    wait_for_condition(check_dead)


def test_shadow_traffic(serve_instance):
    client = serve_instance

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

    client.create_backend("backend1", f)
    client.create_backend("backend2", f_shadow_1)
    client.create_backend("backend3", f_shadow_2)
    client.create_backend("backend4", f_shadow_3)

    client.create_endpoint("endpoint", backend="backend1", route="/api")
    client.shadow_traffic("endpoint", "backend2", 1.0)
    client.shadow_traffic("endpoint", "backend3", 0.5)
    client.shadow_traffic("endpoint", "backend4", 0.1)

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


def test_connect(serve_instance):
    client = serve_instance

    # Check that you can have multiple clients to the same detached instance.
    client2 = serve.connect()
    assert client._controller_name == client2._controller_name

    # Check that you can have detached and non-detached instances.
    client3 = serve.start(http_port=8004)
    assert client3._controller_name != client._controller_name

    # Check that you can call serve.connect() from within a backend for both
    # detached and non-detached instances.

    def connect_in_backend(_):
        client = serve.connect()
        client.create_backend("backend-ception", connect_in_backend)
        return client._controller_name

    client.create_backend("connect_in_backend", connect_in_backend)
    client.create_endpoint("endpoint", backend="connect_in_backend")
    handle = client.get_handle("endpoint")
    assert ray.get(handle.remote()) == client._controller_name
    assert "backend-ception" in client.list_backends()

    client3.create_backend("connect_in_backend", connect_in_backend)
    client3.create_endpoint("endpoint", backend="connect_in_backend")
    handle = client3.get_handle("endpoint")
    assert ray.get(handle.remote()) == client3._controller_name
    assert "backend-ception" in client3.list_backends()


def test_serve_metrics(serve_instance):
    client = serve_instance

    @serve.accept_batch
    def batcher(flask_requests):
        return ["hello"] * len(flask_requests)

    client.create_backend("metrics", batcher)
    client.create_endpoint("metrics", backend="metrics", route="/metrics")
    # send 10 concurrent requests
    url = "http://127.0.0.1:8000/metrics"
    ray.get([block_until_http_ready.remote(url) for _ in range(10)])

    def verify_metrics(do_assert=False):
        resp = requests.get("http://127.0.0.1:9999").text

        expected_metrics = [
            # counter
            "num_router_requests_total",
            "num_http_requests_total",
            "backend_queued_queries_total",
            "backend_request_counter_requests_total",
            "backend_worker_starts_restarts_total",
            # histogram
            "backend_processing_latency_ms_bucket",
            "backend_processing_latency_ms_count",
            "backend_processing_latency_ms_sum",
            "backend_queuing_latency_ms_bucket",
            "backend_queuing_latency_ms_count",
            "backend_queuing_latency_ms_sum",
            # gauge
            "replica_processing_queries",
            "replica_queued_queries",
        ]
        for metric in expected_metrics:
            # For the final error round
            if do_assert:
                assert metric in resp
            # For the wait_for_condition
            else:
                if metric not in resp:
                    return False
        return True

    try:
        wait_for_condition(verify_metrics, retry_interval_ms=500)
    except RuntimeError:
        verify_metrics()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
