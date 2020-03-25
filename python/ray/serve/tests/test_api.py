import time
import pytest
import requests

from ray import serve
from ray.serve import BackendConfig
import ray
from ray.serve.constants import NO_ROUTE_KEY
from ray.serve.exceptions import RayServeException
from ray.serve.handle import RayServeHandle


def test_e2e(serve_instance):
    serve.init()  # so we have access to global state
    serve.create_endpoint("endpoint", "/api", methods=["GET", "POST"])
    result = serve.api._get_global_state().route_table.list_service()
    assert result["/api"] == "endpoint"

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

    serve.create_backend(function, "echo:v1")
    serve.link("endpoint", "echo:v1")

    resp = requests.get("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "GET"

    resp = requests.post("http://127.0.0.1:8000/api").json()["method"]
    assert resp == "POST"


def test_route_decorator(serve_instance):
    @serve.route("/hello_world")
    def hello_world(_):
        return ""

    assert isinstance(hello_world, RayServeHandle)

    hello_world.scale(2)
    assert serve.get_backend_config("hello_world:v0").num_replicas == 2

    with pytest.raises(
            RayServeException, match="method does not accept batching"):
        hello_world.set_max_batch_size(2)


def test_no_route(serve_instance):
    serve.create_endpoint("noroute-endpoint")
    global_state = serve.api._get_global_state()

    result = global_state.route_table.list_service(include_headless=True)
    assert result[NO_ROUTE_KEY] == ["noroute-endpoint"]

    without_headless_result = global_state.route_table.list_service()
    assert NO_ROUTE_KEY not in without_headless_result

    def func(_, i=1):
        return 1

    serve.create_backend(func, "backend:1")
    serve.link("noroute-endpoint", "backend:1")
    service_handle = serve.get_handle("noroute-endpoint")
    result = ray.get(service_handle.remote(i=1))
    assert result == 1


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

    b_config = BackendConfig(num_replicas=2)
    serve.create_backend(Counter, "counter:v1", backend_config=b_config)
    serve.link("counter", "counter:v1")

    counter_result = []
    for _ in range(10):
        resp = requests.get("http://127.0.0.1:8000/increment").json()
        counter_result.append(resp)

    # If the load is shared among two replicas. The max result cannot be 10.
    assert max(counter_result) < 10

    b_config = serve.get_backend_config("counter:v1")
    b_config.num_replicas = 1
    serve.set_backend_config("counter:v1", b_config)

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

    serve.create_endpoint("counter1", "/increment")

    # Keep checking the routing table until /increment is populated
    while "/increment" not in requests.get(
            "http://127.0.0.1:8000/-/routes").json():
        time.sleep(0.2)

    # set the max batch size
    b_config = BackendConfig(max_batch_size=5)
    serve.create_backend(
        BatchingExample, "counter:v11", backend_config=b_config)
    serve.link("counter1", "counter:v11")

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
    b_config = BackendConfig(max_batch_size=5)
    serve.create_backend(
        NoListReturned, "exception:v1", backend_config=b_config)
    serve.link("exception-test", "exception:v1")

    handle = serve.get_handle("exception-test")
    with pytest.raises(ray.exceptions.RayTaskError):
        assert ray.get(handle.remote(temp=1))


def test_killing_replicas(serve_instance):
    class Simple:
        def __init__(self):
            self.count = 0

        def __call__(self, flask_request, temp=None):
            return temp

    serve.create_endpoint("simple", "/simple")
    b_config = BackendConfig(num_replicas=3, num_cpus=2)
    serve.create_backend(Simple, "simple:v1", backend_config=b_config)
    global_state = serve.api._get_global_state()
    old_replica_tag_list = global_state.backend_table.list_replicas(
        "simple:v1")

    bnew_config = serve.get_backend_config("simple:v1")
    # change the config
    bnew_config.num_cpus = 1
    # set the config
    serve.set_backend_config("simple:v1", bnew_config)
    new_replica_tag_list = global_state.backend_table.list_replicas(
        "simple:v1")
    global_state.refresh_actor_handle_cache()
    new_all_tag_list = list(global_state.actor_handle_cache.keys())

    # the new_replica_tag_list must be subset of all_tag_list
    assert set(new_replica_tag_list) <= set(new_all_tag_list)

    # the old_replica_tag_list must not be subset of all_tag_list
    assert not set(old_replica_tag_list) <= set(new_all_tag_list)


def test_not_killing_replicas(serve_instance):
    class BatchSimple:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, flask_request, temp=None):
            batch_size = serve.context.batch_size
            return [1] * batch_size

    serve.create_endpoint("bsimple", "/bsimple")
    b_config = BackendConfig(num_replicas=3, max_batch_size=2)
    serve.create_backend(BatchSimple, "bsimple:v1", backend_config=b_config)
    global_state = serve.api._get_global_state()
    old_replica_tag_list = global_state.backend_table.list_replicas(
        "bsimple:v1")

    bnew_config = serve.get_backend_config("bsimple:v1")
    # change the config
    bnew_config.max_batch_size = 5
    # set the config
    serve.set_backend_config("bsimple:v1", bnew_config)
    new_replica_tag_list = global_state.backend_table.list_replicas(
        "bsimple:v1")
    global_state.refresh_actor_handle_cache()
    new_all_tag_list = list(global_state.actor_handle_cache.keys())

    # the old and new replica tag list should be identical
    # and should be subset of all_tag_list
    assert set(old_replica_tag_list) <= set(new_all_tag_list)
    assert set(old_replica_tag_list) == set(new_replica_tag_list)
