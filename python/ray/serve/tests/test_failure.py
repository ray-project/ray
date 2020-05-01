import os
import requests
import tempfile
import time

import ray
from ray import serve


def request_with_retries(endpoint, timeout=30):
    start = time.time()
    while True:
        try:
            return requests.get(
                "http://127.0.0.1:8000" + endpoint, timeout=timeout)
        except requests.RequestException:
            if time.time() - start > timeout:
                raise TimeoutError
            time.sleep(0.1)


def test_master_failure(serve_instance):
    serve.init()
    serve.create_endpoint("master_failure", "/master_failure")

    def function():
        return "hello1"

    serve.create_backend("master_failure:v1", function)
    serve.set_traffic("master_failure", {"master_failure:v1": 1.0})

    assert request_with_retries("/master_failure", timeout=1).text == "hello1"

    for _ in range(10):
        response = request_with_retries("/master_failure", timeout=30)
        assert response.text == "hello1"

    ray.kill(serve.api._get_master_actor())

    for _ in range(10):
        response = request_with_retries("/master_failure", timeout=30)
        assert response.text == "hello1"

    def function():
        return "hello2"

    ray.kill(serve.api._get_master_actor())

    serve.create_backend("master_failure:v2", function)
    serve.set_traffic("master_failure", {"master_failure:v2": 1.0})

    for _ in range(10):
        response = request_with_retries("/master_failure", timeout=30)
        assert response.text == "hello2"

    def function():
        return "hello3"

    ray.kill(serve.api._get_master_actor())
    serve.create_endpoint("master_failure_2", "/master_failure_2")
    ray.kill(serve.api._get_master_actor())
    serve.create_backend("master_failure_2", function)
    ray.kill(serve.api._get_master_actor())
    serve.set_traffic("master_failure_2", {"master_failure_2": 1.0})

    for _ in range(10):
        response = request_with_retries("/master_failure", timeout=30)
        assert response.text == "hello2"
        response = request_with_retries("/master_failure_2", timeout=30)
        assert response.text == "hello3"


def _kill_http_proxy():
    [http_proxy] = ray.get(
        serve.api._get_master_actor().get_http_proxy.remote())
    ray.kill(http_proxy)


def test_http_proxy_failure(serve_instance):
    serve.init()
    serve.create_endpoint("proxy_failure", "/proxy_failure")

    def function():
        return "hello1"

    serve.create_backend("proxy_failure:v1", function)
    serve.set_traffic("proxy_failure", {"proxy_failure:v1": 1.0})

    assert request_with_retries("/proxy_failure", timeout=1.0).text == "hello1"

    for _ in range(10):
        response = request_with_retries("/proxy_failure", timeout=30)
        assert response.text == "hello1"

    _kill_http_proxy()

    def function():
        return "hello2"

    serve.create_backend("proxy_failure:v2", function)
    serve.set_traffic("proxy_failure", {"proxy_failure:v2": 1.0})

    for _ in range(10):
        response = request_with_retries("/proxy_failure", timeout=30)
        assert response.text == "hello2"


def _kill_router():
    [router] = ray.get(serve.api._get_master_actor().get_router.remote())
    ray.kill(router)


def test_router_failure(serve_instance):
    serve.init()
    serve.create_endpoint("router_failure", "/router_failure")

    def function():
        return "hello1"

    serve.create_backend("router_failure:v1", function)
    serve.set_traffic("router_failure", {"router_failure:v1": 1.0})

    assert request_with_retries("/router_failure", timeout=5).text == "hello1"

    for _ in range(10):
        response = request_with_retries("/router_failure", timeout=30)
        assert response.text == "hello1"

    _kill_router()

    for _ in range(10):
        response = request_with_retries("/router_failure", timeout=30)
        assert response.text == "hello1"

    def function():
        return "hello2"

    serve.create_backend("router_failure:v2", function)
    serve.set_traffic("router_failure", {"router_failure:v2": 1.0})

    for _ in range(10):
        response = request_with_retries("/router_failure", timeout=30)
        assert response.text == "hello2"


def _get_worker_handles(backend):
    master_actor = serve.api._get_master_actor()
    backend_dict = ray.get(master_actor.get_all_worker_handles.remote())

    return list(backend_dict[backend].values())


# Test that a worker dying unexpectedly causes it to restart and continue
# serving requests.
def test_worker_restart(serve_instance):
    serve.init()
    serve.create_endpoint("worker_failure", "/worker_failure")

    class Worker1:
        def __call__(self):
            return os.getpid()

    serve.create_backend("worker_failure:v1", Worker1)
    serve.set_traffic("worker_failure", {"worker_failure:v1": 1.0})

    # Get the PID of the worker.
    old_pid = request_with_retries("/worker_failure", timeout=1).text

    # Kill the worker.
    handles = _get_worker_handles("worker_failure:v1")
    assert len(handles) == 1
    ray.kill(handles[0])

    # Wait until the worker is killed and a one is started.
    start = time.time()
    while time.time() - start < 30:
        response = request_with_retries("/worker_failure", timeout=30)
        if response.text != old_pid:
            break
    else:
        assert False, "Timed out waiting for worker to die."


# Test that if there are multiple replicas for a worker and one dies
# unexpectedly, the others continue to serve requests.
def test_worker_replica_failure(serve_instance):
    serve.http_proxy.MAX_ACTOR_DEAD_RETRIES = 0
    serve.init()
    serve.create_endpoint("replica_failure", "/replica_failure")

    class Worker:
        # Assumes that two replicas are started. Will hang forever in the
        # constructor for any workers that are restarted.
        def __init__(self, path):
            self.should_hang = False
            if not os.path.exists(path):
                with open(path, "w") as f:
                    f.write("1")
            else:
                with open(path, "r") as f:
                    num = int(f.read())

                with open(path, "w") as f:
                    if num == 2:
                        self.should_hang = True
                    else:
                        f.write(str(num + 1))

            if self.should_hang:
                while True:
                    pass

        def __call__(self):
            pass

    temp_path = tempfile.gettempdir() + "/" + serve.utils.get_random_letters()
    serve.create_backend("replica_failure", Worker, temp_path)
    serve.update_backend_config("replica_failure", {"num_replicas": 2})
    serve.set_traffic("replica_failure", {"replica_failure": 1.0})

    # Wait until both replicas have been started.
    responses = set()
    while len(responses) == 1:
        responses.add(request_with_retries("/replica_failure", timeout=1).text)
        time.sleep(0.1)

    # Kill one of the replicas.
    handles = _get_worker_handles("replica_failure")
    assert len(handles) == 2
    ray.kill(handles[0])

    # Check that the other replica still serves requests.
    for _ in range(10):
        while True:
            try:
                # The timeout needs to be small here because the request to
                # the restarting worker will hang.
                request_with_retries("/replica_failure", timeout=0.1)
                break
            except TimeoutError:
                time.sleep(0.1)
