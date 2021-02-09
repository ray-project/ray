import os
import requests
import tempfile
import time

import ray
from ray.test_utils import wait_for_condition
from ray import serve
from ray.serve.config import BackendConfig, ReplicaConfig


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


def test_controller_failure(serve_instance):
    client = serve_instance

    def function(_):
        return "hello1"

    client.create_backend("controller_failure:v1", function)
    client.create_endpoint(
        "controller_failure",
        backend="controller_failure:v1",
        route="/controller_failure")

    assert request_with_retries(
        "/controller_failure", timeout=1).text == "hello1"

    for _ in range(10):
        response = request_with_retries("/controller_failure", timeout=30)
        assert response.text == "hello1"

    ray.kill(client._controller, no_restart=False)

    for _ in range(10):
        response = request_with_retries("/controller_failure", timeout=30)
        assert response.text == "hello1"

    def function(_):
        return "hello2"

    ray.kill(client._controller, no_restart=False)

    client.create_backend("controller_failure:v2", function)
    client.set_traffic("controller_failure", {"controller_failure:v2": 1.0})

    def check_controller_failure():
        response = request_with_retries("/controller_failure", timeout=30)
        return response.text == "hello2"

    wait_for_condition(check_controller_failure)

    def function(_):
        return "hello3"

    ray.kill(client._controller, no_restart=False)
    client.create_backend("controller_failure_2", function)
    ray.kill(client._controller, no_restart=False)
    client.create_endpoint(
        "controller_failure_2",
        backend="controller_failure_2",
        route="/controller_failure_2")
    ray.kill(client._controller, no_restart=False)

    for _ in range(10):
        response = request_with_retries("/controller_failure", timeout=30)
        assert response.text == "hello2"
        response = request_with_retries("/controller_failure_2", timeout=30)
        assert response.text == "hello3"


def _kill_http_proxies(client):
    http_proxies = ray.get(client._controller.get_http_proxies.remote())
    for http_proxy in http_proxies.values():
        ray.kill(http_proxy, no_restart=False)


def test_http_proxy_failure(serve_instance):
    client = serve_instance

    def function(_):
        return "hello1"

    client.create_backend("proxy_failure:v1", function)
    client.create_endpoint(
        "proxy_failure", backend="proxy_failure:v1", route="/proxy_failure")

    assert request_with_retries("/proxy_failure", timeout=1.0).text == "hello1"

    for _ in range(10):
        response = request_with_retries("/proxy_failure", timeout=30)
        assert response.text == "hello1"

    _kill_http_proxies(client)

    def function(_):
        return "hello2"

    client.create_backend("proxy_failure:v2", function)
    client.set_traffic("proxy_failure", {"proxy_failure:v2": 1.0})

    for _ in range(10):
        response = request_with_retries("/proxy_failure", timeout=30)
        assert response.text == "hello2"


def _get_worker_handles(client, backend):
    controller = client._controller
    backend_dict = ray.get(controller._all_replica_handles.remote())

    return list(backend_dict[backend].values())


# Test that a worker dying unexpectedly causes it to restart and continue
# serving requests.
def test_worker_restart(serve_instance):
    client = serve_instance

    class Worker1:
        def __call__(self, *args):
            return os.getpid()

    client.create_backend("worker_failure:v1", Worker1)
    client.create_endpoint(
        "worker_failure", backend="worker_failure:v1", route="/worker_failure")

    # Get the PID of the worker.
    old_pid = request_with_retries("/worker_failure", timeout=1).text

    # Kill the worker.
    handles = _get_worker_handles(client, "worker_failure:v1")
    assert len(handles) == 1
    ray.kill(handles[0], no_restart=False)

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
    client = serve_instance

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

        def __call__(self, *args):
            pass

    temp_path = os.path.join(tempfile.gettempdir(),
                             serve.utils.get_random_letters())
    client.create_backend("replica_failure", Worker, temp_path)
    client.update_backend_config(
        "replica_failure", BackendConfig(num_replicas=2))
    client.create_endpoint(
        "replica_failure", backend="replica_failure", route="/replica_failure")

    # Wait until both replicas have been started.
    responses = set()
    while len(responses) == 1:
        responses.add(request_with_retries("/replica_failure", timeout=1).text)
        time.sleep(0.1)

    # Kill one of the replicas.
    handles = _get_worker_handles(client, "replica_failure")
    assert len(handles) == 2
    ray.kill(handles[0], no_restart=False)

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


def test_create_backend_idempotent(serve_instance):
    client = serve_instance

    def f(_):
        return "hello"

    controller = client._controller

    replica_config = ReplicaConfig(f)
    backend_config = BackendConfig(num_replicas=1)

    for i in range(10):
        ray.get(
            controller.wait_for_goal.remote(
                controller.create_backend.remote("my_backend", backend_config,
                                                 replica_config)))

    assert len(ray.get(controller.get_all_backends.remote())) == 1
    client.create_endpoint(
        "my_endpoint", backend="my_backend", route="/my_route")

    assert requests.get("http://127.0.0.1:8000/my_route").text == "hello"


def test_create_endpoint_idempotent(serve_instance):
    client = serve_instance

    def f(_):
        return "hello"

    client.create_backend("my_backend", f)

    controller = client._controller

    for i in range(10):
        ray.get(
            controller.wait_for_goal.remote(
                controller.create_endpoint.remote(
                    "my_endpoint", {"my_backend": 1.0}, "/my_route", ["GET"])))

    assert len(ray.get(controller.get_all_endpoints.remote())) == 1
    assert requests.get("http://127.0.0.1:8000/my_route").text == "hello"
    resp = requests.get("http://127.0.0.1:8000/-/routes", timeout=0.5).json()
    assert resp == {"/my_route": ["my_endpoint", ["GET"]]}


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
