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


def test_controller_failure(serve_instance):
    serve.init()

    def function():
        return "hello1"

    serve.create_backend("controller_failure:v1", function)
    serve.create_endpoint(
        "controller_failure",
        backend="controller_failure:v1",
        route="/controller_failure")

    assert request_with_retries(
        "/controller_failure", timeout=1).text == "hello1"

    for _ in range(10):
        response = request_with_retries("/controller_failure", timeout=30)
        assert response.text == "hello1"

    ray.kill(serve.api._get_controller(), no_restart=False)

    for _ in range(10):
        response = request_with_retries("/controller_failure", timeout=30)
        assert response.text == "hello1"

    def function():
        return "hello2"

    ray.kill(serve.api._get_controller(), no_restart=False)

    serve.create_backend("controller_failure:v2", function)
    serve.set_traffic("controller_failure", {"controller_failure:v2": 1.0})

    for _ in range(10):
        response = request_with_retries("/controller_failure", timeout=30)
        assert response.text == "hello2"

    def function():
        return "hello3"

    ray.kill(serve.api._get_controller(), no_restart=False)
    serve.create_backend("controller_failure_2", function)
    ray.kill(serve.api._get_controller(), no_restart=False)
    serve.create_endpoint(
        "controller_failure_2",
        backend="controller_failure_2",
        route="/controller_failure_2")
    ray.kill(serve.api._get_controller(), no_restart=False)

    for _ in range(10):
        response = request_with_retries("/controller_failure", timeout=30)
        assert response.text == "hello2"
        response = request_with_retries("/controller_failure_2", timeout=30)
        assert response.text == "hello3"


def _kill_routers():
    routers = ray.get(serve.api._get_controller().get_router.remote())
    for router in routers:
        ray.kill(router, no_restart=False)


def test_http_proxy_failure(serve_instance):
    serve.init()

    def function():
        return "hello1"

    serve.create_backend("proxy_failure:v1", function)
    serve.create_endpoint(
        "proxy_failure", backend="proxy_failure:v1", route="/proxy_failure")

    assert request_with_retries("/proxy_failure", timeout=1.0).text == "hello1"

    for _ in range(10):
        response = request_with_retries("/proxy_failure", timeout=30)
        assert response.text == "hello1"

    _kill_routers()

    def function():
        return "hello2"

    serve.create_backend("proxy_failure:v2", function)
    serve.set_traffic("proxy_failure", {"proxy_failure:v2": 1.0})

    for _ in range(10):
        response = request_with_retries("/proxy_failure", timeout=30)
        assert response.text == "hello2"


def _get_worker_handles(backend):
    controller = serve.api._get_controller()
    backend_dict = ray.get(controller.get_all_worker_handles.remote())

    return list(backend_dict[backend].values())


# Test that a worker dying unexpectedly causes it to restart and continue
# serving requests.
def test_worker_restart(serve_instance):
    serve.init()

    class Worker1:
        def __call__(self):
            return os.getpid()

    serve.create_backend("worker_failure:v1", Worker1)
    serve.create_endpoint(
        "worker_failure", backend="worker_failure:v1", route="/worker_failure")

    # Get the PID of the worker.
    old_pid = request_with_retries("/worker_failure", timeout=1).text

    # Kill the worker.
    handles = _get_worker_handles("worker_failure:v1")
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
    serve.http_proxy.MAX_ACTOR_DEAD_RETRIES = 0
    serve.init()

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

    temp_path = os.path.join(tempfile.gettempdir(),
                             serve.utils.get_random_letters())
    serve.create_backend("replica_failure", Worker, temp_path)
    serve.update_backend_config("replica_failure", {"num_replicas": 2})
    serve.create_endpoint(
        "replica_failure", backend="replica_failure", route="/replica_failure")

    # Wait until both replicas have been started.
    responses = set()
    while len(responses) == 1:
        responses.add(request_with_retries("/replica_failure", timeout=1).text)
        time.sleep(0.1)

    # Kill one of the replicas.
    handles = _get_worker_handles("replica_failure")
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


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
