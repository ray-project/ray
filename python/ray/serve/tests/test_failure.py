import os
import requests
import sys
import time

import pytest
import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.constants import (
    SERVE_DEFAULT_APP_NAME,
    DEPLOYMENT_NAME_PREFIX_SEPARATOR,
)


def request_with_retries(endpoint, timeout=30):
    start = time.time()
    while True:
        try:
            return requests.get("http://127.0.0.1:8000" + endpoint, timeout=timeout)
        except requests.RequestException:
            if time.time() - start > timeout:
                raise TimeoutError
            time.sleep(0.1)


@pytest.mark.skip(reason="Consistently failing.")
def test_controller_failure(serve_instance):
    @serve.deployment(name="controller_failure")
    def function(_):
        return "hello1"

    serve.run(function.bind())

    assert request_with_retries("/controller_failure/", timeout=1).text == "hello1"

    for _ in range(10):
        response = request_with_retries("/controller_failure/", timeout=30)
        assert response.text == "hello1"

    ray.kill(serve.context._global_client._controller, no_restart=False)

    for _ in range(10):
        response = request_with_retries("/controller_failure/", timeout=30)
        assert response.text == "hello1"

    def function2(_):
        return "hello2"

    ray.kill(serve.context._global_client._controller, no_restart=False)

    serve.run(function.options(func_or_class=function2).bind())

    def check_controller_failure():
        response = request_with_retries("/controller_failure/", timeout=30)
        return response.text == "hello2"

    wait_for_condition(check_controller_failure)

    @serve.deployment(name="controller_failure_2")
    def function3(_):
        return "hello3"

    ray.kill(serve.context._global_client._controller, no_restart=False)
    serve.run(function3.bind())
    ray.kill(serve.context._global_client._controller, no_restart=False)

    for _ in range(10):
        response = request_with_retries("/controller_failure/", timeout=30)
        assert response.text == "hello2"
        response = request_with_retries("/controller_failure_2/", timeout=30)
        assert response.text == "hello3"


def _kill_http_proxies():
    http_proxies = ray.get(
        serve.context._global_client._controller.get_http_proxies.remote()
    )
    for http_proxy in http_proxies.values():
        ray.kill(http_proxy, no_restart=False)


def test_http_proxy_failure(serve_instance):
    @serve.deployment(name="proxy_failure")
    def function(_):
        return "hello1"

    serve.run(function.bind())

    assert request_with_retries("/proxy_failure/", timeout=1.0).text == "hello1"

    for _ in range(10):
        response = request_with_retries("/proxy_failure/", timeout=30)
        assert response.text == "hello1"

    _kill_http_proxies()

    def function2(_):
        return "hello2"

    serve.run(function.options(func_or_class=function2).bind())

    def check_new():
        for _ in range(10):
            response = request_with_retries("/proxy_failure/", timeout=30)
            if response.text != "hello2":
                return False
        return True

    wait_for_condition(check_new)


def _get_worker_handles(deployment):
    deployment_name = (
        f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}{deployment}"
    )
    controller = serve.context._global_client._controller
    deployment_dict = ray.get(controller._all_running_replicas.remote())

    return [replica.actor_handle for replica in deployment_dict[deployment_name]]


# Test that a worker dying unexpectedly causes it to restart and continue
# serving requests.
def test_worker_restart(serve_instance):
    @serve.deployment(name="worker_failure")
    class Worker1:
        def __call__(self, *args):
            return os.getpid()

    serve.run(Worker1.bind())

    # Get the PID of the worker.
    old_pid = request_with_retries("/worker_failure/", timeout=1).text

    # Kill the worker.
    handles = _get_worker_handles("worker_failure")
    assert len(handles) == 1
    ray.kill(handles[0], no_restart=False)

    # Wait until the worker is killed and a one is started.
    start = time.time()
    while time.time() - start < 30:
        response = request_with_retries("/worker_failure/", timeout=30)
        if response.text != old_pid:
            break
    else:
        assert False, "Timed out waiting for worker to die."


# Test that if there are multiple replicas for a worker and one dies
# unexpectedly, the others continue to serve requests.
def test_worker_replica_failure(serve_instance):
    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def inc_and_get(self):
            self.count += 1
            return self.count

    @serve.deployment(name="replica_failure")
    class Worker:
        # Assumes that two replicas are started. Will hang forever in the
        # constructor for any workers that are restarted.
        def __init__(self, counter):
            self.should_hang = False
            self.index = ray.get(counter.inc_and_get.remote())
            if self.index > 2:
                while True:
                    pass

        def __call__(self, *args):
            return self.index

    counter = Counter.remote()
    serve.run(Worker.options(num_replicas=2).bind(counter))

    # Wait until both replicas have been started.
    responses = set()
    start = time.time()
    while time.time() - start < 30:
        time.sleep(0.1)
        response = request_with_retries("/replica_failure/", timeout=1).text
        assert response in ["1", "2"]
        responses.add(response)
        if len(responses) > 1:
            break
    else:
        raise TimeoutError("Timed out waiting for replicas after 30s.")

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
                request_with_retries("/replica_failure/", timeout=0.1)
                break
            except TimeoutError:
                time.sleep(0.1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
