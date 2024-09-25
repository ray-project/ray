import ctypes
import os
import random
import sys
import time

import pytest
import requests

import ray
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.exceptions import RayActorError
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve._private.test_utils import Counter, get_deployment_details, tlog


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
        serve.context._global_client._controller.get_proxies.remote()
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


def _get_worker_handles(deployment_name: str, app_name: str = SERVE_DEFAULT_APP_NAME):
    id = DeploymentID(name=deployment_name, app_name=app_name)
    controller = serve.context._global_client._controller
    deployment_dict = ray.get(controller._all_running_replicas.remote())

    return [replica.actor_handle for replica in deployment_dict[id]]


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


def test_no_available_replicas_does_not_block_proxy(serve_instance):
    """Test that handle blocking waiting for replicas doesn't block proxy.

    This is essential so that other requests and health checks can pass while a
    deployment is deploying/updating.

    See https://github.com/ray-project/ray/issues/36460.
    """

    @serve.deployment
    class SlowStarter:
        def __init__(self, starting_actor, finish_starting_actor):
            ray.get(starting_actor.send.remote())
            ray.get(finish_starting_actor.wait.remote())

        def __call__(self):
            return "hi"

    @ray.remote
    def make_blocked_request():
        r = requests.get("http://localhost:8000/")
        r.raise_for_status()
        return r.text

    # Loop twice: first iteration tests deploying from nothing, second iteration
    # tests updating the replicas of an existing deployment.
    for _ in range(2):
        starting_actor = SignalActor.remote()
        finish_starting_actor = SignalActor.remote()
        serve._run(
            SlowStarter.bind(starting_actor, finish_starting_actor), _blocking=False
        )

        # Ensure that the replica has been started (we use _blocking=False).
        ray.get(starting_actor.wait.remote())

        # The request shouldn't complete until the replica has finished started.
        blocked_ref = make_blocked_request.remote()
        with pytest.raises(TimeoutError):
            ray.get(blocked_ref, timeout=1)

        # If the proxy's loop was blocked, these would hang.
        requests.get("http://localhost:8000/-/routes").raise_for_status()
        requests.get("http://localhost:8000/-/healthz").raise_for_status()

        # Signal the replica to finish starting; request should complete.
        ray.get(finish_starting_actor.send.remote())
        assert ray.get(blocked_ref) == "hi"


@pytest.mark.skipif(
    sys.platform == "win32", reason="ActorDiedError not raised properly on windows."
)
@pytest.mark.parametrize("die_during_request", [False, True])
def test_replica_actor_died(serve_instance, die_during_request):
    """Test replica death paired with delayed handle notification.

    If a replica died, but controller hasn't health-checked it and
    broadcasted it to handle routers yet, the router should be able
    to remove it from its replica set.
    """

    counter = Counter.remote(2)

    @serve.deployment
    class Dummy:
        def __call__(self, crash: bool = False):
            if crash:
                ctypes.string_at(0)

            return os.getpid()

        def check_health(self):
            ray.get(counter.inc.remote())

    h = serve.run(Dummy.options(num_replicas=2, health_check_period_s=1000).bind())

    deployment_details = get_deployment_details("Dummy", _client=serve_instance)
    replicas = [r["actor_name"] for r in deployment_details["replicas"]]

    # Wait for controller to health check both replicas
    ray.get(counter.wait.remote(), timeout=1)
    tlog("Controller has health checked both replicas")

    # Send some requests, both replicas should be up and running
    assert len({h.remote().result() for _ in range(10)}) == 2
    tlog("Sent 10 warmup requests.")

    # Kill one replica.
    if die_during_request:
        with pytest.raises(RayActorError):
            h.remote(crash=True).result()
    else:
        replica_to_kill = random.choice(replicas)
        tlog(f"Killing replica {replica_to_kill}")
        ray.kill(ray.get_actor(replica_to_kill, namespace="serve"))

    # The controller just health checked both of them, so it should not
    # be able to health check and notify the handle router in time. Then
    # we test that router can properly recognize that the replica has
    # died and not send requests to that replica.
    pids_returned = set()
    for _ in range(10):
        pids_returned.add(h.remote().result())

    assert len(pids_returned) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
