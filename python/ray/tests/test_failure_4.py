import sys

import ray

import pytest

from ray.cluster_utils import Cluster
import ray.ray_constants as ray_constants
from ray._private.test_utils import (init_error_pubsub, get_error_message,
                                     run_string_as_driver)


def test_retry_system_level_error(ray_start_regular):
    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    @ray.remote(max_retries=1)
    def func(counter):
        count = counter.increment.remote()
        if ray.get(count) == 1:
            import os
            os._exit(0)
        else:
            return 1

    counter1 = Counter.remote()
    r1 = func.remote(counter1)
    assert ray.get(r1) == 1

    counter2 = Counter.remote()
    r2 = func.options(max_retries=0).remote(counter2)
    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(r2)


def test_retry_application_level_error(ray_start_regular):
    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    @ray.remote(max_retries=1, retry_exceptions=True)
    def func(counter):
        count = counter.increment.remote()
        if ray.get(count) == 1:
            raise ValueError()
        else:
            return 2

    counter1 = Counter.remote()
    r1 = func.remote(counter1)
    assert ray.get(r1) == 2

    counter2 = Counter.remote()
    r2 = func.options(max_retries=0).remote(counter2)
    with pytest.raises(ValueError):
        ray.get(r2)

    counter3 = Counter.remote()
    r3 = func.options(retry_exceptions=False).remote(counter3)
    with pytest.raises(ValueError):
        ray.get(r3)


def test_connect_with_disconnected_node(shutdown_only):
    config = {
        "num_heartbeats_timeout": 50,
        "raylet_heartbeat_period_milliseconds": 10,
    }
    cluster = Cluster()
    cluster.add_node(num_cpus=0, _system_config=config)
    ray.init(address=cluster.address)
    p = init_error_pubsub()
    errors = get_error_message(p, 1, timeout=5)
    print(errors)
    assert len(errors) == 0
    # This node is killed by SIGKILL, ray_monitor will mark it to dead.
    dead_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(dead_node, allow_graceful=False)
    errors = get_error_message(p, 1, ray_constants.REMOVED_NODE_ERROR)
    assert len(errors) == 1
    # This node is killed by SIGKILL, ray_monitor will mark it to dead.
    dead_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(dead_node, allow_graceful=False)
    errors = get_error_message(p, 1, ray_constants.REMOVED_NODE_ERROR)
    assert len(errors) == 1
    # This node is killed by SIGTERM, ray_monitor will not mark it again.
    removing_node = cluster.add_node(num_cpus=0)
    cluster.remove_node(removing_node, allow_graceful=True)
    errors = get_error_message(p, 1, timeout=2)
    assert len(errors) == 0
    # There is no connection error to a dead node.
    errors = get_error_message(p, 1, timeout=2)
    assert len(errors) == 0
    p.close()


def test_detached_actor_ref(call_ray_start):
    address = call_ray_start

    driver_script = """
import ray
import time


@ray.remote
def foo(x):
    return ray.put(42)


@ray.remote
class Actor:
    def __init__(self):
        self.ref = None

    def invoke(self):
        self.ref = foo.remote(0)
        # Wait for the task to finish before exiting the driver.
        ray.get(self.ref)

    def get(self):
        print("get", self.ref)
        return self.ref


if __name__ == "__main__":
    ray.init(address="{}", namespace="default")
    a = Actor.options(name="holder", lifetime="detached").remote()
    # Wait for the task to finish before exiting the driver.
    ray.get(a.invoke.remote())
    print("success")
""".format(address)

    out = run_string_as_driver(driver_script)
    assert "success" in out

    import time
    time.sleep(5)

    # connect to the cluster
    ray.init(address=address, namespace="default")
    actor = ray.get_actor("holder")
    x = actor.get.remote()
    while isinstance(x, ray.ObjectRef):
        x = ray.get(x)
    assert x == 42


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("debug_enabled", [False, True])
def test_object_lost_error(ray_start_cluster, debug_enabled):
    cluster = ray_start_cluster
    system_config = {
        "num_heartbeats_timeout": 3,
    }
    if debug_enabled:
        system_config["record_ref_creation_sites"] = True
    cluster.add_node(num_cpus=0, _system_config=system_config)
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            return

        def foo(self):
            return "x" * 1000_000

        def done(self):
            return

    @ray.remote
    def borrower(ref):
        ray.get(ref[0])

    @ray.remote
    def task_arg(ref):
        return

    a = Actor.remote()
    x = a.foo.remote()
    ray.get(a.done.remote())
    cluster.remove_node(worker_node, allow_graceful=False)
    cluster.add_node(num_cpus=1)

    y = borrower.remote([x])

    try:
        ray.get(x)
        assert False
    except ray.exceptions.ObjectLostError as e:
        error = str(e)
        print(error)
        assert ("actor call" in error) == debug_enabled
        assert ("test_object_lost_error" in error) == debug_enabled

    try:
        ray.get(y)
        assert False
    except ray.exceptions.RayTaskError as e:
        error = str(e)
        print(error)
        assert ("actor call" in error) == debug_enabled
        assert ("test_object_lost_error" in error) == debug_enabled

    try:
        ray.get(task_arg.remote(x))
    except ray.exceptions.RayTaskError as e:
        error = str(e)
        print(error)
        assert ("actor call" in error) == debug_enabled
        assert ("test_object_lost_error" in error) == debug_enabled


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
