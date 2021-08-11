import sys

import ray

import numpy as np
import pytest

from ray.cluster_utils import Cluster
import ray.ray_constants as ray_constants
from ray.test_utils import (init_error_pubsub, get_error_message,
                            run_string_as_driver)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
