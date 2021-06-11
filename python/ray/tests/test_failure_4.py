import sys

import ray

import numpy as np
import pytest

from ray.cluster_utils import Cluster
import ray.ray_constants as ray_constants
from ray.test_utils import (init_error_pubsub, get_error_message)


def test_fill_object_store_exception(shutdown_only):
    ray.init(
        num_cpus=2,
        object_store_memory=10**8,
        _system_config={"automatic_object_spilling_enabled": False})

    if ray.worker.global_worker.core_worker.plasma_unlimited():
        return  # No exception is raised.

    @ray.remote
    def expensive_task():
        return np.zeros((10**8) // 10, dtype=np.uint8)

    with pytest.raises(ray.exceptions.RayTaskError) as e:
        ray.get([expensive_task.remote() for _ in range(20)])
        with pytest.raises(ray.exceptions.ObjectStoreFullError):
            raise e.as_instanceof_cause()

    @ray.remote
    class LargeMemoryActor:
        def some_expensive_task(self):
            return np.zeros(10**8 + 2, dtype=np.uint8)

        def test(self):
            return 1

    actor = LargeMemoryActor.remote()
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(actor.some_expensive_task.remote())
    # Make sure actor does not die
    ray.get(actor.test.remote())

    with pytest.raises(ray.exceptions.ObjectStoreFullError):
        ray.put(np.zeros(10**8 + 2, dtype=np.uint8))


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
