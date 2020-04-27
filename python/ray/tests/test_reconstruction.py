import json
import sys

import numpy as np
import pytest

import ray
from ray.cluster_utils import Cluster
from ray.test_utils import (
    wait_for_condition, )


def test_cached_object(ray_start_cluster):
    config = json.dumps({
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_timeout_milliseconds": 100,
    })
    cluster = Cluster()
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _internal_config=config)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8)
    cluster.add_node(
        num_cpus=1, resources={"node2": 1}, object_store_memory=10**8)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def dependent_task(x):
        return

    obj = large_object.options(resources={"node1": 1}).remote()
    ray.get(dependent_task.options(resources={"node2": 1}).remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(
        num_cpus=1, resources={"node1": 1}, object_store_memory=10**8)
    assert wait_for_condition(
        lambda: not all(node["Alive"] for node in ray.nodes()), timeout=10)

    for _ in range(20):
        large_object.options(resources={"node2": 1}).remote()

    ray.get(dependent_task.remote(obj))


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_reconstruction_cached_dependency(ray_start_cluster,
                                          reconstruction_enabled):
    config = json.dumps({
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_timeout_milliseconds": 100,
        "lineage_pinning_enabled": 1 if reconstruction_enabled else 0,
        "free_objects_period_milliseconds": -1,
    })
    cluster = Cluster()
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _internal_config=config)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1,
        resources={"node1": 1},
        object_store_memory=10**8,
        _internal_config=config)
    cluster.add_node(
        num_cpus=1,
        resources={"node2": 1},
        object_store_memory=10**8,
        _internal_config=config)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address, _internal_config=config)

    @ray.remote(max_retries=0)
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def chain(x):
        return x

    @ray.remote
    def dependent_task(x):
        return

    obj = large_object.options(resources={"node2": 1}).remote()
    obj = chain.options(resources={"node1": 1}).remote(obj)
    ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(
        num_cpus=1,
        resources={"node1": 1},
        object_store_memory=10**8,
        _internal_config=config)
    assert wait_for_condition(
        lambda: not all(node["Alive"] for node in ray.nodes()), timeout=10)

    for _ in range(20):
        large_object.options(resources={"node2": 1}).remote()

    if reconstruction_enabled:
        ray.get(dependent_task.remote(obj))
    else:
        with pytest.raises(ray.exceptions.RayTaskError) as e:
            ray.get(dependent_task.remote(obj))
            with pytest.raises(ray.exceptions.UnreconstructableError):
                raise e.as_instanceof_cause()


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_basic_reconstruction(ray_start_cluster, reconstruction_enabled):
    config = json.dumps({
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_timeout_milliseconds": 100,
        "lineage_pinning_enabled": 1 if reconstruction_enabled else 0,
        "free_objects_period_milliseconds": -1,
    })
    cluster = Cluster()
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _internal_config=config)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1,
        resources={"node1": 1},
        object_store_memory=10**8,
        _internal_config=config)
    cluster.add_node(
        num_cpus=1,
        resources={"node2": 1},
        object_store_memory=10**8,
        _internal_config=config)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address, _internal_config=config)

    @ray.remote(max_retries=1 if reconstruction_enabled else 0)
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def dependent_task(x):
        return

    obj = large_object.options(resources={"node1": 1}).remote()
    ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(
        num_cpus=1,
        resources={"node1": 1},
        object_store_memory=10**8,
        _internal_config=config)

    if reconstruction_enabled:
        ray.get(dependent_task.remote(obj))
    else:
        with pytest.raises(ray.exceptions.RayTaskError) as e:
            ray.get(dependent_task.remote(obj))
            with pytest.raises(ray.exceptions.UnreconstructableError):
                raise e.as_instanceof_cause()


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_basic_reconstruction_put(ray_start_cluster, reconstruction_enabled):
    config = json.dumps({
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_timeout_milliseconds": 100,
        "lineage_pinning_enabled": 1 if reconstruction_enabled else 0,
        "free_objects_period_milliseconds": -1,
    })
    cluster = Cluster()
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _internal_config=config)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1,
        resources={"node1": 1},
        object_store_memory=10**8,
        _internal_config=config)
    cluster.add_node(
        num_cpus=1,
        resources={"node2": 1},
        object_store_memory=10**8,
        _internal_config=config)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address, _internal_config=config)

    @ray.remote(max_retries=1 if reconstruction_enabled else 0)
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def dependent_task(x):
        return x

    obj = ray.put(np.zeros(10**7, dtype=np.uint8))
    result = dependent_task.options(resources={"node1": 1}).remote(obj)
    ray.get(result)
    del obj

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(
        num_cpus=1,
        resources={"node1": 1},
        object_store_memory=10**8,
        _internal_config=config)

    for _ in range(20):
        ray.put(np.zeros(10**7, dtype=np.uint8))

    if reconstruction_enabled:
        ray.get(result)
    else:
        with pytest.raises(ray.exceptions.UnreconstructableError):
            ray.get(result)


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_multiple_downstream_tasks(ray_start_cluster, reconstruction_enabled):
    config = json.dumps({
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_timeout_milliseconds": 100,
        "lineage_pinning_enabled": 1 if reconstruction_enabled else 0,
        "free_objects_period_milliseconds": -1,
    })
    cluster = Cluster()
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _internal_config=config)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(
        num_cpus=1,
        resources={"node1": 1},
        object_store_memory=10**8,
        _internal_config=config)
    cluster.add_node(
        num_cpus=1,
        resources={"node2": 1},
        object_store_memory=10**8,
        _internal_config=config)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address, _internal_config=config)

    @ray.remote(max_retries=1 if reconstruction_enabled else 0)
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def chain(x):
        return x

    @ray.remote
    def dependent_task(x):
        return

    obj = large_object.options(resources={"node2": 1}).remote()
    downstream = [chain.remote(obj) for _ in range(4)]
    for obj in downstream:
        ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(
        num_cpus=1,
        resources={"node1": 1},
        object_store_memory=10**8,
        _internal_config=config)

    if reconstruction_enabled:
        for obj in downstream:
            ray.get(dependent_task.options(resources={"node1": 1}).remote(obj))
    else:
        with pytest.raises(ray.exceptions.RayTaskError) as e:
            for obj in downstream:
                ray.get(
                    dependent_task.options(resources={
                        "node1": 1
                    }).remote(obj))
            with pytest.raises(ray.exceptions.UnreconstructableError):
                raise e.as_instanceof_cause()


@pytest.mark.parametrize("reconstruction_enabled", [False, True])
def test_reconstruction_chain(ray_start_cluster, reconstruction_enabled):
    config = json.dumps({
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_timeout_milliseconds": 100,
        "lineage_pinning_enabled": 1 if reconstruction_enabled else 0,
        "free_objects_period_milliseconds": -1,
    })
    cluster = Cluster()
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0, _internal_config=config, object_store_memory=10**8)
    node_to_kill = cluster.add_node(
        num_cpus=1, object_store_memory=10**8, _internal_config=config)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address, _internal_config=config)

    @ray.remote(max_retries=1 if reconstruction_enabled else 0)
    def large_object():
        return np.zeros(10**7, dtype=np.uint8)

    @ray.remote
    def chain(x):
        return x

    @ray.remote
    def dependent_task(x):
        return x

    obj = large_object.remote()
    for _ in range(20):
        obj = chain.remote(obj)
    ray.get(dependent_task.remote(obj))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    cluster.add_node(
        num_cpus=1, object_store_memory=10**8, _internal_config=config)

    if reconstruction_enabled:
        ray.get(dependent_task.remote(obj))
    else:
        with pytest.raises(ray.exceptions.RayTaskError) as e:
            ray.get(dependent_task.remote(obj))
            with pytest.raises(ray.exceptions.UnreconstructableError):
                raise e.as_instanceof_cause()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
