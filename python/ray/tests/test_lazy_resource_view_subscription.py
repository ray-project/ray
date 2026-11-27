import sys

import pytest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

LAZY_CONFIG = {"raylet_lazy_resource_view_subscription": True}


@ray.remote(num_cpus=0)
def get_node_id():
    return ray.get_runtime_context().get_node_id()


def test_spillback_from_head(ray_start_cluster):
    """A lease the head can't run waits for the view, then spills."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, _system_config=LAZY_CONFIG)
    ray.init(address=cluster.address)
    worker = cluster.add_node(num_cpus=1, resources={"worker": 1})
    cluster.wait_for_nodes()

    # Locally runnable work doesn't need the cluster view.
    assert ray.get(get_node_id.remote()) == ray.get_runtime_context().get_node_id()

    task = get_node_id.options(resources={"worker": 1})
    assert ray.get(task.remote(), timeout=60) == worker.node_id

    # A node added after the head subscribed shows up in its view.
    late = cluster.add_node(num_cpus=1, resources={"late": 1})
    cluster.wait_for_nodes()
    task = get_node_id.options(resources={"late": 1})
    assert ray.get(task.remote(), timeout=60) == late.node_id


def test_spillback_from_worker_node(ray_start_cluster):
    """A worker node's raylet subscribes the first time it needs the view."""
    cluster = ray_start_cluster
    head = cluster.add_node(
        num_cpus=1, resources={"head": 1}, _system_config=LAZY_CONFIG
    )
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"worker": 1})
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=0, resources={"worker": 1})
    class Actor:
        def run_on_head(self):
            # Submitted to this worker node's raylet, which must spill it back.
            task = get_node_id.options(resources={"head": 1})
            return ray.get(task.remote())

    actor = Actor.remote()
    assert ray.get(actor.run_on_head.remote(), timeout=60) == head.node_id


def test_hard_node_affinity_to_remote_node(ray_start_cluster):
    """Hard node affinity to a node whose view this raylet may lack still works."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, _system_config=LAZY_CONFIG)
    ray.init(address=cluster.address)
    worker = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    task = get_node_id.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(worker.node_id, soft=False)
    )
    assert ray.get(task.remote(), timeout=60) == worker.node_id


def test_infeasible_until_node_added(ray_start_cluster):
    """Infeasible leases still become schedulable once a fitting node joins."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, _system_config=LAZY_CONFIG)
    ray.init(address=cluster.address)

    ref = get_node_id.options(resources={"missing": 1}).remote()
    ready, _ = ray.wait([ref], timeout=2)
    assert not ready

    node = cluster.add_node(num_cpus=1, resources={"missing": 1})
    assert ray.get(ref, timeout=60) == node.node_id


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
