import sys
import pytest

import ray
import time
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2
from ray._private.test_utils import wait_for_condition
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def test_idle_termination(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id = ray.get(get_node_id.options(resources={"worker": 1}).remote())

    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id, worker_node_id}
    )

    @ray.remote(num_cpus=1, resources={"worker": 1})
    class Actor:
        def ping(self):
            pass

    actor = Actor.remote()
    ray.get(actor.ping.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    # The worker node is not idle so the drain request should be rejected.
    is_accepted = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_IDLE_TERMINATION"),
        "idle for long enough",
    )
    assert not is_accepted

    ray.kill(actor)

    def drain_until_accept():
        # The worker node is idle now so the drain request should be accepted.
        is_accepted = gcs_client.drain_node(
            worker_node_id,
            autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_IDLE_TERMINATION"),
            "idle for long enough",
        )
        return is_accepted

    wait_for_condition(drain_until_accept)

    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id}
    )

    # Draining a dead node is always accepted.
    is_accepted = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_IDLE_TERMINATION"),
        "idle for long enough",
    )
    assert is_accepted


def test_preemption(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id = ray.get(get_node_id.options(resources={"worker": 1}).remote())

    @ray.remote(num_cpus=1, resources={"worker": 1})
    class Actor:
        def ping(self):
            pass

    actor = Actor.remote()
    ray.get(actor.ping.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    # The worker node is not idle but the drain request should be still accepted.
    is_accepted = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
    )
    assert is_accepted

    time.sleep(1)

    # Worker node should still be alive since it's not idle and cannot be drained.
    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id, worker_node_id}
    )

    ray.kill(actor)
    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id}
    )


def test_scheduling_during_draining(ray_start_cluster):
    """Test that the draining node is unschedulable for new tasks and actors."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, resources={"head": 1})
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"worker": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    head_node_id = ray.get(get_node_id.options(resources={"head": 1}).remote())
    worker_node_id = ray.get(get_node_id.options(resources={"worker": 1}).remote())

    @ray.remote
    class Actor:
        def ping(self):
            pass

    actor = Actor.options(num_cpus=0, resources={"worker": 1}).remote()
    ray.get(actor.ping.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    # The worker node is not idle but the drain request should be still accepted.
    is_accepted = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
    )
    assert is_accepted

    assert (
        ray.get(get_node_id.options(scheduling_strategy="SPREAD").remote())
        == head_node_id
    )
    assert (
        ray.get(get_node_id.options(scheduling_strategy="SPREAD").remote())
        == head_node_id
    )

    assert (
        ray.get(
            get_node_id.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    worker_node_id, soft=True
                )
            ).remote()
        )
        == head_node_id
    )

    with pytest.raises(ray.exceptions.TaskUnschedulableError):
        ray.get(
            get_node_id.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    worker_node_id, soft=False
                )
            ).remote()
        )

    head_actor = Actor.options(num_cpus=1, resources={"head": 1}).remote()
    ray.get(head_actor.ping.remote())

    obj = get_node_id.remote()

    # Cannot run on the draining worker node even though it has resources.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(obj, timeout=2)

    ray.kill(head_actor)
    ray.get(obj, timeout=2) == head_node_id


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
