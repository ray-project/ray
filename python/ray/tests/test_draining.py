import sys
import pytest

import ray
import time
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2, gcs_pb2
from ray._private.test_utils import wait_for_condition
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)


def test_idle_termination(ray_start_cluster):
    cluster = ray_start_cluster
    head_node = cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = head_node.node_id
    worker_node_id = worker_node.node_id

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
    is_accepted, rejection_reason_message = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_IDLE_TERMINATION"),
        "idle for long enough",
        2**63 - 1,
    )
    assert not is_accepted
    assert (
        "The node to be idle terminated is no longer idle." in rejection_reason_message
    )

    ray.kill(actor)

    def drain_until_accept():
        # The worker node is idle now so the drain request should be accepted.
        is_accepted, _ = gcs_client.drain_node(
            worker_node_id,
            autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_IDLE_TERMINATION"),
            "idle for long enough",
            2**63 - 1,
        )
        return is_accepted

    wait_for_condition(drain_until_accept)

    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id}
    )

    worker_node = [node for node in ray.nodes() if node["NodeID"] == worker_node_id][0]
    assert worker_node["DeathReason"] == gcs_pb2.NodeDeathInfo.Reason.Value(
        "AUTOSCALER_DRAIN_IDLE"
    )
    assert worker_node["DeathReasonMessage"] == "idle for long enough"

    # Draining a dead node is always accepted.
    is_accepted, _ = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_IDLE_TERMINATION"),
        "idle for long enough",
        2**63 - 1,
    )
    assert is_accepted


def test_preemption(ray_start_cluster):
    cluster = ray_start_cluster
    head_node = cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = head_node.node_id
    worker_node_id = worker_node.node_id

    @ray.remote(num_cpus=1, resources={"worker": 1})
    class Actor:
        def ping(self):
            pass

    actor = Actor.remote()
    ray.get(actor.ping.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    with pytest.raises(ray.exceptions.RpcError):
        # Test invalid draining deadline
        gcs_client.drain_node(
            worker_node_id,
            autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
            "preemption",
            -1,
        )

    # The worker node is not idle but the drain request should be still accepted.
    is_accepted, _ = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        2**63 - 1,
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

    worker_node = [node for node in ray.nodes() if node["NodeID"] == worker_node_id][0]
    assert worker_node["DeathReason"] == gcs_pb2.NodeDeathInfo.Reason.Value(
        "AUTOSCALER_DRAIN_PREEMPTED"
    )
    assert worker_node["DeathReasonMessage"] == "preemption"


def test_preemption_after_draining_deadline(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_health_check_failure_threshold", "3")
    monkeypatch.setenv("RAY_health_check_timeout_ms", "100")
    monkeypatch.setenv("RAY_health_check_period_ms", "1000")
    monkeypatch.setenv("RAY_health_check_initial_delay_ms", "0")

    cluster = ray_start_cluster
    head_node = cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = head_node.node_id
    worker_node_id = worker_node.node_id

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

    # The worker node is not idle but the drain request should be still accepted.
    is_accepted, _ = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        1,
    )
    assert is_accepted

    # Simulate node provider forcefully terminates the worker node
    # after the draining deadline.
    cluster.remove_node(worker_node, False)

    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id},
    )

    worker_node = [node for node in ray.nodes() if node["NodeID"] == worker_node_id][0]
    assert worker_node["DeathReason"] == gcs_pb2.NodeDeathInfo.Reason.Value(
        "AUTOSCALER_DRAIN_PREEMPTED"
    )
    assert worker_node["DeathReasonMessage"] == "preemption"


def test_node_death_before_draining_deadline(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_health_check_failure_threshold", "3")
    monkeypatch.setenv("RAY_health_check_timeout_ms", "100")
    monkeypatch.setenv("RAY_health_check_period_ms", "1000")
    monkeypatch.setenv("RAY_health_check_initial_delay_ms", "0")

    cluster = ray_start_cluster
    head_node = cluster.add_node(resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = head_node.node_id
    worker_node_id = worker_node.node_id

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

    # The worker node is not idle but the drain request should be still accepted.
    is_accepted, _ = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        2**63 - 1,
    )
    assert is_accepted

    # Simulate the worker node crashes before the draining deadline.
    cluster.remove_node(worker_node, False)

    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id},
    )

    # Since worker node failure is detected to be before the draining deadline,
    # this is considered as an unexpected termination.
    worker_node = [node for node in ray.nodes() if node["NodeID"] == worker_node_id][0]
    assert worker_node["DeathReason"] == gcs_pb2.NodeDeathInfo.Reason.Value(
        "UNEXPECTED_TERMINATION"
    )
    assert (
        worker_node["DeathReasonMessage"]
        == "health check failed due to missing too many heartbeats"
    )


def test_scheduling_placement_groups_during_draining(ray_start_cluster):
    """Test that the draining node is unschedulable for new pgs."""
    cluster = ray_start_cluster
    node1 = cluster.add_node(num_cpus=1, resources={"node1": 1})
    ray.init(address=cluster.address)
    node2 = cluster.add_node(num_cpus=1, resources={"node2": 1})
    cluster.add_node(num_cpus=2, resources={"node3": 1})
    cluster.wait_for_nodes()

    node1_id = node1.node_id
    node2_id = node2.node_id
    node3_id = node2.node_id

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    # The node is idle so the draining request should be accepted.
    is_accepted, _ = gcs_client.drain_node(
        node3_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        2**63 - 1,
    )
    assert is_accepted

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    # Even though node3 is the best for pack but it's draining
    # so the pg should be on node1 and node2
    pg = ray.util.placement_group(bundles=[{"CPU": 1}, {"CPU": 1}], strategy="PACK")
    {
        ray.get(
            get_node_id.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg,
                    placement_group_bundle_index=0,
                )
            ).remote()
        ),
        ray.get(
            get_node_id.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg,
                    placement_group_bundle_index=1,
                )
            ).remote()
        ),
    } == {node1_id, node2_id}


def test_scheduling_tasks_and_actors_during_draining(ray_start_cluster):
    """Test that the draining node is unschedulable for new tasks and actors."""
    cluster = ray_start_cluster
    head_node = cluster.add_node(num_cpus=1, resources={"head": 1})
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=1, resources={"worker": 1})
    cluster.wait_for_nodes()

    head_node_id = head_node.node_id
    worker_node_id = worker_node.node_id

    @ray.remote
    class Actor:
        def ping(self):
            pass

    actor = Actor.options(num_cpus=0, resources={"worker": 1}).remote()
    ray.get(actor.ping.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    # The worker node is not idle but the drain request should be still accepted.
    is_accepted, _ = gcs_client.drain_node(
        worker_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        2**63 - 1,
    )
    assert is_accepted

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

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


def test_draining_reason(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, resources={"node1": 1})
    ray.init(
        address=cluster.address,
    )
    node2 = cluster.add_node(num_cpus=1, resources={"node2": 1})

    @ray.remote
    class Actor:
        def ping(self):
            pass

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    node2_id = node2.node_id

    # Schedule actor
    actor = Actor.options(num_cpus=0, resources={"node2": 1}).remote()
    ray.get(actor.ping.remote())

    # Preemption is always accepted.
    is_accepted, _ = gcs_client.drain_node(
        node2_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        1,
    )
    assert is_accepted

    # Simulate node provider forcefully terminates the worker node
    cluster.remove_node(node2, False)
    try:
        ray.get(actor.ping.remote())
        raise
    except ray.exceptions.RayActorError as e:
        assert e.preempted


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
