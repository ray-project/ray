import sys
import time
from collections import Counter

import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2, common_pb2
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
    assert worker_node["DeathReason"] == common_pb2.NodeDeathInfo.Reason.Value(
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

    with pytest.raises(ray.exceptions.RaySystemError):
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
    assert worker_node["DeathReason"] == common_pb2.NodeDeathInfo.Reason.Value(
        "AUTOSCALER_DRAIN_PREEMPTED"
    )
    assert worker_node["DeathReasonMessage"] == "preemption"


@pytest.mark.parametrize(
    "graceful",
    [True, False],
)
def test_preemption_after_draining_deadline(monkeypatch, ray_start_cluster, graceful):
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

    # Simulate autoscaler terminates the worker node after the draining deadline.
    cluster.remove_node(worker_node, graceful)

    wait_for_condition(
        lambda: {node["NodeID"] for node in ray.nodes() if (node["Alive"])}
        == {head_node_id},
    )

    worker_node = [node for node in ray.nodes() if node["NodeID"] == worker_node_id][0]
    assert worker_node["DeathReason"] == common_pb2.NodeDeathInfo.Reason.Value(
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
    assert worker_node["DeathReason"] == common_pb2.NodeDeathInfo.Reason.Value(
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


@pytest.mark.parametrize(
    "graceful",
    [False, True],
)
def test_draining_reason(ray_start_cluster, graceful):
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

    drain_reason_message = "testing node preemption."
    # Preemption is always accepted.
    is_accepted, _ = gcs_client.drain_node(
        node2_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        drain_reason_message,
        1,
    )
    assert is_accepted

    # Simulate autoscaler terminates the worker node after the draining deadline.
    cluster.remove_node(node2, graceful)
    try:
        ray.get(actor.ping.remote())
        raise
    except ray.exceptions.ActorDiedError as e:
        assert e.preempted
        if graceful:
            assert "The actor died because its node has died." in str(e)
            assert "the actor's node was preempted: " + drain_reason_message in str(e)


def test_drain_node_actor_restart(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, resources={"head": 1})
    ray.init(address=cluster.address)

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    @ray.remote(max_restarts=1)
    class Actor:
        def get_node_id(self):
            return ray.get_runtime_context().get_node_id()

    # Prepare the first worker node for the actor.
    cur_worker = cluster.add_node(num_cpus=1, resources={"worker": 1})
    cluster.wait_for_nodes()

    actor = Actor.options(num_cpus=0, resources={"worker": 1}).remote()

    def actor_started():
        node_id = ray.get(actor.get_node_id.remote())
        return node_id == cur_worker.node_id

    wait_for_condition(actor_started, timeout=5)

    # Kill the current worker node.
    cluster.remove_node(cur_worker, True)

    # Prepare a new worker node for the actor to be restarted on later.
    cur_worker = cluster.add_node(num_cpus=1, resources={"worker": 1})
    cluster.wait_for_nodes()

    # Make sure the actor is restarted on the new worker node.
    # This should be counted into the max_restarts of the actor.
    wait_for_condition(actor_started, timeout=5)

    # Preemption the current worker node.
    is_accepted, _ = gcs_client.drain_node(
        cur_worker.node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        1,
    )
    assert is_accepted
    cluster.remove_node(cur_worker, True)

    # Prepare a new worker node for the actor to be restarted on later.
    cur_worker = cluster.add_node(num_cpus=1, resources={"worker": 1})
    cluster.wait_for_nodes()

    # Make sure the actor is restarted on the new worker node.
    # This should not be counted into the max_restarts of the actor because the actor was preempted.
    wait_for_condition(actor_started, timeout=5)

    # Kill the current worker node.
    cluster.remove_node(cur_worker, True)

    # Prepare a new worker node, however, the actor should not be restarted on this node, since
    # the max_restarts is reached.
    cur_worker = cluster.add_node(num_cpus=1, resources={"worker": 1})
    cluster.wait_for_nodes()

    # The actor should not be restarted, thus an exception should be raised.
    with pytest.raises(RuntimeError):
        wait_for_condition(actor_started, timeout=5)


def test_drain_node_task_retry(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, resources={"head": 100})
    ray.init(address=cluster.address)

    cur_worker = cluster.add_node(num_cpus=1, resources={"worker": 1})
    cluster.wait_for_nodes()
    node_ids = Counter()

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    @ray.remote(resources={"head": 1})
    class NodeTracker:
        def __init__(self):
            self._node_ids = Counter()

        def add_node(self, node_id):
            self._node_ids.update([node_id])

        def nodes(self):
            return self._node_ids

    @ray.remote(max_retries=1, resources={"worker": 1})
    def func(signal, nodes):
        node_id = ray.get_runtime_context().get_node_id()
        ray.get(nodes.add_node.remote(node_id))
        ray.get(signal.wait.remote())
        return node_id

    signal = SignalActor.options(resources={"head": 1}).remote()
    node_tracker = NodeTracker.remote()
    r1 = func.remote(signal, node_tracker)

    # Verify the first node is added to the counter by the func.remote task.
    node_ids.update([cur_worker.node_id])
    wait_for_condition(lambda: ray.get(node_tracker.nodes.remote()) == node_ids)

    # Remove the current worker node and add a new one to trigger a retry.
    cluster.remove_node(cur_worker, True)
    cur_worker = cluster.add_node(num_cpus=1, resources={"worker": 1})

    # Verify the second node is added to the counter by the task after a retry.
    node_ids.update([cur_worker.node_id])
    wait_for_condition(lambda: ray.get(node_tracker.nodes.remote()) == node_ids)

    # Preempt the second node and add a new one to trigger a retry.
    is_accepted, _ = gcs_client.drain_node(
        cur_worker.node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        1,
    )
    assert is_accepted
    cluster.remove_node(cur_worker, True)
    cur_worker = cluster.add_node(num_cpus=1, resources={"worker": 1})

    # Verify the third node is added to the counter after a preemption retry.
    node_ids.update([cur_worker.node_id])
    wait_for_condition(lambda: ray.get(node_tracker.nodes.remote()) == node_ids)

    # Remove the third node and add a new one, but the task should not retry.
    cluster.remove_node(cur_worker, True)
    cur_worker = cluster.add_node(num_cpus=1, resources={"worker": 1})

    # max_retries is reached, the task should fail.
    with pytest.raises(ray.exceptions.NodeDiedError):
        ray.get(r1)


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
