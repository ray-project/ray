import signal
import sys
import time
from collections import Counter
from unittest import mock

import pytest

import ray
import ray._private.node as ray_node
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2, common_pb2
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)
from ray.util.state import list_tasks


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
                label_selector={ray._raylet.RAY_NODE_ID_KEY: worker_node_id}
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

    def check_actor_died_error():
        try:
            ray.get(actor.ping.remote())
            return False
        except ray.exceptions.ActorDiedError as e:
            assert e.preempted
            if graceful:
                assert "The actor died because its node has died." in str(e)
                assert "the actor's node was preempted: " + drain_reason_message in str(
                    e
                )
        return True

    wait_for_condition(check_actor_died_error)


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


def test_leases_rescheduling_during_draining(ray_start_cluster):
    """Test that when a node is being drained, leases inside local lease manager
    will be cancelled and re-added to the cluster lease manager for rescheduling
    instead of being marked as permanently infeasible.

    This is regression test for https://github.com/ray-project/ray/pull/57834/
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    worker1 = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    @ray.remote(num_cpus=1)
    class Actor:
        def ping(self):
            pass

    actor = Actor.remote()
    ray.get(actor.ping.remote())

    @ray.remote(num_cpus=1)
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    obj_ref = get_node_id.options(name="f1").remote()

    def verify_f1_pending_node_assignment():
        tasks = list_tasks(filters=[("name", "=", "f1")])
        assert len(tasks) == 1
        assert tasks[0]["state"] == "PENDING_NODE_ASSIGNMENT"
        return True

    # f1 should be in the local lease manager of worker1,
    # waiting for resource to be available.
    wait_for_condition(verify_f1_pending_node_assignment)

    is_accepted, _ = gcs_client.drain_node(
        worker1.node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        2**63 - 1,
    )
    assert is_accepted

    # The task should be rescheduled on another node.
    worker2 = cluster.add_node(num_cpus=1)
    assert ray.get(obj_ref) == worker2.node_id


class _FakeNode:
    """Minimal stand-in for Node to exercise _drain_node_before_shutdown."""

    def __init__(self, node_id, gcs_client, dead_processes):
        self._node_id = node_id
        self._gcs_client = gcs_client
        self.dead_processes = dead_processes

    def get_gcs_client(self):
        return self._gcs_client


def _run_drain(
    monkeypatch,
    *,
    timeout_s,
    node_id,
    drain_return=(True, ""),
    drain_side_effect=None,
    raylet_dead_after_polls=None,
    poll_interval=0.5,
):
    """Invoke Node._drain_node_before_shutdown against a fake clock + raylet.

    Returns (gcs_client_mock, sleep_durations, poll_count). A fake monotonic
    clock advances only when the code sleeps, so waits are deterministic.
    ``raylet_dead_after_polls=k`` makes dead_processes() report the raylet gone
    (self-terminated after draining) on its k-th call.
    """
    monkeypatch.setattr(
        ray_node.ray_constants, "RAY_GRACEFUL_SHUTDOWN_DRAIN_TIMEOUT_S", timeout_s
    )
    monkeypatch.setattr(
        ray_node.ray_constants, "RAY_GRACEFUL_SHUTDOWN_POLL_INTERVAL_S", poll_interval
    )

    clock = {"t": 1000.0}
    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)
        clock["t"] += s

    monkeypatch.setattr(ray_node.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(ray_node.time, "sleep", fake_sleep)

    gcs = mock.MagicMock()
    if drain_side_effect is not None:
        gcs.drain_node.side_effect = drain_side_effect
    else:
        gcs.drain_node.return_value = drain_return

    polls = {"n": 0}

    def dead_processes():
        polls["n"] += 1
        if (
            raylet_dead_after_polls is not None
            and polls["n"] >= raylet_dead_after_polls
        ):
            return [(ray_node.ray_constants.PROCESS_TYPE_RAYLET, object())]
        return []

    fake = _FakeNode(node_id, gcs, dead_processes)
    ray_node.Node._drain_node_before_shutdown(fake)
    return gcs, sleeps, polls["n"]


def test_drain_marks_node_draining_then_polls(monkeypatch):
    node_id = ray.NodeID.from_random().hex()
    before_ms = int(time.time() * 1000)
    gcs, sleeps, polls = _run_drain(
        monkeypatch, timeout_s=5.0, node_id=node_id, raylet_dead_after_polls=None
    )
    after_ms = int(time.time() * 1000)

    gcs.drain_node.assert_called_once()
    args = gcs.drain_node.call_args.args
    # drain_node takes the *hex* node id (it decodes via FromHex); passing binary
    # yields a nil id that the GCS silently accepts without draining the raylet.
    assert args[0] == node_id
    assert args[1] == autoscaler_pb2.DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION
    assert before_ms + 5000 <= args[3] <= after_ms + 5000
    # Raylet never exits -> we poll and wait up to (not beyond) the cap.
    assert polls > 1
    assert sum(sleeps) == pytest.approx(5.0)


def test_drain_exits_early_when_raylet_self_terminates(monkeypatch):
    node_id = ray.NodeID.from_random().hex()
    gcs, sleeps, polls = _run_drain(
        monkeypatch, timeout_s=30.0, node_id=node_id, raylet_dead_after_polls=3
    )
    gcs.drain_node.assert_called_once()
    # Raylet self-terminates (draining+idle) by the 3rd poll -> stop, not 30s.
    assert polls == 3
    assert sum(sleeps) == pytest.approx(1.0)
    assert sum(sleeps) < 30.0


def test_drain_disabled_when_timeout_non_positive(monkeypatch):
    node_id = ray.NodeID.from_random().hex()
    gcs, sleeps, polls = _run_drain(monkeypatch, timeout_s=0.0, node_id=node_id)
    gcs.drain_node.assert_not_called()
    assert sleeps == []
    assert polls == 0


def test_drain_failure_does_not_block_shutdown(monkeypatch):
    node_id = ray.NodeID.from_random().hex()
    gcs, sleeps, polls = _run_drain(
        monkeypatch,
        timeout_s=5.0,
        node_id=node_id,
        drain_side_effect=RuntimeError("gcs unreachable"),
    )
    gcs.drain_node.assert_called_once()
    assert sleeps == []
    assert polls == 0


def test_drain_rejected_skips_wait(monkeypatch):
    node_id = ray.NodeID.from_random().hex()
    gcs, sleeps, polls = _run_drain(
        monkeypatch, timeout_s=5.0, node_id=node_id, drain_return=(False, "rejected")
    )
    gcs.drain_node.assert_called_once()
    assert sleeps == []
    assert polls == 0


def test_sigterm_handler_drains_then_kills(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        "ray._private.utils.set_sigterm_handler",
        lambda handler: captured.__setitem__("handler", handler),
    )
    monkeypatch.setattr(ray_node.atexit, "register", lambda *a, **k: None)

    calls = []
    fake = mock.MagicMock()
    fake._drain_node_before_shutdown.side_effect = lambda: calls.append("drain")
    fake.kill_all_processes.side_effect = lambda **kw: calls.append("kill")

    ray_node.Node._register_shutdown_hooks(fake)
    handler = captured["handler"]

    with pytest.raises(SystemExit):
        handler(signal.SIGTERM, None)
    assert calls == ["drain", "kill"]
    fake.kill_all_processes.assert_called_once_with(
        check_alive=False, allow_graceful=True
    )

    calls.clear()
    handler(signal.SIGTERM, None)
    assert calls == []


def test_drain_skipped_when_node_id_unset(monkeypatch):
    # SIGTERM can arrive mid-Node.__init__, before _node_id is assigned (the
    # shutdown hooks are registered first). The drain must be skipped rather
    # than raise AttributeError, so teardown can still proceed.
    monkeypatch.setattr(
        ray_node.ray_constants, "RAY_GRACEFUL_SHUTDOWN_DRAIN_TIMEOUT_S", 30.0
    )
    gcs = mock.MagicMock()

    class _NodeWithoutId:
        def get_gcs_client(self):
            return gcs

        def dead_processes(self):
            return []

    # Returns without raising and without attempting a drain.
    ray_node.Node._drain_node_before_shutdown(_NodeWithoutId())
    gcs.drain_node.assert_not_called()


def test_sigterm_handler_shuts_down_even_if_drain_raises(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        "ray._private.utils.set_sigterm_handler",
        lambda handler: captured.__setitem__("handler", handler),
    )
    monkeypatch.setattr(ray_node.atexit, "register", lambda *a, **k: None)

    fake = mock.MagicMock()
    fake._drain_node_before_shutdown.side_effect = RuntimeError("boom")

    ray_node.Node._register_shutdown_hooks(fake)
    with pytest.raises(SystemExit):
        captured["handler"](signal.SIGTERM, None)
    # Drain raised, but local processes were still torn down.
    fake.kill_all_processes.assert_called_once_with(
        check_alive=False, allow_graceful=True
    )


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
