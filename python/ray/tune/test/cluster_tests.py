from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import os
import pytest
import shutil
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
from ray.rllib import _register_all
from ray.test.cluster_utils import Cluster
from ray.tune.error import TuneError
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.suggest import BasicVariantGenerator


def _start_new_cluster():
    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "resources": dict(CPU=1),
            "_internal_config": json.dumps({
                "num_heartbeats_timeout": 10
            })
        })
    # Pytest doesn't play nicely with imports
    _register_all()
    return cluster


@pytest.fixture
def start_connected_cluster():
    # Start the Ray processes.
    cluster = _start_new_cluster()
    yield cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def start_connected_emptyhead_cluster():
    """Starts head with no resources."""

    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "resources": dict(CPU=0),
            "_internal_config": json.dumps({
                "num_heartbeats_timeout": 10
            })
        })
    # Pytest doesn't play nicely with imports
    _register_all()
    yield cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


def test_counting_resources(start_connected_cluster):
    """Tests that Tune accounting is consistent with actual cluster."""

    cluster = start_connected_cluster
    nodes = []
    assert ray.global_state.cluster_resources()["CPU"] == 1
    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {"stopping_criterion": {"training_iteration": 10}}

    trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    runner.step()  # run 1
    nodes += [cluster.add_node(resources=dict(CPU=1))]
    assert cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 2
    cluster.remove_node(nodes.pop())
    assert cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 1
    runner.step()  # run 2
    assert sum(t.status == Trial.RUNNING for t in runner.get_trials()) == 1

    for i in range(5):
        nodes += [cluster.add_node(resources=dict(CPU=1))]
    assert cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 6

    runner.step()  # 1 result
    assert sum(t.status == Trial.RUNNING for t in runner.get_trials()) == 2


@pytest.mark.skip("Add this test once reconstruction is fixed")
@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test.")
@pytest.mark.timeout(10, method="thread")
def test_remove_node_before_result(start_connected_cluster):
    """Removing a node should cause a Trial to be requeued."""
    cluster = start_connected_cluster
    node = cluster.add_node(resources=dict(CPU=1))
    # TODO(rliaw): Make blocking an option?
    assert cluster.wait_for_nodes()

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {"stopping_criterion": {"training_iteration": 3}}
    trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    runner.step()  # run 1
    runner.step()  # run 2
    assert all(t.status == Trial.RUNNING for t in trials)

    runner.step()  # 1 result

    cluster.remove_node(node)
    cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources["CPU"] == 1

    runner.step()  # recover
    for i in range(5):
        runner.step()
    assert all(t.status == Trial.TERMINATED for t in trials)

    with pytest.raises(TuneError):
        runner.step()


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test.")
@pytest.mark.timeout(120, method="thread")
def test_trial_migration(start_connected_emptyhead_cluster):
    """Removing a node while cluster has space should migrate trial.

    The trial state should also be consistent with the checkpoint.
    """
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(resources=dict(CPU=1))
    assert cluster.wait_for_nodes()

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 3
        },
        "checkpoint_freq": 2,
        "max_failures": 2
    }

    # Test recovery of trial that hasn't been checkpointed
    t = Trial("__fake", **kwargs)
    runner.add_trial(t)
    runner.step()  # start
    runner.step()  # 1 result
    assert t.last_result is not None
    node2 = cluster.add_node(resources=dict(CPU=1))
    cluster.remove_node(node)
    assert cluster.wait_for_nodes()
    runner.step()  # Recovery step

    # TODO(rliaw): This assertion is not critical but will not pass
    #   because checkpoint handling is messy and should be refactored
    #   rather than hotfixed.
    # assert t.last_result is None, "Trial result not restored correctly."
    for i in range(3):
        runner.step()

    assert t.status == Trial.TERMINATED

    # Test recovery of trial that has been checkpointed
    t2 = Trial("__fake", **kwargs)
    runner.add_trial(t2)
    runner.step()  # start
    runner.step()  # 1 result
    runner.step()  # 2 result and checkpoint
    assert t2.has_checkpoint()
    node3 = cluster.add_node(resources=dict(CPU=1))
    cluster.remove_node(node2)
    assert cluster.wait_for_nodes()
    runner.step()  # Recovery step
    assert t2.last_result["training_iteration"] == 2
    for i in range(1):
        runner.step()

    assert t2.status == Trial.TERMINATED

    # Test recovery of trial that won't be checkpointed
    t3 = Trial("__fake", **{"stopping_criterion": {"training_iteration": 3}})
    runner.add_trial(t3)
    runner.step()  # start
    runner.step()  # 1 result
    cluster.add_node(resources=dict(CPU=1))
    cluster.remove_node(node3)
    assert cluster.wait_for_nodes()
    runner.step()  # Error handling step
    assert t3.status == Trial.ERROR

    with pytest.raises(TuneError):
        runner.step()


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test.")
@pytest.mark.timeout(120, method="thread")
def test_trial_requeue(start_connected_emptyhead_cluster):
    """Removing a node in full cluster causes Trial to be requeued."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(resources=dict(CPU=1))
    assert cluster.wait_for_nodes()

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 5
        },
        "checkpoint_freq": 1,
        "max_failures": 1
    }

    trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    runner.step()  # start
    runner.step()  # 1 result

    cluster.remove_node(node)
    assert cluster.wait_for_nodes()
    runner.step()
    assert all(t.status == Trial.PENDING for t in trials)

    with pytest.raises(TuneError):
        runner.step()


def test_migration_checkpoint_removal(start_connected_emptyhead_cluster):
    """Test checks that trial restarts if checkpoint is lost w/ node fail."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(resources=dict(CPU=1))
    assert cluster.wait_for_nodes()

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 3
        },
        "checkpoint_freq": 2,
        "max_failures": 2
    }

    # Test recovery of trial that has been checkpointed
    t1 = Trial("__fake", **kwargs)
    runner.add_trial(t1)
    runner.step()  # start
    runner.step()  # 1 result
    runner.step()  # 2 result and checkpoint
    assert t1.has_checkpoint()
    cluster.add_node(resources=dict(CPU=1))
    cluster.remove_node(node)
    assert cluster.wait_for_nodes()
    shutil.rmtree(os.path.dirname(t1._checkpoint.value))

    runner.step()  # Recovery step
    for i in range(3):
        runner.step()

    assert t1.status == Trial.TERMINATED
