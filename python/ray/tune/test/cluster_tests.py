from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import inspect
import json
import time
import os
import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
from ray import tune
from ray.rllib import _register_all
from ray.test.cluster_utils import Cluster
from ray.test.test_utils import (
    run_string_as_driver, run_string_as_driver_nonblocking)
from ray.tune.error import TuneError
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.suggest import BasicVariantGenerator


def register_fail_trainable():
    class _Fail(tune.Trainable):
        """Fails on the 4th iteration."""

        def _setup(self, config):
            self.state = {"hi": 0}

        def _train(self):
            self.state["hi"] += 1
            time.sleep(0.5)
            if self.state["hi"] >= 4:
                assert False
            return {}

        def _save(self, path):
            return self.state

        def _restore(self, state):
            self.state = state

    tune.register_trainable("test2", _Fail)


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


@pytest.mark.skip("Add this test once reconstruction is fixed")
def test_counting_resources(start_connected_cluster):
    """Tests that Tune accounting is consistent with actual cluster."""
    cluster = start_connected_cluster
    assert ray.global_state.cluster_resources()["CPU"] == 1
    nodes = []
    nodes += [cluster.add_node(resources=dict(CPU=1))]
    assert cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 2

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {"stopping_criterion": {"training_iteration": 10}}

    trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    runner.step()  # run 1
    cluster.remove_node(nodes.pop())
    assert cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 1
    runner.step()  # run 2

    for i in range(5):
        nodes += [cluster.add_node(resources=dict(CPU=1))]
    assert cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 6

    runner.step()  # 1 result

    for i in range(5):
        node = nodes.pop()
        cluster.remove_node(node)
    assert cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 1


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


def test_cluster_down_simple(start_connected_cluster, tmpdir):
    """Tests that TrialRunner save/restore works on cluster shutdown."""
    cluster = start_connected_cluster
    cluster.add_node(resources=dict(CPU=1))
    dirpath = str(tmpdir)
    runner = TrialRunner(
        BasicVariantGenerator(), checkpoint_freq=2, checkpoint_dir=dirpath)
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 2
        },
        "checkpoint_freq": 1,
        "max_failures": 1
    }
    trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    runner.step()  # start
    runner.step()  # start2
    runner.step()  # step
    assert all(t.status == Trial.RUNNING for t in runner.get_trials())
    runner.save()

    cluster.shutdown()
    ray.shutdown()

    cluster = _start_new_cluster()
    runner = TrialRunner(BasicVariantGenerator())
    runner.restore(dirpath)
    print([t.status for t in runner.get_trials()])
    runner.step()  # start
    runner.step()  # start2

    for i in range(3):
        runner.step()

    with pytest.raises(TuneError):
        runner.step()

    assert all(t.status == Trial.TERMINATED for t in runner.get_trials())
    cluster.shutdown()


def test_cluster_down_full(start_connected_cluster, tmpdir):
    """Tests that run_experiment restoring works on cluster shutdown."""
    cluster = start_connected_cluster
    dirpath = str(tmpdir)
    exp1_args = dict(
        run="__fake",
        stop=dict(training_iteration=3),
        checkpoint_freq=1,
        max_failures=1)
    exp2_args = dict(
        run="__fake",
        stop=dict(training_iteration=3))
    exp3_args = dict(
        run="__fake",
        stop=dict(training_iteration=3),
        config=dict(mock_error=True))
    exp4_args = dict(
        run="__fake",
        stop=dict(training_iteration=3),
        config=dict(mock_error=True),
        checkpoint_freq=1,
        max_failures=1)
    
    tune.run_experiments(
        dict(exp1=exp1_args,
         exp2=exp2_args,
         exp3=exp3_args,
         exp4=exp4_args),
        checkpoint_dir=dirpath,
        checkpoint_freq=2,
        raise_on_failed_trial=False)

    ray.shutdown()
    cluster.shutdown()
    cluster = _start_new_cluster()

    # Check that last_result.iteration = 1
    runner = TrialRunner(BasicVariantGenerator())
    runner.restore(dirpath)
    trials = runner.get_trials()
    trials = tune.run_experiments(restore_from_path=dirpath)
    assert len(trials) == 2
    assert all(t.status in [Trial.TERMINATED, Trial.ERROR] for t in trials)
    cluster.shutdown()


def test_cluster_interrupt(start_connected_cluster, tmpdir):
    """Tests run_experiment on cluster shutdown even with atypical trial.

    The trial fails on the 4th step, and the checkpointing happens on
    the 3rd step, so restoring should actually launch the trial again.
    """
    cluster = start_connected_cluster
    dirpath = str(tmpdir)
    script = """
import time
import ray
from ray import tune

ray.init(redis_address="{redis_address}")

{register_trainable_fn}
{run_register_trainable_fn}()

kwargs = dict(
    run="test2",
    stop=dict(training_iteration=5),
    checkpoint_freq=1,
    max_failures=1)

# This will save to disk on step 0 and step 3
tune.run_experiments(
    dict(experiment1=kwargs),
    checkpoint_dir="{checkpoint_dir}",
    checkpoint_freq=3,
    raise_on_failed_trial=False)
""".format(
        redis_address=cluster.redis_address,
        checkpoint_dir=dirpath,
        register_trainable_fn=inspect.getsource(register_fail_trainable),
        run_register_trainable_fn=register_fail_trainable.__name__)
    run_string_as_driver_nonblocking(script)

    # Wait until the right checkpoint is saved.
    # The trainable returns every 0.5 seconds, so this should not miss
    # the checkpoint.
    for i in range(30):
        if os.path.exists(os.path.join(dirpath, "experiment.state")):
            # Inspect the internal trialrunner
            runner = TrialRunner(BasicVariantGenerator())
            runner.restore(dirpath)
            trials = runner.get_trials()
            last_res = trials[0].last_result
            if last_res is not None and last_res["training_iteration"] == 3:
                break
        time.sleep(0.2)

    ray.shutdown()
    cluster.shutdown()
    cluster = _start_new_cluster()
    register_fail_trainable()

    # Inspect the internal trialrunner just in case
    runner = TrialRunner(BasicVariantGenerator())
    runner.restore(dirpath)
    trials = runner.get_trials()
    assert trials[0].last_result["training_iteration"] == 3
    assert trials[0].status == Trial.PENDING

    # Restore properly from checkpoint
    trials = tune.run_experiments(
        restore_from_path=dirpath, raise_on_failed_trial=False)
    assert all(t.status == Trial.ERROR for t in trials)
    cluster.shutdown()
