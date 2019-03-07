from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import inspect
import json
import time
import os
import pytest
import shutil

import ray
from ray import tune
from ray.rllib import _register_all
from ray.tests.cluster_utils import Cluster
from ray.tests.utils import run_string_as_driver_nonblocking
from ray.tune.error import TuneError
from ray.tune.experiment import Experiment
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.suggest import BasicVariantGenerator


def _start_new_cluster():
    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "num_cpus": 1,
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
            "num_cpus": 0,
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
    nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 2
    cluster.remove_node(nodes.pop())
    cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 1
    runner.step()  # run 2
    assert sum(t.status == Trial.RUNNING for t in runner.get_trials()) == 1

    for i in range(5):
        nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 6

    runner.step()  # 1 result
    assert sum(t.status == Trial.RUNNING for t in runner.get_trials()) == 2


def test_remove_node_before_result(start_connected_emptyhead_cluster):
    """Tune continues when node is removed before trial returns."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 3
        },
        "checkpoint_freq": 2,
        "max_failures": 2
    }
    trial = Trial("__fake", **kwargs)
    runner.add_trial(trial)

    runner.step()  # run 1
    assert trial.status == Trial.RUNNING
    cluster.remove_node(node)
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    assert ray.global_state.cluster_resources()["CPU"] == 1

    for i in range(3):
        runner.step()
    assert trial.status == Trial.TERMINATED

    with pytest.raises(TuneError):
        runner.step()


def test_trial_migration(start_connected_emptyhead_cluster):
    """Removing a node while cluster has space should migrate trial.

    The trial state should also be consistent with the checkpoint.
    """
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

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
    assert t.last_result
    node2 = cluster.add_node(num_cpus=1)
    cluster.remove_node(node)
    cluster.wait_for_nodes()
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
    node3 = cluster.add_node(num_cpus=1)
    cluster.remove_node(node2)
    cluster.wait_for_nodes()
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
    cluster.add_node(num_cpus=1)
    cluster.remove_node(node3)
    cluster.wait_for_nodes()
    runner.step()  # Error handling step
    assert t3.status == Trial.ERROR

    with pytest.raises(TuneError):
        runner.step()


def test_trial_requeue(start_connected_emptyhead_cluster):
    """Removing a node in full cluster causes Trial to be requeued."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

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
    cluster.wait_for_nodes()
    runner.step()
    assert all(t.status == Trial.PENDING for t in trials)

    with pytest.raises(TuneError):
        runner.step()


def test_migration_checkpoint_removal(start_connected_emptyhead_cluster):
    """Test checks that trial restarts if checkpoint is lost w/ node fail."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

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
    cluster.add_node(num_cpus=1)
    cluster.remove_node(node)
    cluster.wait_for_nodes()
    shutil.rmtree(os.path.dirname(t1._checkpoint.value))

    runner.step()  # Recovery step
    for i in range(3):
        runner.step()

    assert t1.status == Trial.TERMINATED


def test_cluster_down_simple(start_connected_cluster, tmpdir):
    """Tests that TrialRunner save/restore works on cluster shutdown."""
    cluster = start_connected_cluster
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    dirpath = str(tmpdir)
    runner = TrialRunner(
        BasicVariantGenerator(), metadata_checkpoint_dir=dirpath)
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
    runner.checkpoint()

    cluster.shutdown()
    ray.shutdown()

    cluster = _start_new_cluster()
    runner = TrialRunner.restore(dirpath)
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
        local_dir=dirpath,
        checkpoint_freq=1)
    exp2_args = dict(run="__fake", stop=dict(training_iteration=3))
    exp3_args = dict(
        run="__fake",
        stop=dict(training_iteration=3),
        config=dict(mock_error=True))
    exp4_args = dict(
        run="__fake",
        stop=dict(training_iteration=3),
        config=dict(mock_error=True),
        checkpoint_freq=1)
    all_experiments = {
        "exp1": exp1_args,
        "exp2": exp2_args,
        "exp3": exp3_args,
        "exp4": exp4_args
    }

    tune.run_experiments(all_experiments, raise_on_failed_trial=False)

    ray.shutdown()
    cluster.shutdown()
    cluster = _start_new_cluster()

    trials = tune.run_experiments(
        all_experiments, resume=True, raise_on_failed_trial=False)
    assert len(trials) == 4
    assert all(t.status in [Trial.TERMINATED, Trial.ERROR] for t in trials)
    cluster.shutdown()


@pytest.mark.skip(reason="Not very consistent.")
def test_cluster_rllib_restore(start_connected_cluster, tmpdir):
    cluster = start_connected_cluster
    dirpath = str(tmpdir)
    script = """
import time
import ray
from ray import tune

ray.init(redis_address="{redis_address}")

kwargs = dict(
    run="PG",
    env="CartPole-v1",
    stop=dict(training_iteration=10),
    local_dir="{checkpoint_dir}",
    checkpoint_freq=1,
    max_failures=1)

tune.run_experiments(
    dict(experiment=kwargs),
    raise_on_failed_trial=False)
""".format(
        redis_address=cluster.redis_address, checkpoint_dir=dirpath)
    run_string_as_driver_nonblocking(script)
    # Wait until the right checkpoint is saved.
    # The trainable returns every 0.5 seconds, so this should not miss
    # the checkpoint.
    metadata_checkpoint_dir = os.path.join(dirpath, "experiment")
    for i in range(100):
        if TrialRunner.checkpoint_exists(metadata_checkpoint_dir):
            # Inspect the internal trialrunner
            runner = TrialRunner.restore(metadata_checkpoint_dir)
            trials = runner.get_trials()
            last_res = trials[0].last_result
            if last_res and last_res.get("training_iteration"):
                break
        time.sleep(0.3)

    if not TrialRunner.checkpoint_exists(metadata_checkpoint_dir):
        raise RuntimeError("Checkpoint file didn't appear.")

    ray.shutdown()
    cluster.shutdown()
    cluster = _start_new_cluster()
    cluster.wait_for_nodes()

    # Restore properly from checkpoint
    trials2 = tune.run_experiments(
        {
            "experiment": {
                "run": "PG",
                "checkpoint_freq": 1,
                "local_dir": dirpath
            }
        },
        resume=True)
    assert all(t.status == Trial.TERMINATED for t in trials2)
    cluster.shutdown()


def test_cluster_interrupt(start_connected_cluster, tmpdir):
    """Tests run_experiment on cluster shutdown with actual interrupt.

    This is an end-to-end test.
    """
    cluster = start_connected_cluster
    dirpath = str(tmpdir)

    # Needs to be in scope for pytest
    class _Mock(tune.Trainable):
        """Finishes on the 4th iteration."""

        def _setup(self, config):
            self.state = {"hi": 0}

        def _train(self):
            self.state["hi"] += 1
            time.sleep(0.5)
            return {"done": self.state["hi"] >= 4}

        def _save(self, path):
            return self.state

        def _restore(self, state):
            self.state = state

    # Removes indent from class.
    reformatted = "\n".join(line[4:] if len(line) else line
                            for line in inspect.getsource(_Mock).split("\n"))

    script = """
import time
import ray
from ray import tune

ray.init(redis_address="{redis_address}")

{fail_class_code}

kwargs = dict(
    run={fail_class},
    stop=dict(training_iteration=5),
    local_dir="{checkpoint_dir}",
    checkpoint_freq=1,
    max_failures=1)

tune.run_experiments(
    dict(experiment=kwargs),
    raise_on_failed_trial=False)
""".format(
        redis_address=cluster.redis_address,
        checkpoint_dir=dirpath,
        fail_class_code=reformatted,
        fail_class=_Mock.__name__)
    run_string_as_driver_nonblocking(script)

    # Wait until the right checkpoint is saved.
    # The trainable returns every 0.5 seconds, so this should not miss
    # the checkpoint.
    metadata_checkpoint_dir = os.path.join(dirpath, "experiment")
    for i in range(50):
        if TrialRunner.checkpoint_exists(metadata_checkpoint_dir):
            # Inspect the internal trialrunner
            runner = TrialRunner.restore(metadata_checkpoint_dir)
            trials = runner.get_trials()
            last_res = trials[0].last_result
            if last_res and last_res.get("training_iteration") == 3:
                break
        time.sleep(0.2)

    if not TrialRunner.checkpoint_exists(metadata_checkpoint_dir):
        raise RuntimeError("Checkpoint file didn't appear.")

    ray.shutdown()
    cluster.shutdown()
    cluster = _start_new_cluster()
    Experiment._register_if_needed(_Mock)

    # Inspect the internal trialrunner
    runner = TrialRunner.restore(metadata_checkpoint_dir)
    trials = runner.get_trials()
    assert trials[0].last_result["training_iteration"] == 3
    assert trials[0].status == Trial.PENDING

    # Restore properly from checkpoint
    trials2 = tune.run_experiments(
        {
            "experiment": {
                "run": _Mock,
                "local_dir": dirpath,
                "checkpoint_freq": 1
            }
        },
        resume=True,
        raise_on_failed_trial=False)
    assert all(t.status == Trial.TERMINATED for t in trials2)
    assert {t.trial_id for t in trials2} == {t.trial_id for t in trials}
    cluster.shutdown()
