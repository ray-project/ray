import inspect
import time
import os

import pytest
import shutil
import sys
from unittest.mock import MagicMock

import ray
from ray import tune
from ray.air import CheckpointConfig
from ray.cluster_utils import Cluster
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.tune.experiment import Experiment
from ray.tune.error import TuneError
from ray.tune.search import BasicVariantGenerator
from ray.tune.syncer import SyncerCallback, SyncConfig
from ray.tune.experiment import Trial
from ray.tune.execution.trial_runner import TrialRunner


def _check_trial_running(trial):
    if trial.runner:
        ray.get(trial.runner.get_info.remote())
        return True
    return False


def _get_running_trials(runner):
    return [t for t in runner.get_live_trials() if t.status == Trial.RUNNING]


def _start_new_cluster():
    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "num_cpus": 1,
            "_system_config": {
                "health_check_initial_delay_ms": 0,
                "health_check_period_ms": 1000,
                "health_check_failure_threshold": 10,
            },
        },
    )
    return cluster


@pytest.fixture
def start_connected_cluster():
    # Start the Ray processes.
    cluster = _start_new_cluster()
    os.environ["TUNE_STATE_REFRESH_PERIOD"] = "0.1"
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
            "_system_config": {
                "health_check_initial_delay_ms": 0,
                "health_check_period_ms": 1000,
                "health_check_failure_threshold": 10,
            },
        },
    )
    os.environ["TUNE_STATE_REFRESH_PERIOD"] = "0.1"
    yield cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


def test_counting_resources(start_connected_cluster):
    """Tests that Tune accounting is consistent with actual cluster."""

    cluster = start_connected_cluster
    nodes = []
    assert ray.cluster_resources()["CPU"] == 1
    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {"stopping_criterion": {"training_iteration": 10}}

    trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    runner.step()
    running_trials = _get_running_trials(runner)
    assert len(running_trials) == 1
    assert _check_trial_running(running_trials[0])
    assert ray.available_resources().get("CPU", 0) == 0
    nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 2
    cluster.remove_node(nodes.pop())
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 1
    runner.step()
    # Only 1 trial can be running due to resource limitation.
    assert sum(t.status == Trial.RUNNING for t in runner.get_trials()) == 1

    for i in range(5):
        nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 6

    # This is to make sure that pg is ready for the previous pending trial,
    # so that when runner.step() is called next, the trial can be started in
    # the same event loop.
    time.sleep(5)
    runner.step()
    assert sum(t.status == Trial.RUNNING for t in runner.get_trials()) == 2


def test_trial_processed_after_node_failure(start_connected_emptyhead_cluster):
    """Tests that Tune processes a trial as failed if its node died."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    runner = TrialRunner(BasicVariantGenerator())
    mock_process_failure = MagicMock(side_effect=runner._process_trial_failure)
    runner._process_trial_failure = mock_process_failure

    runner.add_trial(Trial("__fake"))
    runner.step()
    runner.step()
    assert not mock_process_failure.called

    cluster.remove_node(node)
    runner.step()
    if not mock_process_failure.called:
        runner.step()
    assert mock_process_failure.called


def test_remove_node_before_result(start_connected_emptyhead_cluster):
    """Tune continues when node is removed before trial returns."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {
        "stopping_criterion": {"training_iteration": 3},
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=2),
        "max_failures": 2,
    }
    trial = Trial("__fake", **kwargs)
    runner.add_trial(trial)

    runner.step()  # Start trial, call _train once
    running_trials = _get_running_trials(runner)
    assert len(running_trials) == 1
    assert _check_trial_running(running_trials[0])
    assert not trial.has_reported_at_least_once
    assert trial.status == Trial.RUNNING
    cluster.remove_node(node)
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 1

    # Process result: fetch data, invoke _train again
    runner.step()
    assert trial.last_result.get("training_iteration") == 1

    # Process result: discover failure, recover, _train (from scratch)
    while trial.status != Trial.TERMINATED:
        runner.step()

    assert trial.last_result.get("training_iteration") > 1

    with pytest.raises(TuneError):
        runner.step()


def custom_driver_logdir_callback(tempdir: str):
    class SeparateDriverSyncerCallback(SyncerCallback):
        def _local_trial_logdir(self, trial):
            return os.path.join(tempdir, trial.relative_logdir)

    return SeparateDriverSyncerCallback()


@pytest.mark.parametrize("durable", [False, True])
def test_trial_migration(start_connected_emptyhead_cluster, tmpdir, durable):
    """Removing a node while cluster has space should migrate trial.

    The trial state should also be consistent with the checkpoint.
    """
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    if durable:
        upload_dir = "file://" + str(tmpdir)
        syncer_callback = SyncerCallback()
    else:
        upload_dir = None
        syncer_callback = custom_driver_logdir_callback(str(tmpdir))

    runner = TrialRunner(BasicVariantGenerator(), callbacks=[syncer_callback])
    kwargs = {
        "stopping_criterion": {"training_iteration": 4},
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=2),
        "sync_config": SyncConfig(upload_dir=upload_dir),
        "experiment_dir_name": "exp",
        "max_failures": 2,
    }

    # Test recovery of trial that hasn't been checkpointed
    t = Trial("__fake", **kwargs)
    runner.add_trial(t)
    runner.step()  # Start trial
    runner.step()  # Process result
    assert t.last_result
    node2 = cluster.add_node(num_cpus=1)
    cluster.remove_node(node)
    cluster.wait_for_nodes()
    # TODO(ujvl): Node failure does not propagate until a step after it
    #  actually should. This is possibly a problem with `Cluster`.
    runner.step()
    runner.step()  # Recovery step

    # TODO(rliaw): This assertion is not critical but will not pass
    #   because checkpoint handling is messy and should be refactored
    #   rather than hotfixed.
    # assert t.last_result is None, "Trial result not restored correctly."

    # Process result (x2), process save, process result (x2), process save
    while not runner.is_finished():
        runner.step()

    assert t.status == Trial.TERMINATED, runner.debug_string()

    # Test recovery of trial that has been checkpointed
    t2 = Trial("__fake", **kwargs)
    runner.add_trial(t2)
    # Start trial, process result (x2), process save
    while not t2.has_checkpoint():
        runner.step()
    node3 = cluster.add_node(num_cpus=1)
    cluster.remove_node(node2)
    cluster.wait_for_nodes()
    while not runner.is_finished():
        runner.step()
    assert t2.status == Trial.TERMINATED, runner.debug_string()

    # Test recovery of trial that won't be checkpointed
    kwargs = {
        "stopping_criterion": {"training_iteration": 3},
        "sync_config": SyncConfig(upload_dir=upload_dir),
        "experiment_dir_name": "exp",
    }

    t3 = Trial("__fake", **kwargs)
    runner.add_trial(t3)
    runner.step()  # Start trial
    runner.step()  # Process result 1
    cluster.add_node(num_cpus=1)
    cluster.remove_node(node3)
    cluster.wait_for_nodes()
    while not runner.is_finished():
        runner.step()
    assert t3.status == Trial.ERROR, runner.debug_string()

    with pytest.raises(TuneError):
        runner.step()


@pytest.mark.parametrize("durable", [False, True])
def test_trial_requeue(start_connected_emptyhead_cluster, tmpdir, durable):
    """Removing a node in full cluster causes Trial to be requeued."""
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    if durable:
        upload_dir = "file://" + str(tmpdir)
        syncer_callback = SyncerCallback()
    else:
        upload_dir = None
        syncer_callback = custom_driver_logdir_callback(str(tmpdir))

    runner = TrialRunner(BasicVariantGenerator(), callbacks=[syncer_callback])  # noqa
    kwargs = {
        "stopping_criterion": {"training_iteration": 5},
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
        "sync_config": SyncConfig(upload_dir=upload_dir),
        "experiment_dir_name": "exp",
        "max_failures": 1,
    }

    trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    runner.step()  # Start trial
    runner.step()  # Process result, dispatch save
    runner.step()  # Process save

    running_trials = _get_running_trials(runner)
    assert len(running_trials) == 1
    assert _check_trial_running(running_trials[0])
    cluster.remove_node(node)
    cluster.wait_for_nodes()
    time.sleep(0.1)  # Sleep so that next step() refreshes cluster resources
    runner.step()  # Process result, dispatch save
    runner.step()  # Process save (detect error), requeue trial
    assert all(t.status == Trial.PENDING for t in trials), runner.debug_string()


@pytest.mark.parametrize("durable", [False, True])
def test_migration_checkpoint_removal(
    start_connected_emptyhead_cluster, tmpdir, durable
):
    """Test checks that trial restarts if checkpoint is lost w/ node fail."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    if durable:
        upload_dir = "file://" + str(tmpdir)
        syncer_callback = SyncerCallback()
    else:
        upload_dir = None
        syncer_callback = custom_driver_logdir_callback(str(tmpdir))

    runner = TrialRunner(BasicVariantGenerator(), callbacks=[syncer_callback])
    kwargs = {
        "stopping_criterion": {"training_iteration": 4},
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=2),
        "sync_config": SyncConfig(upload_dir=upload_dir),
        "experiment_dir_name": "exp",
        "max_failures": 2,
    }

    # Test recovery of trial that has been checkpointed
    t1 = Trial("__fake", **kwargs)
    runner.add_trial(t1)

    # Start trial, process result (x2), process save
    while not t1.has_checkpoint():
        runner.step()

    cluster.add_node(num_cpus=1)
    cluster.remove_node(node)
    cluster.wait_for_nodes()

    # Remove checkpoint on "remote" node
    shutil.rmtree(t1.checkpoint.dir_or_data)

    if not durable:
        # Recover from driver file
        t1.checkpoint.dir_or_data = os.path.join(
            tmpdir,
            t1.relative_logdir,
            os.path.relpath(t1.checkpoint.dir_or_data, t1.logdir),
        )

    while not runner.is_finished():
        runner.step()
    assert t1.status == Trial.TERMINATED, runner.debug_string()


@pytest.mark.parametrize("durable", [False, True])
def test_cluster_down_full(start_connected_cluster, tmpdir, durable):
    """Tests that run_experiment restoring works on cluster shutdown."""
    cluster = start_connected_cluster
    dirpath = str(tmpdir)

    if durable:
        upload_dir = "file://" + str(tmpdir)
        syncer_callback = SyncerCallback()
    else:
        upload_dir = None
        syncer_callback = custom_driver_logdir_callback(str(tmpdir))

    from ray.tune.result import DEFAULT_RESULTS_DIR

    local_dir = DEFAULT_RESULTS_DIR

    base_dict = dict(
        run="__fake",
        stop=dict(training_iteration=3),
        local_dir=local_dir,
        sync_config=dict(upload_dir=upload_dir),
    )

    exp1_args = base_dict
    exp2_args = dict(
        base_dict.items(),
        local_dir=dirpath,
        checkpoint_config=dict(checkpoint_frequency=1),
    )
    exp3_args = dict(base_dict.items(), config=dict(mock_error=True))
    exp4_args = dict(
        base_dict.items(),
        config=dict(mock_error=True),
        checkpoint_config=dict(checkpoint_frequency=1),
    )

    all_experiments = {
        "exp1": exp1_args,
        "exp2": exp2_args,
        "exp3": exp3_args,
        "exp4": exp4_args,
    }

    tune.run_experiments(
        all_experiments, callbacks=[syncer_callback], raise_on_failed_trial=False
    )

    ray.shutdown()
    cluster.shutdown()
    cluster = _start_new_cluster()

    trials = tune.run_experiments(
        all_experiments,
        resume=True,
        raise_on_failed_trial=False,
    )

    assert len(trials) == 4
    assert all(t.status in [Trial.TERMINATED, Trial.ERROR] for t in trials)
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.skip(reason="Not very consistent.")
def test_cluster_rllib_restore(start_connected_cluster, tmpdir):
    cluster = start_connected_cluster
    dirpath = str(tmpdir)
    script = """
import time
import ray
from ray import tune

ray.init(address="{address}")


tune.run(
    "PG",
    name="experiment",
    config=dict(env="CartPole-v1", framework="tf"),
    stop=dict(training_iteration=10),
    local_dir="{checkpoint_dir}",
    checkpoint_freq=1,
    max_failures=1,
    dict(experiment=kwargs),
    raise_on_failed_trial=False)
""".format(
        address=cluster.address, checkpoint_dir=dirpath
    )
    run_string_as_driver_nonblocking(script)
    # Wait until the right checkpoint is saved.
    # The trainable returns every 0.5 seconds, so this should not miss
    # the checkpoint.
    local_checkpoint_dir = os.path.join(dirpath, "experiment")
    for i in range(100):
        if TrialRunner.checkpoint_exists(local_checkpoint_dir):
            # Inspect the internal trialrunner
            runner = TrialRunner(
                resume="LOCAL", local_checkpoint_dir=local_checkpoint_dir
            )
            trials = runner.get_trials()
            last_res = trials[0].last_result
            if last_res and last_res.get("training_iteration"):
                break
        time.sleep(0.3)

    if not TrialRunner.checkpoint_exists(local_checkpoint_dir):
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
                "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
                "local_dir": dirpath,
            }
        },
        resume=True,
    )
    assert all(t.status == Trial.TERMINATED for t in trials2)
    ray.shutdown()
    cluster.shutdown()


# TODO(ujvl): Fix test.
@pytest.mark.skip(reason="Not very consistent.")
def test_cluster_interrupt(start_connected_cluster, tmpdir):
    """Tests run_experiment on cluster shutdown with actual interrupt.

    This is an end-to-end test.
    """
    cluster = start_connected_cluster
    dirpath = str(tmpdir)

    # Needs to be in scope for pytest
    class _Mock(tune.Trainable):
        """Finishes on the 4th iteration."""

        def setup(self, config):
            self.state = {"hi": 0}

        def step(self):
            self.state["hi"] += 1
            time.sleep(0.5)
            return {"done": self.state["hi"] >= 4}

        def save_checkpoint(self, path):
            return self.state

        def load_checkpoint(self, state):
            self.state = state

    # Removes indent from class.
    reformatted = "\n".join(
        line[4:] if len(line) else line for line in inspect.getsource(_Mock).split("\n")
    )

    script = """
import os
import time
import ray
from ray import tune

os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"

ray.init(address="{address}")

{fail_class_code}

tune.run(
    {fail_class},
    name="experiment",
    stop=dict(training_iteration=5),
    local_dir="{checkpoint_dir}",
    checkpoint_freq=1,
    max_failures=1,
    raise_on_failed_trial=False)
""".format(
        address=cluster.address,
        checkpoint_dir=dirpath,
        fail_class_code=reformatted,
        fail_class=_Mock.__name__,
    )
    run_string_as_driver_nonblocking(script)

    # Wait until the right checkpoint is saved.
    # The trainable returns every 0.5 seconds, so this should not miss
    # the checkpoint.
    local_checkpoint_dir = os.path.join(dirpath, "experiment")
    for i in range(50):
        if TrialRunner.checkpoint_exists(local_checkpoint_dir):
            # Inspect the internal trialrunner
            runner = TrialRunner(
                resume="LOCAL", local_checkpoint_dir=local_checkpoint_dir
            )
            trials = runner.get_trials()
            last_res = trials[0].last_result
            if last_res and last_res.get("training_iteration") == 3:
                break
        time.sleep(0.2)

    if not TrialRunner.checkpoint_exists(local_checkpoint_dir):
        raise RuntimeError("Checkpoint file didn't appear.")

    ray.shutdown()
    cluster.shutdown()
    cluster = _start_new_cluster()
    Experiment.register_if_needed(_Mock)

    # Inspect the internal trialrunner
    runner = TrialRunner(resume="LOCAL", local_checkpoint_dir=local_checkpoint_dir)
    trials = runner.get_trials()
    assert trials[0].last_result["training_iteration"] == 3
    assert trials[0].status == Trial.PENDING

    # Restore properly from checkpoint
    trials2 = tune.run_experiments(
        {
            "experiment": {
                "run": _Mock,
                "local_dir": dirpath,
                "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
            }
        },
        resume=True,
        raise_on_failed_trial=False,
    )
    assert all(t.status == Trial.TERMINATED for t in trials2)
    assert {t.trial_id for t in trials2} == {t.trial_id for t in trials}
    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
