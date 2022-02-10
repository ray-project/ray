import inspect
import time
import os

import pytest
import shutil
import sys
from unittest.mock import MagicMock, patch

from typing import Callable, Union

import ray
from ray import tune
from ray.rllib import _register_all
from ray.cluster_utils import Cluster
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.tune import register_trainable
from ray.tune.experiment import Experiment
from ray.tune.error import TuneError
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.syncer import CloudSyncer, SyncerCallback, get_node_syncer
from ray.tune.utils.trainable import TrainableUtil
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.utils.mock import (
    MockDurableTrainer,
    MockRemoteTrainer,
    MockNodeSyncer,
    mock_storage_client,
    MOCK_REMOTE_DIR,
)


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
            "_system_config": {"num_heartbeats_timeout": 10},
        },
    )
    # Pytest doesn't play nicely with imports
    register_trainable("__fake_remote", MockRemoteTrainer)
    register_trainable("__fake_durable", MockDurableTrainer)
    _register_all()
    return cluster


class _PerTrialSyncerCallback(SyncerCallback):
    def __init__(self, get_sync_fn: Callable[["Trial"], Union[None, bool, Callable]]):
        self._get_sync_fn = get_sync_fn
        super(_PerTrialSyncerCallback, self).__init__(None)

    def _create_trial_syncer(self, trial: "Trial"):
        sync_fn = self._get_sync_fn(trial)
        return get_node_syncer(
            trial.logdir, remote_dir=trial.logdir, sync_function=sync_fn
        )


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
            "_system_config": {"num_heartbeats_timeout": 10},
        },
    )
    # Pytest doesn't play nicely with imports
    _register_all()
    register_trainable("__fake_remote", MockRemoteTrainer)
    register_trainable("__fake_durable", MockDurableTrainer)
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
        "checkpoint_freq": 2,
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


@pytest.mark.parametrize("trainable_id", ["__fake", "__fake_durable"])
def test_trial_migration(start_connected_emptyhead_cluster, trainable_id):
    """Removing a node while cluster has space should migrate trial.

    The trial state should also be consistent with the checkpoint.
    """
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    syncer_callback = _PerTrialSyncerCallback(
        lambda trial: trial.trainable_name == "__fake"
    )
    runner = TrialRunner(BasicVariantGenerator(), callbacks=[syncer_callback])
    kwargs = {
        "stopping_criterion": {"training_iteration": 4},
        "checkpoint_freq": 2,
        "max_failures": 2,
    }

    if trainable_id == "__fake_durable":
        kwargs["remote_checkpoint_dir"] = MOCK_REMOTE_DIR

    # Test recovery of trial that hasn't been checkpointed
    t = Trial(trainable_id, **kwargs)
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
    t2 = Trial(trainable_id, **kwargs)
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
    }

    if trainable_id == "__fake_durable":
        kwargs["remote_checkpoint_dir"] = MOCK_REMOTE_DIR

    t3 = Trial(trainable_id, **kwargs)
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


@pytest.mark.parametrize("trainable_id", ["__fake", "__fake_durable"])
def test_trial_requeue(start_connected_emptyhead_cluster, trainable_id):
    """Removing a node in full cluster causes Trial to be requeued."""
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    syncer_callback = _PerTrialSyncerCallback(
        lambda trial: trial.trainable_name == "__fake"
    )
    runner = TrialRunner(BasicVariantGenerator(), callbacks=[syncer_callback])  # noqa
    kwargs = {
        "stopping_criterion": {"training_iteration": 5},
        "checkpoint_freq": 1,
        "max_failures": 1,
    }

    if trainable_id == "__fake_durable":
        kwargs["remote_checkpoint_dir"] = MOCK_REMOTE_DIR

    trials = [Trial(trainable_id, **kwargs), Trial(trainable_id, **kwargs)]
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


@pytest.mark.parametrize("trainable_id", ["__fake_remote", "__fake_durable"])
def test_migration_checkpoint_removal(start_connected_emptyhead_cluster, trainable_id):
    """Test checks that trial restarts if checkpoint is lost w/ node fail."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    # Only added for fake_remote case.
    # For durable case, we don't do sync to head.
    class _SyncerCallback(SyncerCallback):
        def _create_trial_syncer(self, trial: "Trial"):
            client = mock_storage_client()
            return MockNodeSyncer(trial.logdir, trial.logdir, client)

    syncer_callback = (
        [_SyncerCallback(None)] if trainable_id == "__fake_remote" else None
    )
    runner = TrialRunner(BasicVariantGenerator(), callbacks=syncer_callback)
    kwargs = {
        "stopping_criterion": {"training_iteration": 4},
        "checkpoint_freq": 2,
        "max_failures": 2,
    }

    if trainable_id == "__fake_durable":
        kwargs["remote_checkpoint_dir"] = MOCK_REMOTE_DIR

    # The following patches only affect __fake_remote.
    def hide_remote_path(path_function):
        def hidden_path_func(checkpoint_path):
            """Converts back to local path first."""
            if MOCK_REMOTE_DIR in checkpoint_path:
                checkpoint_path = checkpoint_path[len(MOCK_REMOTE_DIR) :]
                checkpoint_path = os.path.join("/", checkpoint_path)
            return path_function(checkpoint_path)

        return hidden_path_func

    trainable_util = "ray.tune.ray_trial_executor.TrainableUtil"
    _find_ckpt = trainable_util + ".find_checkpoint_dir"
    find_func = TrainableUtil.find_checkpoint_dir
    _pickle_ckpt = trainable_util + ".pickle_checkpoint"
    pickle_func = TrainableUtil.pickle_checkpoint

    with patch(_find_ckpt) as mock_find, patch(_pickle_ckpt) as mock_pkl_ckpt:
        # __fake_remote trainables save to a separate "remote" directory.
        # TrainableUtil will not check this path unless we mock it.
        mock_find.side_effect = hide_remote_path(find_func)
        mock_pkl_ckpt.side_effect = hide_remote_path(pickle_func)

        # Test recovery of trial that has been checkpointed
        t1 = Trial(trainable_id, **kwargs)
        runner.add_trial(t1)

        # Start trial, process result (x2), process save
        while not t1.has_checkpoint():
            runner.step()

        cluster.add_node(num_cpus=1)
        cluster.remove_node(node)
        cluster.wait_for_nodes()
        shutil.rmtree(os.path.dirname(t1.checkpoint.value))
        while not runner.is_finished():
            runner.step()
    assert t1.status == Trial.TERMINATED, runner.debug_string()


@pytest.mark.skip(reason="Not very consistent.")
@pytest.mark.parametrize("trainable_id", ["__fake", "__fake_durable"])
def test_cluster_down_simple(start_connected_cluster, tmpdir, trainable_id):
    """Tests that TrialRunner save/restore works on cluster shutdown."""
    cluster = start_connected_cluster
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    dirpath = str(tmpdir)
    syncer_callback = _PerTrialSyncerCallback(
        lambda trial: trial.trainable_name == "__fake"
    )
    runner = TrialRunner(
        local_checkpoint_dir=dirpath, checkpoint_period=0, callbacks=[syncer_callback]
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 2},
        "checkpoint_freq": 1,
        "max_failures": 1,
    }

    if trainable_id == "__fake_durable":
        kwargs["remote_checkpoint_dir"] = MOCK_REMOTE_DIR

    trials = [Trial(trainable_id, **kwargs), Trial(trainable_id, **kwargs)]
    for t in trials:
        runner.add_trial(t)

    # Start trial (x2), process result, process save
    for _ in range(4):
        runner.step()
    assert all(t.status == Trial.RUNNING for t in runner.get_trials())
    runner.checkpoint()

    ray.shutdown()
    cluster.shutdown()

    cluster = _start_new_cluster()
    runner = TrialRunner(resume="LOCAL", local_checkpoint_dir=dirpath)
    # Start trial, process restore, process result, process save
    for _ in range(4):
        runner.step()

    # Start trial 2, process result, process save, process result, process save
    for i in range(5):
        runner.step()

    with pytest.raises(TuneError):
        runner.step()

    assert all(t.status == Trial.TERMINATED for t in runner.get_trials())
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.parametrize("trainable_id", ["__fake", "__fake_durable"])
def test_cluster_down_full(start_connected_cluster, tmpdir, trainable_id):
    """Tests that run_experiment restoring works on cluster shutdown."""
    cluster = start_connected_cluster
    dirpath = str(tmpdir)

    use_default_sync = trainable_id == "__fake"
    from ray.tune.result import DEFAULT_RESULTS_DIR

    local_dir = DEFAULT_RESULTS_DIR
    upload_dir = None if use_default_sync else MOCK_REMOTE_DIR

    base_dict = dict(
        run=trainable_id,
        stop=dict(training_iteration=3),
        local_dir=local_dir,
        sync_config=dict(upload_dir=upload_dir, syncer=use_default_sync),
    )

    exp1_args = base_dict
    exp2_args = dict(base_dict.items(), local_dir=dirpath, checkpoint_freq=1)
    exp3_args = dict(base_dict.items(), config=dict(mock_error=True))
    exp4_args = dict(base_dict.items(), config=dict(mock_error=True), checkpoint_freq=1)

    all_experiments = {
        "exp1": exp1_args,
        "exp2": exp2_args,
        "exp3": exp3_args,
        "exp4": exp4_args,
    }

    mock_get_client = "ray.tune.trial_runner.get_cloud_syncer"
    with patch(mock_get_client) as mock_get_cloud_syncer:
        mock_syncer = CloudSyncer(local_dir, upload_dir, mock_storage_client())
        mock_get_cloud_syncer.return_value = mock_syncer

        tune.run_experiments(all_experiments, raise_on_failed_trial=False)

        ray.shutdown()
        cluster.shutdown()
        cluster = _start_new_cluster()

        trials = tune.run_experiments(
            all_experiments, resume=True, raise_on_failed_trial=False
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
                "checkpoint_freq": 1,
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
        {"experiment": {"run": _Mock, "local_dir": dirpath, "checkpoint_freq": 1}},
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
