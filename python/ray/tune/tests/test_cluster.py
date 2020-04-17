import inspect
import json
import time
import os
import pytest
import shutil
import sys
from unittest.mock import MagicMock, patch

import ray
from ray import tune
from ray.rllib import _register_all
from ray.cluster_utils import Cluster
from ray.test_utils import run_string_as_driver_nonblocking
from ray.tune import register_trainable
from ray.tune.experiment import Experiment
from ray.tune.error import TuneError
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.resources import Resources
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.syncer import Syncer
from ray.tune.trainable import TrainableUtil
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.utils.mock import (MockDurableTrainer, MockRemoteTrainer,
                                 MockNodeSyncer, mock_storage_client,
                                 MOCK_REMOTE_DIR)


def _check_trial_running(trial):
    if trial.runner:
        ray.get(trial.runner.get_info.remote())
        return True
    return False


def _get_running_trials(runner):
    return [t for t in runner.get_trials() if t.status == Trial.RUNNING]


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
    register_trainable("__fake_remote", MockRemoteTrainer)
    register_trainable("__fake_durable", MockDurableTrainer)
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
    register_trainable("__fake_remote", MockRemoteTrainer)
    register_trainable("__fake_durable", MockDurableTrainer)
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

    runner.step()  # run 1
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
    runner.step()  # run 2
    assert sum(t.status == Trial.RUNNING for t in runner.get_trials()) == 1

    for i in range(5):
        nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 6

    runner.step()  # 1 result
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
        "stopping_criterion": {
            "training_iteration": 3
        },
        "checkpoint_freq": 2,
        "max_failures": 2
    }
    trial = Trial("__fake", **kwargs)
    runner.add_trial(trial)

    runner.step()  # Start trial, call _train once
    running_trials = _get_running_trials(runner)
    assert len(running_trials) == 1
    assert _check_trial_running(running_trials[0])
    assert not trial.last_result
    assert trial.status == Trial.RUNNING
    cluster.remove_node(node)
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 1

    # Process result: fetch data, invoke _train again
    runner.step()
    assert trial.last_result.get("training_iteration") == 1

    # Process result: discover failure, recover, _train (from scratch)
    runner.step()

    runner.step()  # Process result, invoke _train
    assert trial.last_result.get("training_iteration") == 1
    runner.step()  # Process result, invoke _save
    assert trial.last_result.get("training_iteration") == 2
    # process save, invoke _train
    runner.step()
    # process result
    runner.step()
    assert trial.status == Trial.TERMINATED

    with pytest.raises(TuneError):
        runner.step()


def test_queue_trials(start_connected_emptyhead_cluster):
    """Tests explicit oversubscription for autoscaling.

    Tune oversubscribes a trial when `queue_trials=True`, but
    does not block other trials from running.
    """
    cluster = start_connected_emptyhead_cluster
    runner = TrialRunner()

    def create_trial(cpu, gpu=0):
        kwargs = {
            "resources": Resources(cpu=cpu, gpu=gpu),
            "stopping_criterion": {
                "training_iteration": 3
            }
        }
        return Trial("__fake", **kwargs)

    runner.add_trial(create_trial(cpu=1))
    with pytest.raises(TuneError):
        runner.step()  # run 1

    del runner

    executor = RayTrialExecutor(queue_trials=True)
    runner = TrialRunner(trial_executor=executor)
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    cpu_only = create_trial(cpu=1)
    runner.add_trial(cpu_only)
    runner.step()  # add cpu_only trial

    gpu_trial = create_trial(cpu=1, gpu=1)
    runner.add_trial(gpu_trial)
    runner.step()  # queue gpu_trial

    # This tests that the cpu_only trial should bypass the queued trial.
    for i in range(3):
        runner.step()
    assert cpu_only.status == Trial.TERMINATED
    assert gpu_trial.status == Trial.RUNNING

    # Scale up
    cluster.add_node(num_cpus=1, num_gpus=1)
    cluster.wait_for_nodes()

    for i in range(3):
        runner.step()
    assert gpu_trial.status == Trial.TERMINATED


@pytest.mark.parametrize("trainable_id", ["__fake", "__fake_durable"])
def test_trial_migration(start_connected_emptyhead_cluster, trainable_id):
    """Removing a node while cluster has space should migrate trial.

    The trial state should also be consistent with the checkpoint.
    """
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 4
        },
        "checkpoint_freq": 2,
        "max_failures": 2,
        "remote_checkpoint_dir": MOCK_REMOTE_DIR,
        "sync_to_driver_fn": trainable_id == "__fake",
    }

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
    for _ in range(6):
        runner.step()

    assert t.status == Trial.TERMINATED, runner.debug_string()

    # Test recovery of trial that has been checkpointed
    t2 = Trial(trainable_id, **kwargs)
    runner.add_trial(t2)
    # Start trial, process result (x2), process save
    for _ in range(4):
        runner.step()
    assert t2.has_checkpoint()
    node3 = cluster.add_node(num_cpus=1)
    cluster.remove_node(node2)
    cluster.wait_for_nodes()
    runner.step()  # Process result 3 + start and fail 4 result
    runner.step()  # Dispatch restore
    runner.step()  # Process restore
    runner.step()  # Process result 5
    if t2.status != Trial.TERMINATED:
        runner.step()  # Process result 6, dispatch save
        runner.step()  # Process save
    assert t2.status == Trial.TERMINATED, runner.debug_string()

    # Test recovery of trial that won't be checkpointed
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 3
        },
        "remote_checkpoint_dir": MOCK_REMOTE_DIR,
        "sync_to_driver_fn": trainable_id == "__fake",
    }
    t3 = Trial(trainable_id, **kwargs)
    runner.add_trial(t3)
    runner.step()  # Start trial
    runner.step()  # Process result 1
    cluster.add_node(num_cpus=1)
    cluster.remove_node(node3)
    cluster.wait_for_nodes()
    runner.step()  # Error handling step
    if t3.status != Trial.ERROR:
        runner.step()
    assert t3.status == Trial.ERROR, runner.debug_string()

    with pytest.raises(TuneError):
        runner.step()


@pytest.mark.parametrize("trainable_id", ["__fake", "__fake_durable"])
def test_trial_requeue(start_connected_emptyhead_cluster, trainable_id):
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
        "max_failures": 1,
        "remote_checkpoint_dir": MOCK_REMOTE_DIR,
        "sync_to_driver_fn": trainable_id == "__fake",
    }

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
    runner.step()  # Process result, dispatch save
    runner.step()  # Process save (detect error), requeue trial
    assert all(
        t.status == Trial.PENDING for t in trials), runner.debug_string()

    with pytest.raises(TuneError):
        runner.step()


@pytest.mark.parametrize("trainable_id", ["__fake_remote", "__fake_durable"])
def test_migration_checkpoint_removal(start_connected_emptyhead_cluster,
                                      trainable_id):
    """Test checks that trial restarts if checkpoint is lost w/ node fail."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    runner = TrialRunner(BasicVariantGenerator())
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 4
        },
        "checkpoint_freq": 2,
        "max_failures": 2,
        "remote_checkpoint_dir": MOCK_REMOTE_DIR,
        "sync_to_driver_fn": trainable_id == "__fake_remote",
    }

    # The following patches only affect __fake_remote.
    find_checkpoint_dir = TrainableUtil.find_checkpoint_dir
    with patch("ray.tune.logger.get_node_syncer") as mock_get_node_syncer:
        trainable_util = "ray.tune.ray_trial_executor.TrainableUtil"
        with patch(trainable_util + ".find_checkpoint_dir") as mock_find_dir:

            def mock_get_syncer_fn(local_dir, remote_dir, sync_function):
                client = mock_storage_client()
                return MockNodeSyncer(local_dir, remote_dir, client)

            mock_get_node_syncer.side_effect = mock_get_syncer_fn

            def mock_find_dir_fn(checkpoint_path):
                """Converts back to local path first."""
                checkpoint_path = checkpoint_path[len(MOCK_REMOTE_DIR):]
                checkpoint_path = os.path.join("/", checkpoint_path)
                return find_checkpoint_dir(checkpoint_path)

            # __fake_remote trainables save to a separate "remote" directory.
            # TrainableUtil will not check this path unless we mock it.
            mock_find_dir.side_effect = mock_find_dir_fn

            # Test recovery of trial that has been checkpointed
            t1 = Trial(trainable_id, **kwargs)
            runner.add_trial(t1)

            # Start trial, process result (x2), process save
            for _ in range(4):
                runner.step()
            assert t1.has_checkpoint()

            cluster.add_node(num_cpus=1)
            cluster.remove_node(node)
            cluster.wait_for_nodes()
            shutil.rmtree(os.path.dirname(t1.checkpoint.value))

            runner.step()  # Collect result 3, kick off + fail result 4
            runner.step()  # Dispatch restore
            runner.step()  # Process restore + step 4
            for _ in range(3):
                if t1.status != Trial.TERMINATED:
                    runner.step()
    assert t1.status == Trial.TERMINATED, runner.debug_string()


@pytest.mark.parametrize("trainable_id", ["__fake", "__fake_durable"])
def test_cluster_down_simple(start_connected_cluster, tmpdir, trainable_id):
    """Tests that TrialRunner save/restore works on cluster shutdown."""
    cluster = start_connected_cluster
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    dirpath = str(tmpdir)
    runner = TrialRunner(local_checkpoint_dir=dirpath, checkpoint_period=0)
    kwargs = {
        "stopping_criterion": {
            "training_iteration": 2
        },
        "checkpoint_freq": 1,
        "max_failures": 1,
        "remote_checkpoint_dir": MOCK_REMOTE_DIR,
        "sync_to_driver_fn": trainable_id == "__fake",
    }
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
        upload_dir=upload_dir,
        sync_to_driver=use_default_sync,
    )

    exp1_args = base_dict
    exp2_args = dict(base_dict.items(), local_dir=dirpath, checkpoint_freq=1)
    exp3_args = dict(base_dict.items(), config=dict(mock_error=True))
    exp4_args = dict(
        base_dict.items(), config=dict(mock_error=True), checkpoint_freq=1)

    all_experiments = {
        "exp1": exp1_args,
        "exp2": exp2_args,
        "exp3": exp3_args,
        "exp4": exp4_args
    }

    mock_get_client = "ray.tune.trial_runner.get_cloud_syncer"
    with patch(mock_get_client) as mock_get_cloud_syncer:
        mock_syncer = Syncer(local_dir, upload_dir, mock_storage_client())
        mock_get_cloud_syncer.return_value = mock_syncer

        tune.run_experiments(all_experiments, raise_on_failed_trial=False)

        ray.shutdown()
        cluster.shutdown()
        cluster = _start_new_cluster()

        trials = tune.run_experiments(
            all_experiments, resume=True, raise_on_failed_trial=False)

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
    config=dict(env="CartPole-v1"),
    stop=dict(training_iteration=10),
    local_dir="{checkpoint_dir}",
    checkpoint_freq=1,
    max_failures=1,
    dict(experiment=kwargs),
    raise_on_failed_trial=False)
""".format(
        address=cluster.address, checkpoint_dir=dirpath)
    run_string_as_driver_nonblocking(script)
    # Wait until the right checkpoint is saved.
    # The trainable returns every 0.5 seconds, so this should not miss
    # the checkpoint.
    local_checkpoint_dir = os.path.join(dirpath, "experiment")
    for i in range(100):
        if TrialRunner.checkpoint_exists(local_checkpoint_dir):
            # Inspect the internal trialrunner
            runner = TrialRunner(
                resume="LOCAL", local_checkpoint_dir=local_checkpoint_dir)
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
                "local_dir": dirpath
            }
        },
        resume=True)
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

ray.init(address="{address}")

{fail_class_code}

tune.run(
    {fail_class},
    name="experiment",
    stop=dict(training_iteration=5),
    local_dir="{checkpoint_dir}",
    checkpoint_freq=1,
    global_checkpoint_period=0,
    max_failures=1,
    raise_on_failed_trial=False)
""".format(
        address=cluster.address,
        checkpoint_dir=dirpath,
        fail_class_code=reformatted,
        fail_class=_Mock.__name__)
    run_string_as_driver_nonblocking(script)

    # Wait until the right checkpoint is saved.
    # The trainable returns every 0.5 seconds, so this should not miss
    # the checkpoint.
    local_checkpoint_dir = os.path.join(dirpath, "experiment")
    for i in range(50):
        if TrialRunner.checkpoint_exists(local_checkpoint_dir):
            # Inspect the internal trialrunner
            runner = TrialRunner(
                resume="LOCAL", local_checkpoint_dir=local_checkpoint_dir)
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
    runner = TrialRunner(
        resume="LOCAL", local_checkpoint_dir=local_checkpoint_dir)
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
    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
