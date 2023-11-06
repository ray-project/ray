import time
import os

import pytest
import sys
from unittest.mock import MagicMock

import ray
from ray import tune
from ray.train import CheckpointConfig
from ray.cluster_utils import Cluster
from ray.train._internal.storage import StorageContext
from ray.tune.error import TuneError
from ray.tune.search import BasicVariantGenerator
from ray.tune.experiment import Trial
from ray.tune.execution.tune_controller import TuneController


def _check_trial_running(trial):
    if trial.temporary_state.ray_actor:
        ray.get(trial.temporary_state.ray_actor.get_info.remote())
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


@pytest.fixture
def storage(tmp_path):
    os.makedirs(tmp_path / "exp_name" / "trial_name", exist_ok=True)
    yield StorageContext(
        storage_path=str(tmp_path),
        experiment_dir_name="exp_name",
        trial_dir_name="trial_name",
    )


def test_counting_resources(start_connected_cluster, storage):
    """Tests that Tune accounting is consistent with actual cluster."""

    cluster = start_connected_cluster
    nodes = []
    assert ray.cluster_resources()["CPU"] == 1
    runner = TuneController(search_alg=BasicVariantGenerator(), storage=storage)
    kwargs = {
        "stopping_criterion": {"training_iteration": 10},
        "config": {"sleep": 1.5},
        "storage": storage,
    }

    trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    while not any(t.status == Trial.RUNNING for t in trials):
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

    while not any(t.status == Trial.RUNNING for t in trials):
        runner.step()

    # Only 1 trial can be running due to resource limitation.
    assert sum(t.status == Trial.RUNNING for t in runner.get_trials()) == 1

    for i in range(5):
        nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 6

    while any(t.status == Trial.PENDING for t in trials):
        runner.step()
    assert sum(t.status == Trial.RUNNING for t in runner.get_trials()) == 2, [
        t.status for t in trials
    ]


def test_trial_processed_after_node_failure(start_connected_emptyhead_cluster, storage):
    """Tests that Tune processes a trial as failed if its node died."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    runner = TuneController(search_alg=BasicVariantGenerator(), storage=storage)
    mock_process_failure = MagicMock(side_effect=runner._process_trial_failure)
    runner._process_trial_failure = mock_process_failure
    # Disable recursion in magic mock when saving experiment state
    runner.save_to_dir = lambda *args, **kwargs: None

    runner.add_trial(Trial("__fake", storage=storage))
    trial = runner.get_trials()[0]

    while trial.status != Trial.RUNNING:
        runner.step()

    assert not mock_process_failure.called

    cluster.remove_node(node)
    while not mock_process_failure.called:
        runner.step()
    assert mock_process_failure.called


def test_remove_node_before_result(start_connected_emptyhead_cluster, storage):
    """Tune continues when node is removed before trial returns."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    runner = TuneController(search_alg=BasicVariantGenerator(), storage=storage)
    kwargs = {
        "stopping_criterion": {"training_iteration": 3},
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=2),
        "max_failures": 2,
        "storage": storage,
    }
    trial = Trial("__fake", **kwargs)
    runner.add_trial(trial)

    while trial.status != Trial.RUNNING:
        runner.step()
    running_trials = _get_running_trials(runner)
    assert len(running_trials) == 1
    assert _check_trial_running(running_trials[0])
    assert not trial.has_reported_at_least_once
    assert trial.status == Trial.RUNNING
    cluster.remove_node(node)
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 1

    while not trial.last_result.get("training_iteration") == 1:
        runner.step()
    assert trial.last_result.get("training_iteration") == 1

    # Process result: discover failure, recover, _train (from scratch)
    while trial.status != Trial.TERMINATED:
        runner.step()

    assert trial.last_result.get("training_iteration") > 1

    with pytest.raises(TuneError):
        runner.step()


def test_trial_requeue(start_connected_emptyhead_cluster, tmpdir, storage):
    """Removing a node in full cluster causes Trial to be requeued."""
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    runner = TuneController(search_alg=BasicVariantGenerator(), storage=storage)
    kwargs = {
        "stopping_criterion": {"training_iteration": 5},
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
        "max_failures": 1,
        "storage": storage,
    }

    trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
    for t in trials:
        runner.add_trial(t)

    while not any(t.status == Trial.RUNNING for t in trials):
        runner.step()

    runner.step()
    runner.step()

    running_trials = _get_running_trials(runner)
    assert len(running_trials) == 1
    assert _check_trial_running(running_trials[0])
    cluster.remove_node(node)
    cluster.wait_for_nodes()
    time.sleep(0.1)  # Sleep so that next step() refreshes cluster resources
    runner.step()  # Process result, dispatch save
    runner.step()  # Process save (detect error), requeue trial
    assert all(t.status == Trial.PENDING for t in trials)


def test_migration_checkpoint_removal(
    start_connected_emptyhead_cluster, tmpdir, storage
):
    """Test checks that trial restarts if checkpoint is lost w/ node fail."""
    cluster = start_connected_emptyhead_cluster
    node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    runner = TuneController(search_alg=BasicVariantGenerator(), storage=storage)
    kwargs = {
        "stopping_criterion": {"training_iteration": 4},
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=2),
        "max_failures": 2,
        "storage": storage,
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

    while not runner.is_finished():
        runner.step()
    assert t1.status == Trial.TERMINATED


def test_cluster_down_full(start_connected_cluster, tmpdir):
    """Tests that run_experiment restoring works on cluster shutdown."""
    cluster = start_connected_cluster

    base_dict = dict(run="__fake", stop=dict(training_iteration=3))

    exp1_args = base_dict
    exp2_args = dict(
        base_dict.items(),
        checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
    )
    exp3_args = dict(base_dict.items(), config=dict(mock_error=True))
    exp4_args = dict(
        base_dict.items(),
        config=dict(mock_error=True),
        checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
    )

    all_experiments = {
        "exp1": exp1_args,
        "exp2": exp2_args,
        "exp3": exp3_args,
        "exp4": exp4_args,
    }

    tune.run_experiments(all_experiments, raise_on_failed_trial=False)

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


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", "--reruns", "3", __file__]))
