import time
import os

import pytest
import subprocess
import sys

import ray
from ray.rllib import _register_all
from ray.cluster_utils import Cluster
from ray.tune import register_trainable
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.utils.mock import MockDurableTrainer, MockRemoteTrainer
from ray.tune.utils.mock_trainable import MyTrainableClass


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


@pytest.fixture
def start_connected_cluster():
    # Start the Ray processes.
    cluster = _start_new_cluster()
    os.environ["TUNE_STATE_REFRESH_PERIOD"] = "0.1"
    yield cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.parametrize("searcher", ["hyperopt", "skopt", "bayesopt"])
def test_cluster_interrupt_searcher(start_connected_cluster, tmpdir, searcher):
    """Tests restoration of HyperOptSearch experiment on cluster shutdown
    with actual interrupt.

    Restoration should restore both state of trials
    and previous search algorithm (HyperOptSearch) state.
    This is an end-to-end test.
    """
    cluster = start_connected_cluster
    dirpath = str(tmpdir)
    local_checkpoint_dir = os.path.join(dirpath, "experiment")
    from ray.tune import register_trainable

    register_trainable("trainable", MyTrainableClass)

    def execute_script_with_args(*args):
        current_dir = os.path.dirname(__file__)
        script = os.path.join(current_dir, "_test_cluster_interrupt_searcher.py")
        subprocess.Popen([sys.executable, script] + list(args))

    args = (
        "--ray-address",
        cluster.address,
        "--local-dir",
        dirpath,
        "--searcher",
        searcher,
    )
    execute_script_with_args(*args)
    # Wait until the right checkpoint is saved.
    # The trainable returns every 0.5 seconds, so this should not miss
    # the checkpoint.
    trials = []
    for i in range(100):
        if TrialRunner.checkpoint_exists(local_checkpoint_dir):
            # Inspect the internal trialrunner
            runner = TrialRunner(
                resume="LOCAL", local_checkpoint_dir=local_checkpoint_dir
            )
            trials = runner.get_trials()
            if trials and len(trials) >= 10:
                break
        time.sleep(0.5)
    else:
        raise ValueError(f"Didn't generate enough trials: {len(trials)}")

    if not TrialRunner.checkpoint_exists(local_checkpoint_dir):
        raise RuntimeError(
            f"Checkpoint file didn't appear in {local_checkpoint_dir}. "
            f"Current list: {os.listdir(local_checkpoint_dir)}."
        )

    ray.shutdown()
    cluster.shutdown()

    cluster = _start_new_cluster()
    execute_script_with_args(*(args + ("--resume",)))

    time.sleep(2)

    register_trainable("trainable", MyTrainableClass)
    reached = False
    for i in range(100):
        if TrialRunner.checkpoint_exists(local_checkpoint_dir):
            # Inspect the internal trialrunner
            runner = TrialRunner(
                resume="LOCAL", local_checkpoint_dir=local_checkpoint_dir
            )
            trials = runner.get_trials()

            if len(trials) == 0:
                continue  # nonblocking script hasn't resumed yet, wait

            reached = True
            assert len(trials) >= 10
            assert len(trials) <= 20
            if len(trials) == 20:
                break
            else:
                stop_fn = runner.trial_executor.stop_trial
                [stop_fn(t) for t in trials if t.status is not Trial.ERROR]
        time.sleep(0.5)
    assert reached is True

    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
