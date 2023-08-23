import time
import os
from pathlib import Path

import pytest
import subprocess
import sys

import ray
from ray.cluster_utils import Cluster
from ray.tune.analysis.experiment_analysis import NewExperimentAnalysis
from ray.tune.experiment import Trial
from ray.tune.execution.tune_controller import TuneController
from ray.tune.utils.mock_trainable import MyTrainableClass

from ray.train.tests.util import mock_storage_context


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


@pytest.mark.flaky(retries=3, delay=1)
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
    experiment_path = os.path.join(dirpath, "experiment")
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
    experiment_path = Path(experiment_path)
    sleep_time = 3.0
    for i in range(100):
        if not experiment_path.exists():
            time.sleep(sleep_time)
            continue

        ea = NewExperimentAnalysis(experiment_path)
        if len(ea.trials) >= 10:
            break
        time.sleep(sleep_time)
    else:
        raise ValueError(f"Didn't generate enough trials: {len(ea.trials)}")

    if not list(experiment_path.glob("*.json")):
        raise RuntimeError(
            f"No experiment chekcpoint ever found at {experiment_path}..."
        )

    ray.shutdown()
    cluster.shutdown()

    cluster = _start_new_cluster()
    execute_script_with_args(*(args + ("--resume",)))

    time.sleep(2)

    register_trainable("trainable", MyTrainableClass)
    reached = False
    for i in range(100):
        if not experiment_path.exists():
            time.sleep(sleep_time)
            continue

        # Inspect the internal TuneController
        ea = NewExperimentAnalysis(experiment_path)
        trials = ea.trials

        if len(trials) == 0:
            continue  # nonblocking script hasn't resumed yet, wait

        reached = True
        assert len(trials) >= 10
        assert len(trials) <= 20
        if len(trials) == 20:
            break
        time.sleep(3.0)

    assert reached

    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
