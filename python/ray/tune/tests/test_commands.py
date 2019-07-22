from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import subprocess
import sys
import time
try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

import ray
from ray import tune
from ray.rllib import _register_all
from ray.tune import commands


class Capturing():
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        self.captured = []
        return self

    def __exit__(self, *args):
        self.captured.extend(self._stringio.getvalue().splitlines())
        del self._stringio  # free up some memory
        sys.stdout = self._stdout


@pytest.fixture
def start_ray():
    ray.init(log_to_driver=False)
    _register_all()
    yield
    ray.shutdown()


def test_time(start_ray, tmpdir):
    experiment_name = "test_time"
    experiment_path = os.path.join(str(tmpdir), experiment_name)
    num_samples = 2
    tune.run_experiments({
        experiment_name: {
            "run": "__fake",
            "stop": {
                "training_iteration": 1
            },
            "num_samples": num_samples,
            "local_dir": str(tmpdir)
        }
    })
    times = []
    for i in range(5):
        start = time.time()
        subprocess.check_call(["tune", "ls", experiment_path])
        times += [time.time() - start]

    assert sum(times) / len(times) < 2.0, "CLI is taking too long!"


def test_ls(start_ray, tmpdir):
    """This test captures output of list_trials."""
    experiment_name = "test_ls"
    experiment_path = os.path.join(str(tmpdir), experiment_name)
    num_samples = 3
    tune.run(
        "__fake",
        name=experiment_name,
        stop={"training_iteration": 1},
        num_samples=num_samples,
        local_dir=str(tmpdir),
        global_checkpoint_period=0)

    columns = ["episode_reward_mean", "training_iteration", "trial_id"]
    limit = 2
    with Capturing() as output:
        commands.list_trials(experiment_path, info_keys=columns, limit=limit)
    lines = output.captured
    assert all(col in lines[1] for col in columns)
    assert lines[1].count("|") == len(columns) + 1
    assert len(lines) == 3 + limit + 1

    with Capturing() as output:
        commands.list_trials(
            experiment_path,
            sort=["trial_id"],
            info_keys=("trial_id", "training_iteration"),
            filter_op="training_iteration == 1")
    lines = output.captured
    assert len(lines) == 3 + num_samples + 1


def test_lsx(start_ray, tmpdir):
    """This test captures output of list_experiments."""
    project_path = str(tmpdir)
    num_experiments = 3
    for i in range(num_experiments):
        experiment_name = "test_lsx{}".format(i)
        tune.run(
            "__fake",
            name=experiment_name,
            stop={"training_iteration": 1},
            num_samples=1,
            local_dir=project_path,
            global_checkpoint_period=0)

    limit = 2
    with Capturing() as output:
        commands.list_experiments(
            project_path, info_keys=("total_trials", ), limit=limit)
    lines = output.captured
    assert "total_trials" in lines[1]
    assert lines[1].count("|") == 2
    assert len(lines) == 3 + limit + 1

    with Capturing() as output:
        commands.list_experiments(
            project_path,
            sort=["total_trials"],
            info_keys=("total_trials", ),
            filter_op="total_trials == 1")
    lines = output.captured
    assert sum("1" in line for line in lines) >= num_experiments
    assert len(lines) == 3 + num_experiments + 1
