from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest

import ray
from ray import tune
from ray.rllib import _register_all
from ray.tune import commands


@pytest.fixture
def start_ray():
    ray.init()
    _register_all()
    yield
    ray.shutdown()


def test_ls(start_ray, capsys, tmpdir):
    """This test captures output of list_trials."""
    experiment_name = "test_ls"
    experiment_path = os.path.join(str(tmpdir), experiment_name)
    num_samples = 2
    with capsys.disabled():
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

    commands.list_trials(experiment_path, info_keys=("status", ))
    captured = capsys.readouterr().out.strip()
    lines = captured.split("\n")
    assert sum("TERMINATED" in line for line in lines) == num_samples

    commands.list_trials(
        experiment_path,
        info_keys=("status", ),
        filter_op="status == TERMINATED")
    captured = capsys.readouterr().out.strip()
    lines = captured.split("\n")
    assert sum("TERMINATED" in line for line in lines) == num_samples


def test_lsx(start_ray, capsys, tmpdir):
    """This test captures output of list_experiments."""
    project_path = str(tmpdir)
    num_experiments = 3
    for i in range(num_experiments):
        experiment_name = "test_lsx{}".format(i)
        with capsys.disabled():
            tune.run_experiments({
                experiment_name: {
                    "run": "__fake",
                    "stop": {
                        "training_iteration": 1
                    },
                    "num_samples": 1,
                    "local_dir": project_path
                }
            })

    commands.list_experiments(project_path, info_keys=("total_trials", ))
    captured = capsys.readouterr().out.strip()
    lines = captured.split("\n")
    assert sum("1" in line for line in lines) >= num_experiments

    commands.list_experiments(
        project_path,
        info_keys=("total_trials", ),
        filter_op="total_trials == 1")
    captured = capsys.readouterr().out.strip()
    lines = captured.split("\n")
    assert sum("1" in line for line in lines) >= num_experiments
    assert len(lines) == 3 + num_experiments + 1
