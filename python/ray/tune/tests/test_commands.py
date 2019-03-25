from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import sys
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
    ray.init()
    _register_all()
    yield
    ray.shutdown()


def test_ls(start_ray, tmpdir):
    """This test captures output of list_trials."""
    experiment_name = "test_ls"
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

    with Capturing() as output:
        commands.list_trials(experiment_path, info_keys=("status", ))
    lines = output.captured
    assert sum("TERMINATED" in line for line in lines) == num_samples


def test_lsx(start_ray, tmpdir):
    """This test captures output of list_experiments."""
    project_path = str(tmpdir)
    num_experiments = 3
    for i in range(num_experiments):
        experiment_name = "test_lsx{}".format(i)
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

    with Capturing() as output:
        commands.list_experiments(project_path, info_keys=("total_trials", ))
    lines = output.captured
    assert sum("1" in line for line in lines) >= 3
