import click
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
from ray.tune.cli import commands
from ray.tune.result import CONFIG_PREFIX


class Capturing:
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
    ray.init(log_to_driver=False, local_mode=True)
    _register_all()
    yield
    ray.shutdown()


def test_time(start_ray, tmpdir):
    experiment_name = "test_time"
    experiment_path = os.path.join(str(tmpdir), experiment_name)
    num_samples = 2
    tune.run_experiments(
        {
            experiment_name: {
                "run": "__fake",
                "stop": {"training_iteration": 1},
                "num_samples": num_samples,
                "local_dir": str(tmpdir),
            }
        }
    )
    times = []
    for i in range(5):
        start = time.time()
        subprocess.check_call(["tune", "ls", experiment_path])
        times += [time.time() - start]

    assert sum(times) / len(times) < 7.0, "CLI is taking too long!"


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
    )

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
            filter_op="training_iteration == 1",
        )
    lines = output.captured
    assert len(lines) == 3 + num_samples + 1

    with pytest.raises(click.ClickException):
        commands.list_trials(
            experiment_path, sort=["trial_id"], info_keys=("training_iteration",)
        )

    with pytest.raises(click.ClickException):
        commands.list_trials(experiment_path, info_keys=("asdf",))


def test_ls_with_cfg(start_ray, tmpdir):
    experiment_name = "test_ls_with_cfg"
    experiment_path = os.path.join(str(tmpdir), experiment_name)
    tune.run(
        "__fake",
        name=experiment_name,
        stop={"training_iteration": 1},
        config={"test_variable": tune.grid_search(list(range(5)))},
        local_dir=str(tmpdir),
    )

    columns = [CONFIG_PREFIX + "/test_variable", "trial_id"]
    limit = 4
    with Capturing() as output:
        commands.list_trials(experiment_path, info_keys=columns, limit=limit)
    lines = output.captured
    assert all(col in lines[1] for col in columns)
    assert lines[1].count("|") == len(columns) + 1
    assert len(lines) == 3 + limit + 1


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
        )

    limit = 2
    with Capturing() as output:
        commands.list_experiments(
            project_path, info_keys=("total_trials",), limit=limit
        )
    lines = output.captured
    assert "total_trials" in lines[1]
    assert lines[1].count("|") == 2
    assert len(lines) == 3 + limit + 1

    with Capturing() as output:
        commands.list_experiments(
            project_path,
            sort=["total_trials"],
            info_keys=("total_trials",),
            filter_op="total_trials == 1",
        )
    lines = output.captured
    assert sum("1" in line for line in lines) >= num_experiments
    assert len(lines) == 3 + num_experiments + 1


if __name__ == "__main__":
    # Make click happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main([__file__]))
