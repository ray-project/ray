import os
import random
import subprocess
import sys
import time
from unittest import mock

import click
import pytest

import ray
import ray.train
from ray import tune
from ray.rllib import _register_all
from ray.train.tests.util import create_dict_checkpoint
from ray.tune.cli import commands
from ray.tune.result import CONFIG_PREFIX

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO


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


def test_time(start_ray, tmpdir, monkeypatch):
    experiment_name = "test_time"
    num_samples = 2

    def train_fn(config):
        for i in range(3):
            with create_dict_checkpoint({"dummy": "data"}) as checkpoint:
                ray.train.report(
                    {
                        "epoch": i,
                        "a": random.random(),
                        "b/c": random.random(),
                        "d": random.random(),
                    },
                    checkpoint=checkpoint,
                )

    tuner = tune.Tuner(
        train_fn,
        param_space={f"hp{i}": tune.uniform(0, 1) for i in range(100)},
        tune_config=tune.TuneConfig(num_samples=num_samples),
        run_config=ray.train.RunConfig(name=experiment_name),
    )
    results = tuner.fit()
    times = []
    for _ in range(5):
        start = time.time()
        subprocess.check_call(["tune", "ls", results.experiment_path])
        times += [time.time() - start]

    print("Average CLI time: ", sum(times) / len(times))
    assert sum(times) / len(times) < 2, "CLI is taking too long!"


@mock.patch(
    "ray.tune.cli.commands.print_format_output",
    wraps=ray.tune.cli.commands.print_format_output,
)
def test_ls(mock_print_format_output, start_ray, tmpdir):
    """This test captures output of list_trials."""
    experiment_name = "test_ls"
    experiment_path = os.path.join(str(tmpdir), experiment_name)
    num_samples = 3
    tune.run(
        "__fake",
        name=experiment_name,
        stop={"training_iteration": 1},
        num_samples=num_samples,
        storage_path=str(tmpdir),
    )

    columns = ["episode_reward_mean", "training_iteration", "trial_id"]
    limit = 2
    commands.list_trials(experiment_path, info_keys=columns, limit=limit)

    # The dataframe that is printed as a table is the first arg of the last
    # call made to `ray.tune.cli.commands.print_format_output`.
    mock_print_format_output.assert_called()
    args, _ = mock_print_format_output.call_args_list[-1]
    df = args[0]
    assert sorted(df.columns.to_list()) == sorted(columns), df
    assert len(df.index) == limit, df

    commands.list_trials(
        experiment_path,
        sort=["trial_id"],
        info_keys=("trial_id", "training_iteration"),
        filter_op="training_iteration == 1",
    )
    args, _ = mock_print_format_output.call_args_list[-1]
    df = args[0]
    assert sorted(df.columns.to_list()) == sorted(["trial_id", "training_iteration"])
    assert len(df.index) == num_samples

    with pytest.raises(click.ClickException):
        commands.list_trials(
            experiment_path, sort=["trial_id"], info_keys=("training_iteration",)
        )

    with pytest.raises(click.ClickException):
        commands.list_trials(experiment_path, info_keys=("asdf",))


@mock.patch(
    "ray.tune.cli.commands.print_format_output",
    wraps=ray.tune.cli.commands.print_format_output,
)
def test_ls_with_cfg(mock_print_format_output, start_ray, tmpdir):
    experiment_name = "test_ls_with_cfg"
    experiment_path = os.path.join(str(tmpdir), experiment_name)
    tune.run(
        "__fake",
        name=experiment_name,
        stop={"training_iteration": 1},
        config={"test_variable": tune.grid_search(list(range(5)))},
        storage_path=str(tmpdir),
    )

    columns = [CONFIG_PREFIX + "/test_variable", "trial_id"]
    limit = 4

    commands.list_trials(experiment_path, info_keys=columns, limit=limit)

    # The dataframe that is printed as a table is the first arg of the last
    # call made to `ray.tune.cli.commands.print_format_output`.
    mock_print_format_output.assert_called()
    args, _ = mock_print_format_output.call_args_list[-1]
    df = args[0]
    assert sorted(df.columns.to_list()) == sorted(columns), df
    assert len(df.index) == limit, df


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
            storage_path=project_path,
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
