import json
import os
import pickle
import shutil

import pytest
import pandas as pd

import ray
from ray.air._internal.checkpoint_manager import CheckpointStorage, _TrackedCheckpoint
from ray import tune
from ray.air.checkpoint import Checkpoint
from ray.tune.registry import get_trainable_cls
from ray.tune.result_grid import ResultGrid
from ray.tune.experiment import Trial
from ray.tune.tests.tune_test_util import create_tune_experiment_checkpoint


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_result_grid(ray_start_2_cpus):
    def f(config):
        # simulating the case that no report is called in train.
        with tune.checkpoint_dir(step=0) as checkpoint_dir:
            path = os.path.join(checkpoint_dir, "checkpoint")
            with open(path, "w") as f:
                f.write(json.dumps({"step": 0}))

    analysis = tune.run(f, config={"a": 1})
    result_grid = ResultGrid(analysis)
    result = result_grid[0]
    assert isinstance(result.checkpoint, Checkpoint)
    assert isinstance(result.metrics, dict)
    assert isinstance(result.config, dict)
    assert result.config == {"a": 1}
    assert result.metrics["config"] == result.config


def test_result_grid_metric_mode(ray_start_2_cpus):
    def f(config):
        for i in range(2):
            with tune.checkpoint_dir(step=i) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                with open(path, "w") as f:
                    f.write(json.dumps({"step": i}))
            tune.report(step=i)

    analysis = tune.run(f, config={"a": 1}, metric="step", mode="min")
    analysis._legacy_checkpoint = False
    result_grid = ResultGrid(analysis)
    result = result_grid[0]
    assert isinstance(result.checkpoint, Checkpoint)
    assert isinstance(result.best_checkpoints, list)
    assert isinstance(result.metrics, dict)
    assert isinstance(result.config, dict)
    assert isinstance(result.metrics_dataframe, pd.DataFrame)
    assert os.path.normpath(
        result.checkpoint.get_internal_representation()[1]
    ) != os.path.normpath(
        min((x for x in result.best_checkpoints), key=lambda x: x[1]["step"])[
            0
        ].get_internal_representation()[1]
    )
    assert result.config == {"a": 1}
    assert result.metrics["config"] == result.config
    assert len(result.metrics_dataframe) == 2


def test_result_grid_metric_mode_unset(ray_start_2_cpus):
    def f(config):
        for i in range(2):
            with tune.checkpoint_dir(step=i) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                with open(path, "w") as f:
                    f.write(json.dumps({"step": i}))
            tune.report(step=i)

    analysis = tune.run(f, config={"a": 1})
    analysis._legacy_checkpoint = False
    result_grid = ResultGrid(analysis)
    result = result_grid[0]
    assert isinstance(result.checkpoint, Checkpoint)
    assert isinstance(result.metrics, dict)
    assert isinstance(result.config, dict)
    assert isinstance(result.metrics_dataframe, pd.DataFrame)
    assert result.config == {"a": 1}
    assert result.metrics["config"] == result.config
    assert len(result.metrics_dataframe) == 2


def test_result_grid_no_checkpoint(ray_start_2_cpus):
    def f(config):
        pass

    analysis = tune.run(f)
    result_grid = ResultGrid(analysis)
    result = result_grid[0]
    assert result.checkpoint is None


@pytest.mark.parametrize("to_object", [False, True])
def test_result_grid_future_checkpoint(ray_start_2_cpus, to_object):
    trainable_cls = get_trainable_cls("__fake")
    trial = Trial("__fake", stub=True)
    trial.config = {"some_config": 1}
    trial.last_result = {"some_result": 2, "config": trial.config}

    trainable = ray.remote(trainable_cls).remote()
    ray.get(trainable.set_info.remote({"info": 4}))

    if to_object:
        checkpoint_data = trainable.save_to_object.remote()
    else:
        checkpoint_data = trainable.save.remote()

    trial.on_checkpoint(
        _TrackedCheckpoint(checkpoint_data, storage_mode=CheckpointStorage.MEMORY)
    )
    trial.pickled_error_file = None
    trial.error_file = None
    result_grid = ResultGrid(None)

    # Internal result grid conversion
    result = result_grid._trial_to_result(trial)
    assert isinstance(result.checkpoint, Checkpoint)
    assert isinstance(result.metrics, dict)
    assert isinstance(result.config, dict)
    assert result.metrics_dataframe is None
    assert result.config == {"some_config": 1}
    assert result.metrics["config"] == result.config

    # Load checkpoint data (see ray.rllib.algorithms.mock.MockTrainer definition)
    with result.checkpoint.as_directory() as checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "mock_agent.pkl"), "rb") as f:
            info = pickle.load(f)
            assert info["info"] == 4


def test_best_result(ray_start_2_cpus):
    def f(config):
        for _ in range(2):
            tune.report(x=config["x"])

    analysis = tune.run(f, config={"x": tune.grid_search([1, 2])})
    result_grid = ResultGrid(analysis)
    best_result = result_grid.get_best_result(metric="x", mode="max")
    assert best_result.config["x"] == 2
    assert best_result.metrics["x"] == 2


def test_best_result_checkpoint_history(ray_start_2_cpus):
    def f(config):
        for i in range(2):
            with tune.checkpoint_dir(step=i) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                with open(path, "w") as f:
                    f.write(json.dumps(dict(x=config["x"], step=i)))
            tune.report(x=config["x"], step=i)

    analysis = tune.run(f, config={"x": tune.grid_search([1, 3])})

    # No checkpointing config. Use metric and mode
    result_grid = ResultGrid(analysis)
    best_result = result_grid.get_best_result(metric="x", mode="max")
    assert best_result.metrics["x"] == 3
    print(best_result.best_checkpoints)
    print([x[0].get_internal_representation() for x in best_result.best_checkpoints])
    assert len(best_result.best_checkpoints) == 2
    i = 0
    for checkpoint, metrics in best_result.best_checkpoints:
        assert isinstance(checkpoint, Checkpoint)
        assert metrics["x"] == 3
        assert metrics["step"] == i
        i += 1


def test_best_result_no_report(ray_start_2_cpus):
    def f(config):
        pass

    analysis = tune.run(f, config={"x": tune.grid_search([1, 2])})
    result_grid = ResultGrid(analysis)
    with pytest.raises(RuntimeError, match="No best trial found*"):
        result_grid.get_best_result(metric="x", mode="max")


def test_result_repr(ray_start_2_cpus):
    def f(config):
        from ray.air import session

        session.report({"loss": 1})

    tuner = tune.Tuner(f, param_space={"x": tune.grid_search([1, 2])})
    result_grid = tuner.fit()
    result = result_grid[0]

    from ray.tune.result import AUTO_RESULT_KEYS

    representation = result.__repr__()
    assert not any(key in representation for key in AUTO_RESULT_KEYS)


def test_no_metric_mode(ray_start_2_cpus):
    def f(config):
        tune.report(x=1)

    analysis = tune.run(f, num_samples=2)
    result_grid = ResultGrid(analysis)
    with pytest.raises(ValueError):
        result_grid.get_best_result()

    with pytest.raises(ValueError):
        result_grid.get_best_result(metric="x")

    with pytest.raises(ValueError):
        result_grid.get_best_result(mode="max")


def test_no_metric_mode_one_trial(ray_start_2_cpus):
    def f(config):
        tune.report(x=1)

    results = tune.Tuner(f, tune_config=tune.TuneConfig(num_samples=1)).fit()
    # This should not throw any exception
    best_result = results.get_best_result()
    assert best_result


def test_result_grid_df(ray_start_2_cpus):
    def f(config):
        tune.report(metric=config["nested"]["param"] * 1)
        tune.report(metric=config["nested"]["param"] * 4)
        tune.report(metric=config["nested"]["param"] * 3)

    analysis = tune.run(f, config={"nested": {"param": tune.grid_search([1, 2])}})
    result_grid = ResultGrid(analysis)

    assert len(result_grid) == 2

    # Last result
    df = result_grid.get_dataframe()
    assert sorted(df["metric"]) == [3, 6]

    # Best result (max)
    df = result_grid.get_dataframe(filter_metric="metric", filter_mode="max")
    assert sorted(df["metric"]) == [4, 8]

    # Best result (min)
    df = result_grid.get_dataframe(filter_metric="metric", filter_mode="min")
    assert sorted(df["metric"]) == [1, 2]

    assert sorted(df["config/nested/param"]) == [1, 2]


def test_num_errors_terminated(tmpdir):
    error_file = tmpdir / "error.txt"
    with open(error_file, "w") as fp:
        fp.write("Test error\n")

    trials = [Trial("foo", stub=True) for i in range(10)]
    trials[4].status = Trial.ERROR
    trials[6].status = Trial.ERROR
    trials[8].status = Trial.ERROR

    trials[4].error_file = error_file
    trials[6].error_file = error_file
    trials[8].error_file = error_file

    trials[3].status = Trial.TERMINATED
    trials[5].status = Trial.TERMINATED

    experiment_dir = create_tune_experiment_checkpoint(trials)
    result_grid = ResultGrid(tune.ExperimentAnalysis(experiment_dir))
    assert len(result_grid.errors) == 3
    assert result_grid.num_errors == 3
    assert result_grid.num_terminated == 2
    shutil.rmtree(experiment_dir)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
