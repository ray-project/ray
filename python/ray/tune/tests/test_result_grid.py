import pytest

import ray
from ray import train, tune
from ray.train import Result
from ray.train._checkpoint import Checkpoint
from ray.tune.result_grid import ResultGrid

from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_result_grid_api(ray_start_2_cpus, tmp_path):
    def train_fn(config):
        peak_fn = [0, config["id"], -config["id"], 0]
        for i in range(len(peak_fn)):
            with create_dict_checkpoint({"iter": i}) as checkpoint:
                train.report(
                    {"iter": i, "score": config["id"], "peak": peak_fn[i]},
                    checkpoint=checkpoint,
                )

    tuner = tune.Tuner(
        train_fn,
        param_space={"id": tune.grid_search([1, 2])},
        run_config=train.RunConfig(
            storage_path=str(tmp_path),
            name="test_result_grid_api",
            checkpoint_config=train.CheckpointConfig(num_to_keep=2),
        ),
    )
    result_grid = tuner.fit()

    assert len(result_grid) == 2
    assert result_grid.experiment_path == str(tmp_path / "test_result_grid_api")

    with pytest.raises(ValueError):
        result_grid.get_best_result()
    with pytest.raises(ValueError):
        result_grid.get_best_result(metric="score")
    assert result_grid.get_best_result(metric="score", mode="max").config["id"] == 2

    df = result_grid.get_dataframe()
    assert len(df) == 2
    assert df["iter"].to_list() == [3, 3]

    df = result_grid.get_dataframe(filter_metric="peak", filter_mode="max")
    assert df["iter"].to_list() == [1, 1]
    df = result_grid.get_dataframe(filter_metric="peak", filter_mode="min")
    assert df["iter"].to_list() == [2, 2]

    assert not result_grid.errors
    assert result_grid.num_errors == 0
    assert result_grid.num_terminated == 2

    for result in result_grid:
        assert result.checkpoint is not None
        assert result.error is None
        assert load_dict_checkpoint(result.checkpoint)["iter"] == 3
        assert {metrics["iter"] for _, metrics in result.best_checkpoints} == {2, 3}
        assert {
            load_dict_checkpoint(checkpoint)["iter"]
            for checkpoint, _ in result.best_checkpoints
        } == {2, 3}


def test_result_grid_no_checkpoint(ray_start_2_cpus):
    def f(config):
        pass

    analysis = tune.run(f)
    result_grid = ResultGrid(analysis)
    result = result_grid[0]
    assert result.checkpoint is None


def test_best_result_no_report(ray_start_2_cpus):
    def f(config):
        pass

    analysis = tune.run(f, config={"x": tune.grid_search([1, 2])})
    result_grid = ResultGrid(analysis)
    with pytest.raises(RuntimeError, match="No best trial found*"):
        result_grid.get_best_result(metric="x", mode="max")


def test_result_repr(ray_start_2_cpus):
    def f(config):
        train.report({"loss": 1})

    tuner = tune.Tuner(f, param_space={"x": tune.grid_search([1, 2])})
    result_grid = tuner.fit()
    result = result_grid[0]

    from ray.tune.result import AUTO_RESULT_KEYS
    from ray.tune.experimental.output import BLACKLISTED_KEYS

    representation = result.__repr__()
    assert not any(key in representation for key in AUTO_RESULT_KEYS)
    assert not any(key in representation for key in BLACKLISTED_KEYS)


def test_result_grid_repr(tmp_path):
    class MockExperimentAnalysis:
        trials = []

    result_grid = ResultGrid(experiment_analysis=MockExperimentAnalysis())

    result_grid._results = [
        Result(
            metrics={"loss": 1.0},
            checkpoint=Checkpoint("/tmp/ckpt1"),
            _local_path="log_1",
            error=None,
            metrics_dataframe=None,
        ),
        Result(
            metrics={"loss": 2.0},
            checkpoint=Checkpoint("/tmp/ckpt2"),
            _local_path="log_2",
            error=RuntimeError(),
            metrics_dataframe=None,
            best_checkpoints=None,
        ),
    ]

    from ray.tune.result import AUTO_RESULT_KEYS

    assert len(result_grid) == 2
    assert not any(key in repr(result_grid) for key in AUTO_RESULT_KEYS)

    expected_repr = """ResultGrid<[
  Result(
    metrics={'loss': 1.0},
    path='log_1',
    filesystem=local,
    checkpoint=Checkpoint(filesystem=local, path=/tmp/ckpt1)
  ),
  Result(
    error='RuntimeError',
    metrics={'loss': 2.0},
    path='log_2',
    filesystem=local,
    checkpoint=Checkpoint(filesystem=local, path=/tmp/ckpt2)
  )
]>"""

    assert repr(result_grid) == expected_repr


def test_no_metric_mode_one_trial(ray_start_2_cpus):
    def f(config):
        train.report(dict(x=1))

    results = tune.Tuner(f, tune_config=tune.TuneConfig(num_samples=1)).fit()
    # This should not throw any exception
    best_result = results.get_best_result()
    assert best_result


def test_result_grid_df(ray_start_2_cpus):
    def f(config):
        train.report(dict(metric=config["nested"]["param"] * 1))
        train.report(dict(metric=config["nested"]["param"] * 4))
        train.report(dict(metric=config["nested"]["param"] * 3))

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


def test_num_errors_terminated(ray_start_2_cpus, tmp_path):
    def train_fn(config):
        if config["id"] == 1:
            raise RuntimeError()
        else:
            train.report({"score": config["id"]})

    tuner = tune.Tuner(
        train_fn,
        param_space={"id": tune.grid_search([1, 2])},
        run_config=train.RunConfig(storage_path=str(tmp_path)),
    )

    result_grid = tuner.fit()
    assert result_grid.num_errors == 1
    assert result_grid.num_terminated == 1
    assert isinstance(result_grid.errors[0], RuntimeError)


def test_result_grid_moved_experiment_path(ray_start_2_cpus, tmpdir):
    # TODO(justinvyu): [handle_moved_storage_path]
    pytest.skip("Not implemented yet.")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
