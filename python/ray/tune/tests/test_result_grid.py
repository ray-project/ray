import json
import os

from ray import tune
from ray.ml.checkpoint import Checkpoint
from ray.tune.result_grid import ResultGrid


def test_result_grid():
    def f(config):
        # simulating the case that no report is called in train.
        with tune.checkpoint_dir(step=0) as checkpoint_dir:
            path = os.path.join(checkpoint_dir, "checkpoint")
            with open(path, "w") as f:
                f.write(json.dumps({"step": 0}))

    analysis = tune.run(f, config={"a": 1})
    analysis._legacy_checkpoint = False
    result_grid = ResultGrid(analysis)
    result = result_grid[0]
    assert isinstance(result.checkpoint, Checkpoint)
    assert isinstance(result.metrics, dict)
    assert isinstance(result.config, dict)
    assert result.config == {"a": 1}
    assert result.metrics["config"] == result.config


def test_result_grid_no_checkpoint():
    def f(config):
        pass

    analysis = tune.run(f)
    analysis._legacy_checkpoint = False
    result_grid = ResultGrid(analysis)
    result = result_grid[0]
    assert result.checkpoint is None


def test_result_grid_df():
    def f(config):
        tune.report(metric=config["nested"]["param"] * 1)
        tune.report(metric=config["nested"]["param"] * 4)
        tune.report(metric=config["nested"]["param"] * 3)

    analysis = tune.run(f, config={"nested": {"param": tune.grid_search([1, 2])}})
    analysis._legacy_checkpoint = False
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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
