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

    analysis = tune.run(f)
    analysis._legacy_checkpoint = False
    result_grid = ResultGrid(analysis)
    result = result_grid[0]
    assert isinstance(result.checkpoint, Checkpoint)


def test_result_grid_no_checkpoint():
    def f(config):
        pass

    analysis = tune.run(f)
    analysis._legacy_checkpoint = False
    result_grid = ResultGrid(analysis)
    result = result_grid[0]
    assert result.checkpoint is None


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
