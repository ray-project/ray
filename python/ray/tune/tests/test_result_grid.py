from ray.ml.checkpoint import Checkpoint
from ray.tune import ExperimentAnalysis
from ray.tune.result_grid import ResultGrid


def test_result_grid():
    analysis = ExperimentAnalysis(
        experiment_checkpoint_path="python/ray/tune/tests/experiment_checkpoint"
    )
    analysis._legacy_checkpoint = False
    result_grid = ResultGrid(analysis)
    result = result_grid[0]
    assert isinstance(result.checkpoint, Checkpoint)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
