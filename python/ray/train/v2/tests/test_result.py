import pandas as pd
import pytest

from ray.train import Checkpoint
from ray.train.v2.api.result import Result


def test_result_raises_with_dataframe():
    """Test that the Result init function raises an error when
    metrics_dataframe is passed in as a parameter.
    """
    with pytest.raises(TypeError):
        Result(
            metrics={},
            checkpoint=None,
            error=None,
            path=None,
            metrics_dataframe=pd.DataFrame(),
        )


def test_result_repr():
    """Test that the Result __repr__ function can return a string."""
    res = Result(
        metrics={"iter": 0, "metric": 1.0},
        checkpoint=Checkpoint("/bucket/path/ckpt0"),
        error=None,
        path="/bucket/path",
    )
    assert isinstance(repr(res), str)
    assert "Checkpoint(filesystem=local, path=/bucket/path/ckpt0)" in repr(res)
    assert "metrics={'iter': 0, 'metric': 1.0}" in repr(res)


def test_get_best_checkpoint():
    """Test that the Result get_best_checkpoint function returns the correct"""

    res = Result(
        metrics={},
        checkpoint=None,
        error=None,
        path="/bucket/path",
        best_checkpoints=[
            (Checkpoint("/bucket/path/ckpt0"), {"iter": 0, "metric": 1.0}),
            (Checkpoint("/bucket/path/ckpt1"), {"iter": 1, "metric": 2.0}),
            (Checkpoint("/bucket/path/ckpt2"), {"iter": 2, "metric": 3.0}),
            (Checkpoint("/bucket/path/ckpt3"), {"iter": 3, "metric": 4.0}),
        ],
    )
    assert (
        res.get_best_checkpoint(metric="metric", mode="max").path
        == "/bucket/path/ckpt3"
    )
    assert (
        res.get_best_checkpoint(metric="metric", mode="min").path
        == "/bucket/path/ckpt0"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
