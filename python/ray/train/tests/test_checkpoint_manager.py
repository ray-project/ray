import random

import pytest

from ray.train import CheckpointConfig
from ray.train._internal.checkpoint_manager import (
    _CheckpointManager,
    _TrackedCheckpoint,
)
from ray.train.checkpoint import Checkpoint


def test_unlimited_checkpoints(tmp_path):
    manager = _CheckpointManager(checkpoint_config=CheckpointConfig(num_to_keep=None))

    for i in range(10):
        manager.register_checkpoint(
            _TrackedCheckpoint(
                checkpoint=Checkpoint.from_directory(str(tmp_path)), metrics={"iter": i}
            )
        )

    assert len(manager.best_checkpoints) == 10


def test_limited_checkpoints(tmp_path):
    manager = _CheckpointManager(checkpoint_config=CheckpointConfig(num_to_keep=2))

    for i in range(10):
        manager.register_checkpoint(
            _TrackedCheckpoint(
                checkpoint=Checkpoint.from_directory(str(tmp_path)), metrics={"iter": i}
            )
        )

    assert len(manager.best_checkpoints) == 2

    # Keep the latest checkpoints if no metric is given.
    assert {
        tracked_checkpoint.metrics["iter"]
        for tracked_checkpoint in manager.best_checkpoints
    } == {8, 9}


@pytest.mark.parametrize("order", ["min", "max"])
def test_keep_checkpoints_by_score(order, tmp_path):
    num_to_keep = 2
    score_attribute = "score"

    manager = _CheckpointManager(
        checkpoint_config=CheckpointConfig(
            num_to_keep=num_to_keep,
            checkpoint_score_attribute=score_attribute,
            checkpoint_score_order=order,
        )
    )

    scores = []
    for i in range(10):
        score = random.random()
        manager.register_checkpoint(
            _TrackedCheckpoint(
                checkpoint=Checkpoint.from_directory(str(tmp_path)),
                metrics={"iter": i, "score": score},
            )
        )
        scores.append(score)

    sorted_scores = sorted(scores, reverse=order == "max")
    assert set(sorted_scores[:num_to_keep]) == {
        tracked_checkpoint.metrics[score_attribute]
        for tracked_checkpoint in manager.best_checkpoints
    }


@pytest.mark.parametrize(
    "metrics",
    [
        {"nested": {"sub": {"attr": 5}}},
        {"nested": {"sub/attr": 5}},
        {"nested/sub": {"attr": 5}},
        {"nested/sub/attr": 5},
    ],
)
def test_nested_get_checkpoint_score(metrics):
    manager = _CheckpointManager(
        checkpoint_config=CheckpointConfig(
            num_to_keep=2,
            checkpoint_score_attribute="nested/sub/attr",
            checkpoint_score_order="max",
        )
    )

    tracked_checkpoint = _TrackedCheckpoint(checkpoint=None, metrics=metrics, index=3)
    assert manager._get_checkpoint_score(tracked_checkpoint) == (True, 5.0, 3)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
