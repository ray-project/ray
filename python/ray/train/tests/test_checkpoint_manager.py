import random
from pathlib import Path
from typing import List

import pytest

from ray.train import Checkpoint, CheckpointConfig
from ray.train._internal.checkpoint_manager import _CheckpointManager, _TrainingResult


@pytest.fixture
def checkpoint_paths(tmp_path):
    checkpoint_paths = []
    for i in range(10):
        checkpoint_path = tmp_path / f"ckpt_{i}"
        checkpoint_path.mkdir()
        (checkpoint_path / "dummy.txt").write_text(f"{i}")
        checkpoint_paths.append(checkpoint_path)

    yield [str(path) for path in checkpoint_paths]


def test_unlimited_checkpoints(checkpoint_paths: List[str]):
    manager = _CheckpointManager(checkpoint_config=CheckpointConfig(num_to_keep=None))

    for i in range(10):
        manager.register_checkpoint(
            _TrainingResult(
                checkpoint=Checkpoint.from_directory(checkpoint_paths[i]),
                metrics={"iter": i},
            )
        )

    assert len(manager.best_checkpoint_results) == 10


def test_limited_checkpoints(checkpoint_paths: List[str]):
    manager = _CheckpointManager(checkpoint_config=CheckpointConfig(num_to_keep=2))

    for i in range(10):
        manager.register_checkpoint(
            _TrainingResult(
                checkpoint=Checkpoint.from_directory(checkpoint_paths[i]),
                metrics={"iter": i},
            )
        )

    assert len(manager.best_checkpoint_results) == 2

    # Keep the latest checkpoints if no metric is given.
    assert {
        tracked_checkpoint.metrics["iter"]
        for tracked_checkpoint in manager.best_checkpoint_results
    } == {8, 9}

    # The first 8 checkpoints should be deleted.
    for i in range(8):
        assert not Path(checkpoint_paths[i]).exists()

    assert Path(checkpoint_paths[8]).exists()
    assert Path(checkpoint_paths[9]).exists()


@pytest.mark.parametrize("order", ["min", "max"])
def test_keep_checkpoints_by_score(order, checkpoint_paths):
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
            _TrainingResult(
                checkpoint=Checkpoint.from_directory(checkpoint_paths[i]),
                metrics={"iter": i, score_attribute: score},
            )
        )
        scores.append(score)

    sorted_scores = sorted(scores, reverse=order == "max")
    assert set(sorted_scores[:num_to_keep]) == {
        tracked_checkpoint.metrics[score_attribute]
        for tracked_checkpoint in manager.best_checkpoint_results
    }

    # Make sure the bottom checkpoints are deleted.
    best_checkpoint_iters = {
        tracked_checkpoint.metrics["iter"]
        for tracked_checkpoint in manager.best_checkpoint_results
    }
    for i, checkpoint_path in enumerate(checkpoint_paths):
        if i in best_checkpoint_iters or i == 9:
            # The checkpoint should only exist if it's one of the top K or the latest.
            assert Path(checkpoint_path).exists()
        else:
            assert not Path(checkpoint_path).exists()


def test_keep_latest_checkpoint(checkpoint_paths):
    manager = _CheckpointManager(
        checkpoint_config=CheckpointConfig(
            num_to_keep=2,
            checkpoint_score_attribute="score",
            checkpoint_score_order="max",
        )
    )

    manager.register_checkpoint(
        _TrainingResult(
            checkpoint=Checkpoint.from_directory(checkpoint_paths[0]),
            metrics={"score": 3.0},
        )
    )
    manager.register_checkpoint(
        _TrainingResult(
            checkpoint=Checkpoint.from_directory(checkpoint_paths[1]),
            metrics={"score": 2.0},
        )
    )
    manager.register_checkpoint(
        _TrainingResult(
            checkpoint=Checkpoint.from_directory(checkpoint_paths[2]),
            metrics={"score": 1.0},
        )
    )

    assert len(manager.best_checkpoint_results) == 2

    # The latest checkpoint with the lowest score should not be deleted yet.
    assert manager.latest_checkpoint_result.metrics["score"] == 1.0

    # The latest checkpoint with the lowest score should not be deleted yet.
    assert Path(checkpoint_paths[2]).exists()

    manager.register_checkpoint(
        _TrainingResult(
            checkpoint=Checkpoint.from_directory(checkpoint_paths[3]),
            metrics={"score": 0.0},
        )
    )
    # A newer checkpoint came in. Even though the new one has a lower score, there are
    # already num_to_keep better checkpoints, so the previous one should be deleted.
    assert not Path(checkpoint_paths[2]).exists()

    # Quick sanity check to make sure that the new checkpoint is kept.
    assert manager.latest_checkpoint_result.metrics["score"] == 0.0
    assert Path(checkpoint_paths[3]).exists()

    # The original 2 checkpoints should still exist
    assert Path(checkpoint_paths[0]).exists()
    assert Path(checkpoint_paths[1]).exists()


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

    tracked_checkpoint = _TrainingResult(checkpoint=None, metrics=metrics)
    assert manager._get_checkpoint_score(tracked_checkpoint) == (True, 5.0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
