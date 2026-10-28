import random
from pathlib import Path
from typing import List

import pytest

from ray.train import Checkpoint, CheckpointConfig
from ray.train._internal.checkpoint_manager import (
    _CheckpointManager,
    _insert_into_sorted_list,
    _TrainingResult,
)
from ray.train.constants import TUNE_ONLY_STORE_CHECKPOINT_SCORE_ATTRIBUTE


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


@pytest.mark.parametrize("has_score_attr", [True, False])
def test_only_store_score_attr(has_score_attr, checkpoint_paths, monkeypatch):
    monkeypatch.setenv(TUNE_ONLY_STORE_CHECKPOINT_SCORE_ATTRIBUTE, "1")

    # Set up CheckpointManager.
    if has_score_attr:
        checkpoint_config = CheckpointConfig(
            num_to_keep=None,
            checkpoint_score_attribute="score",
            checkpoint_score_order="max",
        )
    else:
        checkpoint_config = CheckpointConfig(num_to_keep=None)
    manager = _CheckpointManager(checkpoint_config=checkpoint_config)

    # Ensure we insert TrainingResults with score in the right order.
    manager.register_checkpoint(
        _TrainingResult(
            checkpoint=Checkpoint.from_directory(checkpoint_paths[0]),
            metrics={"score": 3.0},
        )
    )
    manager.register_checkpoint(
        _TrainingResult(
            checkpoint=Checkpoint.from_directory(checkpoint_paths[1]),
            metrics={"score": 1.0, "another_unsaved_metric": 6.0},
        )
    )
    manager.register_checkpoint(
        _TrainingResult(
            checkpoint=Checkpoint.from_directory(checkpoint_paths[2]),
            metrics={"another_unsaved_metric": 1.0},
        )
    )
    assert len(manager.best_checkpoint_results) == 3
    if has_score_attr:
        assert manager.best_checkpoint_results[0].metrics == {"score": 1.0}
        assert manager.best_checkpoint_results[0].checkpoint.path == checkpoint_paths[1]
        assert manager.best_checkpoint_results[1].metrics == {"score": 3.0}
        assert manager.best_checkpoint_results[1].checkpoint.path == checkpoint_paths[0]
        assert manager.best_checkpoint_results[2].metrics == {}
        assert manager.best_checkpoint_results[2].checkpoint.path == checkpoint_paths[2]
    else:
        assert manager.best_checkpoint_results[0].metrics == {}
        assert manager.best_checkpoint_results[0].checkpoint.path == checkpoint_paths[0]
        assert manager.best_checkpoint_results[1].metrics == {}
        assert manager.best_checkpoint_results[1].checkpoint.path == checkpoint_paths[1]
        assert manager.best_checkpoint_results[2].metrics == {}
        assert manager.best_checkpoint_results[2].checkpoint.path == checkpoint_paths[2]


def _linear_insert_into_sorted_list(lst, item, key, checkpoint_to_report_index):
    """Reference implementation: linear scan for the first later-ordered element."""
    i = 0
    while i < len(lst):
        if key(lst[i]) > key(item) or (
            key(lst[i]) == key(item)
            and checkpoint_to_report_index.get(lst[i].checkpoint, 0)
            > checkpoint_to_report_index.get(item.checkpoint, 0)
        ):
            break
        i += 1
    lst.insert(i, item)


@pytest.mark.parametrize("use_report_index", [False, True])
def test_insert_into_sorted_list_matches_linear_scan(use_report_index):
    """Binary-search insertion places items exactly like a linear scan, including
    tie-breaking by report index, for both fresh and re-inserted items."""
    rng = random.Random(0)
    # Few distinct scores so there are many ties.
    results = [
        _TrainingResult(
            checkpoint=Checkpoint(path=f"/fake/ckpt_{i}"),
            metrics={"score": float(rng.randint(0, 5))},
        )
        for i in range(300)
    ]
    report_index = {r.checkpoint: i for i, r in enumerate(results)}
    index_arg = report_index if use_report_index else None
    ref_index = report_index if use_report_index else {}
    key = lambda r: r.metrics["score"]  # noqa: E731

    actual, expected = [], []
    for r in results:
        _insert_into_sorted_list(actual, r, key, index_arg)
        _linear_insert_into_sorted_list(expected, r, key, ref_index)
        assert [id(x) for x in actual] == [id(x) for x in expected]

    # Re-insertion after a score update (as done on validation completion).
    for r in rng.sample(results, 100):
        r.metrics["score"] = float(rng.randint(0, 5))
        for lst in (actual, expected):
            lst.remove(r)
        _insert_into_sorted_list(actual, r, key, index_arg)
        _linear_insert_into_sorted_list(expected, r, key, ref_index)
        assert [id(x) for x in actual] == [id(x) for x in expected]

    scores = [key(r) for r in actual]
    assert scores == sorted(scores)


def test_insert_into_sorted_list_key_calls_are_logarithmic():
    """Insertion must not evaluate the (expensive) key on every list element."""
    n = 1024
    calls = 0

    def key(result):
        nonlocal calls
        calls += 1
        return result.metrics["score"]

    lst = []
    for i in range(n):
        _insert_into_sorted_list(
            lst,
            _TrainingResult(
                checkpoint=Checkpoint(path=f"/fake/ckpt_{i}"),
                metrics={"score": float(i % 7)},
            ),
            key,
        )

    # Linear scan needs ~n^2 / 2 (~500k) key calls; binary search ~n * log2(n).
    assert calls <= 2 * n * n.bit_length()


@pytest.mark.parametrize("order", ["min", "max"])
def test_mixed_scored_and_unscored_checkpoints(order, checkpoint_paths):
    """Ensure v1 register_checkpoint keeps the right top-K checkpoints when
    checkpoints omitting checkpoint_score_attribute are interleaved with scored ones."""
    manager = _CheckpointManager(
        checkpoint_config=CheckpointConfig(
            num_to_keep=2,
            checkpoint_score_attribute="score",
            checkpoint_score_order=order,
        )
    )

    # Interleave scored checkpoints with an unscored checkpoint that v1 appends.
    entries = [
        (checkpoint_paths[0], {"score": 3.0}),
        (checkpoint_paths[1], {"other": 1.0}),
        (checkpoint_paths[2], {"score": 1.0}),
        (checkpoint_paths[3], {"score": 2.0}),
    ]
    for path, metrics in entries:
        manager.register_checkpoint(
            _TrainingResult(
                checkpoint=Checkpoint.from_directory(path),
                metrics=metrics,
            )
        )

    # Checkpoints 0 (score=3.0) and 2 (score=1.0) are the best for max and min
    # respectively; with latest=checkpoint 3 (score=2.0), the top scored checkpoint
    # must never be evicted by the appended unscored checkpoint.
    kept_paths = {r.checkpoint.path for r in manager.best_checkpoint_results}
    if order == "max":
        assert checkpoint_paths[0] in kept_paths
        assert Path(checkpoint_paths[0]).exists()
    else:
        assert checkpoint_paths[2] in kept_paths
        assert Path(checkpoint_paths[2]).exists()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
