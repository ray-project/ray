import pytest
from ray.util.ml_utils.checkpoint_manager import (
    _CheckpointManager,
    CheckpointStorage,
    CheckpointStrategy,
    _TrackedCheckpoint,
)


def test_unlimited_persistent_checkpoints():
    cpm = _CheckpointManager(checkpoint_strategy=CheckpointStrategy(num_to_keep=None))

    for i in range(10):
        cpm.register_checkpoint(
            _TrackedCheckpoint({"data": i}, storage_mode=CheckpointStorage.PERSISTENT)
        )

    assert len(cpm._top_persisted_checkpoints) == 10


def test_limited_persistent_checkpoints():
    cpm = _CheckpointManager(checkpoint_strategy=CheckpointStrategy(num_to_keep=2))

    for i in range(10):
        cpm.register_checkpoint(
            _TrackedCheckpoint({"data": i}, storage_mode=CheckpointStorage.PERSISTENT)
        )

    assert len(cpm._top_persisted_checkpoints) == 2


def test_no_persistent_checkpoints():
    cpm = _CheckpointManager(checkpoint_strategy=CheckpointStrategy(num_to_keep=0))

    for i in range(10):
        cpm.register_checkpoint(
            _TrackedCheckpoint({"data": i}, storage_mode=CheckpointStorage.PERSISTENT)
        )

    assert len(cpm._top_persisted_checkpoints) == 0


def test_dont_persist_memory_checkpoints():
    cpm = _CheckpointManager(checkpoint_strategy=CheckpointStrategy(num_to_keep=None))
    cpm._persist_memory_checkpoints = False

    for i in range(10):
        cpm.register_checkpoint(
            _TrackedCheckpoint({"data": i}, storage_mode=CheckpointStorage.MEMORY)
        )

    assert len(cpm._top_persisted_checkpoints) == 0


def test_persist_memory_checkpoints():
    cpm = _CheckpointManager(checkpoint_strategy=CheckpointStrategy(num_to_keep=None))
    cpm._persist_memory_checkpoints = True

    for i in range(10):
        cpm.register_checkpoint(
            _TrackedCheckpoint({"data": i}, storage_mode=CheckpointStorage.MEMORY)
        )

    assert len(cpm._top_persisted_checkpoints) == 10


def test_keep_best_checkpoints():
    cpm = _CheckpointManager(
        checkpoint_strategy=CheckpointStrategy(
            num_to_keep=2,
            checkpoint_score_attribute="metric",
            checkpoint_score_order="min",
        )
    )
    cpm._persist_memory_checkpoints = True

    for i in range(10):
        cpm.register_checkpoint(
            _TrackedCheckpoint(
                {"data": i},
                storage_mode=CheckpointStorage.MEMORY,
                metrics={"metric": i},
            )
        )

    # Sorted from worst (max) to best (min)
    assert [
        cp.tracked_checkpoint.metrics["metric"] for cp in cpm._top_persisted_checkpoints
    ] == [1, 0]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
