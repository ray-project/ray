import uuid
from pathlib import Path
from typing import List, Optional
from unittest.mock import create_autospec

import pytest

import ray
from ray.train import Checkpoint, CheckpointConfig
from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.exceptions import CheckpointManagerInitializationError
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.worker_group import Worker


@pytest.fixture(autouse=True, scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def _create_dummy_training_results(
    num_results: int,
    storage_context: StorageContext,
) -> List[_TrainingResult]:
    return [
        _TrainingResult(
            checkpoint=Checkpoint(
                path=Path(
                    storage_context.experiment_fs_path, f"checkpoint_{i}"
                ).as_posix(),
                filesystem=storage_context.storage_filesystem,
            ),
            metrics={"score": i},
        )
        for i in range(num_results)
    ]


def _checkpoint_managers_equal(cm1: CheckpointManager, cm2: CheckpointManager) -> bool:
    """
    Compare two checkpoint managers for equality.
    Ignore uuid differences of all the checkpoints recorded.
    """

    def _training_results_equal(
        tr1: Optional[_TrainingResult], tr2: Optional[_TrainingResult]
    ) -> bool:
        if not tr1 and not tr2:
            return True
        if not tr1 or not tr2:
            return False
        return (
            tr1.metrics == tr2.metrics
            and tr1.checkpoint.path == tr2.checkpoint.path
            and tr1.checkpoint.filesystem == tr2.checkpoint.filesystem
        )

    if cm1._checkpoint_config != cm2._checkpoint_config:
        return False
    if not _training_results_equal(
        cm1.latest_checkpoint_result, cm2.latest_checkpoint_result
    ):
        return False
    if not _training_results_equal(
        cm1.best_checkpoint_result, cm2.best_checkpoint_result
    ):
        return False
    if len(cm1.best_checkpoint_results) != len(cm2.best_checkpoint_results):
        return False
    for tr1, tr2 in zip(cm1.best_checkpoint_results, cm2.best_checkpoint_results):
        if not _training_results_equal(tr1, tr2):
            return False
    return True


@pytest.mark.parametrize(
    "checkpoint_config",
    [
        CheckpointConfig(),
        CheckpointConfig(
            num_to_keep=1,
            checkpoint_score_attribute="score",
            checkpoint_score_order="max",
        ),
    ],
)
def test_save_load_state_equivalence(
    monkeypatch, tmp_path, checkpoint_config: CheckpointConfig
):
    # Mock the delete function as we don't want report checkpoints to be deleted.
    monkeypatch.setattr(
        ray.train.v2._internal.execution.checkpoint.checkpoint_manager,
        "_delete_fs_path",
        lambda *args, **kwargs: None,
    )
    exp_name = f"checkpoint_manager_test-{uuid.uuid4().hex}"

    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name=exp_name,
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=checkpoint_config,
    )
    training_results = _create_dummy_training_results(
        num_results=3, storage_context=storage_context
    )

    # Register the training results into checkpoint manager
    for tr in training_results:
        checkpoint_manager.register_checkpoint(tr)
        loaded_checkpoint_manager = CheckpointManager(
            storage_context=storage_context,
            checkpoint_config=checkpoint_config,
        )
        assert _checkpoint_managers_equal(checkpoint_manager, loaded_checkpoint_manager)


@pytest.mark.parametrize(
    "json_state",
    [
        '{"dummy": "1", "dummy_dict": {"key": "value"}}',
        '{"dummy": "1", "dummy_dict": {"key": "value"',
    ],
)
def test_load_state_error(tmp_path, json_state):

    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="load_state_error_experiment",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )
    with pytest.raises(
        CheckpointManagerInitializationError,
    ):
        checkpoint_manager._load_state(json_state)


def test_before_init_train_context(tmp_path):
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="my_experiment_name",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )
    workers = [create_autospec(Worker, instance=True) for _ in range(4)]

    # Assert without a checkpoint.
    assert checkpoint_manager.before_init_train_context(workers) == {
        "checkpoint": [None] * 4
    }

    # Assert with a checkpoint
    latest_checkpoint_result = _create_dummy_training_results(1, storage_context)[0]
    checkpoint_manager._latest_checkpoint_result = latest_checkpoint_result
    assert checkpoint_manager.before_init_train_context(workers) == {
        "checkpoint": [latest_checkpoint_result.checkpoint] * 4
    }


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
