import os
import time
import uuid
from pathlib import Path
from typing import List, Optional
from unittest.mock import create_autospec

import pytest

import ray
from ray.train import Checkpoint, CheckpointConfig
from ray.train._internal.session import _TrainingResult, _ValidationSpec
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
    num_results: int, storage_context: StorageContext, create_checkpoint: bool = False
) -> List[_TrainingResult]:
    training_results = []
    for i in range(num_results):
        checkpoint_path = os.path.join(
            storage_context.experiment_fs_path, f"checkpoint_{i}"
        )
        os.makedirs(checkpoint_path, exist_ok=True)
        training_results.append(
            _TrainingResult(
                checkpoint=Checkpoint(
                    path=Path(checkpoint_path).as_posix(),
                    filesystem=storage_context.storage_filesystem,
                ),
                metrics={"score": i},
            )
        )
    return training_results


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
async def test_save_load_state_equivalence(
    monkeypatch, tmp_path, checkpoint_config: CheckpointConfig
):
    # Use async here because register_checkpoint creates an async task

    # Mock the delete function as we don't want report checkpoints to be deleted.
    monkeypatch.setattr(
        ray.train.v2._internal.execution.checkpoint.checkpoint_manager,
        "delete_fs_path",
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
    for i, tr in enumerate(training_results):
        checkpoint_manager.register_checkpoint(tr, None)
        assert checkpoint_manager._current_report_index == i + 1
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


async def test_before_init_train_context(tmp_path):

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
        "checkpoint": [None] * 4,
    }

    # Assert with a checkpoint
    latest_checkpoint_result = _create_dummy_training_results(1, storage_context)[0]
    checkpoint_manager.register_checkpoint(latest_checkpoint_result, None)
    assert checkpoint_manager.before_init_train_context(workers) == {
        "checkpoint": [latest_checkpoint_result.checkpoint] * 4,
    }


async def test_checkpoint_validation_management(tmp_path):
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="checkpoint_validation_management_experiment",
    )
    checkpoint_config = CheckpointConfig(
        num_to_keep=1,
        checkpoint_score_attribute="score",
        checkpoint_score_order="max",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=checkpoint_config,
    )
    training_results = _create_dummy_training_results(
        num_results=4, storage_context=storage_context, create_checkpoint=True
    )

    # Register passing, failing, and timing out checkpoints
    checkpoint_manager.register_checkpoint(
        training_results[0],
        _ValidationSpec(
            validate_function=lambda checkpoint, config: {"score": 200},
            validate_config=None,
        ),
    )

    def failing_validate_function(checkpoint, config):
        raise ValueError("Validation failed")

    checkpoint_manager.register_checkpoint(
        training_results[1],
        _ValidationSpec(
            validate_function=failing_validate_function,
            validate_config=None,
        ),
    )

    def infinite_waiting_validate_function(checkpoint, config):
        while True:
            time.sleep(1)

    checkpoint_manager.register_checkpoint(
        training_results[2],
        _ValidationSpec(
            validate_function=infinite_waiting_validate_function,
            validate_config=None,
        ),
    )
    checkpoint_manager.register_checkpoint(
        training_results[3],
        _ValidationSpec(
            validate_function=lambda checkpoint, config: config,
            validate_config={"score": 100},
        ),
    )
    assert checkpoint_manager.has_pending_validations()
    assert checkpoint_manager._checkpoint_results == [
        training_results[0],
        training_results[1],
        training_results[2],
        training_results[3],
    ]
    assert not checkpoint_manager.failed_validations

    # Assert checkpoint state after most tasks are done
    non_timeout_validations = []
    for (
        report_number,
        validation_task,
    ) in checkpoint_manager._pending_validations.items():
        if report_number != 2:
            non_timeout_validations.append(validation_task)
    ray.wait(
        non_timeout_validations,
        # Pick high timeout to guarantee completion but ray.wait should finish much earlier
        timeout=100,
        num_returns=len(non_timeout_validations),
    )
    checkpoint_manager.poll_validations()
    assert checkpoint_manager.has_pending_validations()
    assert checkpoint_manager._checkpoint_results == [
        training_results[2],  # keep because pending
        training_results[3],  # keep because latest
        training_results[0],  # keep because highest score
    ]
    assert len(checkpoint_manager.failed_validations) == 1

    # Assert checkpoint state after all tasks are done
    ray.cancel(checkpoint_manager._pending_validations[2])
    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(checkpoint_manager._pending_validations[2])
    checkpoint_manager.poll_validations()
    assert not checkpoint_manager.has_pending_validations()
    assert checkpoint_manager._checkpoint_results == [
        training_results[3],  # keep because latest
        training_results[0],  # keep because highest score
    ]
    assert len(checkpoint_manager.failed_validations) == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
