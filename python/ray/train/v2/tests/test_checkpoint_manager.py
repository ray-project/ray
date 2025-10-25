import uuid
from typing import Optional
from unittest.mock import create_autospec

import pytest

import ray
from ray.train import CheckpointConfig
from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.exceptions import CheckpointManagerInitializationError
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.worker_group import Worker
from ray.train.v2.tests.util import create_dummy_training_results


@pytest.fixture(autouse=True, scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


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
    training_results = create_dummy_training_results(
        num_results=3, storage_context=storage_context
    )

    # Register the training results into checkpoint manager
    for i, tr in enumerate(training_results):
        checkpoint_manager.register_checkpoint(tr, False)
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
    latest_checkpoint_result = create_dummy_training_results(1, storage_context)[0]
    checkpoint_manager.register_checkpoint(latest_checkpoint_result, False)
    assert checkpoint_manager.before_init_train_context(workers) == {
        "checkpoint": [latest_checkpoint_result.checkpoint] * 4,
    }


async def test_pending_checkpoint_management(tmp_path):
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="pending_checkpoint_management_experiment",
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
    (
        low_initial_high_final_training_result,
        high_initial_low_final_training_result,
        final_training_result,
    ) = create_dummy_training_results(num_results=3, storage_context=storage_context)
    scoreless_training_result = create_dummy_training_results(
        num_results=1, storage_context=storage_context, include_metrics=False
    )[0]

    # Register pending/final/unknown checkpoints and verify their storage
    checkpoint_manager.register_checkpoint(low_initial_high_final_training_result, True)
    checkpoint_manager.register_checkpoint(final_training_result, False)
    checkpoint_manager.register_checkpoint(scoreless_training_result, False)
    checkpoint_manager.register_checkpoint(high_initial_low_final_training_result, True)
    assert checkpoint_manager._checkpoint_results == [
        low_initial_high_final_training_result,  # keep pending
        high_initial_low_final_training_result,  # keep pending/latest
        final_training_result,  # keep highest final score so far
    ]

    # Assert checkpoint state after all tasks are done
    checkpoint_manager.update_checkpoints_with_metrics(
        {
            low_initial_high_final_training_result.checkpoint: {"score": 200},
            high_initial_low_final_training_result.checkpoint: {"score": 100},
        }
    )
    assert checkpoint_manager._checkpoint_results == [
        high_initial_low_final_training_result,  # keep latest checkpoint
        low_initial_high_final_training_result,  # keep highest score checkpoint
    ]


async def test_pending_checkpoint_management_break_ties_by_report_index(tmp_path):
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="pending_checkpoint_management_break_ties_by_report_index_experiment",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )
    training_results = create_dummy_training_results(
        num_results=2, storage_context=storage_context, include_metrics=False
    )
    checkpoint_manager.register_checkpoint(training_results[0], True)
    checkpoint_manager.register_checkpoint(training_results[1], True)
    assert checkpoint_manager._checkpoint_results == [
        training_results[0],
        training_results[1],
    ]
    checkpoint_manager.update_checkpoints_with_metrics(
        {
            training_results[1].checkpoint: {},
        }
    )
    assert checkpoint_manager._checkpoint_results == [
        training_results[0],
        training_results[1],
    ]
    checkpoint_manager.update_checkpoints_with_metrics(
        {
            training_results[0].checkpoint: {},
        }
    )
    assert checkpoint_manager._checkpoint_results == [
        training_results[0],
        training_results[1],
    ]


async def test_pending_checkpoint_management_finalized_checkpoint(tmp_path):
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="pending_checkpoint_management_experiment",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(
            checkpoint_score_attribute="score",
            checkpoint_score_order="max",
        ),
    )
    training_results = create_dummy_training_results(
        num_results=2, storage_context=storage_context
    )
    checkpoint_manager.register_checkpoint(training_results[0], False)
    checkpoint_manager.register_checkpoint(training_results[1], False)
    assert checkpoint_manager._checkpoint_results == [
        training_results[0],
        training_results[1],
    ]
    checkpoint_manager.update_checkpoints_with_metrics(
        {
            training_results[0].checkpoint: {"score": 100},
        }
    )
    assert checkpoint_manager._checkpoint_results == [
        training_results[0],
        training_results[1],
    ]


def test_update_checkpoints_with_metrics_not_in_checkpoint_results(tmp_path):
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="update_checkpoints_with_metrics_error_experiment",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )
    training_results = create_dummy_training_results(
        num_results=1, storage_context=storage_context
    )
    checkpoint_manager._pending_training_results[
        training_results[0].checkpoint
    ] = training_results[0]
    with pytest.raises(ValueError):
        checkpoint_manager.update_checkpoints_with_metrics(
            {training_results[0].checkpoint: {"score": 100}}
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
