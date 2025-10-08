import time
import unittest.mock
from unittest.mock import create_autospec

import pytest

import ray
from ray.train._checkpoint import Checkpoint
from ray.train.v2._internal.execution.checkpoint import validation_manager
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.training_report import (
    _TrainingReport,
    _ValidationSpec,
)
from ray.train.v2.tests.util import create_dummy_training_results


@pytest.fixture(autouse=True, scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@unittest.mock.patch.object(ray, "wait", autospec=True)
def test_before_controller_shutdown(mock_wait, monkeypatch):
    monkeypatch.setattr(validation_manager, "VALIDATION_TASK_POLL_INTERVAL_S", 0)

    # Create ValidationManager with mocked objects
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    checkpoint1 = create_autospec(Checkpoint, instance=True)
    checkpoint2 = create_autospec(Checkpoint, instance=True)
    checkpoint3 = create_autospec(Checkpoint, instance=True)
    task1 = create_autospec(ray.ObjectRef, instance=True)
    task2 = create_autospec(ray.ObjectRef, instance=True)
    task3 = create_autospec(ray.ObjectRef, instance=True)
    vm = validation_manager.ValidationManager(checkpoint_manager=checkpoint_manager)
    vm._pending_validations = {
        task1: checkpoint1,
        task2: checkpoint2,
        task3: checkpoint3,
    }
    mock_wait.side_effect = [([], [task1, task2, task3]), ([task1, task2, task3], [])]
    monkeypatch.setattr(ray, "get", lambda x: {"score": 1})

    # Call before_controller_shutdown
    vm.before_controller_shutdown()
    assert mock_wait.call_count == 2
    # modoru: interesting test 2 levels
    assert checkpoint_manager.update_checkpoints_with_metrics.mock_calls == [
        unittest.mock.call({checkpoint1: {"score": 1}}),
        unittest.mock.call({checkpoint2: {"score": 1}, checkpoint3: {"score": 1}}),
    ]


def test_checkpoint_validation_management_reordering(tmp_path):
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    vm = validation_manager.ValidationManager(checkpoint_manager=checkpoint_manager)
    (
        low_initial_high_final_training_result,
        high_initial_low_final_training_result,
    ) = create_dummy_training_results(
        num_results=2,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="checkpoint_validation_management_reordering_experiment",
        ),
    )

    # Start validation tasks and wait for them to complete
    vm.after_report(
        training_report=_TrainingReport(
            metrics=low_initial_high_final_training_result.metrics,
            checkpoint=low_initial_high_final_training_result.checkpoint,
            validation_spec=_ValidationSpec(
                validate_fn=lambda checkpoint, config: {"score": 200},
                validate_config={},
            ),
        ),
        metrics={},
    )
    vm.after_report(
        training_report=_TrainingReport(
            metrics=high_initial_low_final_training_result.metrics,
            checkpoint=high_initial_low_final_training_result.checkpoint,
            validation_spec=_ValidationSpec(
                validate_fn=lambda checkpoint, config: config,
                validate_config={"score": 100},
            ),
        ),
        metrics={},
    )
    ray.wait(
        list(vm._pending_validations.keys()),
        num_returns=2,
        # Pick high timeout to guarantee completion but ray.wait should finish much earlier
        timeout=100,
    )

    # Assert ValidationManager state after each poll
    assert vm._poll_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {low_initial_high_final_training_result.checkpoint: {"score": 200}}
    )
    assert vm._poll_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_with(
        {high_initial_low_final_training_result.checkpoint: {"score": 100}}
    )


def test_checkpoint_validation_management_failure(tmp_path):
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    vm = validation_manager.ValidationManager(checkpoint_manager=checkpoint_manager)
    failing_training_result = create_dummy_training_results(
        num_results=1,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="checkpoint_validation_management_failure_experiment",
        ),
    )[0]

    def failing_validate_fn(checkpoint, config):
        return "invalid_return_type"

    vm.after_report(
        training_report=_TrainingReport(
            metrics=failing_training_result.metrics,
            checkpoint=failing_training_result.checkpoint,
            validation_spec=_ValidationSpec(
                validate_fn=failing_validate_fn,
                validate_config={},
            ),
        ),
        metrics={},
    )
    ray.wait(
        list(vm._pending_validations.keys()),
        num_returns=1,
        timeout=100,
    )
    assert vm._poll_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {failing_training_result.checkpoint: {}}
    )


def test_checkpoint_validation_management_slow_validate_fn(tmp_path):
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    vm = validation_manager.ValidationManager(checkpoint_manager=checkpoint_manager)
    timing_out_training_result = create_dummy_training_results(
        num_results=1,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="checkpoint_validation_management_failure_experiment",
        ),
    )[0]

    def infinite_waiting_validate_fn(checkpoint, config):
        while True:
            time.sleep(1)

    vm.after_report(
        training_report=_TrainingReport(
            metrics=timing_out_training_result.metrics,
            checkpoint=timing_out_training_result.checkpoint,
            validation_spec=_ValidationSpec(
                validate_fn=infinite_waiting_validate_fn,
                validate_config={},
            ),
        ),
        metrics={},
    )
    assert vm._poll_validations() == 1

    # Finish the task by cancelling it
    timing_out_task = next(iter(vm._pending_validations))
    ray.cancel(timing_out_task)
    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(timing_out_task)

    # Verify that poll processes finished task
    assert vm._poll_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {
            timing_out_training_result.checkpoint: {},
        }
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
