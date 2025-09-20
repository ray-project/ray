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
from ray.train.v2._internal.execution.training_report import _ValidationSpec
from ray.train.v2.tests.util import create_dummy_training_results


@pytest.fixture(autouse=True, scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@unittest.mock.patch.object(
    validation_manager.ValidationManager, "_poll_validations", autospec=True
)
def test_before_controller_shutdown(mock_poll_validations, monkeypatch):
    # Mock methods/constants
    monkeypatch.setattr(validation_manager, "VALIDATION_TASK_POLL_INTERVAL_S", 0)
    monkeypatch.setattr(ray, "get", lambda x: {"score": 200})
    mock_poll_validations.side_effect = [1, 0]

    # Create ValidationManager with mocked objects
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    checkpoint = create_autospec(Checkpoint, instance=True)
    task = create_autospec(ray.ObjectRef, instance=True)
    vm = validation_manager.ValidationManager(checkpoint_manager=checkpoint_manager)
    vm._finished_validations = {task: checkpoint}

    # Call before_controller_shutdown
    vm.before_controller_shutdown()
    assert mock_poll_validations.call_count == 2
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {checkpoint: {"score": 200}}
    )


def test_checkpoint_validation_management(tmp_path):
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    vm = validation_manager.ValidationManager(checkpoint_manager=checkpoint_manager)
    (
        low_initial_high_final_training_result,
        failing_training_result,
        timing_out_training_result,
        high_initial_low_final_training_result,
    ) = create_dummy_training_results(
        num_results=4,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="checkpoint_validation_management_experiment",
        ),
    )

    # Register passing, failing, and timing out checkpoints
    vm.after_report(
        metrics=[low_initial_high_final_training_result.metrics],
        checkpoint=low_initial_high_final_training_result.checkpoint,
        validation_spec=_ValidationSpec(
            validate_fn=lambda checkpoint, config: {"score": 200},
            validate_config=None,
        ),
    )

    def failing_validate_fn(checkpoint, config):
        return "invalid_return_type"

    vm.after_report(
        metrics=[failing_training_result.metrics],
        checkpoint=failing_training_result.checkpoint,
        validation_spec=_ValidationSpec(
            validate_fn=failing_validate_fn,
            validate_config=None,
        ),
    )

    def infinite_waiting_validate_fn(checkpoint, config):
        while True:
            time.sleep(1)

    vm.after_report(
        metrics=[timing_out_training_result.metrics],
        checkpoint=timing_out_training_result.checkpoint,
        validation_spec=_ValidationSpec(
            validate_fn=infinite_waiting_validate_fn,
            validate_config=None,
        ),
    )

    vm.after_report(
        metrics=[high_initial_low_final_training_result.metrics],
        checkpoint=high_initial_low_final_training_result.checkpoint,
        validation_spec=_ValidationSpec(
            validate_fn=lambda checkpoint, config: config,
            validate_config={"score": 100},
        ),
    )

    assert not vm.failed_validations

    # Assert ValidationManager state after most tasks are done
    non_timeout_validations = [
        validation_task
        for validation_task, checkpoint in vm._pending_validations.items()
        if checkpoint != timing_out_training_result.checkpoint
    ]
    ray.wait(
        non_timeout_validations,
        # Pick high timeout to guarantee completion but ray.wait should finish much earlier
        timeout=100,
        num_returns=len(non_timeout_validations),
    )
    assert vm._poll_validations() == 1
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {low_initial_high_final_training_result.checkpoint: {"score": 200}}
    )
    assert vm._poll_validations() == 1
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_with(
        {failing_training_result.checkpoint: {}}
    )
    assert vm._poll_validations() == 1
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_with(
        {high_initial_low_final_training_result.checkpoint: {"score": 100}}
    )
    assert len(vm.failed_validations) == 1
    assert vm.failed_validations[0].checkpoint == failing_training_result.checkpoint
    assert isinstance(
        vm.failed_validations[0].validation_failed_error.validation_failure, ValueError
    )

    # Assert ValidationManager state after all tasks are done
    timing_out_task = next(iter(vm._pending_validations))
    ray.cancel(timing_out_task)
    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(timing_out_task)
    assert vm._poll_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_with(
        {
            timing_out_training_result.checkpoint: {},
        }
    )
    assert len(vm.failed_validations) == 2
    assert vm.failed_validations[0].checkpoint == failing_training_result.checkpoint
    assert isinstance(
        vm.failed_validations[0].validation_failed_error.validation_failure, ValueError
    )
    assert vm.failed_validations[1].checkpoint == timing_out_training_result.checkpoint
    assert isinstance(
        vm.failed_validations[1].validation_failed_error.validation_failure,
        ray.exceptions.TaskCancelledError,
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
