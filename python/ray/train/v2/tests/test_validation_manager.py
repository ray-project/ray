import time
import unittest.mock
from unittest.mock import create_autospec

import pytest

import ray
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
    validation_manager.ValidationManager, "poll_validations", autospec=True
)
def test_before_controller_shutdown(mock_poll_validations, monkeypatch):
    monkeypatch.setattr(validation_manager, "VALIDATION_TASK_POLL_INTERVAL_S", 0)
    vm = validation_manager.ValidationManager(
        checkpoint_manager=create_autospec(CheckpointManager, instance=True)
    )
    mock_poll_validations.side_effect = [True, False]
    vm.before_controller_shutdown()
    assert mock_poll_validations.call_count == 2


def test_checkpoint_validation_management(tmp_path):
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    vm = validation_manager.ValidationManager(checkpoint_manager=checkpoint_manager)
    training_results = create_dummy_training_results(
        num_results=4,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="checkpoint_validation_management_experiment",
        ),
    )

    # Register passing, failing, and timing out checkpoints
    vm.after_report(
        metrics=[training_results[0].metrics],
        checkpoint=training_results[0].checkpoint,
        validation_spec=_ValidationSpec(
            validate_fn=lambda checkpoint, config: {"score": 200},
            validate_config=None,
        ),
    )

    def failing_validate_fn(checkpoint, config):
        return "invalid_return_type"

    vm.after_report(
        metrics=[training_results[1].metrics],
        checkpoint=training_results[1].checkpoint,
        validation_spec=_ValidationSpec(
            validate_fn=failing_validate_fn,
            validate_config=None,
        ),
    )

    def infinite_waiting_validate_fn(checkpoint, config):
        while True:
            time.sleep(1)

    vm.after_report(
        metrics=[training_results[2].metrics],
        checkpoint=training_results[2].checkpoint,
        validation_spec=_ValidationSpec(
            validate_fn=infinite_waiting_validate_fn,
            validate_config=None,
        ),
    )

    vm.after_report(
        metrics=[training_results[3].metrics],
        checkpoint=training_results[3].checkpoint,
        validation_spec=_ValidationSpec(
            validate_fn=lambda checkpoint, config: config,
            validate_config={"score": 100},
        ),
    )

    assert not vm.failed_validations

    # Assert ValidationManager state after most tasks are done
    non_timeout_validations = [
        validation_task
        for checkpoint, validation_task in vm._pending_validations.items()
        if checkpoint != training_results[2].checkpoint
    ]
    ray.wait(
        non_timeout_validations,
        # Pick high timeout to guarantee completion but ray.wait should finish much earlier
        timeout=100,
        num_returns=len(non_timeout_validations),
    )
    assert vm.poll_validations()
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {
            training_results[0].checkpoint: {"score": 200},
            training_results[
                1
            ].checkpoint: {},  # failed validation results in empty dict
            training_results[3].checkpoint: {"score": 100},
        }
    )
    assert len(vm.failed_validations) == 1
    assert vm.failed_validations[0].checkpoint == training_results[1].checkpoint
    assert isinstance(
        vm.failed_validations[0].validation_failed_error.validation_failure, ValueError
    )

    # Assert ValidationManager state after all tasks are done
    ray.cancel(vm._pending_validations[training_results[2].checkpoint])
    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(vm._pending_validations[training_results[2].checkpoint])
    vm.poll_validations()
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_with(
        {
            training_results[2].checkpoint: {},
        }
    )
    assert len(vm.failed_validations) == 2
    assert vm.failed_validations[0].checkpoint == training_results[1].checkpoint
    assert isinstance(
        vm.failed_validations[0].validation_failed_error.validation_failure, ValueError
    )
    assert vm.failed_validations[1].checkpoint == training_results[2].checkpoint
    assert isinstance(
        vm.failed_validations[1].validation_failed_error.validation_failure,
        ray.exceptions.TaskCancelledError,
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
