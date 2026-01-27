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
)
from ray.train.v2._internal.execution.worker_group.worker import Worker
from ray.train.v2.api.validation_config import ValidationConfig, ValidationTaskConfig
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
    vm = validation_manager.ValidationManager(
        checkpoint_manager=checkpoint_manager,
        validation_config=ValidationConfig(fn=lambda x: None),
    )
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
    assert checkpoint_manager.update_checkpoints_with_metrics.mock_calls == [
        unittest.mock.call({checkpoint1: {"score": 1}}),
        unittest.mock.call({checkpoint2: {"score": 1}, checkpoint3: {"score": 1}}),
    ]


def test_before_init_train_context():
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    vm = validation_manager.ValidationManager(
        checkpoint_manager=checkpoint_manager,
        validation_config=ValidationConfig(fn=lambda x: None),
    )
    workers = [create_autospec(Worker, instance=True) for _ in range(4)]
    assert vm.before_init_train_context(workers) == {
        "has_validation_fn": [True] * 4,
    }


def test_checkpoint_validation_management_reordering(tmp_path):
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)

    def validation_fn(checkpoint, score):
        return {"score": score}

    vm = validation_manager.ValidationManager(
        checkpoint_manager=checkpoint_manager,
        validation_config=ValidationConfig(
            fn=validation_fn,
            task_config=ValidationTaskConfig(fn_kwargs={"score": 100}),
        ),
    )
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

    # Enqueue validation tasks
    vm.after_report(
        training_report=_TrainingReport(
            metrics=low_initial_high_final_training_result.metrics,
            checkpoint=low_initial_high_final_training_result.checkpoint,
            validation=ValidationTaskConfig(fn_kwargs={"score": 200}),
        ),
        metrics={},
    )
    vm.after_report(
        training_report=_TrainingReport(
            metrics=high_initial_low_final_training_result.metrics,
            checkpoint=high_initial_low_final_training_result.checkpoint,
            validation=True,
        ),
        metrics={},
    )

    # Assert ValidationManager state after each poll
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 1
    ray.wait(
        list(vm._pending_validations.keys()),
        num_returns=1,
    )
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 1
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {low_initial_high_final_training_result.checkpoint: {"score": 200}}
    )
    ray.wait(
        list(vm._pending_validations.keys()),
        num_returns=1,
    )
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_with(
        {high_initial_low_final_training_result.checkpoint: {"score": 100}}
    )


def test_checkpoint_validation_management_failure(tmp_path):
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)

    def failing_validation_fn(checkpoint):
        return "invalid_return_type"

    vm = validation_manager.ValidationManager(
        checkpoint_manager=checkpoint_manager,
        validation_config=ValidationConfig(fn=failing_validation_fn),
    )
    failing_training_result = create_dummy_training_results(
        num_results=1,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="checkpoint_validation_management_failure_experiment",
        ),
    )[0]

    vm.after_report(
        training_report=_TrainingReport(
            metrics=failing_training_result.metrics,
            checkpoint=failing_training_result.checkpoint,
            validation=ValidationTaskConfig(),
        ),
        metrics={},
    )
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 1
    ray.wait(
        list(vm._pending_validations.keys()),
        num_returns=1,
    )
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {failing_training_result.checkpoint: {}}
    )


@pytest.mark.parametrize(
    "base_task_config,override_task_config",
    [
        (
            None,
            ValidationTaskConfig(
                ray_remote_kwargs={"max_retries": 1, "retry_exceptions": [ValueError]}
            ),
        ),
        (
            ValidationTaskConfig(
                ray_remote_kwargs={"max_retries": 1, "retry_exceptions": [ValueError]}
            ),
            True,
        ),
        (
            ValidationTaskConfig(
                ray_remote_kwargs={"max_retries": 0, "retry_exceptions": [ValueError]}
            ),
            ValidationTaskConfig(ray_remote_kwargs={"max_retries": 1}),
        ),
    ],
)
def test_checkpoint_validation_management_success_after_retry(
    tmp_path, base_task_config, override_task_config
):
    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    counter = Counter.remote()

    def one_time_failing_validation_fn(checkpoint):
        print("one_time_failing_validation_fn called")
        if ray.get(counter.increment.remote()) < 2:
            raise ValueError("Fail on first attempt")
        return {"score": 100}

    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    vm = validation_manager.ValidationManager(
        checkpoint_manager=checkpoint_manager,
        validation_config=ValidationConfig(
            fn=one_time_failing_validation_fn,
            task_config=base_task_config,
        ),
    )
    training_result = create_dummy_training_results(
        num_results=1,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="checkpoint_validation_management_success_after_retry_experiment",
        ),
    )[0]

    vm.after_report(
        training_report=_TrainingReport(
            metrics=training_result.metrics,
            checkpoint=training_result.checkpoint,
            validation=override_task_config,
        ),
        metrics={},
    )
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 1
    ray.wait(
        list(vm._pending_validations.keys()),
        num_returns=1,
        timeout=100,
    )
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {training_result.checkpoint: {"score": 100}}
    )


def test_checkpoint_validation_management_slow_validation_fn(tmp_path):
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)

    def infinite_waiting_validation_fn(checkpoint):
        while True:
            time.sleep(1)

    vm = validation_manager.ValidationManager(
        checkpoint_manager=checkpoint_manager,
        validation_config=ValidationConfig(fn=infinite_waiting_validation_fn),
    )
    timing_out_training_result = create_dummy_training_results(
        num_results=1,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="checkpoint_validation_management_failure_experiment",
        ),
    )[0]

    vm.after_report(
        training_report=_TrainingReport(
            metrics=timing_out_training_result.metrics,
            checkpoint=timing_out_training_result.checkpoint,
            validation=True,
        ),
        metrics={},
    )
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 1

    # Finish the task by cancelling it
    timing_out_task = next(iter(vm._pending_validations))
    ray.cancel(timing_out_task)
    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(timing_out_task)

    # Verify that poll processes finished task
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {
            timing_out_training_result.checkpoint: {},
        }
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
