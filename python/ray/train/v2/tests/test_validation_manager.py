import asyncio
import time
import unittest.mock
from unittest.mock import create_autospec

import pytest

import ray
from ray.train._checkpoint import Checkpoint
from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.execution.checkpoint import validation_manager
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.training_report import (
    _TrainingReport,
)
from ray.train.v2._internal.execution.worker_group.worker import Worker
from ray.train.v2.api.reported_checkpoint import ReportedCheckpointStatus
from ray.train.v2.api.validation_config import ValidationConfig, ValidationTaskConfig
from ray.train.v2.tests.util import create_dummy_training_reports


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
        task1: (checkpoint1, 0.0, None),
        task2: (checkpoint2, 0.0, None),
        task3: (checkpoint3, 0.0, None),
    }
    mock_wait.side_effect = [([], [task1, task2, task3]), ([task1, task2, task3], [])]
    monkeypatch.setattr(ray, "get", lambda x: {"score": 1})

    # Call before_controller_shutdown
    asyncio.run(vm.before_controller_shutdown())
    assert mock_wait.call_count == 2
    assert checkpoint_manager.update_checkpoints_with_metrics.mock_calls == [
        unittest.mock.call(
            {checkpoint1: {"score": 1}},
            {checkpoint1: ReportedCheckpointStatus.VALIDATED},
        ),
        unittest.mock.call(
            {checkpoint2: {"score": 1}, checkpoint3: {"score": 1}},
            {
                checkpoint2: ReportedCheckpointStatus.VALIDATED,
                checkpoint3: ReportedCheckpointStatus.VALIDATED,
            },
        ),
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
    ) = create_dummy_training_reports(
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
        {low_initial_high_final_training_result.checkpoint: {"score": 200}},
        {
            low_initial_high_final_training_result.checkpoint: ReportedCheckpointStatus.VALIDATED
        },
    )
    ray.wait(
        list(vm._pending_validations.keys()),
        num_returns=1,
    )
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_with(
        {high_initial_low_final_training_result.checkpoint: {"score": 100}},
        {
            high_initial_low_final_training_result.checkpoint: ReportedCheckpointStatus.VALIDATED
        },
    )


def test_checkpoint_validation_management_failure(tmp_path):
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)

    def failing_validation_fn(checkpoint):
        return "invalid_return_type"

    vm = validation_manager.ValidationManager(
        checkpoint_manager=checkpoint_manager,
        validation_config=ValidationConfig(fn=failing_validation_fn),
    )
    failing_training_result = create_dummy_training_reports(
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
        {failing_training_result.checkpoint: {}},
        {
            failing_training_result.checkpoint: ReportedCheckpointStatus.VALIDATION_FAILED
        },
    )


def test_checkpoint_validation_management_success_after_retry(tmp_path):
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
            ray_remote_kwargs={"max_retries": 1, "retry_exceptions": [ValueError]},
        ),
    )
    training_result = create_dummy_training_reports(
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
            validation=True,
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
        {training_result.checkpoint: {"score": 100}},
        {training_result.checkpoint: ReportedCheckpointStatus.VALIDATED},
    )


def test_checkpoint_validation_management_slow_validation_fn(tmp_path, monkeypatch):
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)

    def infinite_waiting_validation_fn(checkpoint):
        while True:
            time.sleep(1)

    vm = validation_manager.ValidationManager(
        checkpoint_manager=checkpoint_manager,
        validation_config=ValidationConfig(
            fn=infinite_waiting_validation_fn,
            task_config=ValidationTaskConfig(timeout_s=1),
        ),
    )
    timing_out_training_result = create_dummy_training_reports(
        num_results=1,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="checkpoint_validation_management_slow_validation_fn_experiment",
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

    # Advance time past the timeout so the next poll cancels the task.
    monkeypatch.setattr(time, "monotonic", lambda: float("inf"))
    # This poll detects the timeout and calls ray.cancel internally.
    vm._poll_validations()
    # Wait for the cancellation to propagate.
    ray.wait(list(vm._pending_validations.keys()), num_returns=1, timeout=10)

    # This poll moves the cancelled task to finished and processes it as VALIDATION_TIMEOUT.
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {timing_out_training_result.checkpoint: {}},
        {
            timing_out_training_result.checkpoint: ReportedCheckpointStatus.VALIDATION_TIMEOUT
        },
    )


def test_checkpoint_validation_management_resume(tmp_path):
    training_reports = create_dummy_training_reports(
        num_results=3,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="checkpoint_validation_management_resume_experiment",
        ),
    )
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    checkpoint_manager.get_pending_training_results.return_value = {
        training_reports[0].checkpoint: (
            _TrainingResult(
                checkpoint=training_reports[0].checkpoint,
                metrics=training_reports[0].metrics,
            ),
            True,
        ),
        training_reports[1].checkpoint: (
            _TrainingResult(
                checkpoint=training_reports[1].checkpoint,
                metrics=training_reports[1].metrics,
            ),
            False,
        ),
        training_reports[2].checkpoint: (
            _TrainingResult(
                checkpoint=training_reports[2].checkpoint,
                metrics=training_reports[2].metrics,
            ),
            ValidationTaskConfig(fn_kwargs={"score": 2}),
        ),
    }

    def validation_fn(checkpoint, score):
        return {"score": score}

    vm = validation_manager.ValidationManager(
        checkpoint_manager=checkpoint_manager,
        validation_config=ValidationConfig(
            fn=validation_fn,
            task_config=ValidationTaskConfig(fn_kwargs={"score": 1}),
        ),
    )

    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 1
    ray.wait(
        list(vm._pending_validations.keys()),
        num_returns=1,
    )
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 1
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_once_with(
        {training_reports[0].checkpoint: {"score": 1}},
        {training_reports[0].checkpoint: ReportedCheckpointStatus.VALIDATED},
    )
    ray.wait(
        list(vm._pending_validations.keys()),
        num_returns=1,
    )
    assert vm._poll_validations() == 0
    assert vm._kick_off_validations() == 0
    checkpoint_manager.update_checkpoints_with_metrics.assert_called_with(
        {training_reports[2].checkpoint: {"score": 2}},
        {training_reports[2].checkpoint: ReportedCheckpointStatus.VALIDATED},
    )


def test_timeout_overrides_config_default(tmp_path):
    """Per-task timeout_s should override ValidationConfig.task_config.timeout_s."""
    checkpoint_manager = create_autospec(CheckpointManager, instance=True)
    vm = validation_manager.ValidationManager(
        checkpoint_manager=checkpoint_manager,
        validation_config=ValidationConfig(
            fn=lambda checkpoint: {},
            task_config=ValidationTaskConfig(timeout_s=100),
        ),
    )
    training_result = create_dummy_training_reports(
        num_results=1,
        storage_context=StorageContext(
            storage_path=tmp_path,
            experiment_dir_name="test_timeout_overrides_config_default",
        ),
    )[0]

    vm.after_report(
        training_report=_TrainingReport(
            metrics=training_result.metrics,
            checkpoint=training_result.checkpoint,
            validation=ValidationTaskConfig(timeout_s=5),
        ),
        metrics={},
    )
    vm._kick_off_validations()
    task = next(iter(vm._pending_validations))
    _, _, effective_timeout_s = vm._pending_validations[task]
    assert effective_timeout_s == 5


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
