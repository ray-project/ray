import asyncio
import logging
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import ray
from ray.train._checkpoint import Checkpoint
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    ReportCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.training_report import (
    _TrainingReport,
)
from ray.train.v2.api.reported_checkpoint import ReportedCheckpointStatus
from ray.train.v2.api.validation_config import ValidationConfig, ValidationTaskConfig

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.controller import TrainControllerState
    from ray.train.v2._internal.execution.worker_group.worker import Worker

logger = logging.getLogger(__name__)


VALIDATION_TASK_POLL_INTERVAL_S = 1
MAX_IN_FLIGHT_VALIDATIONS = 1


@dataclass
class _PendingValidation:
    checkpoint: Checkpoint
    start_time: float
    # None when no timeout applies.
    timeout_s: Optional[float]


@ray.remote
def run_validation_fn(
    validation_config: ValidationConfig,
    validation_task_config: Union[bool, ValidationTaskConfig],
    checkpoint: Checkpoint,
) -> Dict:
    """Run the user-defined validation function.

    Merges fn_kwargs from validation_config.task_config (defaults) with
    fn_kwargs from validation_task_config (per-report overrides).
    """
    # Merge kwargs: defaults from validation_config, overrides from validation_task_config
    if validation_task_config is True:
        merged_kwargs = validation_config.task_config.fn_kwargs
    else:
        merged_kwargs = {
            **validation_config.task_config.fn_kwargs,
            **validation_task_config.fn_kwargs,
        }
    metrics_dict = validation_config.fn(
        checkpoint,
        **merged_kwargs,
    )
    if not isinstance(metrics_dict, dict):
        raise ValueError(
            "The validation function must return a dictionary of metrics. "
            f"Got {type(metrics_dict)} instead."
        )
    return metrics_dict


class ValidationManager(ControllerCallback, ReportCallback, WorkerGroupCallback):
    def __init__(
        self,
        checkpoint_manager: CheckpointManager,
        validation_config: ValidationConfig,
    ):
        self._checkpoint_manager = checkpoint_manager
        self._validation_config = validation_config

        # _TrainingReports that we will validate
        self._training_report_queue = deque()

        # Map from in flight validation task to its pending-validation record
        # (checkpoint + start_time + resolved timeout).
        self._pending_validations: "OrderedDict[ray.ObjectRef, _PendingValidation]" = (
            OrderedDict()
        )

        # Tasks that this manager proactively cancelled due to timeout. Used to
        # distinguish timeout-cancels from controller-abort-cancels (both raise
        # TaskCancelledError on ray.get).
        self._timed_out_tasks: set = set()

        # Map from validation task to checkpoint
        # Finished validations that have yet to be processed
        self._finished_validations = OrderedDict()

        self._requeue_incomplete_validations()

    def _requeue_incomplete_validations(self):
        """Add _TrainingReports for incomplete validations to the queue."""
        for checkpoint, (
            training_result,
            validation,
        ) in self._checkpoint_manager.get_pending_training_results().items():
            if validation:
                self._training_report_queue.append(
                    _TrainingReport(
                        metrics=training_result.metrics,
                        checkpoint=checkpoint,
                        validation=validation,
                    )
                )

    def after_report(
        self,
        training_report: _TrainingReport,
        metrics: List[Dict[str, Any]],
    ):
        if training_report.validation:
            self._training_report_queue.append(training_report)

    def _cancel_timed_out_validations(self):
        """Cancel any in-flight validation that has exceeded its timeout_s.

        Cancelled tasks are moved directly from `_pending_validations` to
        `_finished_validations` so the MAX_IN_FLIGHT slot is freed immediately
        and the task flows through the normal finished-processing pipeline
        without waiting for `ray.wait` to echo the cancellation.
        """
        now = time.monotonic()
        for task, pending in list(self._pending_validations.items()):
            if pending.timeout_s is None:
                continue
            if now - pending.start_time < pending.timeout_s:
                continue
            self._pending_validations.pop(task)
            logger.info(
                f"Validation for checkpoint {pending.checkpoint} exceeded timeout_s="
                f"{pending.timeout_s}s. Cancelling."
            )
            self._timed_out_tasks.add(task)
            ray.cancel(task)
            self._finished_validations[task] = pending.checkpoint

    def _poll_validations(self) -> int:
        """Poll/process validations, update checkpoint manager, return num pending validations."""
        self._cancel_timed_out_validations()

        # Move pending validations to finished validations
        validation_tasks = list(self._pending_validations.keys())
        done, _ = ray.wait(
            validation_tasks, timeout=0, num_returns=len(validation_tasks)
        )
        done_checkpoints = []
        for task in done:
            pending = self._pending_validations.pop(task)
            done_checkpoints.append(pending.checkpoint)
            self._finished_validations[task] = pending.checkpoint
        if done_checkpoints:
            logger.info(
                f"Finished async validation task(s) for checkpoint(s): {done_checkpoints}.\n"
                f"Running validations for checkpoint(s): {[p.checkpoint for p in self._pending_validations.values()]}.\n"
                f"Staged validations for checkpoint(s): {[tr.checkpoint for tr in self._training_report_queue]}."
            )

        # Process next finished validation
        # TODO: consider configuration to process multiple at a time
        if self._finished_validations:
            task, checkpoint = next(iter(self._finished_validations.items()))
            self._finished_validations.pop(task)
            update = self._process_finished_validation(task, checkpoint)
            if update is not None:
                self._checkpoint_manager.update_checkpoints_with_validation_result(
                    {checkpoint: update}
                )
        return len(self._pending_validations)

    def _resolve_timeout_s(
        self, validation: Union[bool, ValidationTaskConfig]
    ) -> Optional[float]:
        """Resolve the effective timeout for a validation task.

        Per-task ``timeout_s=None`` falls back to the ValidationConfig default.
        """
        default_timeout_s = self._validation_config.task_config.timeout_s
        if isinstance(validation, ValidationTaskConfig):
            task_timeout_s = validation.timeout_s
        else:
            task_timeout_s = None
        return default_timeout_s if task_timeout_s is None else task_timeout_s

    def _kick_off_validations(self) -> int:
        """Kick off validations and return the number of pending validations."""
        # TODO: figure out where to place run_validation_fn task:
        # TODO: provide option to run this on gpu?
        num_validations_to_start = max(
            MAX_IN_FLIGHT_VALIDATIONS - len(self._pending_validations), 0
        )
        num_validations_to_start = min(
            num_validations_to_start, len(self._training_report_queue)
        )
        for _ in range(num_validations_to_start):
            training_report = self._training_report_queue.popleft()
            run_validation_fn_with_options = run_validation_fn.options(
                **self._validation_config.ray_remote_kwargs,
            )
            validate_task = run_validation_fn_with_options.remote(
                self._validation_config,
                training_report.validation,
                training_report.checkpoint,
            )
            self._pending_validations[validate_task] = _PendingValidation(
                checkpoint=training_report.checkpoint,
                start_time=time.monotonic(),
                timeout_s=self._resolve_timeout_s(training_report.validation),
            )
            logger.info(
                f"Launched async validation task for checkpoint {training_report.checkpoint}"
            )
        return len(self._pending_validations)

    def _process_finished_validation(
        self, task: ray.ObjectRef, checkpoint: Checkpoint
    ) -> Optional[Tuple[Dict[str, Any], ReportedCheckpointStatus]]:
        """Process finished validation. Returns (metrics, status) or None.

        Returns None when the task was cancelled by a controller abort (not a
        timeout), leaving it pending so it re-queues on resumption.
        """
        was_timed_out = task in self._timed_out_tasks
        self._timed_out_tasks.discard(task)
        try:
            metrics = ray.get(task)
            return metrics, ReportedCheckpointStatus.VALIDATED
        except ray.exceptions.TaskCancelledError:
            if was_timed_out:
                logger.info(
                    f"Validation for checkpoint {checkpoint} was cancelled due to "
                    "timeout."
                )
                return {}, ReportedCheckpointStatus.VALIDATION_TIMEOUT
            logger.info(
                f"Validation was cancelled for checkpoint {checkpoint}, likely because the train run was aborted. "
                "It will be retried in the next train run with the same storage path if there is one."
            )
            return None
        except ray.exceptions.RayTaskError:
            logger.exception(f"Validation failed for checkpoint {checkpoint}")
            return {}, ReportedCheckpointStatus.VALIDATION_FAILED

    async def before_controller_shutdown(self):
        while self._poll_validations() != 0 or self._kick_off_validations() != 0:
            await asyncio.sleep(VALIDATION_TASK_POLL_INTERVAL_S)
        checkpoint_updates: Dict[
            Checkpoint, Tuple[Dict[str, Any], ReportedCheckpointStatus]
        ] = {}
        tasks = list(self._finished_validations.keys())
        for task in tasks:
            checkpoint = self._finished_validations[task]
            self._finished_validations.pop(task)
            update = self._process_finished_validation(task, checkpoint)
            if update is not None:
                checkpoint_updates[checkpoint] = update
        self._checkpoint_manager.update_checkpoints_with_validation_result(
            checkpoint_updates
        )

    def before_controller_abort(self):
        for task in self._pending_validations.keys():
            ray.cancel(task)

    def after_controller_state_update(
        self,
        previous_state: "TrainControllerState",
        current_state: "TrainControllerState",
    ):
        # TODO: figure out if there's a better place to poll validations
        if current_state.is_terminal():
            return
        self._poll_validations()
        self._kick_off_validations()

    def before_init_train_context(
        self, workers: List["Worker"]
    ) -> Dict[str, List[bool]]:
        return {
            "has_validation_fn": [True] * len(workers),
        }
