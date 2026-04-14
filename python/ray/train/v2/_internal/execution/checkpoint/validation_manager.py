import asyncio
import logging
import time
from collections import OrderedDict, deque
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Union

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

        # Map from in flight validation task to (checkpoint, start_time, timeout_s)
        self._pending_validations = OrderedDict()

        # Map from validation task to checkpoint
        # Finished validations that have yet to be processed
        self._finished_validations = OrderedDict()

        # Map from validation task to checkpoint which have been timed out
        self._timed_out_validations = OrderedDict()

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

    def _poll_validations(self) -> int:
        """Poll/process validations, update checkpoint manager, return num pending validations."""
        # Check for timed-out tasks and cancel them
        now = time.monotonic()
        for task, (checkpoint, start_time, timeout_s) in list(
            self._pending_validations.items()
        ):
            if (
                timeout_s is not None
                and timeout_s != -1
                and (now - start_time) > timeout_s
            ):
                ray.cancel(task)
                self._timed_out_validations[task] = checkpoint
                logger.warning(f"Validation timed out for checkpoint {checkpoint}")

        # Move pending validations to finished validations
        validation_tasks = list(self._pending_validations.keys())
        done, _ = ray.wait(
            validation_tasks, timeout=0, num_returns=len(validation_tasks)
        )
        done_checkpoints = []
        for task in done:
            checkpoint, _, _ = self._pending_validations[task]
            done_checkpoints.append(checkpoint)
            self._finished_validations[task] = checkpoint
            self._pending_validations.pop(task)
        if done_checkpoints:
            logger.info(
                f"Finished async validation task(s) for checkpoint(s): {done_checkpoints}.\n"
                f"Running validations for checkpoint(s): {[v[0] for v in self._pending_validations.values()]}.\n"
                f"Staged validations for checkpoint(s): {[tr.checkpoint for tr in self._training_report_queue]}."
            )

        # Process next finished validation
        # TODO: consider configuration to process multiple at a time
        if self._finished_validations:
            task, checkpoint = next(iter(self._finished_validations.items()))
            self._finished_validations.pop(task)
            (
                checkpoint_to_metrics,
                checkpoint_to_status,
            ) = self._process_finished_validation(task, checkpoint)
            self._checkpoint_manager.update_checkpoints_with_metrics(
                checkpoint_to_metrics, checkpoint_to_status
            )
        return len(self._pending_validations)

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
            # TODO: handle timeouts - ray.remote() does not have them
            run_validation_fn_with_options = run_validation_fn.options(
                **self._validation_config.ray_remote_kwargs,
            )
            validate_task = run_validation_fn_with_options.remote(
                self._validation_config,
                training_report.validation,
                training_report.checkpoint,
            )

            if (
                isinstance(training_report.validation, ValidationTaskConfig)
                and training_report.validation.timeout_s is not None
            ):
                effective_timeout_s = training_report.validation.timeout_s
            else:
                effective_timeout_s = self._validation_config.task_config.timeout_s

            self._pending_validations[validate_task] = (
                training_report.checkpoint,
                time.monotonic(),
                effective_timeout_s,
            )
            logger.info(
                f"Launched async validation task for checkpoint {training_report.checkpoint}"
            )
        return len(self._pending_validations)

    def _process_finished_validation(
        self, task: ray.ObjectRef, checkpoint: Checkpoint
    ) -> Tuple[
        Dict[Checkpoint, Dict[str, Any]], Dict[Checkpoint, ReportedCheckpointStatus]
    ]:
        """Process finished validation, return (metrics, status) dicts."""
        checkpoint_to_metrics = {}
        checkpoint_to_status = {}
        try:
            checkpoint_to_metrics[checkpoint] = ray.get(task)
            checkpoint_to_status[checkpoint] = ReportedCheckpointStatus.VALIDATED
        except ray.exceptions.TaskCancelledError:
            logger.info(
                f"Validation was cancelled for checkpoint {checkpoint}, likely because the train run was aborted. "
                "It will be retried in the next train run with the same storage path if there is one."
            )
        except ray.exceptions.RayTaskError:
            checkpoint_to_metrics[checkpoint] = {}
            if task in self._timed_out_validations:
                self._timed_out_validations.pop(task)
                checkpoint_to_status[
                    checkpoint
                ] = ReportedCheckpointStatus.VALIDATION_TIMEOUT
                logger.warning(f"Validation timed out for checkpoint {checkpoint}")
            else:
                checkpoint_to_status[
                    checkpoint
                ] = ReportedCheckpointStatus.VALIDATION_FAILED
                logger.warning(f"Validation cancelled for checkpoint {checkpoint}")

        return checkpoint_to_metrics, checkpoint_to_status

    async def before_controller_shutdown(self):
        while self._poll_validations() != 0 or self._kick_off_validations() != 0:
            await asyncio.sleep(VALIDATION_TASK_POLL_INTERVAL_S)
        checkpoint_to_metrics = {}
        checkpoint_to_status = {}
        tasks = list(self._finished_validations.keys())
        for task in tasks:
            checkpoint = self._finished_validations[task]
            self._finished_validations.pop(task)
            metrics, status = self._process_finished_validation(task, checkpoint)
            checkpoint_to_metrics.update(metrics)
            checkpoint_to_status.update(status)
        self._checkpoint_manager.update_checkpoints_with_metrics(
            checkpoint_to_metrics, checkpoint_to_status
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
