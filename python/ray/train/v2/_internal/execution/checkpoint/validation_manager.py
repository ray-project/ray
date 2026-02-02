import logging
import time
from collections import OrderedDict, deque
from typing import TYPE_CHECKING, Any, Dict, List, Union

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
            "The validate function must return a dictionary of metrics. "
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

        # Map from in flight validation task to checkpoint
        self._pending_validations = OrderedDict()

        # Map from validation task to checkpoint
        # Finished validations that have yet to be processed
        self._finished_validations = OrderedDict()

        # TODO: checkpoint/restore validation manager state

    def after_report(
        self,
        training_report: _TrainingReport,
        metrics: List[Dict[str, Any]],
    ):
        if training_report.validation:
            self._training_report_queue.append(training_report)

    def _poll_validations(self) -> int:
        """Poll/process validations, update checkpoint manager, return num pending validations."""
        # Move pending validations to finished validations
        validation_tasks = list(self._pending_validations.keys())
        done, _ = ray.wait(
            validation_tasks, timeout=0, num_returns=len(validation_tasks)
        )
        done_checkpoints = []
        for task in done:
            done_checkpoints.append(self._pending_validations[task])
            self._finished_validations[task] = self._pending_validations[task]
            self._pending_validations.pop(task)
        if done_checkpoints:
            logger.info(
                f"Finished async validation task(s) for checkpoint(s): {done_checkpoints}.\n"
                f"Running validations for checkpoint(s): {list(self._pending_validations.values())}.\n"
                f"Staged validations for checkpoint(s): {[tr.checkpoint for tr in self._training_report_queue]}."
            )

        # Process next finished validation
        # TODO: consider configuration to process multiple at a time
        if self._finished_validations:
            task, checkpoint = next(iter(self._finished_validations.items()))
            self._finished_validations.pop(task)
            checkpoint_to_metrics = self._process_finished_validation(task, checkpoint)
            self._checkpoint_manager.update_checkpoints_with_metrics(
                checkpoint_to_metrics
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
            if isinstance(training_report.validation, ValidationTaskConfig):
                merged_kwargs = {
                    **self._validation_config.task_config.ray_remote_kwargs,
                    **training_report.validation.ray_remote_kwargs,
                }
            else:
                merged_kwargs = self._validation_config.task_config.ray_remote_kwargs
            run_validation_fn_with_options = run_validation_fn.options(**merged_kwargs)
            validate_task = run_validation_fn_with_options.remote(
                self._validation_config,
                training_report.validation,
                training_report.checkpoint,
            )
            self._pending_validations[validate_task] = training_report.checkpoint
            logger.info(
                f"Launched async validation task for checkpoint {training_report.checkpoint}"
            )
        return len(self._pending_validations)

    def _process_finished_validation(
        self, task: ray.ObjectRef, checkpoint: Checkpoint
    ) -> Dict[Checkpoint, Dict[str, Any]]:
        """Process finished validation, update checkpoint manager, return metrics."""
        checkpoint_to_metrics = {}
        try:
            checkpoint_to_metrics[checkpoint] = ray.get(task)
        except (ray.exceptions.RayTaskError, ray.exceptions.TaskCancelledError):
            checkpoint_to_metrics[checkpoint] = {}
            logger.exception(f"Validation failed for checkpoint {checkpoint}")
            # TODO: track failed validations - see ed45912bb6ed435de06ac1cd58e9918e6825b4fe
        return checkpoint_to_metrics

    def before_controller_shutdown(self):
        while self._poll_validations() != 0 or self._kick_off_validations() != 0:
            time.sleep(VALIDATION_TASK_POLL_INTERVAL_S)
        checkpoint_to_metrics = {}
        tasks = list(self._finished_validations.keys())
        for task in tasks:
            checkpoint = self._finished_validations[task]
            self._finished_validations.pop(task)
            checkpoint_to_metrics.update(
                self._process_finished_validation(task, checkpoint)
            )
        self._checkpoint_manager.update_checkpoints_with_metrics(checkpoint_to_metrics)

    def after_controller_state_update(
        self,
        previous_state: "TrainControllerState",
        current_state: "TrainControllerState",
    ):
        # TODO: figure out if there's a better place to poll validations
        # TODO: consider cleaning up validation tasks in before_controller_abort
        self._poll_validations()
        self._kick_off_validations()

    def before_init_train_context(
        self, workers: List["Worker"]
    ) -> Dict[str, List[bool]]:
        return {
            "has_validation_fn": [True] * len(workers),
        }
