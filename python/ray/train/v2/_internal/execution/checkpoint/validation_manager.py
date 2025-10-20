import logging
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List

import ray
from ray.train._checkpoint import Checkpoint
from ray.train.v2._internal.execution.callback import ControllerCallback, ReportCallback
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.training_report import (
    _TrainingReport,
    _ValidationSpec,
)

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.controller import TrainControllerState

logger = logging.getLogger(__name__)


VALIDATION_TASK_POLL_INTERVAL_S = 1


@ray.remote
def run_validate_fn(validation_spec: _ValidationSpec, checkpoint: Checkpoint) -> Dict:
    """Run the user-defined validation function."""
    metrics_dict = validation_spec.validate_fn(
        checkpoint,
        validation_spec.validate_config,
    )
    if not isinstance(metrics_dict, dict):
        raise ValueError(
            "The validate function must return a dictionary of metrics. "
            f"Got {type(metrics_dict)} instead."
        )
    return metrics_dict


class ValidationManager(ControllerCallback, ReportCallback):
    def __init__(
        self,
        checkpoint_manager: CheckpointManager,
    ):
        self._checkpoint_manager = checkpoint_manager

        # Map from validation task to checkpoint
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
        if (
            training_report.validation_spec
            and training_report.validation_spec.validate_fn
        ):
            # TODO: rate limit this by using a queue?
            # TODO: figure out where to place run_validate_fn task:
            # head node is faster but want to avoid putting too much there
            # TODO: provide option to run this on gpu
            validate_task = run_validate_fn.remote(
                training_report.validation_spec, training_report.checkpoint
            )
            self._pending_validations[validate_task] = training_report.checkpoint
            logger.info(
                f"Launched async validation task for checkpoint {training_report.checkpoint}"
            )

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
            # TODO: consider better logging
            logger.info(
                f"Finished async validation task for checkpoint(s) {done_checkpoints}. "
                f"Remaining pending validations for checkpoint(s): {self._pending_validations.values()}"
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
            # TODO: retry validations and time out appropriately.
            # TODO: track failed validations - see ed45912bb6ed435de06ac1cd58e9918e6825b4fe
        return checkpoint_to_metrics

    def before_controller_shutdown(self):
        while self._poll_validations() != 0:
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
