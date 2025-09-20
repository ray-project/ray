import logging
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray.train._checkpoint import Checkpoint
from ray.train.v2._internal.execution.callback import ControllerCallback, ReportCallback
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.training_report import _ValidationSpec
from ray.train.v2.api.exceptions import ValidationFailedError
from ray.train.v2.api.validation_failure import ValidationFailure

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

        # Map from checkpoint to ValidationFailure
        self._failed_validations = OrderedDict()

        # TODO: checkpoint/restore validation manager state

    def after_report(
        self,
        metrics: List[Dict[str, Any]],
        checkpoint: Optional[Checkpoint],
        validation_spec: Optional[_ValidationSpec],
    ):
        if validation_spec and validation_spec.validate_fn:
            # TODO: rate limit this by using a queue?
            # TODO: figure out where to place run_validate_fn task:
            # head node is faster but want to avoid putting too much there
            validate_task = run_validate_fn.remote(validation_spec, checkpoint)
            self._pending_validations[validate_task] = checkpoint
            logger.info(f"Launched async validation task for checkpoint {checkpoint}")

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
        self._finished_validations.pop(task)
        try:
            checkpoint_to_metrics[checkpoint] = ray.get(task)
        except ray.exceptions.RayTaskError as e:
            checkpoint_to_metrics[checkpoint] = {}
            self._failed_validations[checkpoint] = ValidationFailure(
                checkpoint=checkpoint,
                validation_failed_error=ValidationFailedError(e.cause),
            )
            logger.exception(f"Validation failed for checkpoint {checkpoint}")
            # TODO: retry validations and time out appropriately.
        return checkpoint_to_metrics

    @property
    def failed_validations(self) -> Optional[List[ValidationFailure]]:
        """Return all the failed validations."""
        return (
            list(self._failed_validations.values())
            if self._failed_validations
            else None
        )

    def before_controller_shutdown(self):
        while self._poll_validations() != 0:
            time.sleep(VALIDATION_TASK_POLL_INTERVAL_S)
        checkpoint_to_metrics = {}
        tasks = list(self._finished_validations.keys())
        for task in tasks:
            checkpoint = self._finished_validations[task]
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
        self._poll_validations()
