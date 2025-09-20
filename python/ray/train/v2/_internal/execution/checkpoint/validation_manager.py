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
from ray.train.v2.api.validation_info import ValidationInfo

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

        # Map from checkpoint to validation task
        self._pending_validations = {}

        # Map from checkpoint to ValidationInfo
        self._failed_validations = OrderedDict()

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
            self._pending_validations[checkpoint] = run_validate_fn.remote(
                validation_spec, checkpoint
            )

    def poll_validations(self) -> bool:
        """Poll pending validations, update checkpoint manager state, return whether pending validations remain."""
        checkpoint_to_metrics = {}
        checkpoints = list(self._pending_validations.keys())
        for checkpoint in checkpoints:
            validation_task = self._pending_validations[checkpoint]
            done, _ = ray.wait([validation_task], timeout=0)
            if done:
                try:
                    checkpoint_to_metrics[checkpoint] = ray.get(done[0])
                except ray.exceptions.RayTaskError as e:
                    checkpoint_to_metrics[checkpoint] = {}
                    self._failed_validations[checkpoint] = ValidationInfo(
                        checkpoint=checkpoint,
                        validation_failed_error=ValidationFailedError(e.cause),
                    )
                    logger.exception(f"Validation failed for checkpoint {checkpoint}")
                # TODO: retry validations and time out appropriately.
                self._pending_validations.pop(checkpoint)
        self._checkpoint_manager.update_checkpoints_with_metrics(checkpoint_to_metrics)
        return bool(self._pending_validations)

    @property
    def failed_validations(self) -> Optional[List[ValidationInfo]]:
        """Return all the failed validations."""
        return (
            list(self._failed_validations.values())
            if self._failed_validations
            else None
        )

    def before_controller_shutdown(self):
        while self.poll_validations():
            time.sleep(VALIDATION_TASK_POLL_INTERVAL_S)

    def after_controller_state_update(
        self,
        previous_state: "TrainControllerState",
        current_state: "TrainControllerState",
    ):
        # TODO: figure out if there's a better place to poll validations
        self.poll_validations()
