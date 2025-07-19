import logging
from typing import Union

from .failure_policy import FailureDecision, FailurePolicy
from ray.train.v2._internal.exceptions import (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)
from ray.train.v2.api.config import FailureConfig
from ray.train.v2.api.exceptions import ControllerError, TrainingFailedError

logger = logging.getLogger(__name__)


RETRYABLE_CONTROLLER_ERRORS = (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)


class DefaultFailurePolicy(FailurePolicy):
    def __init__(self, failure_config: FailureConfig):
        super().__init__(failure_config)
        self._running_failures = 0
        self._controller_failures = 0

    def _log_non_retryable_controller_error(self, controller_error: ControllerError):
        logger.info(
            "Decided to terminate the training operation, since the controller error is non-retryable. "
            f"Error: {controller_error}"
        )

    def _log_controller_error_limit_exceeded(self, controller_error: ControllerError):
        logger.info(
            "Decided to terminate the training operation, since the controller error count exceeded the maximum allowed failures. "
            f"FailureConfig(controller_failure_limit={self.failure_config.controller_failure_limit}). "
            f"Encountered {self._controller_failures} controller failures so far. "
            f"Error: {controller_error}"
        )

    def _log_controller_error_limit_not_exceeded(
        self, controller_error: ControllerError
    ):
        logger.info(
            "Decided to reschedule the training operation, since the controller error count did not exceed the maximum allowed failures. "
            f"FailureConfig(controller_failure_limit={self.failure_config.controller_failure_limit}). "
            f"Encountered {self._controller_failures} controller failures so far. "
            f"Error: {controller_error}"
        )

    def _log_infinite_controller_retry_enabled(self, controller_error: ControllerError):
        logger.info(
            "Decided to restart the training operation, since infinite controller retry is enabled. "
            f"Error: {controller_error}"
        )

    def _log_max_training_error_exceeded(
        self, training_failed_error: TrainingFailedError
    ):
        logger.info(
            "Decided to terminate the training operation, since the total training error count exceeded the maximum allowed failures. "
            f"FailureConfig(max_failures={self.failure_config.max_failures}). "
            f"Encountered {self._running_failures} training errors so far. "
            f"Error: {training_failed_error}"
        )

    def _log_max_training_error_not_exceeded(
        self, training_failed_error: TrainingFailedError
    ):
        logger.info(
            "Decided to restart the training operation, since the total training error count did not exceed the maximum allowed failures. "
            f"FailureConfig(max_failures={self.failure_config.max_failures}). "
            f"Encountered {self._running_failures} training errors so far. "
            f"Error: {training_failed_error}"
        )

    def _log_infinite_training_retry_enabled(
        self, training_failed_error: TrainingFailedError
    ):
        logger.info(
            "Decided to restart the training operation, since infinite training retry is enabled. "
            f"Error: {training_failed_error}"
        )

    def make_decision(
        self,
        error: Union[TrainingFailedError, ControllerError],
    ) -> FailureDecision:

        if isinstance(error, ControllerError):
            controller_exception = error.controller_failure
            if isinstance(controller_exception, RETRYABLE_CONTROLLER_ERRORS):
                self._controller_failures += 1
                if self.failure_config.controller_failure_limit == -1:
                    self._log_infinite_controller_retry_enabled(error)
                    return FailureDecision.RESCHEDULE
                elif (
                    self._controller_failures
                    > self.failure_config.controller_failure_limit
                ):
                    self._log_controller_error_limit_exceeded(error)
                    return FailureDecision.RAISE
                else:
                    self._log_controller_error_limit_not_exceeded(error)
                    return FailureDecision.RESCHEDULE
            else:
                self._log_non_retryable_controller_error(error)
                return FailureDecision.RAISE

        elif isinstance(error, TrainingFailedError):

            self._running_failures += 1
            if self.failure_config.max_failures == -1:
                self._log_infinite_training_retry_enabled(error)
                return FailureDecision.RESTART
            elif self._running_failures > self.failure_config.max_failures:
                self._log_max_training_error_exceeded(error)
                return FailureDecision.RAISE
            else:
                self._log_max_training_error_not_exceeded(error)
                return FailureDecision.RESTART
        else:
            raise ValueError(f"Unexpected error type: {type(error)}")
