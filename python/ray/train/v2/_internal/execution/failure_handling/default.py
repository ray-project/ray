import logging

from .failure_policy import FailureDecision, FailurePolicy
from ray.train.v2._internal.exceptions import (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)
from ray.train.v2.api.config import FailureConfig
from ray.train.v2.api.exceptions import (
    ControllerError,
    TrainingFailedError,
    WorkerGroupError,
)

logger = logging.getLogger(__name__)


RETRYABLE_CONTROLLER_ERRORS = (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)


class DefaultFailurePolicy(FailurePolicy):
    def __init__(self, failure_config: FailureConfig):
        super().__init__(failure_config)
        self._worker_group_failures = 0
        self._controller_failures = 0

    def _log_decision(
        self,
        decision: FailureDecision,
        training_failed_error: TrainingFailedError,
        error_count: int,
        retry_limit: int,
    ):
        if isinstance(training_failed_error, ControllerError):
            error_source = "controller"
        elif isinstance(training_failed_error, WorkerGroupError):
            error_source = "worker group"
        else:
            raise ValueError(f"Unknown error type: {type(training_failed_error)}")

        logger.info(
            f"[FailurePolicy] {decision.value}\n"
            f"  Source: {error_source}\n"
            f"  Error count: {error_count} (max allowed: {retry_limit})\n\n",
            exc_info=(
                type(training_failed_error),
                training_failed_error,
                training_failed_error.__traceback__,
            ),
        )

    def _is_retryable_error(self, training_failed_error: TrainingFailedError) -> bool:
        if isinstance(training_failed_error, WorkerGroupError):
            return True
        elif isinstance(training_failed_error, ControllerError):
            return isinstance(
                training_failed_error.controller_failure, RETRYABLE_CONTROLLER_ERRORS
            )
        return False

    def make_decision(
        self,
        training_failed_error: TrainingFailedError,
    ) -> FailureDecision:

        if not self._is_retryable_error(training_failed_error):
            decision = FailureDecision.RAISE
            error_count = 1
            retry_limit = 0
        else:
            if isinstance(training_failed_error, ControllerError):
                self._controller_failures += 1
                error_count = self._controller_failures
                retry_limit = (
                    self.failure_config.controller_failure_limit
                    if self.failure_config.controller_failure_limit != -1
                    else float("inf")
                )
            elif isinstance(training_failed_error, WorkerGroupError):
                self._worker_group_failures += 1
                error_count = self._worker_group_failures
                retry_limit = (
                    self.failure_config.max_failures
                    if self.failure_config.max_failures != -1
                    else float("inf")
                )
            else:
                raise ValueError(f"Unknown error type: {type(training_failed_error)}")

            if error_count > retry_limit:
                decision = FailureDecision.RAISE
            else:
                decision = FailureDecision.RETRY

        self._log_decision(decision, training_failed_error, error_count, retry_limit)
        return decision
