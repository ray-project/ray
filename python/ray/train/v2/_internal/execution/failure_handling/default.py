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
)

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

    def _log_decision(
        self,
        decision: FailureDecision,
        training_failed_error: TrainingFailedError,
        error_count: int,
        error_limit: int,
    ):
        logger.info(
            f"[FailurePolicy] Decision: {decision}, "
            f"Error count: {error_count} / {error_limit}, "
            f"Error: {training_failed_error}"
        )

    def make_decision(
        self,
        training_failed_error: TrainingFailedError,
    ) -> FailureDecision:

        if not self._is_retryable_error(training_failed_error):
            decision = FailureDecision.RAISE
        else:
            if isinstance(training_failed_error, ControllerError):
                error_count = self._controller_failures + 1
                error_limit = (
                    self.failure_config.controller_failure_limit
                    if self.failure_config.controller_failure_limit != -1
                    else float("inf")
                )
            else:
                error_count = self._running_failures + 1
                error_limit = (
                    self.failure_config.max_failures
                    if self.failure_config.max_failures != -1
                    else float("inf")
                )

            if error_count >= error_limit:
                decision = FailureDecision.RAISE
            else:
                decision = FailureDecision.RETRY

        self._log_decision(decision, training_failed_error, error_count, error_limit)
        return decision
