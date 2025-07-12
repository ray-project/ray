import logging
from typing import Dict, Union

from .failure_policy import FailureDecision, FailurePolicy
from .utils import get_error_string
from ray.train.v2._internal.exceptions import (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)
from ray.train.v2.api.config import FailureConfig

logger = logging.getLogger(__name__)


RETRYABLE_SCHEDULING_ERRORS = (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)

NON_RETRYABLE_ERRORS = ()


class DefaultFailurePolicy(FailurePolicy):
    def __init__(self, failure_config: FailureConfig):
        super().__init__(failure_config)
        self._running_failures = 0
        # TODO: change to count the consecutive reschedule failures.
        self._schedule_failures = 0

    def _log_non_retryable_error(
        self, scheduling_or_poll_error: Union[Exception, Dict[int, Exception]]
    ):
        logger.info(
            "Decided to terminate the training operation, since the error is non-retryable. "
            f"Error: {get_error_string(scheduling_or_poll_error)}"
        )

    def _log_scheduling_failure_limit_exceeded(
        self, scheduling_or_poll_error: Union[Exception, Dict[int, Exception]]
    ):
        logger.info(
            "Decided to terminate the scheduling operation, since the scheduling failure count exceeded the maximum allowed failures. "
            f"FailureConfig(scheduling_failure_limit={self.failure_config.scheduling_failure_limit}). "
            f"Encountered {self._schedule_failures} schedule failures so far. "
            f"Error: {get_error_string(scheduling_or_poll_error)}"
        )

    def _log_scheduling_failure_limit_not_exceeded(
        self, scheduling_or_poll_error: Union[Exception, Dict[int, Exception]]
    ):
        logger.info(
            "Decided to reschedule the scheduling operation, since the scheduling failure count did not exceed the maximum allowed failures. "
            f"FailureConfig(scheduling_failure_limit={self.failure_config.scheduling_failure_limit}). "
            f"Encountered {self._schedule_failures} schedule failures so far. "
            f"Error: {get_error_string(scheduling_or_poll_error)}"
        )

    def _log_infinite_retry_enabled(
        self, scheduling_or_poll_error: Union[Exception, Dict[int, Exception]]
    ):
        logger.info(
            "Decided to restart the training operation, since infinite retry is enabled. "
            f"Error: {get_error_string(scheduling_or_poll_error)}"
        )

    def _log_max_running_failures_exceeded(
        self, scheduling_or_poll_error: Union[Exception, Dict[int, Exception]]
    ):
        logger.info(
            "Decided to terminate the training operation, since the total failure count exceeded the maximum allowed failures. "
            f"FailureConfig(max_failures={self.failure_config.max_failures}). "
            f"Encountered {self._running_failures} failures so far. "
            f"Error: {get_error_string(scheduling_or_poll_error)}"
        )

    def _log_max_running_failures_not_exceeded(
        self, scheduling_or_poll_error: Union[Exception, Dict[int, Exception]]
    ):
        logger.info(
            "Decided to restart the training operation, since the total failure count did not exceed the maximum allowed failures. "
            f"FailureConfig(max_failures={self.failure_config.max_failures}). "
            f"Encountered {self._running_failures} failures so far. "
            f"Error: {get_error_string(scheduling_or_poll_error)}"
        )

    def make_decision(
        self,
        scheduling_or_poll_error: Union[Exception, Dict[int, Exception]],
    ) -> FailureDecision:
        errors_list = []
        if isinstance(scheduling_or_poll_error, Exception):
            errors_list.append(scheduling_or_poll_error)
        else:
            errors_list.extend(scheduling_or_poll_error.values())

        # For non-retryable errors and scheduling errors, we would fall in this category if we encounter any of them.
        for error in errors_list:
            if isinstance(error, NON_RETRYABLE_ERRORS):
                self._log_non_retryable_error(error)
                return FailureDecision.RAISE

            if isinstance(error, RETRYABLE_SCHEDULING_ERRORS):
                self._schedule_failures += 1
                if self.failure_config.scheduling_failure_limit == -1:
                    self._log_infinite_retry_enabled(error)
                    return FailureDecision.RETRY
                elif (
                    self._schedule_failures
                    > self.failure_config.scheduling_failure_limit
                ):
                    self._log_scheduling_failure_limit_exceeded(error)
                    return FailureDecision.RAISE
                else:
                    self._log_scheduling_failure_limit_not_exceeded(error)
                    return FailureDecision.RETRY

        # All other errors are retryable runnable errors.
        self._running_failures += 1
        if self.failure_config.max_failures == -1:
            self._log_infinite_retry_enabled(scheduling_or_poll_error)
            return FailureDecision.RETRY
        elif self._running_failures > self.failure_config.max_failures:
            self._log_max_running_failures_exceeded(scheduling_or_poll_error)
            return FailureDecision.RAISE
        else:
            self._log_max_running_failures_not_exceeded(scheduling_or_poll_error)
            return FailureDecision.RETRY
