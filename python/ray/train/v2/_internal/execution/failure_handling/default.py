import logging
from typing import Union

from ray.train.v2._internal.exceptions import (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupPollStatus,
    WorkerGroupSchedulingStatus,
)
from ray.train.v2.api.config import FailureConfig

logger = logging.getLogger(__name__)


RETRYABLE_SCHEDULING_ERRORS = (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)


class DefaultFailurePolicy(FailurePolicy):
    def __init__(self, failure_config: FailureConfig):
        super().__init__(failure_config)
        self._running_failures = 0
        # TODO: change to count the consecutive reschedule failures.
        self._schedule_failures = 0

    def make_decision(
        self,
        worker_group_status: Union[WorkerGroupPollStatus, WorkerGroupSchedulingStatus],
    ) -> FailureDecision:
        if not worker_group_status.has_error:
            return FailureDecision.NOOP

        if isinstance(worker_group_status, WorkerGroupSchedulingStatus):
            self._schedule_failures += 1
            if not isinstance(worker_group_status.error, RETRYABLE_SCHEDULING_ERRORS):
                logger.info(
                    "Decided to terminate the scheduling operation, since the scheduling failure is not retryable. "
                    f"Error: {worker_group_status.get_error_string()}"
                )
                return FailureDecision.RAISE
            if (
                self.failure_config.scheduling_failure_limit != -1
                and self._schedule_failures
                > self.failure_config.scheduling_failure_limit
            ):
                logger.info(
                    "Decided to terminate the scheduling operation, since the scheduling failure count exceeded the maximum allowed failures. "
                    f"FailureConfig(scheduling_failure_limit={self.failure_config.scheduling_failure_limit}). "
                    f"Encountered {self._schedule_failures} schedule failures so far. "
                    f"Error: {worker_group_status.get_error_string()}"
                )
                return FailureDecision.RAISE
            else:
                logger.info(
                    "Decided to reschedule the scheduling operation, since the scheduling failure count did not exceed the maximum allowed failures. "
                    f"FailureConfig(scheduling_failure_limit={self.failure_config.scheduling_failure_limit}). "
                    f"Encountered {self._schedule_failures} schedule failures so far. "
                    f"Error: {worker_group_status.get_error_string()}"
                )
                return FailureDecision.RESCHEDULE
        else:
            self._running_failures += 1

            if self.failure_config.max_failures == -1:
                logger.info(
                    "Decided to restart the training operation, since infinite retry is enabled. "
                    f"Encountered {self._running_failures} failures so far. "
                    f"Error: {worker_group_status.get_error_string()}"
                )
                return FailureDecision.RESTART

            if self._running_failures > self.failure_config.max_failures:
                logger.info(
                    "Decided to terminate the training operation, since the total failure count exceeded the maximum allowed failures. "
                    f"FailureConfig(max_failures={self.failure_config.max_failures}). "
                    f"Encountered {self._running_failures} failures so far. "
                    f"Error: {worker_group_status.get_error_string()}"
                )
                return FailureDecision.RAISE

            logger.info(
                "Decided to restart the training operation, since the total failure count did not exceed the maximum allowed failures. "
                f"FailureConfig(max_failures={self.failure_config.max_failures}). "
                f"Encountered {self._running_failures} failures so far. "
                f"Error: {worker_group_status.get_error_string()}"
            )
            return FailureDecision.RESTART
