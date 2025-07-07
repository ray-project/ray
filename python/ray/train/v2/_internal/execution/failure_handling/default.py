import logging

from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.worker_group import WorkerGroupStatus
from ray.train.v2._internal.execution.worker_group.state import (
    WorkerGroupSchedulingStatus,
)
from ray.train.v2.api.config import FailureConfig

logger = logging.getLogger(__name__)


class DefaultFailurePolicy(FailurePolicy):
    def __init__(self, failure_config: FailureConfig):
        super().__init__(failure_config)
        self._running_failures = 0
        # TODO: change to count the consecutive reschedule failures.
        self._schedule_failures = 0

    def make_decision(self, worker_group_status: WorkerGroupStatus) -> FailureDecision:
        if not worker_group_status.errors:
            return FailureDecision.NOOP

        if isinstance(worker_group_status, WorkerGroupSchedulingStatus):
            self._schedule_failures += 1
            if (
                self.failure_config.scheduling_failure_limit != -1
                and self._schedule_failures
                > self.failure_config.scheduling_failure_limit
            ):
                logger.info(
                    "Deciding to TERMINATE, since the reschedule failure count "
                    f"({self._schedule_failures}) exceeded the maximum allowed failures: "
                    f"FailureConfig(scheduling_failure_limit={self.failure_config.scheduling_failure_limit})."
                )
                return FailureDecision.RAISE
            logger.info(
                "Deciding to reschedule."
                f"Encountered {self._schedule_failures} schedule failures so far."
            )
            return FailureDecision.RETRY
        else:
            self._running_failures += 1

            if self.failure_config.max_failures == -1:
                logger.info(
                    "Deciding to RESTART, since infinite retry is enabled. "
                    f"Encountered {self._running_failures} failures so far."
                )
                return FailureDecision.RETRY

            if self._running_failures > self.failure_config.max_failures:
                logger.info(
                    "Deciding to TERMINATE, since the total failure count "
                    f"({self._running_failures}) exceeded the maximum allowed failures: "
                    f"FailureConfig(max_failures={self.failure_config.max_failures})."
                )
                return FailureDecision.RAISE

            logger.info(
                "Deciding to RESTART, since the total "
                f"failure count ({self._running_failures}) <= "
                f"FailureConfig(max_failures={self.failure_config.max_failures})."
            )
            return FailureDecision.RETRY
