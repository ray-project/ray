import logging

from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.worker_group import PolicyHandledStatus
from ray.train.v2._internal.execution.worker_group.state import WorkerGroupResizeStatus
from ray.train.v2.api.config import FailureConfig

logger = logging.getLogger(__name__)


class DefaultFailurePolicy(FailurePolicy):
    def __init__(self, failure_config: FailureConfig):
        super().__init__(failure_config)
        self._total_failures = 0
        self._resize_failure_count = 0

    def make_decision(
        self, worker_group_status: PolicyHandledStatus
    ) -> FailureDecision:
        if not worker_group_status.errors:
            return FailureDecision.NOOP

        if isinstance(worker_group_status, WorkerGroupResizeStatus):
            self._resize_failure_count += 1
            if (
                self.failure_config.resize_failure_limit != -1
                and self._resize_failure_count
                > self.failure_config.resize_failure_limit
            ):
                logger.info(
                    "Deciding to TERMINATE, since the resize failure count "
                    f"({self._resize_failure_count}) exceeded the maximum allowed failures: "
                    f"FailureConfig(resize_failure_limit={self.failure_config.resize_failure_limit})."
                )
                return FailureDecision.RAISE
            logger.info(
                "Deciding to reschedule."
                f"Encountered {self._resize_failure_count} resize failures so far."
            )
            return FailureDecision.RESCHEDULE
        else:
            self._total_failures += 1

            if self.failure_config.max_failures == -1:
                logger.info(
                    "Deciding to RESTART, since infinite retry is enabled. "
                    f"Encountered {self._total_failures} failures so far."
                )
                return FailureDecision.RESTART

            if self._total_failures > self.failure_config.max_failures:
                logger.info(
                    "Deciding to TERMINATE, since the total failure count "
                    f"({self._total_failures}) exceeded the maximum allowed failures: "
                    f"FailureConfig(max_failures={self.failure_config.max_failures})."
                )
                return FailureDecision.RAISE

            logger.info(
                "Deciding to RESTART, since the total "
                f"failure count ({self._total_failures}) <= "
                f"FailureConfig(max_failures={self.failure_config.max_failures})."
            )
            return FailureDecision.RESTART
