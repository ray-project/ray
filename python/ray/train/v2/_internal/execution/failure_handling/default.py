import logging

from ray.train import FailureConfig
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.worker_group import WorkerGroupPollStatus

logger = logging.getLogger(__name__)


class DefaultFailurePolicy(FailurePolicy):
    def __init__(self, failure_config: FailureConfig):
        super().__init__(failure_config)
        self._total_failures = 0

    def make_decision(
        self, worker_group_status: WorkerGroupPollStatus
    ) -> FailureDecision:
        if not worker_group_status.errors:
            return FailureDecision.NOOP

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
