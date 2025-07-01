import logging

from ray.train import FailureConfig
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.worker_group import PolicyHandledStatus
from ray.train.v2._internal.execution.worker_group.state import WorkerGroupResizeStatus

logger = logging.getLogger(__name__)


class DefaultFailurePolicy(FailurePolicy):
    def __init__(self, failure_config: FailureConfig):
        super().__init__(failure_config)
        self._total_failures = 0

    def make_decision(
        self, worker_group_status: PolicyHandledStatus
    ) -> FailureDecision:
        if not worker_group_status.errors:
            return FailureDecision.NOOP

        self._total_failures += 1

        # TODO: add a limit for reschedule.
        if isinstance(worker_group_status, WorkerGroupResizeStatus):
            logger.info(
                "Deciding to reschedule. For now resize failure will reschedule infinite times."
                f"Encountered {self._total_failures} failures so far."
            )
            return FailureDecision.RESCHEDULE

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
