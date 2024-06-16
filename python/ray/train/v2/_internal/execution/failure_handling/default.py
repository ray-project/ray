import logging

from ray.exceptions import RayActorError
from ray.train import FailureConfig
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.worker_group import WorkerGroupStatus

logger = logging.getLogger(__name__)


class DefaultFailurePolicy(FailurePolicy):
    def __init__(self, failure_config: FailureConfig):
        super().__init__(failure_config)

        self._total_failures = 0

    def make_decision(self, worker_group_status: WorkerGroupStatus) -> FailureDecision:
        if not worker_group_status.errors:
            return FailureDecision.NOOP

        for error in worker_group_status.errors.values():
            # Always try restarting in the case of a preempted actor.
            if isinstance(error, RayActorError) and error.preempted:
                logger.warning(
                    "At least one of the worker failures was caused by "
                    "node preemption. Ray Train will not increment the "
                    "failure count and will trigger a restart."
                )
                return FailureDecision.RESTART

        self._total_failures += 1

        if self.failure_config.max_failures == -1:
            return FailureDecision.RESTART

        if self._total_failures > self.failure_config.max_failures:
            return FailureDecision.RAISE

        return FailureDecision.RESTART
