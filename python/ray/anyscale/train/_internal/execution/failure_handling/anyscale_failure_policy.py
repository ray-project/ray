import logging
from typing import Dict

from ray.exceptions import RayActorError
from ray.train.v2._internal.exceptions import WorkerHealthCheckFailedError
from ray.train.v2._internal.execution.failure_handling import (
    DefaultFailurePolicy,
    FailureDecision,
)
from ray.train.v2._internal.execution.worker_group import WorkerGroupStatus

logger = logging.getLogger(__name__)


def _contains_preemption_error(errors: Dict[str, Exception]) -> bool:
    """Returns True if any one of the workers died due to node preemption."""
    if not errors:
        return False

    for error in errors.values():
        if isinstance(error, WorkerHealthCheckFailedError):
            health_check_failure = error.health_check_failure
            if (
                isinstance(health_check_failure, RayActorError)
                and health_check_failure.preempted
            ):
                return True
    return False


class AnyscaleFailurePolicy(DefaultFailurePolicy):
    def make_decision(self, worker_group_status: WorkerGroupStatus) -> FailureDecision:
        # TODO: Generic hardware failures (node/GPU failures) should be handled
        # the same way as preemption errors. These are expected errors when
        # running at larger scale for longer durations and should always be retried
        # because the fix (requesting a new node) can fix the issue.
        # At the moment, it's not possible to get ActorDiedError cause information
        # (NodeDeathInfo). Only the `preempted` flag is available.

        # Try restarting in the case any worker failed due to preemption.
        if _contains_preemption_error(worker_group_status.errors):
            logger.info(
                "Deciding to RESTART, since at least one of the worker failures "
                "was caused by node preemption. Ray Train will not increment the "
                "total failure count and will restart the worker group."
            )
            return FailureDecision.RESTART

        return super().make_decision(worker_group_status)
