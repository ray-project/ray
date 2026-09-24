from typing import Optional

from ray.train.health.decision import HealthDecision
from ray.train.v2.api.exceptions import WorkerGroupError
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class HealthDecisionError(WorkerGroupError):
    """Raised when a ``Reattempt`` or ``Evict`` decision restarts the worker group.

    Takes the same path as a worker error, so ``FailureConfig`` applies.
    """

    def __init__(self, error_message: str, decision: Optional[HealthDecision] = None):
        super().__init__(error_message, worker_failures={})
        self.decision = decision

    def __reduce__(self):
        return (self.__class__, (self._error_message, self.decision))
