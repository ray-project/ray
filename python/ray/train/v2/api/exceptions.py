from typing import TYPE_CHECKING, Dict, Optional

from ray.train.v2._internal.exceptions import RayTrainError
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.preemption import PreemptionInfo


@PublicAPI(stability="alpha")
class TrainingFailedError(RayTrainError):
    """Exception raised when training fails from a `trainer.fit()` call.
    This is either :class:`ray.train.WorkerGroupError` or :class:`ray.train.ControllerError`.
    """


@PublicAPI(stability="alpha")
class WorkerGroupError(TrainingFailedError):
    """Exception raised from the worker group during training.

    Args:
        error_message: A human-readable error message describing the training worker failures.
        worker_failures: A mapping from worker rank to the exception that
            occurred on that worker during training.
    """

    def __init__(self, error_message: str, worker_failures: Dict[int, Exception]):
        super().__init__("Training failed due to worker errors:\n" + error_message)
        self._error_message = error_message
        self.worker_failures = worker_failures

    def __reduce__(self):
        return (self.__class__, (self._error_message, self.worker_failures))


@PublicAPI(stability="alpha")
class ControllerError(TrainingFailedError):
    """Exception raised when training fails due to a controller error.

    Args:
        controller_failure: The exception that occurred on the controller.
    """

    def __init__(self, controller_failure: Exception):
        super().__init__(
            "Training failed due to controller error:\n" + str(controller_failure)
        )
        self.controller_failure = controller_failure
        self.with_traceback(controller_failure.__traceback__)

    def __reduce__(self):
        return (self.__class__, (self.controller_failure,))


@PublicAPI(stability="alpha")
class PreemptionError(TrainingFailedError):
    """Exception raised when training is interrupted by node preemption.

    Distinct from :class:`WorkerGroupError` so that a planned preemption
    consumes a separate retry budget (``FailureConfig.max_preemption_failures``,
    default -1 = unlimited) rather than ``max_failures``, which is reserved for
    real failures (OOM, hardware faults, user-code bugs).

    Args:
        preemption_info: Details of the preemption (the affected node IDs and
            ranks, and the reclaim deadline).
        worker_failures: A mapping from worker rank to the exception observed on
            that worker at teardown, if any.
        deadline_exceeded: True if the worker group was torn down because the
            preemption deadline elapsed before all workers exited.
    """

    def __init__(
        self,
        preemption_info: "PreemptionInfo",
        worker_failures: Optional[Dict[int, Exception]] = None,
        deadline_exceeded: bool = False,
    ):
        self.preemption_info = preemption_info
        self.worker_failures = worker_failures or {}
        self.deadline_exceeded = deadline_exceeded
        super().__init__(
            "Training was interrupted by node preemption "
            f"(preempted_ranks={preemption_info.preempted_ranks}, "
            f"deadline_exceeded={deadline_exceeded})."
        )

    def __reduce__(self):
        return (
            self.__class__,
            (self.preemption_info, self.worker_failures, self.deadline_exceeded),
        )
