from typing import Dict, List, Optional

from ray.train.v2._internal.exceptions import RayTrainError
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class TrainingFailedError(RayTrainError):
    """Exception raised when training fails from a `trainer.fit()` call.
    This is either :class:`ray.train.WorkerGroupError`,
    :class:`ray.train.ControllerError`, or :class:`ray.train.PreemptionError`.
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
    """Exception raised when training is interrupted by a planned preemption
    (e.g., GKE node drain). Distinguished from :class:`WorkerGroupError` so
    that the failure policy can apply a separate retry budget
    (``FailureConfig.max_preemption_failures``).

    Args:
        error_message: A human-readable description of the preemption event.
        preempted_ranks: List of worker ranks whose hosts were drained.
        preempted_node_ids: List of corresponding Ray node IDs.
        deadline_exceeded: Whether the preemption deadline elapsed before
            survivors finished their JIT work. True means at least one
            ``RayActorError`` was observed during the PREEMPTING state.
        worker_failures: Mapping from rank to the underlying exception
            observed on that worker (if any). May be empty for paths where
            every worker exited cleanly via UDF return.
    """

    def __init__(
        self,
        error_message: str,
        preempted_ranks: List[int],
        preempted_node_ids: List[str],
        deadline_exceeded: bool = False,
        worker_failures: Optional[Dict[int, Exception]] = None,
    ):
        super().__init__("Training interrupted by preemption:\n" + error_message)
        self._error_message = error_message
        self.preempted_ranks = preempted_ranks
        self.preempted_node_ids = preempted_node_ids
        self.deadline_exceeded = deadline_exceeded
        self.worker_failures = worker_failures or {}

    def __reduce__(self):
        return (
            self.__class__,
            (
                self._error_message,
                self.preempted_ranks,
                self.preempted_node_ids,
                self.deadline_exceeded,
                self.worker_failures,
            ),
        )
