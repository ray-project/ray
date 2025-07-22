from typing import Dict

from ray.train.v2._internal.exceptions import RayTrainError
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class TrainingFailedError(RayTrainError):
    """Exception raised from the training workers.

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
class ControllerError(RayTrainError):
    """Exception raised when training fails due to a controller error.

    Args:
        controller_failure: The exception that occurred on the controller.
    """

    def __init__(self, controller_failure: Exception):
        super().__init__(
            "Training failed due to controller error:\n" + str(controller_failure)
        )
        self.controller_failure = controller_failure

    def __reduce__(self):
        return (self.__class__, (self.controller_failure,))
