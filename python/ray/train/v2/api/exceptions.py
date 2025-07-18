from typing import Dict

from ray.train.v2._internal.exceptions import RayTrainError
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class TrainingFailedError(RayTrainError):
    """Exception raised after the controller finishes and training starts."""

    def __init__(self, error_message: str, worker_failures: Dict[int, Exception]):
        super().__init__("Training failed due to worker errors:\n" + error_message)
        self._error_message = error_message
        self.worker_failures = worker_failures

    def __reduce__(self):
        return (self.__class__, (self._error_message, self.worker_failures))


@PublicAPI(stability="alpha")
class ControllerError(RayTrainError):
    """Exception raised when training fails due to controller errors."""

    def __init__(self, error_message: str, controller_failure: Exception):
        super().__init__("Training failed due to controller errors:\n" + error_message)
        self._error_message = error_message
        self.controller_failure = controller_failure

    def __reduce__(self):
        return (self.__class__, (self._error_message, self.controller_failure))
