from typing import Dict

from ray.train.v2._internal.exceptions import RayTrainError
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class TrainingFailedError(RayTrainError):
    """Base exception raised by `<Framework>Trainer.fit()` when training fails."""

    def __init__(self, error_message: str):
        super().__init__(error_message)
        self._error_message = error_message


@PublicAPI(stability="alpha")
class WorkerTrainingFailedError(TrainingFailedError):
    """Exception raised when training fails due to worker errors."""

    def __init__(self, error_message: str, worker_failures: Dict[int, Exception]):
        super().__init__("Training failed due to worker errors:\n" + error_message)
        self.worker_failures = worker_failures

    def __reduce__(self):
        return (self.__class__, (self._error_message, self.worker_failures))


@PublicAPI(stability="alpha")
class SchedulingTrainingFailedError(TrainingFailedError):
    """Exception raised when training fails due to scheduling errors."""

    def __init__(self, error_message: str, scheduling_error: Exception):
        super().__init__("Training failed due to scheduling error:\n" + error_message)
        self.scheduling_error = scheduling_error

    def __reduce__(self):
        return (self.__class__, (self._error_message, self.scheduling_error))
