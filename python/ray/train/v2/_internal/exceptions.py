from typing import Dict


class RayTrainError(Exception):
    """Base class for all RayTrain exceptions."""


class WorkerHealthCheckMissedError(RayTrainError):
    """Exception raised when enough worker health checks are missed."""


class WorkerHealthCheckFailedError(RayTrainError):
    """Exception raised when a worker health check fails."""

    def __init__(self, message, failure: Exception):
        super().__init__(message)
        self._message = message
        self.health_check_failure = failure

    def __reduce__(self):
        return (self.__class__, (self._message, self.health_check_failure))


class TrainingFailedError(RayTrainError):
    """Exception raised when training fails."""

    def __init__(self, worker_failures: Dict[int, Exception]):
        super().__init__(
            "Training failed due to worker errors. "
            "Please inspect the error logs above, "
            "or access the latest worker failures in this "
            "exception's `worker_failures` attribute."
        )
        self.worker_failures = worker_failures

    def __reduce__(self):
        return (self.__class__, (self.worker_failures,))
