class RayTrainError(Exception):
    """Base class for all RayTrain exceptions."""


class WorkerHealthCheckMissedError(RayTrainError):
    """Exception raised when enough worker health checks are missed."""


class WorkerHealthCheckFailedError(RayTrainError):
    """Exception raised when a worker health check fails."""

    def __init__(self, message, failure: Exception):
        super().__init__(message)
        self.health_check_failure = failure
