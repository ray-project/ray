import os
from typing import Dict

from ray.train.v2._internal.constants import (
    DEFAULT_WORKER_GROUP_START_TIMEOUT_S,
    WORKER_GROUP_START_TIMEOUT_S_ENV_VAR,
)


# TODO: Distinguish between user and system exceptions.
class RayTrainError(Exception):
    """Base class for all Ray Train exceptions."""


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


class WorkerGroupStartupTimeoutError(RayTrainError):
    """Exception raised when the worker group startup times out.

    Example scenario: 4 GPUs are detected in the cluster, but when the worker
    are actually scheduled, one of the nodes goes down and only 3 GPUs are
    available. One of the worker tasks may be stuck pending, until a timeout is reached.
    """

    def __init__(self, num_workers: int):
        timeout = float(
            os.environ.get(
                WORKER_GROUP_START_TIMEOUT_S_ENV_VAR,
                DEFAULT_WORKER_GROUP_START_TIMEOUT_S,
            )
        )
        self.num_workers = num_workers
        super().__init__(
            f"The worker group startup timed out after {timeout} seconds waiting "
            f"for {num_workers} workers. "
            "Possible causes include insufficient cluster resources and "
            f"transient network issues. Set the {WORKER_GROUP_START_TIMEOUT_S_ENV_VAR} "
            "environment variable to increase the timeout."
        )

    def __reduce__(self):
        return (self.__class__, (self.num_workers,))


class WorkerGroupStartupFailedError(RayTrainError):
    """Exception raised when the worker group fails to start.

    Example scenario: A worker is scheduled onto a node that dies while
    the worker actor is initializing.
    """
