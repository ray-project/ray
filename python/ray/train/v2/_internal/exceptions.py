import os
from typing import List, Optional

from ray.train.v2._internal.constants import (
    DEFAULT_WORKER_GROUP_START_TIMEOUT_S,
    DEFAULT_WORKER_HEALTH_CHECK_TIMEOUT_S,
    REPORT_BARRIER_TIMEOUT_S_ENV_VAR,
    WORKER_GROUP_START_TIMEOUT_S_ENV_VAR,
    WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR,
)


# TODO: Distinguish between user and system exceptions.
class RayTrainError(Exception):
    """Base class for all Ray Train exceptions."""


class WorkerHealthCheckTimeoutError(RayTrainError):
    """Exception raised when a worker health check hangs for long enough."""

    def __init__(self, message):
        timeout = os.getenv(
            WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR, DEFAULT_WORKER_HEALTH_CHECK_TIMEOUT_S
        )
        message += (
            f"\nSet the {WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR} "
            "environment variable to increase the timeout "
            f"(current value: {timeout} seconds)."
        )
        super().__init__(message)


class WorkerHealthCheckFailedError(RayTrainError):
    """Exception raised when a worker health check fails."""

    def __init__(self, message, failure: Exception):
        super().__init__(message)
        self._message = message
        self.health_check_failure = failure

    def __reduce__(self):
        return (self.__class__, (self._message, self.health_check_failure))


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
            "Potential causes include: "
            "(1) temporary insufficient cluster resources while waiting for "
            "autoscaling (ignore this warning in this case), "
            "(2) infeasible resource request where the provided `ScalingConfig` "
            "cannot be satisfied), "
            "and (3) transient network issues. "
            f"Set the {WORKER_GROUP_START_TIMEOUT_S_ENV_VAR} "
            "environment variable to increase the timeout."
        )

    def __reduce__(self):
        return (self.__class__, (self.num_workers,))


class WorkerGroupStartupFailedError(RayTrainError):
    """Exception raised when the worker group fails to start.

    Example scenario: A worker is scheduled onto a node that dies while
    the worker actor is initializing.
    """


class CheckpointManagerInitializationError(RayTrainError):
    """Exception raised when the checkpoint manager fails to initialize from a snapshot.

    Example scenarios:
    1. The checkpoint manager snapshot version is old and
        incompatible with the current version of Ray Train.
    2. The checkpoint manager snapshot JSON file is corrupted.
    3. The checkpoint manager snapshot references checkpoints that cannot be found
        in the run storage path.
    """


class CollectiveTimeoutError(RayTrainError):
    """Exception raised when an internal Ray Train collective operation of
    the worker group times out.
    """


class BroadcastCollectiveTimeoutError(CollectiveTimeoutError):
    """Exception raised when the broadcast operation times out.

    There are two main timeout examples:
    1. If not all workers call `ray.train.report`, the entire worker group will
        hang until the timeout before raising. This prevents indefinite worker
        group hangs.
    2. If a worker is slow in the training loop and fails to reach the broadcast
        time, the collective will time out.
    """

    def __init__(
        self, time_elapsed: Optional[float], missing_ranks: List[int], timeout_s: float
    ):
        self._time_elapsed = time_elapsed
        self._missing_ranks = missing_ranks
        self._timeout_s = timeout_s

        message = (
            f"The broadcast operation timed out after {time_elapsed:.2f} seconds. "
            "Please make sure all worker ranks call `ray.train.report`. \n"
            f"The following ranks have not called it: {missing_ranks}\n"
            f"You can set this timeout with the {REPORT_BARRIER_TIMEOUT_S_ENV_VAR} "
            f"environment variable (current value: {timeout_s:.2f} s)."
        )
        super().__init__(message)

    def __reduce__(self):
        return (
            self.__class__,
            (self._time_elapsed, self._missing_ranks, self._timeout_s),
        )


class UserExceptionWithTraceback(RayTrainError):
    """This class wraps a user code exception raised on the worker
    with its original traceback string, for logging and debugging purposes.

    This is needed because the original exception traceback is not serialized
    with the exception when it is *returned* back to the main process.
    """

    def __init__(self, exc: BaseException, traceback_str: str):
        self._base_exc = exc
        self._traceback_str = traceback_str

    def __reduce__(self):
        return (self.__class__, (self._base_exc, self._traceback_str))

    def __str__(self):
        return self._traceback_str
