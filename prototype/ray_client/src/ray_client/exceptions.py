"""Exceptions for the REST-only Ray Client."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ray_client.client import JobResult


class RayClientError(Exception):
    """Base exception for Ray Client errors."""


class RayClientSerializationError(RayClientError, TypeError):
    """Raised when inputs, captured variables, or outputs are not JSON-serializable."""


class RayJobTimeoutError(RayClientError, TimeoutError):
    """Raised when waiting for a driver job exceeds the timeout.

    The job is still running on the cluster; `job` is the handle to it.
    """

    def __init__(self, message: str, job: "JobResult"):
        super().__init__(message)
        self.job = job


class RayJobExecutionError(RayClientError):
    """Raised when a remote driver job fails, stops, or raises an exception."""

    def __init__(
        self,
        submission_id: str,
        status: str,
        message: str = "",
        exc_type: Optional[str] = None,
        exc_message: Optional[str] = None,
        remote_traceback: Optional[str] = None,
        logs: str = "",
    ):
        self.submission_id = submission_id
        self.status = status
        self.message = message
        self.exc_type = exc_type
        self.exc_message = exc_message
        self.remote_traceback = remote_traceback
        self.logs = logs

        text = f"Ray driver job '{submission_id}' ended with status {status}"
        if exc_type:
            text += f" ({exc_type}: {exc_message})"
        elif message:
            text += f": {message}"
        if remote_traceback:
            text += f"\n--- Remote Driver Traceback ---\n{remote_traceback.rstrip()}"
        elif logs:
            tail = "\n".join(logs.strip().splitlines()[-20:])
            text += f"\n--- Driver Log Tail ---\n{tail}"
        super().__init__(text)
