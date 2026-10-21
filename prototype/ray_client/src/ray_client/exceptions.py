"""Exceptions for the REST-only Ray Client."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ray_client.client import JobResult


class RayClientError(Exception):
    """Base exception for Ray Client errors."""

    def __init__(
        self,
        message: str = "",
        *,
        job: Optional["JobResult"] = None,
        submission_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.job = job
        self.submission_id = submission_id or (
            job.submission_id if job is not None else None
        )


class RayClientSerializationError(RayClientError, TypeError):
    """Raised when inputs, captured variables, or outputs are not JSON-serializable."""


class RayJobTimeoutError(RayClientError, TimeoutError):
    """Raised when waiting for a driver job exceeds the timeout.

    `job` is the handle to the submitted job.
    """

    def __init__(self, message: str, job: Optional["JobResult"] = None):
        super().__init__(message, job=job)

    def __reduce__(self):
        return (type(self), (self.args[0] if self.args else "", self.job))


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

    def __reduce__(self):
        # The default reduce re-invokes __init__ with the formatted message only.
        return (
            type(self),
            (
                self.submission_id,
                self.status,
                self.message,
                self.exc_type,
                self.exc_message,
                self.remote_traceback,
                self.logs,
            ),
        )
