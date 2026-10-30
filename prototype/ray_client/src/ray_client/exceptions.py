"""Exception hierarchy for RayCluster."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ray_client.client import JobResult


class RayClientError(Exception):
    """Base exception for RayCluster errors."""

    def __init__(
        self,
        message: str = "",
        *,
        job: Optional["JobResult"] = None,
        submission_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.job = job
        self.submission_id = submission_id or (
            job.submission_id if job is not None else None
        )


class RayClientSerializationError(RayClientError, TypeError):
    """Raised when callables, arguments, captured globals, or return values cannot be serialized."""


class RayJobTimeoutError(RayClientError, TimeoutError):
    """Raised when waiting for a job, driver function, or actor call exceeds the timeout."""

    def __init__(
        self,
        message: str,
        job: Optional["JobResult"] = None,
        submission_id: Optional[str] = None,
    ) -> None:
        super().__init__(message, job=job, submission_id=submission_id)

    def __reduce__(self):
        return (
            type(self),
            (
                self.args[0] if self.args else "",
                self.job,
                self.submission_id,
            ),
        )


class RayJobExecutionError(RayClientError):
    """Raised when a driver function, actor method, or blocking CLI job fails."""

    def __init__(
        self,
        submission_id: str,
        status: str = "FAILED",
        message: str = "",
        exc_type: Optional[str] = None,
        exc_message: Optional[str] = None,
        remote_traceback: Optional[str] = None,
        logs: str = "",
        job: Optional["JobResult"] = None,
    ) -> None:
        self.submission_id = submission_id
        self.status = status
        self.message = message
        self.exc_type = exc_type
        self.exc_message = exc_message
        self.remote_traceback = remote_traceback
        self.logs = logs

        text = f"Ray execution '{submission_id}' failed with status {status}"
        if exc_type:
            text += f" ({exc_type}: {exc_message})"
        elif message:
            text += f": {message}"
        if remote_traceback:
            text += f"\n--- Remote Traceback ---\n{remote_traceback.rstrip()}"
        elif logs:
            tail = "\n".join(logs.strip().splitlines()[-20:])
            if tail:
                text += f"\n--- Logs ---\n{tail}"
        super().__init__(text, job=job, submission_id=submission_id)

    def __reduce__(self):
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
                self.job,
            ),
        )
