"""Exceptions for the REST-only Ray Client."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ray_client.client import RemoteRef


class RayClientError(Exception):
    """Base exception for Ray Client errors."""

    def __init__(
        self,
        message: str = "",
        *,
        ref: Optional["RemoteRef"] = None,
    ):
        super().__init__(message)
        self.ref = ref


class RayClientSerializationError(RayClientError, TypeError):
    """Raised when inputs, captured variables, or outputs are not JSON-serializable."""


class RayTimeoutError(RayClientError, TimeoutError):
    """Raised when waiting for a remote task, actor method, or ref exceeds the timeout."""

    def __init__(self, message: str, ref: Optional["RemoteRef"] = None):
        super().__init__(message, ref=ref)

    def __reduce__(self):
        return (type(self), (self.args[0] if self.args else "", self.ref))


class RayExecutionError(RayClientError):
    """Raised when a remote task or actor method raises an exception on the cluster."""

    def __init__(
        self,
        target: str,
        status: str = "FAILED",
        message: str = "",
        exc_type: Optional[str] = None,
        exc_message: Optional[str] = None,
        remote_traceback: Optional[str] = None,
    ):
        self.target = target
        self.status = status
        self.message = message
        self.exc_type = exc_type
        self.exc_message = exc_message
        self.remote_traceback = remote_traceback

        text = f"Ray remote execution '{target}' failed"
        if exc_type:
            text += f" ({exc_type}: {exc_message})"
        elif message:
            text += f": {message}"
        if remote_traceback:
            text += f"\n--- Remote Traceback ---\n{remote_traceback.rstrip()}"
        super().__init__(text)

    def __reduce__(self):
        return (
            type(self),
            (
                self.target,
                self.status,
                self.message,
                self.exc_type,
                self.exc_message,
                self.remote_traceback,
            ),
        )


# Backward-compatible aliases
RayJobTimeoutError = RayTimeoutError
RayJobExecutionError = RayExecutionError
