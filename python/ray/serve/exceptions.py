from typing import Optional

import grpc

from ray.exceptions import TaskCancelledError
from ray.serve._private.common import DeploymentID
from ray.util.annotations import PublicAPI


@PublicAPI(stability="stable")
class RayServeException(Exception):
    pass


@PublicAPI(stability="stable")
class gRPCStatusError(RayServeException):
    """Internal exception that wraps an exception with user-set gRPC status code.

    This is used to preserve user-set gRPC status codes when exceptions are raised
    in deployments. When a user sets a status code on the gRPC context before raising
    an exception, this wrapper carries that status code through the error handling
    path so the proxy can return the user's intended status code instead of INTERNAL.
    """

    def __init__(
        self,
        original_exception: BaseException,
        code: Optional[grpc.StatusCode] = None,
        details: Optional[str] = None,
    ):
        # Store attributes with underscore prefix to avoid conflicts with
        # Ray's exception handling (Ray uses 'cause' internally).
        self._original_exception = original_exception
        self._grpc_code = code
        self._grpc_details = details
        super().__init__(str(original_exception))

    @property
    def original_exception(self) -> BaseException:
        """The original exception that was raised."""
        return self._original_exception

    @property
    def grpc_code(self) -> Optional[grpc.StatusCode]:
        """The user-set gRPC status code, if any."""
        return self._grpc_code

    @property
    def grpc_details(self) -> Optional[str]:
        """The user-set gRPC status details, if any."""
        return self._grpc_details

    def __str__(self) -> str:
        return str(self._original_exception)


@PublicAPI(stability="alpha")
class BackPressureError(RayServeException):
    """Raised when max_queued_requests is exceeded on a DeploymentHandle."""

    def __init__(self, num_queued_requests: int, max_queued_requests: int):
        super().__init__(num_queued_requests, max_queued_requests)
        self._message = (
            f"Request dropped due to backpressure "
            f"(num_queued_requests={num_queued_requests}, "
            f"max_queued_requests={max_queued_requests})."
        )

    def __str__(self) -> str:
        return self._message

    @property
    def message(self) -> str:
        return self._message


@PublicAPI(stability="alpha")
class RequestCancelledError(RayServeException, TaskCancelledError):
    """Raise when a Serve request is cancelled."""

    def __init__(self, request_id: Optional[str] = None):
        self._request_id: Optional[str] = request_id

    def __str__(self):
        if self._request_id:
            return f"Request {self._request_id} was cancelled."
        else:
            return "Request was cancelled."


@PublicAPI(stability="alpha")
class DeploymentUnavailableError(RayServeException):
    """Raised when a Serve deployment is unavailable to receive requests.

    Currently this happens because the deployment failed to deploy.
    """

    def __init__(self, deployment_id: DeploymentID):
        self._deployment_id = deployment_id

    @property
    def message(self) -> str:
        return f"{self._deployment_id} is unavailable because it failed to deploy."
