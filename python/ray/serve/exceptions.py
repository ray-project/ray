from typing import Optional

from ray.exceptions import TaskCancelledError
from ray.util.annotations import PublicAPI


@PublicAPI(stability="stable")
class RayServeException(Exception):
    pass


@PublicAPI(stability="alpha")
class BackPressureError(RayServeException):
    """Raised when max_queued_requests is exceeded on a DeploymentHandle."""

    def __init__(self, *, num_queued_requests: int, max_queued_requests: int):
        self._message = (
            f"Request dropped due to backpressure "
            f"(num_queued_requests={num_queued_requests}, "
            f"max_queued_requests={max_queued_requests})."
        )
        super().__init__(self._message)

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
