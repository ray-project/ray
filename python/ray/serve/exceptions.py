from ray.util.annotations import PublicAPI


@PublicAPI(stability="stable")
class RayServeException(Exception):
    pass


class BackPressureError(RayServeException):
    """Raised when max_queued_requests is exceeded on a handle (or in the proxy)."""

    pass
