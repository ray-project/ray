import contextvars
from dataclasses import dataclass

# Serve request context var which is used for storing the internal
# request context information.
# route_prefix: http url route path, e.g. http://127.0.0.1:/app
#     the route is "/app". When you send requests by handle,
#     the route is "N/A.
# request_id: the request id is generated from http proxy, the value
#     shouldn't be changed when the variable is set.
# note:
#   The request context is readonly to avoid potential
#       async task conflicts when using it concurrently.


@dataclass(frozen=True)
class RequestContext:
    route: str = "N/A"
    request_id: str = "N/A"


_serve_request_context = contextvars.ContextVar(
    "Serve internal request context variable", default=RequestContext()
)
