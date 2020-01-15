import ray
from ray.experimental import serve
from ray.experimental.serve.context import TaskContext
from ray.experimental.serve.exceptions import RayServeException
from ray.experimental.serve.constants import DEFAULT_HTTP_ADDRESS


class RayServeHandle:
    """A handle to a service endpoint.

    Invoking this endpoint with .remote is equivalent to pinging
    an HTTP endpoint.

    Example:
       >>> handle = serve.get_handle("my_endpoint")
       >>> handle
       RayServeHandle(
            Endpoint="my_endpoint",
            URL="...",
            Traffic=...
       )
       >>> handle.remote(my_request_content)
       ObjectID(...)
       >>> ray.get(handle.remote(...))
       # result
       >>> ray.get(handle.remote(let_it_crash_request))
       # raises RayTaskError Exception
    """

    def __init__(self, router_handle, endpoint_name):
        self.router_handle = router_handle
        self.endpoint_name = endpoint_name

    def remote(self, *args, **kwargs):
        if len(args) != 0:
            raise RayServeException(
                "handle.remote must be invoked with keyword arguments.")

        # get slo_ms before enqueuing the query
        request_slo_ms = kwargs.pop("slo_ms", None)
        if request_slo_ms is not None:
            try:
                request_slo_ms = float(request_slo_ms)
                if request_slo_ms < 0:
                    raise ValueError(
                        "Request SLO must be positive, it is {}".format(
                            request_slo_ms))
            except ValueError as e:
                raise RayServeException(str(e))

        result_object_id_bytes = ray.get(
            self.router_handle.enqueue_request.remote(
                service=self.endpoint_name,
                request_args=(),
                request_kwargs=kwargs,
                request_context=TaskContext.Python,
                request_slo_ms=request_slo_ms))
        return ray.ObjectID(result_object_id_bytes)

    def get_traffic_policy(self):
        # TODO(simon): This method is implemented via checking global state
        # because we are sure handle and global_state are in the same process.
        # However, once global_state is deprecated, this method need to be
        # updated accordingly.
        history = serve.global_state.policy_action_history[self.endpoint_name]
        if len(history):
            return history[-1]
        else:
            return None

    def get_http_endpoint(self):
        return DEFAULT_HTTP_ADDRESS

    def __repr__(self):
        return """
RayServeHandle(
    Endpoint="{endpoint_name}",
    URL="{http_endpoint}/{endpoint_name}",
    Traffic={traffic_policy}
)
""".format(endpoint_name=self.endpoint_name,
           http_endpoint=self.get_http_endpoint(),
           traffic_policy=self.get_traffic_policy())

    # TODO(simon): a convenience function that dumps equivalent requests
    # code for a given call.
