from ray import serve
from ray.serve.context import TaskContext
from ray.serve.exceptions import RayServeException
from ray.serve.constants import DEFAULT_HTTP_ADDRESS
from ray.serve.request_params import RequestMetadata


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

    def __init__(self,
                 router_handle,
                 endpoint_name,
                 relative_slo_ms=None,
                 absolute_slo_ms=None):
        self.router_handle = router_handle
        self.endpoint_name = endpoint_name
        assert (relative_slo_ms is None
                or absolute_slo_ms is None), ("Can't specify both "
                                              "relative and absolute "
                                              "slo's together!")
        self.relative_slo_ms = self._check_slo_ms(relative_slo_ms)
        self.absolute_slo_ms = self._check_slo_ms(absolute_slo_ms)

    def _check_slo_ms(self, slo_value):
        if slo_value is not None:
            try:
                slo_value = float(slo_value)
                if slo_value < 0:
                    raise ValueError(
                        "Request SLO must be positive, it is {}".format(
                            slo_value))
                return slo_value
            except ValueError as e:
                raise RayServeException(str(e))
        return None

    def remote(self, *args, **kwargs):
        if len(args) != 0:
            raise RayServeException(
                "handle.remote must be invoked with keyword arguments.")

        # create RequestMetadata instance
        request_in_object = RequestMetadata(
            self.endpoint_name, TaskContext.Python, self.relative_slo_ms,
            self.absolute_slo_ms)
        return self.router_handle.enqueue_request.remote(
            request_in_object, **kwargs)

    def options(self, relative_slo_ms=None, absolute_slo_ms=None):
        # If both the slo's are None then then we use a high default
        # value so other queries can be prioritize and put in front of these
        # queries.
        assert (relative_slo_ms is None
                or absolute_slo_ms is None), ("Can't specify both "
                                              "relative and absolute "
                                              "slo's together!")
        return RayServeHandle(self.router_handle, self.endpoint_name,
                              relative_slo_ms, absolute_slo_ms)

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
