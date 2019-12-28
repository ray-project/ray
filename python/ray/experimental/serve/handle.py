import ray
from ray.experimental import serve
from ray.experimental.serve.context import TaskContext
from ray.experimental.serve.exceptions import RayServeException
from ray.experimental.serve.constants import DEFAULT_HTTP_ADDRESS
from ray.experimental.serve.request_params import RequestParams, RequestInfo


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

    def _check_slo_ms(self, request_slo_ms):
        if request_slo_ms is not None:
            request_slo_ms = float(request_slo_ms)
            if request_slo_ms < 0:
                raise ValueError(
                    "Request SLO must be positive, it is {}".format(
                        request_slo_ms))
        return request_slo_ms

    def _fix_kwarg_name(self, name):
        if name == "slo_ms":
            return "request_slo_ms"
        return name

    def __init__(self, router_handle, endpoint_name):
        self.router_handle = router_handle
        self.endpoint_name = endpoint_name

    def remote(self, *args, **kwargs):
        if len(args) != 0:
            raise RayServeException(
                "handle.remote must be invoked with keyword arguments.")

        # get request params defaults before enqueuing the query
        default_kwargs = RequestParams.get_default_kwargs()
        request_param_kwargs = {}
        for k in default_kwargs.keys():
            fixed_kwarg_name = self._fix_kwarg_name(k)
            request_param_kwargs[fixed_kwarg_name] = kwargs.pop(
                k, default_kwargs[k])

        try:
            # check if request_slo_ms specified is correct or not
            slo_ms = request_param_kwargs["request_slo_ms"]
            slo_ms = self._check_slo_ms(slo_ms)
            request_param_kwargs["request_slo_ms"] = slo_ms
        except ValueError as e:
            raise RayServeException(str(e))

        # create request parameters required for enqueuing the request
        request_params = RequestParams(self.endpoint_name, TaskContext.Python,
                                       **request_param_kwargs)
        req_info_object_id = self.router_handle.enqueue_request.remote(
            request_params, *args, **kwargs)

        # check if it is necessary to wait for enqueue to be completed
        # NOTE: This will make remote call completely non-blocking for
        #       certain cases.
        if RequestInfo.wait_for_requestInfo(request_params):
            req_info = ray.get(req_info_object_id)
            return_value = tuple(req_info)
            if len(return_value) == 1:
                return return_value[0]
            return return_value

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
