import ray
from ray.experimental import serve


class RayServeHandle:
    """A handle to service endpoint.

    Invoking this endpoint with .remote is equivalent to pinging
    a HTTP endpoint.

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

    def remote(self, *args):
        result_oid_bytes = ray.get(
            self.router_handle.produce.remote(self.endpoint_name, *args))
        return ray.ObjectID(result_oid_bytes)

    def get_traffic_policy(self):
        # Currently checks global state because we are sure handle and
        # global_state are in the same process.
        history = serve.global_state.policy_action_history[self.endpoint_name]
        if len(history):
            return history[-1]
        else:
            return None

    def get_http_endpoint(self):
        return serve.global_state.http_address

    def __repr__(self):
        return f"""
RayServeHandle(
    Endpoint="{self.endpoint_name}",
    URL="{self.get_http_endpoint()}/{self.endpoint_name},
    Traffic={self.get_traffic_policy()}
)
"""
