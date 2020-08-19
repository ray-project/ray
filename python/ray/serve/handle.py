import ray
from ray import serve
from ray.serve.context import TaskContext
from ray.serve.exceptions import RayServeException
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
       ObjectRef(...)
       >>> ray.get(handle.remote(...))
       # result
       >>> ray.get(handle.remote(let_it_crash_request))
       # raises RayTaskError Exception
    """

    def __init__(
            self,
            router_handle,
            endpoint_name,
            method_name=None,
            shard_key=None,
    ):
        self.router_handle = router_handle
        self.endpoint_name = endpoint_name
        self.method_name = method_name
        self.shard_key = shard_key

    def remote(self, *args, **kwargs):
        if len(args) != 0:
            raise RayServeException(
                "handle.remote must be invoked with keyword arguments.")

        method_name = self.method_name
        if method_name is None:
            method_name = "__call__"

        # create RequestMetadata instance
        request_in_object = RequestMetadata(
            self.endpoint_name,
            TaskContext.Python,
            call_method=method_name,
            shard_key=self.shard_key,
        )
        return self.router_handle.enqueue_request.remote(
            request_in_object, **kwargs)

    def options(self, method_name=None, shard_key=None):

        # Don't override existing method
        if method_name is None and self.method_name is not None:
            method_name = self.method_name

        if shard_key is None and self.shard_key is not None:
            shard_key = self.shard_key

        return RayServeHandle(
            self.router_handle,
            self.endpoint_name,
            method_name=method_name,
            shard_key=shard_key,
        )

    def get_traffic_policy(self):
        controller = serve.api._get_controller()
        return ray.get(
            controller.get_traffic_policy.remote(self.endpoint_name))

    def __repr__(self):
        return """
RayServeHandle(
    Endpoint="{endpoint_name}",
    Traffic={traffic_policy}
)
""".format(
            endpoint_name=self.endpoint_name,
            traffic_policy=self.get_traffic_policy(),
        )
