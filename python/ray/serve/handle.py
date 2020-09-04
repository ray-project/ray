from typing import Optional

import ray
from ray import serve
from ray.serve.context import TaskContext
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
            http_method=None,
            method_name=None,
            shard_key=None,
    ):
        self.router_handle = router_handle
        self.endpoint_name = endpoint_name
        self.http_method = http_method
        self.method_name = method_name
        self.shard_key = shard_key

    def remote(self, *args, **kwargs):
        if len(args) > 0:
            raise ValueError(
                "handle.remote must be invoked with keyword arguments.")
        request_metadata = RequestMetadata(
            self.endpoint_name,
            TaskContext.Python,
            http_method=self.http_method or "GET",
            call_method=self.method_name or "__call__",
            shard_key=self.shard_key,
        )
        return self.router_handle.enqueue_request.remote(
            request_metadata, **kwargs)

    def options(self,
                method_name: Optional[str] = None,
                http_method: Optional[str] = None,
                shard_key: Optional[str] = None):
        return RayServeHandle(
            self.router_handle,
            self.endpoint_name,
            # Don't override existing method
            http_method=self.http_method or http_method,
            method_name=self.method_name or method_name,
            shard_key=self.shard_key or shard_key,
        )

    def _get_traffic_policy(self):
        controller = serve.api._get_controller()
        return ray.get(
            controller.get_traffic_policy.remote(self.endpoint_name))

    def __repr__(self):
        return (f"RayServeHandle(Endpoint='{self.endpoint_name}', "
                f"Traffic={self._get_traffic_policy()})")
