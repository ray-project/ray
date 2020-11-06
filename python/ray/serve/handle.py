import asyncio
from typing import Optional, Dict, Any, Union

import ray
from ray.serve.context import TaskContext
from ray.serve.router import RequestMetadata


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
            *,
            method_name=None,
            shard_key=None,
            http_method=None,
            http_headers=None,
    ):
        self.router_handle = router_handle
        self.endpoint_name = endpoint_name

        self.method_name = method_name
        self.shard_key = shard_key
        self.http_method = http_method
        self.http_headers = http_headers

        from ray import serve
        self.controller = serve.connect()._controller
        # asyncio.get_event_loop().create_task(self.pull_state())

        self.snapshot_id = None
        self.pull_state_sync_future = None

    async def pull_state(self):
        snapshot_id = None
        while True:
            workers, snapshot_id = await self.controller.long_pull_state.remote(
                "workers", snapshot_id)
            print(workers, snapshot_id)

    def pull_state_sync(self):
        if self.pull_state_sync_future is None:
            self.pull_state_sync_future = self.controller.long_pull_state.remote(
                "workers", self.snapshot_id)
        done, not_done = ray.wait(
            [self.pull_state_sync_future], num_returns=1, timeout=0)
        if self.pull_state_sync_future in done:
            workers, self.snapshot_id = ray.get(self.pull_state_sync_future)
            print(workers, self.snapshot_id)
            self.pull_state_sync_future = self.controller.long_pull_state.remote(
                "workers", self.snapshot_id)
        else:
            print("not done")

    def remote(self, request_data: Optional[Union[Dict, Any]] = None,
               **kwargs):
        """Issue an asynchrounous request to the endpoint.

        Returns a Ray ObjectRef whose results can be waited for or retrieved
        using ray.wait or ray.get, respectively.

        Returns:
            ray.ObjectRef
        Args:
            request_data(dict, Any): If it's a dictionary, the data will be
                available in ``request.json()`` or ``request.form()``.
                Otherwise, it will be available in ``request.data``.
            ``**kwargs``: All keyword arguments will be available in
                ``request.args``.
        """
        request_metadata = RequestMetadata(
            self.endpoint_name,
            TaskContext.Python,
            call_method=self.method_name or "__call__",
            shard_key=self.shard_key,
            http_method=self.http_method or "GET",
            http_headers=self.http_headers or dict(),
        )
        return self.router_handle.enqueue_request.remote(
            request_metadata, request_data, **kwargs)

    def options(self,
                method_name: Optional[str] = None,
                *,
                shard_key: Optional[str] = None,
                http_method: Optional[str] = None,
                http_headers: Optional[Dict[str, str]] = None):
        """Set options for this handle.

        Args:
            method_name(str): The method to invoke on the backend.
            http_method(str): The HTTP method to use for the request.
            shard_key(str): A string to use to deterministically map this
                request to a backend if there are multiple for this endpoint.
        """
        return RayServeHandle(
            self.router_handle,
            self.endpoint_name,
            # Don't override existing method
            method_name=self.method_name or method_name,
            shard_key=self.shard_key or shard_key,
            http_method=self.http_method or http_method,
            http_headers=self.http_headers or http_headers,
        )

    def __repr__(self):
        return f"RayServeHandle(endpoint='{self.endpoint_name}')"
