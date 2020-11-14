import asyncio
import threading
from typing import Coroutine, Optional, Dict, Any, Union
from contextlib import contextmanager

from ray.serve.context import TaskContext
from ray.serve.router import RequestMetadata, Router
from ray.serve.utils import get_random_letters

global_async_loop = None


@contextmanager
def reset_event_loop_policy():
    policy = asyncio.get_event_loop_policy()
    asyncio.set_event_loop_policy(None)
    yield
    asyncio.set_event_loop_policy(policy)


def create_or_get_async_loop_in_thread():
    global global_async_loop
    if global_async_loop is None:
        # We have to asyncio's loop because uvloop will segfault when there
        # are multiple copies of loop running.
        with reset_event_loop_policy():
            global_async_loop = asyncio.new_event_loop()
        thread = threading.Thread(
            daemon=True,
            target=global_async_loop.run_forever,
        )
        thread.start()
    return global_async_loop


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
            controller_handle,
            endpoint_name,
            sync: bool,
            *,
            method_name=None,
            shard_key=None,
            http_method=None,
            http_headers=None,
    ):
        self.controller_handle = controller_handle
        self.endpoint_name = endpoint_name

        self.method_name = method_name
        self.shard_key = shard_key
        self.http_method = http_method
        self.http_headers = http_headers

        self.router = Router(self.controller_handle)
        self.sync = sync
        if self.sync:
            self.async_loop = create_or_get_async_loop_in_thread()
            asyncio.run_coroutine_threadsafe(
                self.router.setup_in_async_loop(),
                self.async_loop,
            )
        else:  # async
            self.async_loop = asyncio.get_event_loop()
            self.async_loop.create_task(self.router.setup_in_async_loop())

    def _remote(self, request_data, kwargs) -> Coroutine:
        request_metadata = RequestMetadata(
            get_random_letters(10),  # Used for debugging.
            self.endpoint_name,
            TaskContext.Python,
            call_method=self.method_name or "__call__",
            shard_key=self.shard_key,
            http_method=self.http_method or "GET",
            http_headers=self.http_headers or dict(),
        )
        coro = self.router.enqueue_request(request_metadata, request_data,
                                           **kwargs)
        return coro

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
        assert self.sync, "handle.remote() should be called from sync handle."
        coro = self._remote(request_data, kwargs)
        future = asyncio.run_coroutine_threadsafe(coro, self.async_loop)
        return future.result()

    async def remote_async(self, request_data, **kwargs):
        assert not self.sync, "handle.remote_async() must be called from async context."
        return await self._remote(request_data, kwargs)

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
        # Don't override default non-null values.
        self.method_name = self.method_name or method_name
        self.shard_key = self.shard_key or shard_key
        self.http_method = self.http_method or http_method
        self.http_headers = self.http_headers or http_headers
        return self

    def __repr__(self):
        return f"RayServeHandle(endpoint='{self.endpoint_name}')"
