import asyncio
import concurrent.futures
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union
from enum import Enum

from ray.serve.utils import get_random_letters
from ray.util import metrics


@dataclass(frozen=True)
class HandleOptions:
    """Options for each ServeHandle instances. These fields are immutable."""
    method_name: str = "__call__"
    shard_key: Optional[str] = None
    http_method: str = "GET"
    http_headers: Dict[str, str] = field(default_factory=dict)


# Use a global singleton enum to emulate default options. We cannot use None
# for those option because None is a valid new value.
class DEFAULT(Enum):
    VALUE = 1


class RayServeHandle:
    """A handle to a service endpoint.

    Invoking this endpoint with .remote is equivalent to pinging
    an HTTP endpoint.

    Example:
       >>> handle = serve_client.get_handle("my_endpoint")
       >>> handle
       RayServeSyncHandle(endpoint="my_endpoint")
       >>> handle.remote(my_request_content)
       ObjectRef(...)
       >>> ray.get(handle.remote(...))
       # result
       >>> ray.get(handle.remote(let_it_crash_request))
       # raises RayTaskError Exception

       >>> async_handle = serve_client.get_handle("my_endpoint", sync=False)
       >>> async_handle
       RayServeHandle(endpoint="my_endpoint")
       >>> await async_handle.remote(my_request_content)
       ObjectRef(...)
       >>> ray.get(await async_handle.remote(...))
       # result
       >>> ray.get(await async_handle.remote(let_it_crash_request))
       # raises RayTaskError Exception
    """

    def __init__(
            self,
            router,  # ThreadProxiedRouter
            endpoint_name,
            handle_options: Optional[HandleOptions] = None):
        self.router = router
        self.endpoint_name = endpoint_name
        self.handle_options = handle_options or HandleOptions()
        self.handle_tag = f"{self.endpoint_name}#{get_random_letters()}"

        self.request_counter = metrics.Count(
            "serve_handle_request_counter",
            description=("The number of handle.remote() calls that have been "
                         "made on this handle."),
            tag_keys=("handle", "endpoint"))
        self.request_counter.set_default_tags({
            "handle": self.handle_tag,
            "endpoint": self.endpoint_name
        })

    def options(self,
                *,
                method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
                shard_key: Union[str, DEFAULT] = DEFAULT.VALUE,
                http_method: Union[str, DEFAULT] = DEFAULT.VALUE,
                http_headers: Union[Dict[str, str], DEFAULT] = DEFAULT.VALUE):
        """Set options for this handle.

        Args:
            method_name(str): The method to invoke on the backend.
            http_method(str): The HTTP method to use for the request.
            shard_key(str): A string to use to deterministically map this
                request to a backend if there are multiple for this endpoint.
        """
        new_options_dict = self.handle_options.__dict__.copy()
        user_modified_options_dict = {
            key: value
            for key, value in
            zip(["method_name", "shard_key", "http_method", "http_headers"],
                [method_name, shard_key, http_method, http_headers])
            if value != DEFAULT.VALUE
        }
        new_options_dict.update(user_modified_options_dict)
        new_options = HandleOptions(**new_options_dict)

        return self.__class__(self.router, self.endpoint_name, new_options)

    async def remote(self,
                     request_data: Optional[Union[Dict, Any]] = None,
                     **kwargs):
        """Issue an asynchronous request to the endpoint.

        Returns a Ray ObjectRef whose results can be waited for or retrieved
        using ray.wait or ray.get (or ``await object_ref``), respectively.

        Returns:
            ray.ObjectRef
        Args:
            request_data(dict, Any): If it's a dictionary, the data will be
                available in ``request.json()`` or ``request.form()``.
                Otherwise, it will be available in ``request.body()``.
            ``**kwargs``: All keyword arguments will be available in
                ``request.query_params``.
        """
        self.request_counter.record(1)
        return await self.router._remote(
            self.endpoint_name, self.handle_options, request_data, kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(endpoint='{self.endpoint_name}')"

    def __reduce__(self):
        deserializer = RayServeHandle
        serialized_data = (self.router, self.endpoint_name,
                           self.handle_options)
        return deserializer, serialized_data


class RayServeSyncHandle(RayServeHandle):
    def remote(self, request_data: Optional[Union[Dict, Any]] = None,
               **kwargs):
        """Issue an asynchronous request to the endpoint.

        Returns a Ray ObjectRef whose results can be waited for or retrieved
        using ray.wait or ray.get (or ``await object_ref``), respectively.

        Returns:
            ray.ObjectRef
        Args:
            request_data(dict, Any): If it's a dictionary, the data will be
                available in ``request.json()`` or ``request.form()``.
                If it's a Starlette Request object, it will be passed in to the
                backend directly, unmodified. Otherwise, the data will be
                available in ``request.data``.
            ``**kwargs``: All keyword arguments will be available in
                ``request.args``.
        """
        self.request_counter.record(1)
        coro = self.router._remote(self.endpoint_name, self.handle_options,
                                   request_data, kwargs)
        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            coro, self.router.async_loop)
        return future.result()

    def __reduce__(self):
        deserializer = RayServeSyncHandle
        serialized_data = (self.router, self.endpoint_name,
                           self.handle_options)
        return deserializer, serialized_data
