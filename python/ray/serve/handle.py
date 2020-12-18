import asyncio
import concurrent.futures
from dataclasses import dataclass, field
from typing import Any, Coroutine, Dict, Optional, Type, TypeVar, Union

import ray
from ray.serve.context import TaskContext
from ray.serve.router import RequestMetadata, Router
from ray.serve.utils import get_random_letters
from ray.serve.exceptions import RayServeException


@dataclass
class HandleOptions:
    method_name: str = "__call__"
    shard_key: Optional[str] = None
    http_method: str = "GET"
    http_headers: Dict[str, str] = field(default_factory=dict)


# Use a global singleton
class Default(object):
    pass


DEFAULT_VALUE = Default()


class RayServeHandle:
    """A handle to a service endpoint.

    Invoking this endpoint with .remote is equivalent to pinging
    an HTTP endpoint.

    Example:
       >>> handle = serve_client.get_handle("my_endpoint")
       >>> handle
       RayServeHandle(endpoint="my_endpoint")
       >>> await handle.remote(my_request_content)
       ObjectRef(...)
       >>> ray.get(await handle.remote(...))
       # result
       >>> ray.get(await handle.remote(let_it_crash_request))
       # raises RayTaskError Exception
    """

    def __init__(self,
                 router,
                 endpoint_name,
                 handle_options: Optional[HandleOptions] = None):
        self.router = router
        self.endpoint_name = endpoint_name
        self.handle_options = handle_options or HandleOptions()

    def options(self,
                *,
                method_name: Union[str, Default] = DEFAULT_VALUE,
                shard_key: Union[str, Default] = DEFAULT_VALUE,
                http_method: Union[str, Default] = DEFAULT_VALUE,
                http_headers: Union[Dict[str, str], Default] = DEFAULT_VALUE):
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
            if value is not DEFAULT_VALUE
        }
        new_options_dict.update(user_modified_options_dict)
        new_options = HandleOptions(**new_options_dict)

        return self.__class__(self.router, self.endpoint_name, new_options)

    async def remote(self,
                     request_data: Optional[Union[Dict, Any]] = None,
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
        return await self.router._remote(
            self.endpoint_name, self.handle_options, request_data, kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(endpoint='{self.endpoint_name}')"


class RayServeSyncHandle(RayServeHandle):
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
        coro = self.router._remote(self.endpoint_name, self.handle_options,
                                   request_data, kwargs)
        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            coro, self.router.async_loop)
        return future.result()
