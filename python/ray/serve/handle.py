import asyncio
import concurrent.futures
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union, Coroutine
import threading
from enum import Enum

from ray.serve.common import EndpointTag
from ray.actor import ActorHandle
from ray.serve.utils import get_random_letters
from ray.serve.router import EndpointRouter, RequestMetadata
from ray.util import metrics

_global_async_loop = None


def create_or_get_async_loop_in_thread():
    global _global_async_loop
    if _global_async_loop is None:
        _global_async_loop = asyncio.new_event_loop()
        thread = threading.Thread(
            daemon=True,
            target=_global_async_loop.run_forever,
        )
        thread.start()
    return _global_async_loop


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
            controller_handle: ActorHandle,
            endpoint_name: EndpointTag,
            handle_options: Optional[HandleOptions] = None,
            *,
            known_python_methods: List[str] = [],
            _router: Optional[EndpointRouter] = None,
            _internal_use_serve_request: Optional[bool] = True,
            _internal_pickled_http_request: bool = False,
    ):
        self.controller_handle = controller_handle
        self.endpoint_name = endpoint_name
        self.handle_options = handle_options or HandleOptions()
        self.known_python_methods = known_python_methods
        self.handle_tag = f"{self.endpoint_name}#{get_random_letters()}"
        self._use_serve_request = _internal_use_serve_request
        self._pickled_http_request = _internal_pickled_http_request

        self.request_counter = metrics.Counter(
            "serve_handle_request_counter",
            description=("The number of handle.remote() calls that have been "
                         "made on this handle."),
            tag_keys=("handle", "endpoint"))
        self.request_counter.set_default_tags({
            "handle": self.handle_tag,
            "endpoint": self.endpoint_name
        })

        self.router: EndpointRouter = _router or self._make_router()

    def _make_router(self) -> EndpointRouter:
        return EndpointRouter(
            self.controller_handle,
            self.endpoint_name,
            asyncio.get_event_loop(),
        )

    def options(
            self,
            *,
            method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
            shard_key: Union[str, DEFAULT] = DEFAULT.VALUE,
            http_method: Union[str, DEFAULT] = DEFAULT.VALUE,
            http_headers: Union[Dict[str, str], DEFAULT] = DEFAULT.VALUE,
    ):
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

        return self.__class__(
            self.controller_handle,
            self.endpoint_name,
            new_options,
            _router=self.router,
            _internal_use_serve_request=self._use_serve_request,
            _internal_pickled_http_request=self._pickled_http_request,
        )

    def _remote(self, endpoint_name, handle_options, args,
                kwargs) -> Coroutine:
        request_metadata = RequestMetadata(
            get_random_letters(10),  # Used for debugging.
            endpoint_name,
            call_method=handle_options.method_name,
            shard_key=handle_options.shard_key,
            http_method=handle_options.http_method,
            http_headers=handle_options.http_headers,
            use_serve_request=self._use_serve_request,
            http_arg_is_pickled=self._pickled_http_request,
        )
        coro = self.router.assign_request(request_metadata, *args, **kwargs)
        return coro

    async def remote(self, *args, **kwargs):
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
        self.request_counter.inc()
        return await self._remote(self.endpoint_name, self.handle_options,
                                  args, kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(endpoint='{self.endpoint_name}')"

    def __reduce__(self):
        serialized_data = {
            "controller_handle": self.controller_handle,
            "endpoint_name": self.endpoint_name,
            "handle_options": self.handle_options,
            "known_python_methods": self.known_python_methods,
            "_internal_use_serve_request": self._use_serve_request,
            "_internal_pickled_http_request": self._pickled_http_request,
        }
        return lambda kwargs: RayServeHandle(**kwargs), (serialized_data, )

    def __getattr__(self, name):
        if name not in self.known_python_methods:
            raise AttributeError(
                f"ServeHandle for endpoint {self.endpoint_name} doesn't have "
                f"python method {name}. Please check all Python methods via "
                "`serve.list_endpoints()`. If you used the "
                f"get_handle('{self.endpoint_name}', missing_ok=True) flag, "
                f"Serve cannot know all methods for {self.endpoint_name}. "
                "You can set the method manually via "
                f"handle.options(method_name='{name}').remote().")

        return self.options(method_name=name)


class RayServeSyncHandle(RayServeHandle):
    def _make_router(self) -> EndpointRouter:
        # Delayed import because ray.serve.api depends on handles.
        return EndpointRouter(
            self.controller_handle,
            self.endpoint_name,
            create_or_get_async_loop_in_thread(),
        )

    def remote(self, *args, **kwargs):
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
        self.request_counter.inc()
        coro = self._remote(self.endpoint_name, self.handle_options, args,
                            kwargs)
        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            coro, self.router._loop)
        return future.result()

    def __reduce__(self):
        serialized_data = {
            "controller_handle": self.controller_handle,
            "endpoint_name": self.endpoint_name,
            "handle_options": self.handle_options,
            "known_python_methods": self.known_python_methods,
            "_internal_use_serve_request": self._use_serve_request,
            "_internal_pickled_http_request": self._pickled_http_request,
        }
        return lambda kwargs: RayServeSyncHandle(**kwargs), (serialized_data, )
