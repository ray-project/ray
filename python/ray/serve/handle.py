import asyncio
import concurrent.futures
from dataclasses import dataclass, field
from typing import Dict, Optional, Union, Coroutine
import threading

from ray.serve.common import EndpointTag
from ray.actor import ActorHandle
from ray.serve.utils import get_random_letters, DEFAULT
from ray.serve.router import Router, RequestMetadata
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


class RayServeHandle:
    """A handle to a service deployment.

    Invoking this deployment with .remote is equivalent to pinging
    an HTTP deployment.

    Example:
       >>> handle = serve_client.get_handle("my_deployment")
       >>> handle
       RayServeSyncHandle(deployment_name="my_deployment")
       >>> handle.remote(my_request_content)
       ObjectRef(...)
       >>> ray.get(handle.remote(...))
       # result
       >>> ray.get(handle.remote(let_it_crash_request))
       # raises RayTaskError Exception

       >>> async_handle = serve_client.get_handle("my_deployment", sync=False)
       >>> async_handle
       RayServeHandle(deployment="my_deployment")
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
            deployment_name: EndpointTag,
            handle_options: Optional[HandleOptions] = None,
            *,
            _router: Optional[Router] = None,
            _internal_pickled_http_request: bool = False,
    ):
        self.controller_handle = controller_handle
        self.deployment_name = deployment_name
        self.handle_options = handle_options or HandleOptions()
        self.handle_tag = f"{self.deployment_name}#{get_random_letters()}"
        self._pickled_http_request = _internal_pickled_http_request

        self.request_counter = metrics.Counter(
            "serve_handle_request_counter",
            description=("The number of handle.remote() calls that have been "
                         "made on this handle."),
            tag_keys=("handle", "deployment"))
        self.request_counter.set_default_tags({
            "handle": self.handle_tag,
            "deployment": self.deployment_name
        })

        self.router: Router = _router or self._make_router()

    def _make_router(self) -> Router:
        return Router(
            self.controller_handle,
            self.deployment_name,
            event_loop=asyncio.get_event_loop(),
        )

    @property
    def is_polling(self) -> bool:
        """Whether this handle is actively polling for replica updates."""
        return self.router.long_poll_client.is_running

    @property
    def is_same_loop(self) -> bool:
        """Whether the caller's asyncio loop is the same loop for handle.

        This is only useful for async handles.
        """
        return asyncio.get_event_loop() == self.router._event_loop

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
            method_name(str): The method to invoke.
            http_method(str): The HTTP method to use for the request.
            shard_key(str): A string to use to deterministically map this
                request to a deployment if there are multiple.
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
            self.deployment_name,
            new_options,
            _router=self.router,
            _internal_pickled_http_request=self._pickled_http_request,
        )

    def _remote(self, deployment_name, handle_options, args,
                kwargs) -> Coroutine:
        request_metadata = RequestMetadata(
            get_random_letters(10),  # Used for debugging.
            deployment_name,
            call_method=handle_options.method_name,
            shard_key=handle_options.shard_key,
            http_method=handle_options.http_method,
            http_headers=handle_options.http_headers,
            http_arg_is_pickled=self._pickled_http_request,
        )
        coro = self.router.assign_request(request_metadata, *args, **kwargs)
        return coro

    async def remote(self, *args, **kwargs):
        """Issue an asynchronous request to the deployment.

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
        return await self._remote(self.deployment_name, self.handle_options,
                                  args, kwargs)

    def __repr__(self):
        return (f"{self.__class__.__name__}"
                f"(deployment='{self.deployment_name}')")

    def __reduce__(self):
        serialized_data = {
            "controller_handle": self.controller_handle,
            "deployment_name": self.deployment_name,
            "handle_options": self.handle_options,
            "_internal_pickled_http_request": self._pickled_http_request,
        }
        return lambda kwargs: RayServeHandle(**kwargs), (serialized_data, )

    def __getattr__(self, name):
        return self.options(method_name=name)


class RayServeSyncHandle(RayServeHandle):
    @property
    def is_same_loop(self) -> bool:
        # NOTE(simon): For sync handle, the caller doesn't have to be in the
        # same loop as the handle's loop, so we always return True here.
        return True

    def _make_router(self) -> Router:
        # Delayed import because ray.serve.api depends on handles.
        return Router(
            self.controller_handle,
            self.deployment_name,
            event_loop=create_or_get_async_loop_in_thread(),
        )

    def remote(self, *args, **kwargs):
        """Issue an asynchronous request to the deployment.

        Returns a Ray ObjectRef whose results can be waited for or retrieved
        using ray.wait or ray.get (or ``await object_ref``), respectively.

        Returns:
            ray.ObjectRef
        Args:
            request_data(dict, Any): If it's a dictionary, the data will be
                available in ``request.json()`` or ``request.form()``.
                If it's a Starlette Request object, it will be passed in to the
                handler directly, unmodified. Otherwise, the data will be
                available in ``request.data``.
            ``**kwargs``: All keyword arguments will be available in
                ``request.args``.
        """
        self.request_counter.inc()
        coro = self._remote(self.deployment_name, self.handle_options, args,
                            kwargs)
        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            coro, self.router._event_loop)
        return future.result()

    def __reduce__(self):
        serialized_data = {
            "controller_handle": self.controller_handle,
            "deployment_name": self.deployment_name,
            "handle_options": self.handle_options,
            "_internal_pickled_http_request": self._pickled_http_request,
        }
        return lambda kwargs: RayServeSyncHandle(**kwargs), (serialized_data, )
