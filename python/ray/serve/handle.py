import asyncio
import concurrent.futures
from dataclasses import dataclass
from typing import Coroutine, Dict, Optional, Union
import threading

from ray.actor import ActorHandle

from ray import serve
from ray.serve.common import EndpointTag
from ray.serve.constants import (
    SERVE_HANDLE_JSON_KEY,
    ServeHandleType,
)
from ray.serve.utils import (
    get_random_letters,
    DEFAULT,
)
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


class RayServeHandle:
    """A handle to a service deployment.

    Invoking this deployment with .remote is equivalent to pinging
    an HTTP deployment.

    Example:
        >>> import ray
        >>> serve_client = ... # doctest: +SKIP
        >>> handle = serve_client.get_handle("my_deployment") # doctest: +SKIP
        >>> handle # doctest: +SKIP
        RayServeSyncHandle(deployment_name="my_deployment")
        >>> my_request_content = ... # doctest: +SKIP
        >>> handle.remote(my_request_content) # doctest: +SKIP
        ObjectRef(...)
        >>> ray.get(handle.remote(...)) # doctest: +SKIP
        # result
        >>> let_it_crash_request = ... # doctest: +SKIP
        >>> ray.get(handle.remote(let_it_crash_request)) # doctest: +SKIP
        # raises RayTaskError Exception
        >>> async_handle = serve_client.get_handle( # doctest: +SKIP
        ...     "my_deployment", sync=False)
        >>> async_handle  # doctest: +SKIP
        RayServeHandle(deployment="my_deployment")
        >>> await async_handle.remote(my_request_content) # doctest: +SKIP
        ObjectRef(...)
        >>> ray.get(await async_handle.remote(...)) # doctest: +SKIP
        # result
        >>> ray.get( # doctest: +SKIP
        ...     await async_handle.remote(let_it_crash_request)
        ... )
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
            description=(
                "The number of handle.remote() calls that have been "
                "made on this handle."
            ),
            tag_keys=("handle", "deployment"),
        )
        self.request_counter.set_default_tags(
            {"handle": self.handle_tag, "deployment": self.deployment_name}
        )

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
    ):
        """Set options for this handle.

        Args:
            method_name(str): The method to invoke.
        """
        new_options_dict = self.handle_options.__dict__.copy()
        user_modified_options_dict = {
            key: value
            for key, value in zip(["method_name"], [method_name])
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

    def _remote(self, deployment_name, handle_options, args, kwargs) -> Coroutine:
        request_metadata = RequestMetadata(
            get_random_letters(10),  # Used for debugging.
            deployment_name,
            call_method=handle_options.method_name,
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
        return await self._remote(
            self.deployment_name, self.handle_options, args, kwargs
        )

    def __repr__(self):
        return f"{self.__class__.__name__}" f"(deployment='{self.deployment_name}')"

    @classmethod
    def _deserialize(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(**kwargs)

    def __reduce__(self):
        serialized_data = {
            "controller_handle": self.controller_handle,
            "deployment_name": self.deployment_name,
            "handle_options": self.handle_options,
            "_internal_pickled_http_request": self._pickled_http_request,
        }
        return RayServeHandle._deserialize, (serialized_data,)

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
        coro = self._remote(self.deployment_name, self.handle_options, args, kwargs)
        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            coro, self.router._event_loop
        )
        return future.result()

    def __reduce__(self):
        serialized_data = {
            "controller_handle": self.controller_handle,
            "deployment_name": self.deployment_name,
            "handle_options": self.handle_options,
            "_internal_pickled_http_request": self._pickled_http_request,
        }
        return RayServeSyncHandle._deserialize, (serialized_data,)


class RayServeLazySyncHandle:
    """Lazily initialized handle that only gets fulfilled upon first execution."""

    def __init__(
        self,
        deployment_name: str,
        handle_options: Optional[HandleOptions] = None,
    ):
        self.deployment_name = deployment_name
        self.handle_options = handle_options or HandleOptions()
        # For Serve DAG we need placeholder in DAG binding and building without
        # requirement of serve.start; Thus handle is fulfilled at runtime.
        self.handle = None

    def options(self, *, method_name: str):
        return self.__class__(
            self.deployment_name, HandleOptions(method_name=method_name)
        )

    def remote(self, *args, **kwargs):
        if not self.handle:
            handle = serve.get_deployment(self.deployment_name).get_handle()
            self.handle = handle.options(method_name=self.handle_options.method_name)
        # TODO (jiaodong): Polish async handles later for serve pipeline
        return self.handle.remote(*args, **kwargs)

    @classmethod
    def _deserialize(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(**kwargs)

    def __reduce__(self):
        serialized_data = {
            "deployment_name": self.deployment_name,
            "handle_options": self.handle_options,
        }
        return RayServeLazySyncHandle._deserialize, (serialized_data,)

    def __getattr__(self, name):
        return self.options(method_name=name)

    def __repr__(self):
        return f"{self.__class__.__name__}" f"(deployment='{self.deployment_name}')"


def serve_handle_to_json_dict(handle: RayServeHandle) -> Dict[str, str]:
    """Converts a Serve handle to a JSON-serializable dictionary.

    The dictionary can be converted back to a ServeHandle using
    serve_handle_from_json_dict.
    """
    if isinstance(handle, RayServeSyncHandle):
        handle_type = ServeHandleType.SYNC
    else:
        handle_type = ServeHandleType.ASYNC

    return {
        SERVE_HANDLE_JSON_KEY: handle_type,
        "deployment_name": handle.deployment_name,
    }


def serve_handle_from_json_dict(d: Dict[str, str]) -> RayServeHandle:
    """Converts a JSON-serializable dictionary back to a ServeHandle.

    The dictionary should be constructed using serve_handle_to_json_dict.
    """
    if SERVE_HANDLE_JSON_KEY not in d:
        raise ValueError(f"dict must contain {SERVE_HANDLE_JSON_KEY} key.")

    return serve.api.internal_get_global_client().get_handle(
        d["deployment_name"],
        sync=d[SERVE_HANDLE_JSON_KEY] == ServeHandleType.SYNC,
        missing_ok=True,
    )
