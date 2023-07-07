import asyncio
import concurrent.futures
from dataclasses import dataclass
from functools import wraps
import inspect
import threading
from typing import Coroutine, Optional, Union

import ray
from ray._private.utils import get_or_create_event_loop
from ray.actor import ActorHandle

from ray import serve
from ray.serve._private.common import EndpointTag
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_NEW_ROUTING,
)
from ray.serve._private.utils import (
    get_random_letters,
    DEFAULT,
)
from ray.serve._private.router import Router, RequestMetadata
from ray.util import metrics
from ray.util.annotations import DeveloperAPI, PublicAPI

_global_async_loop = None


def _wrap_into_async_task(async_func):
    """Wrap an async function so it returns async task instead of coroutine

    This makes the returned value awaitable more than once.
    """
    assert inspect.iscoroutinefunction(async_func)

    @wraps(async_func)
    def wrapper(*args, **kwargs):
        return asyncio.ensure_future(async_func(*args, **kwargs))

    return wrapper


def _create_or_get_async_loop_in_thread():
    global _global_async_loop
    if _global_async_loop is None:
        _global_async_loop = asyncio.new_event_loop()
        thread = threading.Thread(
            daemon=True,
            target=_global_async_loop.run_forever,
        )
        thread.start()
    return _global_async_loop


@PublicAPI(stability="beta")
@dataclass(frozen=True)
class HandleOptions:
    """Options for each ServeHandle instance.

    These fields can be changed by calling `.options()` on a handle.
    """

    method_name: str = "__call__"
    multiplexed_model_id: str = ""
    stream: bool = False

    def copy_and_update(
        self,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
    ) -> "HandleOptions":
        return HandleOptions(
            method_name=(
                self.method_name if method_name == DEFAULT.VALUE else method_name
            ),
            multiplexed_model_id=(
                self.multiplexed_model_id
                if multiplexed_model_id == DEFAULT.VALUE
                else multiplexed_model_id
            ),
            stream=self.stream if stream == DEFAULT.VALUE else stream,
        )


@PublicAPI(stability="beta")
class RayServeHandle:
    """A handle used to make requests from one deployment to another.

    This is used to compose multiple deployments into a single application. After
    building the application, this handle is substituted at runtime for deployments
    passed as arguments via `.bind()`.

    Example:

    .. code-block:: python

        import ray
        from ray import serve
        from ray.serve.handle import RayServeHandle, RayServeSyncHandle

        @serve.deployment
        class Downstream:
            def __init__(self, message: str):
                self._message = message

        def __call__(self, name: str) -> str:
            return self._message + name

        @serve.deployment
        class Ingress:
            def __init__(self, handle: RayServeHandle):
                self._handle = handle

            async def __call__(self, name: str) -> str:
                obj_ref: ray.ObjectRef = await self._handle.remote(name)
                return await obj_ref

        app = Ingress.bind(Downstream.bind("Hello "))
        handle: RayServeSyncHandle = serve.run(app)

        # Prints "Hello Mr. Magoo"
        print(ray.get(handle.remote("Mr. Magoo")))

    """

    def __init__(
        self,
        controller_handle: ActorHandle,
        deployment_name: EndpointTag,
        handle_options: Optional[HandleOptions] = None,
        *,
        _router: Optional[Router] = None,
        _is_for_http_requests: bool = False,
    ):
        self.controller_handle = controller_handle
        self.deployment_name = deployment_name
        self.handle_options = handle_options or HandleOptions()
        self.handle_tag = f"{self.deployment_name}#{get_random_letters()}"
        self._is_for_http_requests = _is_for_http_requests

        self.request_counter = metrics.Counter(
            "serve_handle_request_counter",
            description=(
                "The number of handle.remote() calls that have been "
                "made on this handle."
            ),
            tag_keys=("handle", "deployment", "route", "application"),
        )
        self.request_counter.set_default_tags(
            {"handle": self.handle_tag, "deployment": self.deployment_name}
        )

        self.router: Router = _router or self._make_router()

    def _make_router(self) -> Router:
        return Router(
            self.controller_handle,
            self.deployment_name,
            event_loop=get_or_create_event_loop(),
            _use_new_routing=RAY_SERVE_ENABLE_NEW_ROUTING,
        )

    @property
    def _is_polling(self) -> bool:
        """Whether this handle is actively polling for replica updates."""
        return self.router.long_poll_client.is_running

    @property
    def _is_same_loop(self) -> bool:
        """Whether the caller's asyncio loop is the same loop for handle.

        This is only useful for async handles.
        """
        return get_or_create_event_loop() == self.router._event_loop

    def _options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
    ):
        new_handle_options = self.handle_options.copy_and_update(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
        )
        return self.__class__(
            self.controller_handle,
            self.deployment_name,
            new_handle_options,
            _router=self.router,
            _is_for_http_requests=self._is_for_http_requests,
        )

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
    ) -> "RayServeHandle":
        """Set options for this handle and return an updated copy of it.

        Example:

        .. code-block:: python

            # The following two lines are equivalent:
            obj_ref = await handle.other_method.remote(*args)
            obj_ref = await handle.options(method_name="other_method").remote(*args)
            obj_ref = await handle.options(
                multiplexed_model_id="model:v1").remote(*args)
        """
        return self._options(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
        )

    def _remote(self, deployment_name, handle_options, args, kwargs) -> Coroutine:
        _request_context = ray.serve.context._serve_request_context.get()
        request_metadata = RequestMetadata(
            _request_context.request_id,
            deployment_name,
            call_method=handle_options.method_name,
            is_http_request=self._is_for_http_requests,
            route=_request_context.route,
            app_name=_request_context.app_name,
            multiplexed_model_id=handle_options.multiplexed_model_id,
            is_streaming=handle_options.stream,
        )
        self.request_counter.inc(
            tags={
                "route": _request_context.route,
                "application": _request_context.app_name,
            }
        )
        coro = self.router.assign_request(request_metadata, *args, **kwargs)
        return coro

    @_wrap_into_async_task
    async def remote(self, *args, **kwargs) -> asyncio.Task:
        """Issue an asynchronous request to the __call__ method of the deployment.

        Returns an `asyncio.Task` whose underlying result is a Ray ObjectRef that
        points to the final result of the request.

        The final result can be retrieved by awaiting the ObjectRef.

        Example:

        .. code-block:: python

            obj_ref = await handle.remote(*args)
            result = await obj_ref

        """
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
            "_is_for_http_requests": self._is_for_http_requests,
        }
        return RayServeHandle._deserialize, (serialized_data,)

    def __getattr__(self, name):
        return self.options(method_name=name)


@PublicAPI(stability="beta")
class RayServeSyncHandle(RayServeHandle):
    """A handle used to make requests to the ingress deployment of an application.

    This is returned by `serve.run` and can be used to invoke the application from
    Python rather than over HTTP. For example:

    .. code-block:: python

        import ray
        from ray import serve
        from ray.serve.handle import RayServeSyncHandle

        @serve.deployment
        class Ingress:
            def __call__(self, name: str) -> str:
                return f"Hello {name}"

        app = Ingress.bind()
        handle: RayServeSyncHandle = serve.run(app)

        # Prints "Hello Mr. Magoo"
        print(ray.get(handle.remote("Mr. Magoo")))

    """

    @property
    def _is_same_loop(self) -> bool:
        # NOTE(simon): For sync handle, the caller doesn't have to be in the
        # same loop as the handle's loop, so we always return True here.
        return True

    def _make_router(self) -> Router:
        return Router(
            self.controller_handle,
            self.deployment_name,
            event_loop=_create_or_get_async_loop_in_thread(),
            _use_new_routing=RAY_SERVE_ENABLE_NEW_ROUTING,
        )

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
    ) -> "RayServeSyncHandle":
        """Set options for this handle and return an updated copy of it.

        Example:

        .. code-block:: python

            # The following two lines are equivalent:
            obj_ref = handle.other_method.remote(*args)
            obj_ref = handle.options(method_name="other_method").remote(*args)
            obj_ref = handle.options(multiplexed_model_id="model1").remote(*args)

        """
        return self._options(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
        )

    def remote(self, *args, **kwargs) -> ray.ObjectRef:
        """Issue an asynchronous request to the __call__ method of the deployment.

        Returns a Ray ObjectRef whose results can be waited for or retrieved
        using ray.wait or ray.get, respectively.

        .. code-block:: python

            obj_ref = handle.remote(*args)
            result = ray.get(obj_ref)

        """
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
            "_is_for_http_requests": self._is_for_http_requests,
        }
        return RayServeSyncHandle._deserialize, (serialized_data,)


@DeveloperAPI
class RayServeDeploymentHandle:
    """Send requests to a deployment. This class should not be manually created."""

    # """Lazily initialized handle that only gets fulfilled upon first execution."""
    def __init__(
        self,
        deployment_name: str,
        handle_options: Optional[HandleOptions] = None,
    ):
        self.deployment_name = deployment_name
        self.handle_options = handle_options or HandleOptions()
        # For Serve DAG we need placeholder in DAG binding and building without
        # requirement of serve.start; Thus handle is fulfilled at runtime.
        self.handle: RayServeHandle = None

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
    ) -> "RayServeDeploymentHandle":
        new_handle_options = self.handle_options.copy_and_update(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
        )
        return self.__class__(self.deployment_name, new_handle_options)

    def remote(self, *args, _ray_cache_refs: bool = False, **kwargs) -> asyncio.Task:
        if not self.handle:
            self.handle = (
                serve._private.api.get_deployment(self.deployment_name)
                ._get_handle(sync=False)
                .options(
                    method_name=self.handle_options.method_name,
                    stream=self.handle_options.stream,
                    multiplexed_model_id=self.handle_options.multiplexed_model_id,
                )
            )
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
        return RayServeDeploymentHandle._deserialize, (serialized_data,)

    def __getattr__(self, name):
        return self.options(method_name=name)

    def __repr__(self):
        return f"{self.__class__.__name__}" f"(deployment='{self.deployment_name}')"
