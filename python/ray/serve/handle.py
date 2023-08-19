import asyncio
import concurrent.futures
from dataclasses import dataclass, asdict
from functools import wraps
import inspect
import threading
from typing import Coroutine, Optional, Union

import ray
from ray._private.utils import get_or_create_event_loop

from ray import serve
from ray.serve._private.common import EndpointTag, RequestProtocol
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_NEW_ROUTING,
)
from ray.serve._private.utils import (
    get_random_letters,
    DEFAULT,
)
from ray.serve._private.router import Router, RequestMetadata
from ray.util import metrics
from ray.util.annotations import Deprecated, PublicAPI

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


@dataclass(frozen=True)
class _HandleOptions:
    """Options for each ServeHandle instance.

    These fields can be changed by calling `.options()` on a handle.
    """

    method_name: str = "__call__"
    multiplexed_model_id: str = ""
    stream: bool = False
    _router_cls: str = ""
    _request_protocol: str = RequestProtocol.UNDEFINED

    def copy_and_update(
        self,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _router_cls: Union[str, DEFAULT] = DEFAULT.VALUE,
    ) -> "_HandleOptions":
        return _HandleOptions(
            method_name=(
                self.method_name if method_name == DEFAULT.VALUE else method_name
            ),
            multiplexed_model_id=(
                self.multiplexed_model_id
                if multiplexed_model_id == DEFAULT.VALUE
                else multiplexed_model_id
            ),
            stream=self.stream if stream == DEFAULT.VALUE else stream,
            _router_cls=self._router_cls
            if _router_cls == DEFAULT.VALUE
            else _router_cls,
            _request_protocol=self._request_protocol,
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
        deployment_name: str,
        app_name: str,
        *,
        handle_options: Optional[_HandleOptions] = None,
        _router: Optional[Router] = None,
        _request_counter: Optional[metrics.Counter] = None,
    ):
        self.deployment_id = EndpointTag(deployment_name, app_name)
        self.handle_options = handle_options or _HandleOptions()

        self.request_counter = _request_counter or metrics.Counter(
            "serve_handle_request_counter",
            description=(
                "The number of handle.remote() calls that have been "
                "made on this handle."
            ),
            tag_keys=("handle", "deployment", "route", "application"),
        )
        if app_name:
            handle_tag = f"{app_name}#{deployment_name}#{get_random_letters()}"
        else:
            handle_tag = f"{deployment_name}#{get_random_letters()}"

        # TODO(zcin): Separate deployment_id into deployment and application tags
        self.request_counter.set_default_tags(
            {"handle": handle_tag, "deployment": str(self.deployment_id)}
        )

        self._router: Optional[Router] = _router

    def _set_request_protocol(self, request_protocol: RequestProtocol):
        self.handle_options = _HandleOptions(
            **{**asdict(self.handle_options), **{"_request_protocol": request_protocol}}
        )

    def _get_or_create_router(self) -> Router:
        if self._router is None:
            self._router = Router(
                serve.context.get_global_client()._controller,
                self.deployment_id,
                ray.get_runtime_context().get_node_id(),
                event_loop=get_or_create_event_loop(),
                _use_new_routing=RAY_SERVE_ENABLE_NEW_ROUTING,
                _router_cls=self.handle_options._router_cls,
            )

        return self._router

    @property
    def deployment_name(self) -> str:
        return self.deployment_id.name

    @property
    def app_name(self) -> str:
        return self.deployment_id.app

    @property
    def _is_same_loop(self) -> bool:
        """Whether the caller's asyncio loop is the same loop for handle.

        This is only useful for async handles.
        """
        return get_or_create_event_loop() == self._get_or_create_router()._event_loop

    def _options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _router_cls: Union[str, DEFAULT] = DEFAULT.VALUE,
    ):
        new_handle_options = self.handle_options.copy_and_update(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
            _router_cls=_router_cls,
        )

        if self._router is None and _router_cls == DEFAULT.VALUE:
            self._get_or_create_router()

        return self.__class__(
            self.deployment_name,
            self.app_name,
            handle_options=new_handle_options,
            _router=None if _router_cls != DEFAULT.VALUE else self._router,
            _request_counter=self.request_counter,
        )

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _router_cls: Union[str, DEFAULT] = DEFAULT.VALUE,
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
            _router_cls=_router_cls,
        )

    def _remote(self, deployment_id, handle_options, args, kwargs) -> Coroutine:
        _request_context = ray.serve.context._serve_request_context.get()
        request_metadata = RequestMetadata(
            _request_context.request_id,
            deployment_id.name,
            call_method=handle_options.method_name,
            route=_request_context.route,
            app_name=_request_context.app_name,
            multiplexed_model_id=handle_options.multiplexed_model_id,
            is_streaming=handle_options.stream,
            _request_protocol=handle_options._request_protocol,
        )
        self.request_counter.inc(
            tags={
                "route": _request_context.route,
                "application": _request_context.app_name,
            }
        )
        return self._get_or_create_router().assign_request(
            request_metadata, *args, **kwargs
        )

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
        return await self._remote(self.deployment_id, self.handle_options, args, kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}" f"(deployment='{self.deployment_id}')"

    @classmethod
    def _deserialize(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(**kwargs)

    def __reduce__(self):
        serialized_data = {
            "deployment_name": self.deployment_name,
            "app_name": self.app_name,
            "handle_options": self.handle_options,
        }
        return RayServeHandle._deserialize, (serialized_data,)

    def __getattr__(self, name):
        return self.options(method_name=name)

    def shutdown(self):
        if self._router:
            self._router.shutdown()


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

    def _get_or_create_router(self) -> Router:
        if self._router is None:
            self._router = Router(
                serve.context.get_global_client()._controller,
                self.deployment_id,
                ray.get_runtime_context().get_node_id(),
                event_loop=_create_or_get_async_loop_in_thread(),
                _use_new_routing=RAY_SERVE_ENABLE_NEW_ROUTING,
                _router_cls=self.handle_options._router_cls,
            )

        return self._router

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _router_cls: Union[str, DEFAULT] = DEFAULT.VALUE,
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
            _router_cls=_router_cls,
        )

    def remote(self, *args, **kwargs) -> ray.ObjectRef:
        """Issue an asynchronous request to the __call__ method of the deployment.

        Returns a Ray ObjectRef whose results can be waited for or retrieved
        using ray.wait or ray.get, respectively.

        .. code-block:: python

            obj_ref = handle.remote(*args)
            result = ray.get(obj_ref)

        """
        coro = self._remote(self.deployment_id, self.handle_options, args, kwargs)
        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            coro, self._get_or_create_router()._event_loop
        )
        return future.result()

    def __reduce__(self):
        serialized_data = {
            "deployment_name": self.deployment_name,
            "app_name": self.app_name,
            "handle_options": self.handle_options,
        }
        return RayServeSyncHandle._deserialize, (serialized_data,)


@Deprecated(
    message="RayServeDeploymentHandle is no longer used, use RayServeHandle instead."
)
class RayServeDeploymentHandle(RayServeHandle):
    # We had some examples using this class for type hinting. To avoid breakig them,
    # leave this as an alias.
    pass
