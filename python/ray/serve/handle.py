import asyncio
import concurrent.futures
from dataclasses import dataclass
import threading
from typing import Any, AsyncIterator, Coroutine, Iterator, Optional, Union

import ray
from ray._private.utils import get_or_create_event_loop
from ray._raylet import StreamingObjectRefGenerator

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
from ray.util.annotations import Deprecated, PublicAPI

_global_async_loop = None


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
    _router_cls: str = ""

    def copy_and_update(
        self,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _router_cls: Union[str, DEFAULT] = DEFAULT.VALUE,
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
            _router_cls=self._router_cls
            if _router_cls == DEFAULT.VALUE
            else _router_cls,
        )


class DeploymentHandleBase:
    def __init__(
        self,
        deployment_name: EndpointTag,
        *,
        handle_options: Optional[HandleOptions] = None,
        _router: Optional[Router] = None,
        _is_for_http_requests: bool = False,
        _is_for_sync_context: bool = False,
    ):
        self.deployment_name = deployment_name
        self.handle_options = handle_options or HandleOptions()
        self._is_for_http_requests = _is_for_http_requests
        self._is_for_sync_context = _is_for_sync_context

        self.request_counter = metrics.Counter(
            "serve_handle_request_counter",
            description=(
                "The number of handle.remote() calls that have been "
                "made on this handle."
            ),
            tag_keys=("handle", "deployment", "route", "application"),
        )
        handle_tag = f"{self.deployment_name}#{get_random_letters()}"
        self.request_counter.set_default_tags(
            {"handle": handle_tag, "deployment": self.deployment_name}
        )

        self._router: Optional[Router] = _router

    def _get_or_create_router(self) -> Router:
        if self._router is None:
            if self._is_for_sync_context:
                event_loop = _create_or_get_async_loop_in_thread()
            else:
                event_loop = get_or_create_event_loop()

            self._router = Router(
                serve.context.get_global_client()._controller,
                self.deployment_name,
                event_loop=event_loop,
                _use_new_routing=RAY_SERVE_ENABLE_NEW_ROUTING,
                _router_cls=self.handle_options._router_cls,
            )

        return self._router

    @property
    def _is_same_loop(self) -> bool:
        """Whether the caller's asyncio loop is the same loop for handle.

        This is only useful for async handles.
        """
        if self._is_for_sync_context:
            return True

        return get_or_create_event_loop() == self._get_or_create_router()._event_loop

    def _options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        use_new_handle_api: Union[bool, DEFAULT] = DEFAULT.VALUE,
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

        if use_new_handle_api is True:
            cls = DeploymentHandle
        else:
            cls = self.__class__

        return cls(
            self.deployment_name,
            handle_options=new_handle_options,
            _router=None if _router_cls != DEFAULT.VALUE else self._router,
            _is_for_http_requests=self._is_for_http_requests,
            _is_for_sync_context=self._is_for_sync_context,
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
        return self._get_or_create_router().assign_request(
            request_metadata, *args, **kwargs
        )

    def __getattr__(self, name):
        return self.options(method_name=name)

    def shutdown(self):
        if self._router:
            self._router.shutdown()

    def __repr__(self):
        return f"{self.__class__.__name__}" f"(deployment='{self.deployment_name}')"

    @classmethod
    def _deserialize(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(**kwargs)

    def __reduce__(self):
        serialized_constructor_args = {
            "deployment_name": self.deployment_name,
            "handle_options": self.handle_options,
            "_is_for_http_requests": self._is_for_http_requests,
            "_is_for_sync_context": self._is_for_sync_context,
        }
        return self.__class__._deserialize, (serialized_constructor_args,)


@PublicAPI(stability="beta")
class RayServeHandle(DeploymentHandleBase):
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

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        use_new_handle_api: Union[bool, DEFAULT] = DEFAULT.VALUE,
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
            use_new_handle_api=use_new_handle_api,
            _router_cls=_router_cls,
        )

    def remote(self, *args, **kwargs) -> asyncio.Task:
        """Issue an asynchronous request to the __call__ method of the deployment.

        Returns an `asyncio.Task` whose underlying result is a Ray ObjectRef that
        points to the final result of the request.

        The final result can be retrieved by awaiting the ObjectRef.

        Example:

        .. code-block:: python

            obj_ref = await handle.remote(*args)
            result = await obj_ref

        """
        loop = self._get_or_create_router()._event_loop
        result_coro = self._remote(
            self.deployment_name, self.handle_options, args, kwargs
        )
        return asyncio.ensure_future(result_coro, loop=loop)


@PublicAPI(stability="beta")
class RayServeSyncHandle(DeploymentHandleBase):
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

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        use_new_handle_api: Union[bool, DEFAULT] = DEFAULT.VALUE,
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
            use_new_handle_api=use_new_handle_api,
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
        coro = self._remote(self.deployment_name, self.handle_options, args, kwargs)
        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            coro, self._get_or_create_router()._event_loop
        )
        return future.result()


@Deprecated(
    message="RayServeDeploymentHandle is no longer used, use RayServeHandle instead."
)
class RayServeDeploymentHandle(RayServeHandle):
    # We had some examples using this class for type hinting. To avoid breaking them,
    # leave this as an alias.
    pass


class DeploymentHandleResultBase:
    def __init__(
        self,
        assign_request_coro: Coroutine,
        loop: asyncio.AbstractEventLoop,
    ):
        self._assign_request_task = loop.create_task(assign_request_coro)
        self._loop = loop

    def cancel(self):
        # TODO(edoakes): when actor task cancellation is supported, we should cancel
        # the scheduled actor task here if the assign request task is done.
        self._assign_request_task.cancel()

    async def _to_obj_ref_or_gen(
        self,
    ) -> Union[ray.ObjectRef, StreamingObjectRefGenerator]:
        return await self._assign_request_task

    def _to_obj_ref_or_gen_sync(
        self,
    ) -> Union[ray.ObjectRef, StreamingObjectRefGenerator]:
        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            self._to_obj_ref_or_gen(), self._loop
        )
        return future.result()


class DeploymentHandleRef(DeploymentHandleResultBase):
    def __await__(self):
        obj_ref = yield from self._assign_request_task.__await__()
        result = yield from obj_ref.__await__()
        return result

    def result(self, timeout_s: Optional[float] = None) -> Any:
        return ray.get(self._to_obj_ref_sync(), timeout=timeout_s)

    async def _to_obj_ref(self) -> ray.ObjectRef:
        return await self._to_obj_ref_or_gen()

    def _to_obj_ref_sync(self) -> ray.ObjectRef:
        return self._to_obj_ref_or_gen_sync()


class DeploymentHandleGenerator(DeploymentHandleResultBase):
    def __init__(
        self,
        assign_request_coro: Coroutine,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        super().__init__(assign_request_coro, loop=loop)
        self._obj_ref_gen: Optional[StreamingObjectRefGenerator] = None

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        if self._obj_ref_gen is None:
            self._obj_ref_gen = await self.to_obj_ref_gen()

        next_obj_ref = await self._obj_ref_gen.__anext__()
        return await next_obj_ref

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        if self._obj_ref_gen is None:
            self._obj_ref_gen = self.to_obj_ref_gen_sync()

        next_obj_ref = self._obj_ref_gen.__next__()
        return ray.get(next_obj_ref)

    async def _to_obj_ref_gen(self) -> StreamingObjectRefGenerator:
        return await self._to_obj_ref_or_gen()

    def _to_obj_ref_gen_sync(self) -> StreamingObjectRefGenerator:
        return self._to_obj_ref_or_gen_sync()


@PublicAPI(stability="beta")
class DeploymentHandle(DeploymentHandleBase):
    """A handle used to make requests to a deployment at runtime.

    This is used to compose multiple deployments into a single application. After
    building the application, this handle is substituted at runtime for deployments
    passed as arguments via `.bind()`.

    Example:

    .. code-block:: python

        import ray
        from ray import serve
        from ray.serve.handle import DeploymentHandle

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
                return await self._handle.remote(name)

        app = Ingress.bind(Downstream.bind("Hello "))
        handle: DeploymentHandle = serve.run(app)

        # Prints "Hello Mr. Magoo"
        print(handle.remote("Mr. Magoo").get())

    """

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        use_new_handle_api: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _router_cls: Union[str, DEFAULT] = DEFAULT.VALUE,
    ) -> "DeploymentHandle":
        """Set options for this handle and return an updated copy of it.

        Example:

        .. code-block:: python

            # The following two lines are equivalent:
            result = await handle.other_method.remote(*args)
            result = await handle.options(method_name="other_method").remote(*args)
            result = await handle.options(
                multiplexed_model_id="model:v1").remote(*args)
        """
        return self._options(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
            use_new_handle_api=use_new_handle_api,
            _router_cls=_router_cls,
        )

    def remote(
        self, *args, **kwargs
    ) -> Union[DeploymentHandleRef, DeploymentHandleGenerator]:
        """Issue an asynchronous request to the __call__ method of the deployment.

        Returns an `asyncio.Task` whose underlying result is a Ray ObjectRef that
        points to the final result of the request.

        The final result can be retrieved by awaiting the ObjectRef.

        Example:

        .. code-block:: python

            obj_ref = await handle.remote(*args)
            result = await obj_ref

        """
        loop = self._get_or_create_router()._event_loop
        result_coro = self._remote(
            self.deployment_name, self.handle_options, args, kwargs
        )
        if self.handle_options.stream:
            return DeploymentHandleGenerator(result_coro, loop=loop)
        else:
            return DeploymentHandleRef(result_coro, loop=loop)
