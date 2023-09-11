import asyncio
import concurrent.futures
from dataclasses import dataclass
import threading
from typing import Any, AsyncIterator, Coroutine, Dict, Iterator, Optional, Tuple, Union
import warnings

import ray
from ray.util import metrics
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI
from ray._private.utils import get_or_create_event_loop
from ray._raylet import StreamingObjectRefGenerator, GcsClient

from ray import serve
from ray.serve._private.common import DeploymentID, RequestProtocol
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_NEW_ROUTING,
)
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    get_random_letters,
    DEFAULT,
)
from ray.serve._private.router import Router, RequestMetadata

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


@dataclass(frozen=True)
class _HandleOptions:
    """Options for each ServeHandle instance.

    These fields can be changed by calling `.options()` on a handle.
    """

    method_name: str = "__call__"
    multiplexed_model_id: str = ""
    stream: bool = False
    _prefer_local_routing: bool = False
    _router_cls: str = ""
    _request_protocol: str = RequestProtocol.UNDEFINED

    def copy_and_update(
        self,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _prefer_local_routing: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _router_cls: Union[str, DEFAULT] = DEFAULT.VALUE,
        _request_protocol: Union[str, DEFAULT] = DEFAULT.VALUE,
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
            _prefer_local_routing=self._prefer_local_routing
            if _prefer_local_routing == DEFAULT.VALUE
            else _prefer_local_routing,
            _router_cls=self._router_cls
            if _router_cls == DEFAULT.VALUE
            else _router_cls,
            _request_protocol=self._request_protocol
            if _request_protocol == DEFAULT.VALUE
            else _request_protocol,
        )


class _DeploymentHandleBase:
    def __init__(
        self,
        deployment_name: str,
        app_name: str,
        *,
        handle_options: Optional[_HandleOptions] = None,
        _router: Optional[Router] = None,
        _is_for_sync_context: bool = False,
        _request_counter: Optional[metrics.Counter] = None,
        _recorded_telemetry: bool = False,
    ):
        self.deployment_id = DeploymentID(deployment_name, app_name)
        self.handle_options = handle_options or _HandleOptions()
        self._is_for_sync_context = _is_for_sync_context
        self._recorded_telemetry = _recorded_telemetry

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
            {
                "handle": handle_tag,
                "deployment": self.deployment_id.name,
                "application": self.deployment_id.app,
            }
        )

        self._router: Optional[Router] = _router

    def _record_telemetry_if_needed(self):
        # Record telemetry once per handle and not when used from the proxy
        # (detected via request protocol).
        if (
            not self._recorded_telemetry
            and self.handle_options._request_protocol == RequestProtocol.UNDEFINED
        ):
            if self.__class__ == DeploymentHandle:
                ServeUsageTag.DEPLOYMENT_HANDLE_API_USED.record("1")
            elif self.__class__ == RayServeHandle:
                ServeUsageTag.RAY_SERVE_HANDLE_API_USED.record("1")
            else:
                ServeUsageTag.RAY_SERVE_SYNC_HANDLE_API_USED.record("1")

            self._recorded_telemetry = True

    def _set_request_protocol(self, request_protocol: RequestProtocol):
        self.handle_options = self.handle_options.copy_and_update(
            _request_protocol=request_protocol
        )

    def _get_or_create_router(self) -> Router:
        if self._router is None:
            if self._is_for_sync_context:
                event_loop = _create_or_get_async_loop_in_thread()
            else:
                event_loop = get_or_create_event_loop()

            node_id = ray.get_runtime_context().get_node_id()
            try:
                cluster_node_info_cache = create_cluster_node_info_cache(
                    GcsClient(address=ray.get_runtime_context().gcs_address)
                )
                cluster_node_info_cache.update()
                availability_zone = cluster_node_info_cache.get_node_az(node_id)
            except Exception:
                availability_zone = None

            self._router = Router(
                serve.context._get_global_client()._controller,
                self.deployment_id,
                node_id,
                availability_zone,
                event_loop=event_loop,
                _use_new_routing=RAY_SERVE_ENABLE_NEW_ROUTING,
                _prefer_local_node_routing=self.handle_options._prefer_local_routing,
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
        _prefer_local_routing: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _router_cls: Union[str, DEFAULT] = DEFAULT.VALUE,
    ):
        new_handle_options = self.handle_options.copy_and_update(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
            _prefer_local_routing=_prefer_local_routing,
            _router_cls=_router_cls,
        )

        if (
            self._router is None
            and _router_cls == DEFAULT.VALUE
            and _prefer_local_routing == DEFAULT.VALUE
        ):
            self._get_or_create_router()

        if use_new_handle_api is True:
            cls = DeploymentHandle
        else:
            cls = self.__class__

        return cls(
            self.deployment_name,
            self.app_name,
            handle_options=new_handle_options,
            _router=None if _router_cls != DEFAULT.VALUE else self._router,
            _request_counter=self.request_counter,
            _is_for_sync_context=self._is_for_sync_context,
            _recorded_telemetry=self._recorded_telemetry,
        )

    def _remote(self, args: Tuple[Any], kwargs: Dict[str, Any]) -> Coroutine:
        if not self.__class__ == DeploymentHandle:
            warnings.warn(
                "Ray 2.7 introduces a new `DeploymentHandle` API that will "
                "replace the existing `RayServeHandle` and `RayServeSyncHandle` "
                "APIs in a future release. You are encouraged to migrate to the "
                "new API to avoid breakages in the future. To opt in, either use "
                "`handle.options(use_new_handle_api=True)` or set the global "
                "environment variable `export RAY_SERVE_ENABLE_NEW_HANDLE_API=1`. "
                "See https://docs.ray.io/en/latest/serve/model_composition.html "
                "for more details."
            )

        self._record_telemetry_if_needed()
        _request_context = ray.serve.context._serve_request_context.get()
        request_metadata = RequestMetadata(
            _request_context.request_id,
            self.deployment_name,
            call_method=self.handle_options.method_name,
            route=_request_context.route,
            app_name=self.app_name,
            multiplexed_model_id=self.handle_options.multiplexed_model_id,
            is_streaming=self.handle_options.stream,
            _request_protocol=self.handle_options._request_protocol,
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
            "app_name": self.app_name,
            "handle_options": self.handle_options,
            "_is_for_sync_context": self._is_for_sync_context,
        }
        return self.__class__._deserialize, (serialized_constructor_args,)


@Deprecated(
    message=(
        "This API is being replaced by `ray.serve.handle.DeploymentHandle`. "
        "Opt into the new API by using `handle.options(use_new_handle_api=True)` "
        "or setting the environment variable `RAY_SERVE_USE_NEW_HANDLE_API=1`."
    )
)
class RayServeHandle(_DeploymentHandleBase):
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
        _prefer_local_routing: Union[bool, DEFAULT] = DEFAULT.VALUE,
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
            _prefer_local_routing=_prefer_local_routing,
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
        result_coro = self._remote(args, kwargs)
        return asyncio.ensure_future(result_coro, loop=loop)


@Deprecated(
    message=(
        "This API is being replaced by `ray.serve.handle.DeploymentHandle`. "
        "Opt into the new API by using `handle.options(use_new_handle_api=True)` "
        "or setting the environment variable `RAY_SERVE_USE_NEW_HANDLE_API=1`."
    )
)
class RayServeSyncHandle(_DeploymentHandleBase):
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
        _prefer_local_routing: Union[bool, DEFAULT] = DEFAULT.VALUE,
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
            _prefer_local_routing=_prefer_local_routing,
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
        coro = self._remote(args, kwargs)
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


class _DeploymentResponseBase:
    def __init__(
        self,
        assign_request_coro: Coroutine,
        loop: asyncio.AbstractEventLoop,
        loop_is_in_another_thread: bool,
    ):
        self._assign_request_task = loop.create_task(assign_request_coro)

        if loop_is_in_another_thread:
            # For the "sync" case where the handle is likely used in a driver for
            # testing, we need to call `run_coroutine_threadsafe` to eagerly execute
            # the request.
            self._object_ref_future = asyncio.run_coroutine_threadsafe(
                self._to_object_ref_or_gen(_record_telemetry=False), loop
            )
        else:
            self._object_ref_future = None

    async def _to_object_ref_or_gen(
        self,
        _record_telemetry: bool = True,
    ) -> Union[ray.ObjectRef, StreamingObjectRefGenerator]:
        # Record telemetry for using the developer API to convert to an object
        # ref. Recorded here because all of the other codepaths go through this.
        # `_record_telemetry` is used to filter other API calls that go through
        # this path as well as calls from the proxy.
        if _record_telemetry:
            ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED.record("1")
        return await self._assign_request_task

    def _to_object_ref_or_gen_sync(
        self,
        _record_telemetry: bool = True,
    ) -> Union[ray.ObjectRef, StreamingObjectRefGenerator]:
        if self._object_ref_future is None:
            raise RuntimeError(
                "Sync methods should not be called from within an `asyncio` event "
                "loop. Use `await response` or `await response._to_object_ref()` "
                "instead."
            )

        if _record_telemetry:
            ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED.record("1")

        return self._object_ref_future.result()

    def cancel(self):
        """Attempt to cancel the `DeploymentHandle` call.

        This is best effort.

        - If the request hasn't been assigned to a replica actor, the assignment will be
          cancelled.
        - If the request has been assigned to a replica actor, `ray.cancel` will be
          called on the object ref, attempting to cancel the request and any downstream
          requests it makes.

        If the request is successfully cancelled, subsequent operations on the ref will
        raise an exception:

            - If the request was cancelled before assignment, they'll raise
              `asyncio.CancelledError` (or a `concurrent.futures.CancelledError` for
              synchronous methods like `.result()`.).
            - If the request was cancelled after assignment, they'll raise
              `ray.exceptions.TaskCancelledError`.
        """
        if not self._assign_request_task.done():
            self._assign_request_task.cancel()
        elif self._assign_request_task.exception() is None:
            ray.cancel(self._assign_request_task.result())


@PublicAPI(stability="beta")
class DeploymentResponse(_DeploymentResponseBase):
    """A future-like object wrapping the result of a unary deployment handle call.

    From inside a deployment, a `DeploymentResponse` can be awaited to retrieve the
    output of the call without blocking the asyncio event loop.

    From outside a deployment, `.result()` can be used to retrieve the output in a
    blocking manner.

    Example:

    .. code-block:: python

        from ray import serve
        from ray.serve.handle import DeploymentHandle

        @serve.deployment
        class Downstream:
            def say_hi(self, message: str) -> str:
                return f"Hello {message}!"

        @serve.deployment
        class Caller:
            def __init__(self, handle: DeploymentHandle):
                self._downstream_handle = handle

        async def __call__(self, message: str) -> str:
            # Inside a deployment: `await` the result to enable concurrency.
            response = self._downstream_handle.say_hi.remote(message)
            return await response

        app = Caller.bind(Downstream.bind())
        handle: DeploymentHandle = serve.run(app)

        # Outside a deployment: call `.result()` to get output.
        response = handle.remote("world")
        assert response.result() == "Hello world!"

    A `DeploymentResponse` can be passed directly to another `DeploymentHandle` call
    without fetching the result to enable composing multiple deployments together.

    Example:

    .. code-block:: python

        from ray import serve
        from ray.serve.handle import DeploymentHandle

        @serve.deployment
        class Adder:
            def add(self, val: int) -> int:
                return val + 1

        @serve.deployment
        class Caller:
            def __init__(self, handle: DeploymentHandle):
                self._adder_handle = handle

        async def __call__(self, start: int) -> int:
            return await self._adder_handle.add.remote(
                # Pass the response directly to another handle call without awaiting.
                self._adder_handle.add.remote(start)
            )

        app = Caller.bind(Adder.bind())
        handle: DeploymentHandle = serve.run(app)
        assert handle.remote(0).result() == 2
    """

    def __await__(self):
        """Yields the final result of the deployment handle call."""
        obj_ref = yield from self._assign_request_task.__await__()
        result = yield from obj_ref.__await__()
        return result

    def result(self, timeout_s: Optional[float] = None) -> Any:
        """Fetch the result of the handle call synchronously.

        This should *not* be used from within a deployment as it runs in an asyncio
        event loop. For model composition, `await` the response instead.

        If `timeout_s` is provided and the result is not available before the timeout,
        a `TimeoutError` is raised.
        """
        return ray.get(
            self._to_object_ref_sync(_record_telemetry=False), timeout=timeout_s
        )

    @DeveloperAPI
    async def _to_object_ref(self, _record_telemetry: bool = True) -> ray.ObjectRef:
        """Advanced API to convert the response to a Ray `ObjectRef`.

        This is used to pass the output of a `DeploymentHandle` call to a Ray task or
        actor method call.

        This method is `async def` because it will block until the handle call has been
        assigned to a replica actor. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.
        """
        return await self._to_object_ref_or_gen(_record_telemetry=_record_telemetry)

    @DeveloperAPI
    def _to_object_ref_sync(self, _record_telemetry: bool = True) -> ray.ObjectRef:
        """Advanced API to convert the response to a Ray `ObjectRef`.

        This is used to pass the output of a `DeploymentHandle` call to a Ray task or
        actor method call.

        This method is a *blocking* call because it will block until the handle call has
        been assigned to a replica actor. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.

        From inside a deployment, `_to_object_ref` should be used instead to avoid
        blocking the asyncio event loop.
        """
        return self._to_object_ref_or_gen_sync(_record_telemetry=_record_telemetry)


@PublicAPI(stability="beta")
class DeploymentResponseGenerator(_DeploymentResponseBase):
    """A future-like object wrapping the result of a streaming deployment handle call.

    This is returned when using `handle.options(stream=True)` and calling a generator
    deployment method.

    `DeploymentResponseGenerator` is both a synchronous and asynchronous iterator.

    When iterating over results from inside a deployment, `async for` should be used to
    avoid blocking the asyncio event loop.

    When iterating over results from outside a deployment, use a standard `for` loop.

    Example:

    .. code-block:: python

        from typing import AsyncGenerator, Generator

        from ray import serve
        from ray.serve.handle import DeploymentHandle

        @serve.deployment
        class Streamer:
            def generate_numbers(self, limit: int) -> Generator[int]:
                for i in range(limit):
                    yield i

        @serve.deployment
        class Caller:
            def __init__(self, handle: DeploymentHandle):
                # Set `stream=True` on the handle to enable streaming calls.
                self._streaming_handle = handle.options(stream=True)

        async def __call__(self, limit: int) -> AsyncIterator[int]:
            gen: DeploymentResponseGenerator = (
                self._streaming_handle.generate_numbers.remote(limit)
            )

            # Inside a deployment: use `async for` to enable concurrency.
            async for i in gen:
                yield i

        app = Caller.bind(Streamer.bind())
        handle: DeploymentHandle = serve.run(app)

        # Outside a deployment: use a standard `for` loop.
        gen: DeploymentResponseGenerator = handle.options(stream=True).remote(10)
        assert [i for i in gen] == list(range(10))

    A `DeploymentResponseGenerator` *cannot* currently be passed to another
    `DeploymentHandle` call.
    """

    def __init__(
        self,
        assign_request_coro: Coroutine,
        loop: asyncio.AbstractEventLoop,
        loop_is_in_another_thread: bool,
    ):
        super().__init__(
            assign_request_coro,
            loop=loop,
            loop_is_in_another_thread=loop_is_in_another_thread,
        )
        self._obj_ref_gen: Optional[StreamingObjectRefGenerator] = None

    def __await__(self):
        raise TypeError(
            "`DeploymentResponseGenerator` cannot be awaited directly. Use `async for` "
            "or `_to_object_ref_gen` instead."
        )

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        if self._obj_ref_gen is None:
            self._obj_ref_gen = await self._to_object_ref_gen(_record_telemetry=False)

        next_obj_ref = await self._obj_ref_gen.__anext__()
        return await next_obj_ref

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        if self._obj_ref_gen is None:
            self._obj_ref_gen = self._to_object_ref_gen_sync(_record_telemetry=False)

        next_obj_ref = self._obj_ref_gen.__next__()
        return ray.get(next_obj_ref)

    @DeveloperAPI
    async def _to_object_ref_gen(
        self, _record_telemetry: bool = True
    ) -> StreamingObjectRefGenerator:
        """Advanced API to convert the generator to a Ray `StreamingObjectRefGenerator`.

        This method is `async def` because it will block until the handle call has been
        assigned to a replica actor. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.
        """
        return await self._to_object_ref_or_gen(_record_telemetry=_record_telemetry)

    @DeveloperAPI
    def _to_object_ref_gen_sync(
        self, _record_telemetry: bool = True
    ) -> StreamingObjectRefGenerator:
        """Advanced API to convert the generator to a Ray `StreamingObjectRefGenerator`.

        This method is a *blocking* call because it will block until the handle call has
        been assigned to a replica actor. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.

        From inside a deployment, `_to_object_ref_gen` should be used instead to avoid
        blocking the asyncio event loop.
        """
        return self._to_object_ref_or_gen_sync(_record_telemetry=_record_telemetry)


@PublicAPI(stability="beta")
class DeploymentHandle(_DeploymentHandleBase):
    """A handle used to make requests to a deployment at runtime.

    This is primarily used to compose multiple deployments within a single application.
    It can also be used to make calls to the ingress deployment of an application (e.g.,
    for programmatic testing).

    Example:


    .. code-block:: python

        import ray
        from ray import serve
        from ray.serve.handle import DeploymentHandle, DeploymentResponse

        @serve.deployment
        class Downstream:
            def say_hi(self, message: str):
                return f"Hello {message}!"
                self._message = message

        @serve.deployment
        class Ingress:
            def __init__(self, handle: DeploymentHandle):
                self._downstream_handle = handle

            async def __call__(self, name: str) -> str:
                response = self._handle.say_hi.remote(name)
                return await response

        app = Ingress.bind(Downstream.bind())
        handle: DeploymentHandle = serve.run(app)
        response = handle.remote("world")
        assert response.result() == "Hello world!"
    """

    def options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        use_new_handle_api: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _prefer_local_routing: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _router_cls: Union[str, DEFAULT] = DEFAULT.VALUE,
    ) -> "DeploymentHandle":
        """Set options for this handle and return an updated copy of it.

        Example:

        .. code-block:: python

            response: DeploymentResponse = handle.options(
                method_name="other_method",
                multiplexed_model_id="model:v1",
            ).remote()
        """
        return self._options(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
            use_new_handle_api=use_new_handle_api,
            _prefer_local_routing=_prefer_local_routing,
            _router_cls=_router_cls,
        )

    def remote(
        self, *args, **kwargs
    ) -> Union[DeploymentResponse, DeploymentResponseGenerator]:
        """Issue a remote call to a method of the deployment.

        By default, the result is a `DeploymentResponse` that can be awaited to fetch
        the result of the call or passed to another `.remote()` call to compose multiple
        deployments.

        If `handle.options(stream=True)` is set and a generator method is called, this
        returns a `DeploymentResponseGenerator` instead.

        Example:

        .. code-block:: python

            # Fetch the result directly.
            response = handle.remote()
            result = await response

            # Pass the result to another handle call.
            composed_response = handle2.remote(handle1.remote())
            composed_result = await composed_response

        Args:
            *args: Positional arguments to be serialized and passed to the
                remote method call.
            **kwargs: Keyword arguments to be serialized and passed to the
                remote method call.
        """
        loop = self._get_or_create_router()._event_loop
        result_coro = self._remote(args, kwargs)
        if self.handle_options.stream:
            response_cls = DeploymentResponseGenerator
        else:
            response_cls = DeploymentResponse

        return response_cls(
            result_coro,
            loop=loop,
            loop_is_in_another_thread=self._is_for_sync_context,
        )
