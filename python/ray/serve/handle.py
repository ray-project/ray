import asyncio
import concurrent.futures
import logging
import threading
import time
import warnings
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Iterator, Optional, Tuple, Union

import ray
from ray import serve
from ray._raylet import GcsClient, ObjectRefGenerator
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    RequestMetadata,
    RequestProtocol,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.router import Router
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    DEFAULT,
    calculate_remaining_timeout,
    generate_request_id,
    get_current_actor_id,
    get_random_string,
    inside_ray_client_context,
    is_running_in_asyncio_loop,
)
from ray.util import metrics
from ray.util.annotations import DeveloperAPI, PublicAPI

_global_async_loop = None
_global_async_loop_creation_lock = threading.Lock()
logger = logging.getLogger(SERVE_LOGGER_NAME)


def _create_or_get_global_asyncio_event_loop_in_thread():
    """Provides a global singleton asyncio event loop running in a daemon thread.

    Thread-safe.
    """
    global _global_async_loop
    if _global_async_loop is None:
        with _global_async_loop_creation_lock:
            if _global_async_loop is not None:
                return _global_async_loop

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
    _request_protocol: str = RequestProtocol.UNDEFINED
    _source: DeploymentHandleSource = DeploymentHandleSource.UNKNOWN

    def copy_and_update(
        self,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _prefer_local_routing: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _request_protocol: Union[str, DEFAULT] = DEFAULT.VALUE,
        _source: Union[DeploymentHandleSource, DEFAULT] = DEFAULT.VALUE,
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
            _request_protocol=self._request_protocol
            if _request_protocol == DEFAULT.VALUE
            else _request_protocol,
            _source=self._source if _source == DEFAULT.VALUE else _source,
        )


class _DeploymentHandleBase:
    def __init__(
        self,
        deployment_name: str,
        app_name: str,
        *,
        handle_options: Optional[_HandleOptions] = None,
        _router: Optional[Router] = None,
        _request_counter: Optional[metrics.Counter] = None,
        _recorded_telemetry: bool = False,
    ):
        self.deployment_id = DeploymentID(name=deployment_name, app_name=app_name)
        self.handle_options = handle_options or _HandleOptions()
        self._recorded_telemetry = _recorded_telemetry

        self.handle_id = get_random_string()
        self.request_counter = _request_counter or self._create_request_counter(
            app_name, deployment_name, self.handle_id
        )

        self._router: Optional[Router] = _router

        logger.info(
            f"Created DeploymentHandle '{self.handle_id}' for {self.deployment_id}.",
            extra={"log_to_stderr": False},
        )

    def _record_telemetry_if_needed(self):
        # Record telemetry once per handle and not when used from the proxy
        # (detected via request protocol).
        if (
            not self._recorded_telemetry
            and self.handle_options._request_protocol == RequestProtocol.UNDEFINED
        ):
            if self.__class__ == DeploymentHandle:
                ServeUsageTag.DEPLOYMENT_HANDLE_API_USED.record("1")

            self._recorded_telemetry = True

    def _set_request_protocol(self, request_protocol: RequestProtocol):
        self.handle_options = self.handle_options.copy_and_update(
            _request_protocol=request_protocol
        )

    def _get_or_create_router(self) -> Tuple[Router, asyncio.AbstractEventLoop]:

        if self._router is None:
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
                self.handle_id,
                node_id,
                get_current_actor_id(),
                availability_zone,
                handle_source=self.handle_options._source,
                event_loop=_create_or_get_global_asyncio_event_loop_in_thread(),
                _prefer_local_node_routing=self.handle_options._prefer_local_routing,
            )

        return self._router, self._router._event_loop

    @staticmethod
    def _gen_handle_tag(app_name: str, deployment_name: str, handle_id: str):
        if app_name:
            return f"{app_name}#{deployment_name}#{handle_id}"
        else:
            return f"{deployment_name}#{handle_id}"

    @classmethod
    def _create_request_counter(
        cls, app_name: str, deployment_name: str, handle_id: str
    ):
        return metrics.Counter(
            "serve_handle_request_counter",
            description=(
                "The number of handle.remote() calls that have been "
                "made on this handle."
            ),
            tag_keys=("handle", "deployment", "route", "application"),
        ).set_default_tags(
            {
                "handle": cls._gen_handle_tag(
                    app_name, deployment_name, handle_id=handle_id
                ),
                "deployment": deployment_name,
                "application": app_name,
            }
        )

    @property
    def deployment_name(self) -> str:
        return self.deployment_id.name

    @property
    def app_name(self) -> str:
        return self.deployment_id.app_name

    def _options(
        self,
        *,
        method_name: Union[str, DEFAULT] = DEFAULT.VALUE,
        multiplexed_model_id: Union[str, DEFAULT] = DEFAULT.VALUE,
        stream: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _prefer_local_routing: Union[bool, DEFAULT] = DEFAULT.VALUE,
        _source: Union[DeploymentHandleSource, DEFAULT] = DEFAULT.VALUE,
    ):
        if stream is True and inside_ray_client_context():
            raise RuntimeError(
                "Streaming DeploymentHandles are not currently supported when "
                "connected to a remote Ray cluster using Ray Client."
            )

        new_handle_options = self.handle_options.copy_and_update(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
            _prefer_local_routing=_prefer_local_routing,
            _source=_source,
        )

        if self._router is None and _prefer_local_routing == DEFAULT.VALUE:
            self._get_or_create_router()

        return DeploymentHandle(
            self.deployment_name,
            self.app_name,
            handle_options=new_handle_options,
            _router=self._router,
            _request_counter=self.request_counter,
            _recorded_telemetry=self._recorded_telemetry,
        )

    def _remote(
        self, args: Tuple[Any], kwargs: Dict[str, Any]
    ) -> concurrent.futures.Future:
        self._record_telemetry_if_needed()
        _request_context = ray.serve.context._serve_request_context.get()
        request_metadata = RequestMetadata(
            request_id=_request_context.request_id
            if _request_context.request_id
            else generate_request_id(),
            internal_request_id=_request_context._internal_request_id
            if _request_context._internal_request_id
            else generate_request_id(),
            endpoint=self.deployment_name,
            call_method=self.handle_options.method_name,
            route=_request_context.route,
            app_name=self.app_name,
            multiplexed_model_id=self.handle_options.multiplexed_model_id,
            is_streaming=self.handle_options.stream,
            _request_protocol=self.handle_options._request_protocol,
            grpc_context=_request_context.grpc_context,
        )
        self.request_counter.inc(
            tags={
                "route": _request_context.route,
                "application": _request_context.app_name,
            }
        )
        router, event_loop = self._get_or_create_router()

        # Schedule the coroutine to run on the router loop. This is always a separate
        # loop running in another thread to avoid user code blocking the router, so we
        # use the `concurrent.futures.Future` thread safe API.
        return asyncio.run_coroutine_threadsafe(
            router.assign_request(request_metadata, *args, **kwargs),
            loop=event_loop,
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
        }
        return self.__class__._deserialize, (serialized_constructor_args,)


class _DeploymentResponseBase:
    def __init__(self, object_ref_future: concurrent.futures.Future):
        self._cancelled = False
        # The result of `object_ref_future` must be an ObjectRef or ObjectRefGenerator.
        self._object_ref_future = object_ref_future

        # Cached result of the `object_ref_future`.
        # This is guarded by the below locks for async and sync methods.
        # It's not expected that user code can mix async and sync methods (sync methods
        # raise an exception when running in an `asyncio` loop).
        # The `asyncio` lock is lazily constructed because the constructor may run on
        # a different `asyncio` loop than method calls (or not run on one at all).
        self._object_ref_or_gen = None
        self.__lazy_object_ref_or_gen_asyncio_lock = None
        self._object_ref_or_gen_sync_lock = threading.Lock()

    @property
    def _object_ref_or_gen_asyncio_lock(self) -> asyncio.Lock:
        """Lazy `asyncio.Lock` object."""
        if self.__lazy_object_ref_or_gen_asyncio_lock is None:
            self.__lazy_object_ref_or_gen_asyncio_lock = asyncio.Lock()

        return self.__lazy_object_ref_or_gen_asyncio_lock

    def _should_resolve_gen_to_obj_ref(
        self, obj_ref_or_gen: Union[ray.ObjectRef, ray.ObjectRefGenerator]
    ) -> bool:
        """Check if the ref is a generator that needs to be resolved to its first ref.

        This is an edge case to handle the routing code path with replica rejection.
        In that case, the output of `router.assign_request` is *always* a generator,
        so if this is a unary response we need to resolve it to its first (and only)
        output ObjectRef.
        """
        return isinstance(obj_ref_or_gen, ray.ObjectRefGenerator) and isinstance(
            self, DeploymentResponse
        )

    async def _to_object_ref_or_gen(
        self,
        _record_telemetry: bool = True,
    ) -> Union[ray.ObjectRef, ObjectRefGenerator]:
        # Record telemetry for using the developer API to convert to an object
        # ref. Recorded here because all of the other codepaths go through this.
        # `_record_telemetry` is used to filter other API calls that go through
        # this path as well as calls from the proxy.
        if _record_telemetry:
            ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED.record("1")

        # NOTE(edoakes): this section needs to be guarded with a lock and the resulting
        # object ref or generator cached in order to avoid calling `__anext__()` to
        # resolve to the underlying object ref more than once.
        #
        # See: https://github.com/ray-project/ray/issues/43879.
        async with self._object_ref_or_gen_asyncio_lock:
            if self._object_ref_or_gen is None:
                # Use `asyncio.wrap_future` so `self._object_ref_future` can be awaited
                # safely from any asyncio loop.
                obj_ref_or_gen = await asyncio.wrap_future(self._object_ref_future)
                if self._should_resolve_gen_to_obj_ref(obj_ref_or_gen):
                    obj_ref_or_gen = await obj_ref_or_gen.__anext__()

                self._object_ref_or_gen = obj_ref_or_gen

            return self._object_ref_or_gen

    def _to_object_ref_or_gen_sync(
        self,
        _record_telemetry: bool = True,
        _timeout_s: Optional[float] = None,
        _allow_running_in_asyncio_loop: bool = False,
    ) -> Union[ray.ObjectRef, ObjectRefGenerator]:
        if not _allow_running_in_asyncio_loop and is_running_in_asyncio_loop():
            raise RuntimeError(
                "Sync methods should not be called from within an `asyncio` event "
                "loop. Use `await response` or `await response._to_object_ref()` "
                "instead."
            )

        if _record_telemetry:
            ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED.record("1")

        start_time_s = time.time()
        # NOTE(edoakes): this section needs to be guarded with a lock and the resulting
        # object ref or generator cached in order to avoid calling `__next__()` to
        # resolve to the underlying object ref more than once.
        #
        # See: https://github.com/ray-project/ray/issues/43879.
        with self._object_ref_or_gen_sync_lock:
            if self._object_ref_or_gen is None:
                try:
                    obj_ref_or_gen = self._object_ref_future.result(timeout=_timeout_s)
                except concurrent.futures.TimeoutError:
                    raise TimeoutError("Timed out resolving to ObjectRef.") from None

                if self._should_resolve_gen_to_obj_ref(obj_ref_or_gen):
                    obj_ref_or_gen = obj_ref_or_gen._next_sync(
                        timeout_s=calculate_remaining_timeout(
                            timeout_s=_timeout_s,
                            start_time_s=start_time_s,
                            curr_time_s=time.time(),
                        )
                    )
                    if obj_ref_or_gen.is_nil():
                        raise TimeoutError("Timed out resolving to ObjectRef.")
                self._object_ref_or_gen = obj_ref_or_gen

        return self._object_ref_or_gen

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
        if self._cancelled:
            return

        self._cancelled = True
        if not self._object_ref_future.done():
            self._object_ref_future.cancel()
        elif self._object_ref_future.exception() is None:
            ray.cancel(self._object_ref_future.result())

    @DeveloperAPI
    def cancelled(self) -> bool:
        """Whether or not the request has been cancelled.

        This is `True` if `.cancel()` is called, but the request may actually have run
        to completion.
        """
        return self._cancelled


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
        obj_ref = yield from self._to_object_ref_or_gen(
            _record_telemetry=False
        ).__await__()
        result = yield from obj_ref.__await__()
        return result

    def result(self, *, timeout_s: Optional[float] = None) -> Any:
        """Fetch the result of the handle call synchronously.

        This should *not* be used from within a deployment as it runs in an asyncio
        event loop. For model composition, `await` the response instead.

        If `timeout_s` is provided and the result is not available before the timeout,
        a `TimeoutError` is raised.
        """
        start_time_s = time.time()
        obj_ref = self._to_object_ref_sync(
            _record_telemetry=False, _timeout_s=timeout_s
        )
        remaining_timeout_s = calculate_remaining_timeout(
            timeout_s=timeout_s, start_time_s=start_time_s, curr_time_s=time.time()
        )
        return ray.get(obj_ref, timeout=remaining_timeout_s)

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
    def _to_object_ref_sync(
        self,
        _record_telemetry: bool = True,
        _timeout_s: Optional[float] = None,
        _allow_running_in_asyncio_loop: bool = False,
    ) -> ray.ObjectRef:
        """Advanced API to convert the response to a Ray `ObjectRef`.

        This is used to pass the output of a `DeploymentHandle` call to a Ray task or
        actor method call.

        This method is a *blocking* call because it will block until the handle call has
        been assigned to a replica actor. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.

        From inside a deployment, `_to_object_ref` should be used instead to avoid
        blocking the asyncio event loop.
        """
        return self._to_object_ref_or_gen_sync(
            _record_telemetry=_record_telemetry,
            _timeout_s=_timeout_s,
            _allow_running_in_asyncio_loop=_allow_running_in_asyncio_loop,
        )


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
        object_ref_future: concurrent.futures.Future,
    ):
        super().__init__(object_ref_future)
        self._obj_ref_gen: Optional[ObjectRefGenerator] = None

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
    ) -> ObjectRefGenerator:
        """Advanced API to convert the generator to a Ray `ObjectRefGenerator`.

        This method is `async def` because it will block until the handle call has been
        assigned to a replica actor. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.
        """
        return await self._to_object_ref_or_gen(_record_telemetry=_record_telemetry)

    @DeveloperAPI
    def _to_object_ref_gen_sync(
        self,
        _record_telemetry: bool = True,
        _timeout_s: Optional[float] = None,
        _allow_running_in_asyncio_loop: bool = False,
    ) -> ObjectRefGenerator:
        """Advanced API to convert the generator to a Ray `ObjectRefGenerator`.

        This method is a *blocking* call because it will block until the handle call has
        been assigned to a replica actor. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.

        From inside a deployment, `_to_object_ref_gen` should be used instead to avoid
        blocking the asyncio event loop.
        """
        return self._to_object_ref_or_gen_sync(
            _record_telemetry=_record_telemetry,
            _timeout_s=_timeout_s,
            _allow_running_in_asyncio_loop=_allow_running_in_asyncio_loop,
        )


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
        _source: Union[bool, DEFAULT] = DEFAULT.VALUE,
    ) -> "DeploymentHandle":
        """Set options for this handle and return an updated copy of it.

        Example:

        .. code-block:: python

            response: DeploymentResponse = handle.options(
                method_name="other_method",
                multiplexed_model_id="model:v1",
            ).remote()
        """
        if use_new_handle_api is not DEFAULT.VALUE:
            warnings.warn(
                "Setting `use_new_handle_api` no longer has any effect. "
                "This argument will be removed in a future version."
            )

        return self._options(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
            _prefer_local_routing=_prefer_local_routing,
            _source=_source,
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
        future = self._remote(args, kwargs)
        if self.handle_options.stream:
            response_cls = DeploymentResponseGenerator
        else:
            response_cls = DeploymentResponse

        return response_cls(future)
