import asyncio
import concurrent.futures
import logging
import time
import warnings
from typing import Any, AsyncIterator, Dict, Iterator, Optional, Tuple, Union

import ray
from ray._raylet import ObjectRefGenerator
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    RequestMetadata,
    RequestProtocol,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.default_impl import (
    CreateRouterCallable,
    create_dynamic_handle_options,
    create_init_handle_options,
    create_router,
)
from ray.serve._private.handle_options import (
    DynamicHandleOptionsBase,
    InitHandleOptionsBase,
)
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.router import Router
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    DEFAULT,
    calculate_remaining_timeout,
    generate_request_id,
    get_random_string,
    inside_ray_client_context,
    is_running_in_asyncio_loop,
)
from ray.serve.exceptions import RayServeException, RequestCancelledError
from ray.util import metrics
from ray.util.annotations import DeveloperAPI, PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


class _DeploymentHandleBase:
    def __init__(
        self,
        deployment_name: str,
        app_name: str,
        *,
        init_options: Optional[InitHandleOptionsBase] = None,
        handle_options: Optional[DynamicHandleOptionsBase] = None,
        _router: Optional[Router] = None,
        _create_router: Optional[CreateRouterCallable] = None,
        _request_counter: Optional[metrics.Counter] = None,
    ):
        self.deployment_id = DeploymentID(name=deployment_name, app_name=app_name)
        self.init_options: Optional[InitHandleOptionsBase] = init_options
        self.handle_options: DynamicHandleOptionsBase = (
            handle_options or create_dynamic_handle_options()
        )

        self.handle_id = get_random_string()
        self.request_counter = _request_counter or self._create_request_counter(
            app_name, deployment_name, self.handle_id
        )

        self._router: Optional[Router] = _router
        if _create_router is None:
            self._create_router = create_router
        else:
            self._create_router = _create_router

        logger.info(
            f"Created DeploymentHandle '{self.handle_id}' for {self.deployment_id}.",
            extra={"log_to_stderr": False},
        )

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

    def running_replicas_populated(self) -> bool:
        if self._router is None:
            return False

        return self._router.running_replicas_populated()

    @property
    def deployment_name(self) -> str:
        return self.deployment_id.name

    @property
    def app_name(self) -> str:
        return self.deployment_id.app_name

    @property
    def is_initialized(self) -> bool:
        return self._router is not None

    def _init(self, **kwargs):
        """Initialize this handle with arguments.

        A handle can only be initialized once. A handle is implicitly
        initialized when `.options()` or `.remote()` is called. Therefore
        to initialize a handle with custom init options, you must do it
        before calling `.options()` or `.remote()`.
        """
        if self._router is not None:
            raise RuntimeError(
                "Handle has already been initialized. Note that a handle is implicitly "
                "initialized when you call `.options()` or `.remote()`. You either "
                "tried to call `._init()` twice or called `._init()` after calling "
                "`.options()` or `.remote()`. If you want to modify the init options, "
                "please do so before calling `.options()` or `.remote()`. This handle "
                f"was initialized with {self.init_options}."
            )

        init_options = create_init_handle_options(**kwargs)
        self._router = self._create_router(
            handle_id=self.handle_id,
            deployment_id=self.deployment_id,
            handle_options=init_options,
        )
        self.init_options = init_options

        # Record handle api telemetry when not in the proxy
        if (
            self.init_options._source != DeploymentHandleSource.PROXY
            and self.__class__ == DeploymentHandle
        ):
            ServeUsageTag.DEPLOYMENT_HANDLE_API_USED.record("1")

    def _options(self, _prefer_local_routing=DEFAULT.VALUE, **kwargs):
        if kwargs.get("stream") is True and inside_ray_client_context():
            raise RuntimeError(
                "Streaming DeploymentHandles are not currently supported when "
                "connected to a remote Ray cluster using Ray Client."
            )

        new_handle_options = self.handle_options.copy_and_update(**kwargs)

        # TODO(zcin): remove when _prefer_local_routing is removed from options() path
        if _prefer_local_routing != DEFAULT.VALUE:
            self._init(_prefer_local_routing=_prefer_local_routing)

        if not self.is_initialized:
            self._init()

        return DeploymentHandle(
            self.deployment_name,
            self.app_name,
            init_options=self.init_options,
            handle_options=new_handle_options,
            _router=self._router,
            _create_router=self._create_router,
            _request_counter=self.request_counter,
        )

    def _remote(
        self,
        request_metadata: RequestMetadata,
        args: Tuple[Any],
        kwargs: Dict[str, Any],
    ) -> concurrent.futures.Future:
        self.request_counter.inc(
            tags={
                "route": request_metadata.route,
                "application": request_metadata.app_name,
            }
        )

        if not self.is_initialized:
            self._init()

        return self._router.assign_request(request_metadata, *args, **kwargs)

    def __getattr__(self, name):
        return self.options(method_name=name)

    def shutdown(self):
        if self._router:
            shutdown_future = self._router.shutdown()
            shutdown_future.result()

    async def shutdown_async(self):
        if self._router:
            shutdown_future = self._router.shutdown()
            await asyncio.wrap_future(shutdown_future)

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
    def __init__(
        self,
        replica_result_future: concurrent.futures.Future[ReplicaResult],
        request_metadata: RequestMetadata,
    ):
        self._cancelled = False
        self._replica_result_future = replica_result_future
        self._replica_result: Optional[ReplicaResult] = None
        self._request_metadata: RequestMetadata = request_metadata

    @property
    def request_id(self) -> str:
        return self._request_metadata.request_id

    def _fetch_future_result_sync(
        self, _timeout_s: Optional[float] = None
    ) -> ReplicaResult:
        """Synchronously fetch the replica result.

        The result is cached in `self._replica_result`.
        """

        if self._replica_result is None:
            try:
                self._replica_result = self._replica_result_future.result(
                    timeout=_timeout_s
                )
            except concurrent.futures.TimeoutError:
                raise TimeoutError("Timed out resolving to ObjectRef.") from None
            except concurrent.futures.CancelledError:
                raise RequestCancelledError(self.request_id) from None

        return self._replica_result

    async def _fetch_future_result_async(self) -> ReplicaResult:
        """Asynchronously fetch replica result.

        The result is cached in `self._replica_result`..
        """

        if self._replica_result is None:
            # Use `asyncio.wrap_future` so `self._replica_result_future` can be awaited
            # safely from any asyncio loop.
            try:
                self._replica_result = await asyncio.wrap_future(
                    self._replica_result_future
                )
            except asyncio.CancelledError:
                raise RequestCancelledError(self.request_id) from None

        return self._replica_result

    def cancel(self):
        """Attempt to cancel the `DeploymentHandle` call.

        This is best effort.

        - If the request hasn't been assigned to a replica, the assignment will be
          cancelled.
        - If the request has been assigned to a replica, `ray.cancel` will be
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
        if not self._replica_result_future.done():
            self._replica_result_future.cancel()
        elif self._replica_result_future.exception() is None:
            self._fetch_future_result_sync()
            self._replica_result.cancel()

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
        replica_result = yield from self._fetch_future_result_async().__await__()
        result = yield from replica_result.get_async().__await__()
        return result

    def __reduce__(self):
        raise RayServeException(
            "`DeploymentResponse` is not serializable. If you are passing the "
            "`DeploymentResponse` in a nested object (e.g. a list or dictionary) to a "
            "downstream deployment handle call, that is no longer supported. Please "
            "only pass `DeploymentResponse` objects as top level arguments."
        )

    def result(
        self,
        *,
        timeout_s: Optional[float] = None,
        _skip_asyncio_check: bool = False,
    ) -> Any:
        """Fetch the result of the handle call synchronously.

        This should *not* be used from within a deployment as it runs in an asyncio
        event loop. For model composition, `await` the response instead.

        If `timeout_s` is provided and the result is not available before the timeout,
        a `TimeoutError` is raised.
        """

        if not _skip_asyncio_check and is_running_in_asyncio_loop():
            raise RuntimeError(
                "Sync methods should not be called from within an `asyncio` event "
                "loop. Use `await response` instead."
            )

        start_time_s = time.time()
        replica_result = self._fetch_future_result_sync(timeout_s)

        remaining_timeout_s = calculate_remaining_timeout(
            timeout_s=timeout_s, start_time_s=start_time_s, curr_time_s=time.time()
        )
        return replica_result.get(remaining_timeout_s)

    @DeveloperAPI
    async def _to_object_ref(self) -> ray.ObjectRef:
        """Advanced API to convert the response to a Ray `ObjectRef`.

        This is used to pass the output of a `DeploymentHandle` call to a Ray task or
        actor method call.

        This method is `async def` because it will block until the handle call has been
        assigned to a replica. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.
        """

        ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED.record("1")

        replica_result = await self._fetch_future_result_async()
        return await replica_result.to_object_ref_async()

    @DeveloperAPI
    def _to_object_ref_sync(
        self,
        _timeout_s: Optional[float] = None,
        _allow_running_in_asyncio_loop: bool = False,
    ) -> ray.ObjectRef:
        """Advanced API to convert the response to a Ray `ObjectRef`.

        This is used to pass the output of a `DeploymentHandle` call to a Ray task or
        actor method call.

        This method is a *blocking* call because it will block until the handle call has
        been assigned to a replica. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.

        From inside a deployment, `_to_object_ref` should be used instead to avoid
        blocking the asyncio event loop.
        """

        ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED.record("1")

        if not _allow_running_in_asyncio_loop and is_running_in_asyncio_loop():
            raise RuntimeError(
                "Sync methods should not be called from within an `asyncio` event "
                "loop. Use `await response._to_object_ref()` instead."
            )

        # First, fetch the result of the future
        start_time_s = time.time()
        replica_result = self._fetch_future_result_sync(_timeout_s)

        # Then, if necessary, resolve generator to ref
        remaining_timeout_s = calculate_remaining_timeout(
            timeout_s=_timeout_s,
            start_time_s=start_time_s,
            curr_time_s=time.time(),
        )
        return replica_result.to_object_ref(timeout_s=remaining_timeout_s)


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

    def __await__(self):
        raise TypeError(
            "`DeploymentResponseGenerator` cannot be awaited directly. Use `async for` "
            "or `await response.__anext__() instead`."
        )

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        replica_result = await self._fetch_future_result_async()
        return await replica_result.__anext__()

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        if is_running_in_asyncio_loop():
            raise RuntimeError(
                "Sync methods should not be called from within an `asyncio` event "
                "loop. Use `async for` or `await response.__anext__()` instead."
            )

        replica_result = self._fetch_future_result_sync()
        return replica_result.__next__()

    @DeveloperAPI
    async def _to_object_ref_gen(self) -> ObjectRefGenerator:
        """Advanced API to convert the generator to a Ray `ObjectRefGenerator`.

        This method is `async def` because it will block until the handle call has been
        assigned to a replica. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.
        """

        ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED.record("1")

        replica_result = await self._fetch_future_result_async()
        return replica_result.to_object_ref_gen()

    @DeveloperAPI
    def _to_object_ref_gen_sync(
        self,
        _timeout_s: Optional[float] = None,
        _allow_running_in_asyncio_loop: bool = False,
    ) -> ObjectRefGenerator:
        """Advanced API to convert the generator to a Ray `ObjectRefGenerator`.

        This method is a *blocking* call because it will block until the handle call has
        been assigned to a replica. If there are many requests in flight and all
        replicas' queues are full, this may be a slow operation.

        From inside a deployment, `_to_object_ref_gen` should be used instead to avoid
        blocking the asyncio event loop.
        """

        ServeUsageTag.DEPLOYMENT_HANDLE_TO_OBJECT_REF_API_USED.record("1")

        if not _allow_running_in_asyncio_loop and is_running_in_asyncio_loop():
            raise RuntimeError(
                "Sync methods should not be called from within an `asyncio` event "
                "loop. Use `await response._to_object_ref()` instead."
            )

        replica_result = self._fetch_future_result_sync(_timeout_s)
        return replica_result.to_object_ref_gen()


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

        if _prefer_local_routing is not DEFAULT.VALUE:
            warnings.warn(
                "Modifying `_prefer_local_routing` with `options()` is "
                "deprecated. Please use `init()` instead."
            )

        return self._options(
            method_name=method_name,
            multiplexed_model_id=multiplexed_model_id,
            stream=stream,
            _prefer_local_routing=_prefer_local_routing,
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
        _request_context = ray.serve.context._serve_request_context.get()

        request_protocol = RequestProtocol.UNDEFINED
        if (
            self.init_options
            and self.init_options._source == DeploymentHandleSource.PROXY
        ):
            if _request_context.is_http_request:
                request_protocol = RequestProtocol.HTTP
            elif _request_context.grpc_context:
                request_protocol = RequestProtocol.GRPC

        request_metadata = RequestMetadata(
            request_id=_request_context.request_id
            if _request_context.request_id
            else generate_request_id(),
            internal_request_id=_request_context._internal_request_id
            if _request_context._internal_request_id
            else generate_request_id(),
            call_method=self.handle_options.method_name,
            route=_request_context.route,
            app_name=self.app_name,
            multiplexed_model_id=self.handle_options.multiplexed_model_id,
            is_streaming=self.handle_options.stream,
            _request_protocol=request_protocol,
            grpc_context=_request_context.grpc_context,
        )

        future = self._remote(request_metadata, args, kwargs)
        if self.handle_options.stream:
            response_cls = DeploymentResponseGenerator
        else:
            response_cls = DeploymentResponse

        return response_cls(future, request_metadata)
