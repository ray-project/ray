import asyncio
import inspect
import logging
import os
import pickle
import time
import traceback
from contextlib import asynccontextmanager
from importlib import import_module
from typing import Any, AsyncGenerator, Callable, Dict, Optional, Tuple

import aiorwlock
import starlette.responses
from starlette.requests import Request
from starlette.types import Message, Receive, Scope, Send

import ray
from ray import cloudpickle
from ray._private.async_compat import sync_to_async
from ray._private.utils import get_or_create_event_loop
from ray.actor import ActorClass, ActorHandle
from ray.remote_function import RemoteFunction
from ray.serve import metrics
from ray.serve._private.autoscaling_metrics import InMemoryMetricsStore
from ray.serve._private.common import (
    DeploymentID,
    ReplicaTag,
    ServeComponentType,
    StreamingHTTPRequest,
    gRPCRequest,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    DEFAULT_LATENCY_BUCKET_MS,
    HEALTH_CHECK_METHOD,
    RAY_SERVE_GAUGE_METRIC_SET_PERIOD_S,
    RAY_SERVE_REPLICA_AUTOSCALING_METRIC_RECORD_PERIOD_S,
    RECONFIGURE_METHOD,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.deployment_info import CONTROL_PLANE_CONCURRENCY_GROUP
from ray.serve._private.http_util import (
    ASGIAppReplicaWrapper,
    ASGIMessageQueue,
    ASGIReceiveProxy,
    Response,
)
from ray.serve._private.logging_utils import (
    access_log_msg,
    configure_component_cpu_profiler,
    configure_component_logger,
    configure_component_memory_profiler,
    get_component_logger_file_path,
)
from ray.serve._private.router import RequestMetadata
from ray.serve._private.utils import (
    MetricsPusher,
    merge_dict,
    parse_import_path,
    wrap_to_ray_error,
)
from ray.serve._private.version import DeploymentVersion
from ray.serve.deployment import Deployment
from ray.serve.exceptions import RayServeException
from ray.serve.schema import LoggingConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


def create_replica_wrapper(actor_class_name: str):
    """Creates a replica class wrapping the provided function or class.

    This approach is picked over inheritance to avoid conflict between user
    provided class and the RayServeReplica class.
    """

    # TODO(architkulkarni): Add type hints after upgrading cloudpickle
    class RayServeWrappedReplica(object):
        async def __init__(
            self,
            deployment_name,
            replica_tag,
            serialized_deployment_def: bytes,
            serialized_init_args: bytes,
            serialized_init_kwargs: bytes,
            deployment_config_proto_bytes: bytes,
            version: DeploymentVersion,
            controller_name: str,
            app_name: str = None,
        ):
            self._replica_tag = replica_tag
            deployment_config = DeploymentConfig.from_proto_bytes(
                deployment_config_proto_bytes
            )
            if deployment_config.logging_config is None:
                logging_config = LoggingConfig()
            else:
                logging_config = LoggingConfig(**deployment_config.logging_config)

            configure_component_logger(
                component_type=ServeComponentType.DEPLOYMENT,
                component_name=deployment_name,
                component_id=replica_tag,
                logging_config=logging_config,
            )
            configure_component_memory_profiler(
                component_type=ServeComponentType.DEPLOYMENT,
                component_name=deployment_name,
                component_id=replica_tag,
            )
            self.cpu_profiler, self.cpu_profiler_log = configure_component_cpu_profiler(
                component_type=ServeComponentType.DEPLOYMENT,
                component_name=deployment_name,
                component_id=replica_tag,
            )

            self._event_loop = get_or_create_event_loop()

            deployment_def = cloudpickle.loads(serialized_deployment_def)

            if isinstance(deployment_def, str):
                import_path = deployment_def
                module_name, attr_name = parse_import_path(import_path)
                deployment_def = getattr(import_module(module_name), attr_name)
                # For ray or serve decorated class or function, strip to return
                # original body
                if isinstance(deployment_def, RemoteFunction):
                    deployment_def = deployment_def._function
                elif isinstance(deployment_def, ActorClass):
                    deployment_def = deployment_def.__ray_metadata__.modified_class
                elif isinstance(deployment_def, Deployment):
                    logger.warning(
                        f'The import path "{import_path}" contains a '
                        "decorated Serve deployment. The decorator's settings "
                        "are ignored when deploying via import path."
                    )
                    deployment_def = deployment_def.func_or_class

            init_args = cloudpickle.loads(serialized_init_args)
            init_kwargs = cloudpickle.loads(serialized_init_kwargs)

            if inspect.isfunction(deployment_def):
                is_function = True
            elif inspect.isclass(deployment_def):
                is_function = False
            else:
                assert False, (
                    "deployment_def must be function, class, or "
                    "corresponding import path. Instead, it's type was "
                    f"{type(deployment_def)}."
                )

            # Set the controller name so that serve.connect() in the user's
            # code will connect to the instance that this deployment is running
            # in.
            ray.serve.context._set_internal_replica_context(
                app_name=app_name,
                deployment=deployment_name,
                replica_tag=replica_tag,
                servable_object=None,
                controller_name=controller_name,
            )

            assert controller_name, "Must provide a valid controller_name"

            controller_handle = ray.get_actor(
                controller_name, namespace=SERVE_NAMESPACE
            )

            # Indicates whether the replica has finished initializing.
            self._initialized = False

            # This closure initializes user code and finalizes replica
            # startup. By splitting the initialization step like this,
            # we can already access this actor before the user code
            # has finished initializing.
            # The supervising state manager can then wait
            # for allocation of this replica by using the `is_allocated`
            # method. After that, it calls `reconfigure` to trigger
            # user code initialization.
            async def initialize_replica():
                if is_function:
                    _callable = deployment_def
                else:
                    # This allows deployments to define an async __init__
                    # method (mostly used for testing).
                    _callable = deployment_def.__new__(deployment_def)
                    await sync_to_async(_callable.__init__)(*init_args, **init_kwargs)

                    if isinstance(_callable, ASGIAppReplicaWrapper):
                        await _callable._run_asgi_lifespan_startup()

                # Setting the context again to update the servable_object.
                ray.serve.context._set_internal_replica_context(
                    app_name=app_name,
                    deployment=deployment_name,
                    replica_tag=replica_tag,
                    servable_object=_callable,
                    controller_name=controller_name,
                )

                self.replica = RayServeReplica(
                    _callable,
                    deployment_name,
                    replica_tag,
                    deployment_config.autoscaling_config,
                    version,
                    is_function,
                    controller_handle,
                    app_name,
                )
                self._initialized = True

            # Is it fine that replica is None here?
            # Should we add a check in all methods that use self.replica
            # or, alternatively, create an async get_replica() method?
            self.replica = None
            self._initialize_replica = initialize_replica

            # Used to guard `initialize_replica` so that it isn't called twice.
            self._replica_init_lock = asyncio.Lock()

        @ray.method(concurrency_group=CONTROL_PLANE_CONCURRENCY_GROUP)
        def get_num_ongoing_requests(self) -> int:
            """Fetch the number of ongoing requests at this replica (queue length).

            This runs on a separate thread (using a Ray concurrency group) so it will
            not be blocked by user code.
            """
            return self.replica.get_num_pending_and_running_requests()

        async def handle_request(
            self,
            pickled_request_metadata: bytes,
            *request_args,
            **request_kwargs,
        ) -> Tuple[bytes, Any]:
            request_metadata = pickle.loads(pickled_request_metadata)
            if request_metadata.is_grpc_request:
                # Ensure the request args are a single gRPCRequest object.
                assert len(request_args) == 1 and isinstance(
                    request_args[0], gRPCRequest
                )
                result = await self.replica.call_user_method_grpc_unary(
                    request_metadata=request_metadata, request=request_args[0]
                )
            else:
                result = await self.replica.call_user_method(
                    request_metadata, request_args, request_kwargs
                )

            return result

        async def _handle_http_request_generator(
            self,
            request_metadata: RequestMetadata,
            request: StreamingHTTPRequest,
        ) -> AsyncGenerator[Message, None]:
            """Handle an HTTP request and stream ASGI messages to the caller.

            This is a generator that yields ASGI-compliant messages sent by user code
            via an ASGI send interface.
            """
            receiver_task = None
            call_user_method_task = None
            wait_for_message_task = None
            try:
                receiver = ASGIReceiveProxy(
                    request_metadata.request_id, request.http_proxy_handle
                )
                receiver_task = self._event_loop.create_task(
                    receiver.fetch_until_disconnect()
                )

                scope = pickle.loads(request.pickled_asgi_scope)
                asgi_queue_send = ASGIMessageQueue()
                request_args = (scope, receiver, asgi_queue_send)
                request_kwargs = {}

                # Handle the request in a background asyncio.Task. It's expected that
                # this task will use the provided ASGI send interface to send its HTTP
                # the response. We will poll for the sent messages and yield them back
                # to the caller.
                call_user_method_task = self._event_loop.create_task(
                    self.replica.call_user_method(
                        request_metadata, request_args, request_kwargs
                    )
                )

                while True:
                    wait_for_message_task = self._event_loop.create_task(
                        asgi_queue_send.wait_for_message()
                    )
                    done, _ = await asyncio.wait(
                        [call_user_method_task, wait_for_message_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    # Consume and yield all available messages in the queue.
                    # The messages are batched into a list to avoid unnecessary RPCs and
                    # we use vanilla pickle because it's faster than cloudpickle and we
                    # know it's safe for these messages containing primitive types.
                    yield pickle.dumps(asgi_queue_send.get_messages_nowait())

                    # Exit once `call_user_method` has finished. In this case, all
                    # messages must have already been sent.
                    if call_user_method_task in done:
                        break

                e = call_user_method_task.exception()
                if e is not None:
                    raise e from None
            finally:
                if receiver_task is not None:
                    receiver_task.cancel()

                if (
                    call_user_method_task is not None
                    and not call_user_method_task.done()
                ):
                    call_user_method_task.cancel()

                if (
                    wait_for_message_task is not None
                    and not wait_for_message_task.done()
                ):
                    wait_for_message_task.cancel()

        async def handle_request_streaming(
            self,
            pickled_request_metadata: bytes,
            *request_args,
            **request_kwargs,
        ) -> AsyncGenerator[Any, None]:
            """Generator that is the entrypoint for all `stream=True` handle calls."""
            request_metadata = pickle.loads(pickled_request_metadata)
            if request_metadata.is_grpc_request:
                # Ensure the request args are a single gRPCRequest object.
                assert len(request_args) == 1 and isinstance(
                    request_args[0], gRPCRequest
                )
                generator = self.replica.call_user_method_with_grpc_unary_stream(
                    request_metadata, request_args[0]
                )
            elif request_metadata.is_http_request:
                assert len(request_args) == 1 and isinstance(
                    request_args[0], StreamingHTTPRequest
                )
                generator = self._handle_http_request_generator(
                    request_metadata, request_args[0]
                )
            else:
                generator = self.replica.call_user_method_generator(
                    request_metadata, request_args, request_kwargs
                )

            async for result in generator:
                yield result

        async def handle_request_from_java(
            self,
            proto_request_metadata: bytes,
            *request_args,
            **request_kwargs,
        ) -> Any:
            from ray.serve.generated.serve_pb2 import (
                RequestMetadata as RequestMetadataProto,
            )

            proto = RequestMetadataProto.FromString(proto_request_metadata)
            request_metadata: RequestMetadata = RequestMetadata(
                proto.request_id,
                proto.endpoint,
                call_method=proto.call_method,
                multiplexed_model_id=proto.multiplexed_model_id,
                route=proto.route,
            )
            request_args = request_args[0]
            return await self.replica.call_user_method(
                request_metadata, request_args, request_kwargs
            )

        async def is_allocated(self) -> str:
            """poke the replica to check whether it's alive.

            When calling this method on an ActorHandle, it will complete as
            soon as the actor has started running. We use this mechanism to
            detect when a replica has been allocated a worker slot.
            At this time, the replica can transition from PENDING_ALLOCATION
            to PENDING_INITIALIZATION startup state.

            Returns:
                The PID, actor ID, node ID, node IP, and log filepath id of the replica.
            """

            return (
                os.getpid(),
                ray.get_runtime_context().get_actor_id(),
                ray.get_runtime_context().get_worker_id(),
                ray.get_runtime_context().get_node_id(),
                ray.util.get_node_ip_address(),
                get_component_logger_file_path(),
            )

        async def initialize_and_get_metadata(
            self,
            deployment_config: DeploymentConfig = None,
            _after: Optional[Any] = None,
        ) -> Tuple[DeploymentConfig, DeploymentVersion]:
            # Unused `_after` argument is for scheduling: passing an ObjectRef
            # allows delaying this call until after the `_after` call has returned.
            try:
                # Ensure that initialization is only performed once.
                # When controller restarts, it will call this method again.
                async with self._replica_init_lock:
                    if not self._initialized:
                        await self._initialize_replica()
                    if deployment_config:
                        await self.replica.update_user_config(
                            deployment_config.user_config
                        )

                # A new replica should not be considered healthy until it passes
                # an initial health check. If an initial health check fails,
                # consider it an initialization failure.
                await self.check_health()
                return await self._get_metadata()
            except Exception:
                raise RuntimeError(traceback.format_exc()) from None

        async def reconfigure(
            self,
            deployment_config: DeploymentConfig,
        ) -> Tuple[DeploymentConfig, DeploymentVersion]:
            try:
                await self.replica.reconfigure(deployment_config)
                return await self._get_metadata()
            except Exception:
                raise RuntimeError(traceback.format_exc()) from None

        async def _get_metadata(
            self,
        ) -> Tuple[DeploymentConfig, DeploymentVersion]:
            return self.replica.version.deployment_config, self.replica.version

        def _save_cpu_profile_data(self) -> str:
            """Saves CPU profiling data, if CPU profiling is enabled.

            Logs a warning if CPU profiling is disabled.
            """

            if self.cpu_profiler is not None:
                import marshal

                self.cpu_profiler.snapshot_stats()
                with open(self.cpu_profiler_log, "wb") as f:
                    marshal.dump(self.cpu_profiler.stats, f)
                logger.info(f'Saved CPU profile data to file "{self.cpu_profiler_log}"')
                return self.cpu_profiler_log
            else:
                logger.error(
                    "Attempted to save CPU profile data, but failed because no "
                    "CPU profiler was running! Enable CPU profiling by enabling "
                    "the RAY_SERVE_ENABLE_CPU_PROFILING env var."
                )

        async def prepare_for_shutdown(self):
            if self.replica is not None:
                return await self.replica.prepare_for_shutdown()

        @ray.method(concurrency_group=CONTROL_PLANE_CONCURRENCY_GROUP)
        async def check_health(self):
            await self.replica.check_health()

    # Dynamically create a new class with custom name here so Ray picks it up
    # correctly in actor metadata table and observability stack.
    return type(
        actor_class_name,
        (RayServeWrappedReplica,),
        dict(RayServeWrappedReplica.__dict__),
    )


class RayServeReplica:
    """Handles requests with the provided callable."""

    def __init__(
        self,
        _callable: Callable,
        deployment_name: str,
        replica_tag: ReplicaTag,
        autoscaling_config: Any,
        version: DeploymentVersion,
        is_function: bool,
        controller_handle: ActorHandle,
        app_name: str,
    ) -> None:
        self.deployment_id = DeploymentID(deployment_name, app_name)
        self.replica_tag = replica_tag
        self.callable = _callable
        self.is_function = is_function
        self.version = version
        self.deployment_config: DeploymentConfig = version.deployment_config
        self.rwlock = aiorwlock.RWLock()
        self.delete_lock = asyncio.Lock()

        user_health_check = getattr(_callable, HEALTH_CHECK_METHOD, None)
        if not callable(user_health_check):

            def user_health_check():
                pass

        self.user_health_check = sync_to_async(user_health_check)

        self.request_counter = metrics.Counter(
            "serve_deployment_request_counter",
            description=(
                "The number of queries that have been processed in this replica."
            ),
            tag_keys=("route",),
        )

        self.error_counter = metrics.Counter(
            "serve_deployment_error_counter",
            description=(
                "The number of exceptions that have occurred in this replica."
            ),
            tag_keys=("route",),
        )

        self.restart_counter = metrics.Counter(
            "serve_deployment_replica_starts",
            description=(
                "The number of times this replica has been restarted due to failure."
            ),
        )

        self.processing_latency_tracker = metrics.Histogram(
            "serve_deployment_processing_latency_ms",
            description="The latency for queries to be processed.",
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("route",),
        )

        self.num_processing_items = metrics.Gauge(
            "serve_replica_processing_queries",
            description="The current number of queries being processed.",
        )

        self.num_pending_items = metrics.Gauge(
            "serve_replica_pending_queries",
            description="The current number of pending queries.",
        )

        self.restart_counter.inc()

        self.autoscaling_metrics_store = InMemoryMetricsStore()
        self.metrics_pusher = MetricsPusher()
        if autoscaling_config:
            process_remote_func = controller_handle.record_autoscaling_metrics.remote
            config = autoscaling_config
            self.metrics_pusher.register_task(
                self.collect_autoscaling_metrics,
                config.metrics_interval_s,
                process_remote_func,
            )
            self.metrics_pusher.register_task(
                lambda: {self.replica_tag: self.get_num_pending_and_running_requests()},
                min(
                    RAY_SERVE_REPLICA_AUTOSCALING_METRIC_RECORD_PERIOD_S,
                    config.metrics_interval_s,
                ),
                self._add_autoscaling_metrics_point,
            )

        self.metrics_pusher.register_task(
            self._set_replica_requests_metrics,
            RAY_SERVE_GAUGE_METRIC_SET_PERIOD_S,
        )
        self.metrics_pusher.start()

    def _add_autoscaling_metrics_point(self, data, send_timestamp: float):
        self.autoscaling_metrics_store.add_metrics_point(data, send_timestamp)

    def _set_replica_requests_metrics(self):
        self.num_processing_items.set(self.get_num_running_requests())
        self.num_pending_items.set(self.get_num_pending_requests())

    async def check_health(self):
        await self.user_health_check()

    def _get_handle_request_stats(self) -> Optional[Dict[str, int]]:
        replica_actor_name = self.deployment_id.to_replica_actor_class_name()
        actor_stats = ray.runtime_context.get_runtime_context()._get_actor_call_stats()
        method_stats = actor_stats.get(f"{replica_actor_name}.handle_request")
        streaming_method_stats = actor_stats.get(
            f"{replica_actor_name}.handle_request_streaming"
        )
        method_stats_java = actor_stats.get(
            f"{replica_actor_name}.handle_request_from_java"
        )
        return merge_dict(
            merge_dict(method_stats, streaming_method_stats), method_stats_java
        )

    def get_num_running_requests(self) -> int:
        stats = self._get_handle_request_stats() or {}
        return stats.get("running", 0)

    def get_num_pending_requests(self) -> int:
        stats = self._get_handle_request_stats() or {}
        return stats.get("pending", 0)

    def get_num_pending_and_running_requests(self) -> int:
        stats = self._get_handle_request_stats() or {}
        return stats.get("pending", 0) + stats.get("running", 0)

    def collect_autoscaling_metrics(self):
        look_back_period = self.deployment_config.autoscaling_config.look_back_period_s
        return self.replica_tag, self.autoscaling_metrics_store.window_average(
            self.replica_tag, time.time() - look_back_period
        )

    def get_runner_method(self, request_metadata: RequestMetadata) -> Callable:
        method_name = request_metadata.call_method
        if not hasattr(self.callable, method_name):
            # Filter to methods that don't start with '__' prefix.
            def callable_method_filter(attr):
                if attr.startswith("__"):
                    return False
                elif not callable(getattr(self.callable, attr)):
                    return False

                return True

            methods = list(filter(callable_method_filter, dir(self.callable)))
            raise RayServeException(
                f"Tried to call a method '{method_name}' "
                "that does not exist. Available methods: "
                f"{methods}."
            )
        if self.is_function:
            return self.callable

        return getattr(self.callable, method_name)

    async def send_user_result_over_asgi(
        self, result: Any, scope: Scope, receive: Receive, send: Send
    ):
        """Handle the result from user code and send it over the ASGI interface.

        If the result is already a Response type, it is sent directly. Otherwise, it
        is converted to a custom Response type that handles serialization for
        common Python objects.
        """
        if isinstance(result, starlette.responses.Response):
            await result(scope, receive, send)
        else:
            await Response(result).send(scope, receive, send)

    async def reconfigure(self, deployment_config: DeploymentConfig):
        old_user_config = self.deployment_config.user_config
        self.deployment_config = deployment_config
        self.version = DeploymentVersion.from_deployment_version(
            self.version, self.deployment_config
        )

        if deployment_config.logging_config:
            logging_config = LoggingConfig(**deployment_config.logging_config)
            configure_component_logger(
                component_type=ServeComponentType.DEPLOYMENT,
                component_name=self.deployment_id.name,
                component_id=self.replica_tag,
                logging_config=logging_config,
            )

        if old_user_config != deployment_config.user_config:
            await self.update_user_config(deployment_config.user_config)

    async def update_user_config(self, user_config: Any):
        async with self.rwlock.writer:
            if user_config is not None:
                if self.is_function:
                    raise ValueError(
                        "deployment_def must be a class to use user_config"
                    )
                elif not hasattr(self.callable, RECONFIGURE_METHOD):
                    raise RayServeException(
                        "user_config specified but deployment "
                        + self.deployment_id
                        + " missing "
                        + RECONFIGURE_METHOD
                        + " method"
                    )
                reconfigure_method = sync_to_async(
                    getattr(self.callable, RECONFIGURE_METHOD)
                )
                await reconfigure_method(user_config)

    @asynccontextmanager
    async def wrap_user_method_call(self, request_metadata: RequestMetadata):
        """Context manager that should be used to wrap user method calls.

        This sets up the serve request context, grabs the reader lock to avoid mutating
        user_config during method calls, and records metrics based on the result of the
        method.
        """
        # Set request context variables for subsequent handle so that
        # handle can pass the correct request context to subsequent replicas.
        ray.serve.context._serve_request_context.set(
            ray.serve.context._RequestContext(
                request_metadata.route,
                request_metadata.request_id,
                self.deployment_id.app,
                request_metadata.multiplexed_model_id,
            )
        )

        logger.info(
            f"Started executing request {request_metadata.request_id}",
            extra={"log_to_stderr": False, "serve_access_log": True},
        )
        start_time = time.time()
        user_exception = None
        try:
            yield
        except Exception as e:
            user_exception = e
            logger.exception(f"Request failed due to {type(e).__name__}:")
            if ray.util.pdb._is_ray_debugger_enabled():
                ray.util.pdb._post_mortem()

        latency_ms = (time.time() - start_time) * 1000
        self.processing_latency_tracker.observe(
            latency_ms, tags={"route": request_metadata.route}
        )

        if user_exception is None:
            status_str = "OK"
        elif isinstance(user_exception, asyncio.CancelledError):
            status_str = "CANCELLED"
        else:
            status_str = "ERROR"

        logger.info(
            access_log_msg(
                method=request_metadata.call_method,
                status=status_str,
                latency_ms=latency_ms,
            ),
            extra={"serve_access_log": True},
        )
        if user_exception is None:
            self.request_counter.inc(tags={"route": request_metadata.route})
        else:
            self.error_counter.inc(tags={"route": request_metadata.route})
            raise user_exception from None

    async def call_user_method_with_grpc_unary_stream(
        self, request_metadata: RequestMetadata, request: gRPCRequest
    ) -> AsyncGenerator[bytes, None]:
        """Call a user method that is expected to be a generator.

        Deserializes gRPC request into protobuf object and pass into replica's runner
        method. Returns a generator of serialized protobuf bytes from the replica.
        """
        async with self.wrap_user_method_call(request_metadata):
            user_method = self.get_runner_method(request_metadata)
            user_request = pickle.loads(request.grpc_user_request)
            result_generator = user_method(user_request)
            if inspect.iscoroutine(result_generator):
                result_generator = await result_generator

            if inspect.isgenerator(result_generator):
                for result in result_generator:
                    yield result.SerializeToString()
            elif inspect.isasyncgen(result_generator):
                async for result in result_generator:
                    yield result.SerializeToString()
            else:
                raise TypeError(
                    "When using `stream=True`, the called method must be a generator "
                    f"function, but '{user_method.__name__}' is not."
                )

    async def call_user_method_grpc_unary(
        self, request_metadata: RequestMetadata, request: gRPCRequest
    ) -> bytes:
        """Call a user method that is *not* expected to be a generator.

        Deserializes gRPC request into protobuf object and pass into replica's runner
        method. Returns a serialized protobuf bytes from the replica.
        """
        async with self.wrap_user_method_call(request_metadata):
            user_request = pickle.loads(request.grpc_user_request)

            runner_method = self.get_runner_method(request_metadata)
            if inspect.isgeneratorfunction(runner_method) or inspect.isasyncgenfunction(
                runner_method
            ):
                raise TypeError(
                    f"Method '{runner_method.__name__}' is a generator function. "
                    "You must use `handle.options(stream=True)` to call "
                    "generators on a deployment."
                )

            method_to_call = sync_to_async(runner_method)

            result = await method_to_call(user_request)
            return result.SerializeToString()

    async def call_user_method(
        self,
        request_metadata: RequestMetadata,
        request_args: Tuple[Any],
        request_kwargs: Dict[str, Any],
    ) -> Any:
        """Call a user method that is *not* expected to be a generator.

        Raises any exception raised by the user code so it can be propagated as a
        `RayTaskError`.
        """
        async with self.wrap_user_method_call(request_metadata):
            if request_metadata.is_http_request:
                # For HTTP requests we always expect (scope, receive, send) as args.
                assert len(request_args) == 3
                scope, receive, send = request_args

                if isinstance(self.callable, ASGIAppReplicaWrapper):
                    request_args = (scope, receive, send)
                else:
                    request_args = (Request(scope, receive, send),)

            runner_method = None
            try:
                runner_method = self.get_runner_method(request_metadata)
                if inspect.isgeneratorfunction(
                    runner_method
                ) or inspect.isasyncgenfunction(runner_method):
                    raise TypeError(
                        f"Method '{runner_method.__name__}' is a generator function. "
                        "You must use `handle.options(stream=True)` to call "
                        "generators on a deployment."
                    )

                method_to_call = sync_to_async(runner_method)

                # Edge case to support empty HTTP handlers: don't pass the Request
                # argument if the callable has no parameters.
                if (
                    request_metadata.is_http_request
                    and len(inspect.signature(runner_method).parameters) == 0
                ):
                    request_args, request_kwargs = tuple(), {}

                result = await method_to_call(*request_args, **request_kwargs)
                if inspect.isgenerator(result) or inspect.isasyncgen(result):
                    raise TypeError(
                        f"Method '{runner_method.__name__}' returned a generator. You "
                        "must use `handle.options(stream=True)` to call "
                        "generators on a deployment."
                    )

            except Exception as e:
                function_name = "unknown"
                if runner_method is not None:
                    function_name = runner_method.__name__
                e = wrap_to_ray_error(function_name, e)
                if request_metadata.is_http_request:
                    result = starlette.responses.Response(
                        f"Unexpected error, traceback: {e}.", status_code=500
                    )
                    await self.send_user_result_over_asgi(result, scope, receive, send)

                raise e from None

            if request_metadata.is_http_request and not isinstance(
                self.callable, ASGIAppReplicaWrapper
            ):
                # For the FastAPI codepath, the response has already been sent over the
                # ASGI interface, but for the vanilla deployment codepath we need to
                # send it.
                await self.send_user_result_over_asgi(result, scope, receive, send)

            return result

    async def call_user_method_generator(
        self,
        request_metadata: RequestMetadata,
        request_args: Tuple[Any],
        request_kwargs: Dict[str, Any],
    ) -> AsyncGenerator[Any, None]:
        """Call a user method that is expected to be a generator.

        Raises any exception raised by the user code so it can be propagated as a
        `RayTaskError`.
        """
        # TODO(edoakes): this is only here because there is an issue where async
        # generators in actors have the `asyncio.current_task()` change between
        # iterations: https://github.com/ray-project/ray/issues/37147. `aiorwlock`
        # relies on the current task being stable, so it raises an exception.
        # This flag should be removed once the above issue is closed.
        async with self.wrap_user_method_call(request_metadata):
            assert (
                not request_metadata.is_http_request
            ), "HTTP requests should go through `call_user_method`."
            user_method = self.get_runner_method(request_metadata)
            result_generator = user_method(*request_args, **request_kwargs)
            if inspect.iscoroutine(result_generator):
                result_generator = await result_generator

            if inspect.isgenerator(result_generator):
                for result in result_generator:
                    yield result
            elif inspect.isasyncgen(result_generator):
                async for result in result_generator:
                    yield result
            else:
                raise TypeError(
                    "When using `stream=True`, the called method must be a generator "
                    f"function, but '{user_method.__name__}' is not."
                )

    async def prepare_for_shutdown(self):
        """Perform graceful shutdown.

        Trigger a graceful shutdown protocol that will wait for all the queued
        tasks to be completed and return to the controller.
        """
        while True:
            # Sleep first because we want to make sure all the routers receive
            # the notification to remove this replica first.
            await asyncio.sleep(self.deployment_config.graceful_shutdown_wait_loop_s)

            num_ongoing_requests = self.get_num_pending_and_running_requests()
            if num_ongoing_requests > 0:
                logger.info(
                    "Waiting for an additional "
                    f"{self.deployment_config.graceful_shutdown_wait_loop_s}s to shut "
                    f"down because there are {num_ongoing_requests} ongoing "
                    "requests."
                )
            else:
                logger.info(
                    "Graceful shutdown complete; replica exiting.",
                    extra={"log_to_stderr": False},
                )
                break

        # Explicitly call the del method to trigger clean up.
        # We set the del method to noop after successfully calling it so the
        # destructor is called only once.
        async with self.delete_lock:
            if self.metrics_pusher:
                self.metrics_pusher.shutdown()

            if not hasattr(self, "callable"):
                return

            try:
                if hasattr(self.callable, "__del__"):
                    # Make sure to accept `async def __del__(self)` as well.
                    await sync_to_async(self.callable.__del__)()
                    setattr(self.callable, "__del__", lambda _: None)

                if hasattr(self.callable, "__serve_multiplex_wrapper"):
                    await getattr(self.callable, "__serve_multiplex_wrapper").shutdown()

            except Exception as e:
                logger.exception(f"Exception during graceful shutdown of replica: {e}")
            finally:
                if hasattr(self.callable, "__del__"):
                    del self.callable
