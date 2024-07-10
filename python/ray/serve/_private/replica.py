import asyncio
import inspect
import logging
import os
import pickle
import threading
import time
import traceback
from contextlib import contextmanager
from functools import wraps
from importlib import import_module
from typing import Any, AsyncGenerator, Callable, Dict, Optional, Tuple, Union

import starlette.responses

import ray
from ray import cloudpickle
from ray._private.utils import get_or_create_event_loop
from ray.actor import ActorClass
from ray.remote_function import RemoteFunction
from ray.serve import metrics
from ray.serve._private.common import (
    DeploymentID,
    ReplicaID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    ServeComponentType,
    StreamingHTTPRequest,
    gRPCRequest,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    DEFAULT_LATENCY_BUCKET_MS,
    GRPC_CONTEXT_ARG_NAME,
    HEALTH_CHECK_METHOD,
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    RAY_SERVE_REPLICA_AUTOSCALING_METRIC_RECORD_PERIOD_S,
    RECONFIGURE_METHOD,
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.http_util import (
    ASGIAppReplicaWrapper,
    ASGIArgs,
    ASGIReceiveProxy,
    MessageQueue,
    Response,
)
from ray.serve._private.logging_utils import (
    access_log_msg,
    configure_component_cpu_profiler,
    configure_component_logger,
    configure_component_memory_profiler,
    get_component_logger_file_path,
)
from ray.serve._private.metrics_utils import InMemoryMetricsStore, MetricsPusher
from ray.serve._private.utils import get_component_file_name  # noqa: F401
from ray.serve._private.utils import parse_import_path, wrap_to_ray_error
from ray.serve._private.version import DeploymentVersion
from ray.serve.config import AutoscalingConfig
from ray.serve.deployment import Deployment
from ray.serve.exceptions import RayServeException
from ray.serve.schema import LoggingConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _load_deployment_def_from_import_path(import_path: str) -> Callable:
    module_name, attr_name = parse_import_path(import_path)
    deployment_def = getattr(import_module(module_name), attr_name)

    # For ray or serve decorated class or function, strip to return
    # original body.
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

    return deployment_def


class ReplicaMetricsManager:
    """Manages metrics for the replica.

    A variety of metrics are managed:
        - Fine-grained metrics are set for every request.
        - Autoscaling statistics are periodically pushed to the controller.
        - Queue length metrics are periodically recorded as user-facing gauges.
    """

    PUSH_METRICS_TO_CONTROLLER_TASK_NAME = "push_metrics_to_controller"
    RECORD_METRICS_TASK_NAME = "record_metrics"
    SET_REPLICA_REQUEST_METRIC_GAUGE_TASK_NAME = "set_replica_request_metric_gauge"

    def __init__(
        self,
        replica_id: ReplicaID,
        event_loop: asyncio.BaseEventLoop,
        autoscaling_config: Optional[AutoscalingConfig],
    ):
        self._replica_id = replica_id
        self._metrics_pusher = MetricsPusher()
        self._metrics_store = InMemoryMetricsStore()
        self._autoscaling_config = autoscaling_config
        self._controller_handle = ray.get_actor(
            SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE
        )
        self._num_ongoing_requests = 0

        # Request counter (only set on replica startup).
        self._restart_counter = metrics.Counter(
            "serve_deployment_replica_starts",
            description=(
                "The number of times this replica has been restarted due to failure."
            ),
        )
        self._restart_counter.inc()

        # Per-request metrics.
        self._request_counter = metrics.Counter(
            "serve_deployment_request_counter",
            description=(
                "The number of queries that have been processed in this replica."
            ),
            tag_keys=("route",),
        )

        self._error_counter = metrics.Counter(
            "serve_deployment_error_counter",
            description=(
                "The number of exceptions that have occurred in this replica."
            ),
            tag_keys=("route",),
        )

        self._processing_latency_tracker = metrics.Histogram(
            "serve_deployment_processing_latency_ms",
            description="The latency for queries to be processed.",
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("route",),
        )

        self._num_ongoing_requests_gauge = metrics.Gauge(
            "serve_replica_processing_queries",
            description="The current number of queries being processed.",
        )

        self.set_autoscaling_config(autoscaling_config)

    async def shutdown(self):
        """Stop periodic background tasks."""

        await self._metrics_pusher.graceful_shutdown()

    def set_autoscaling_config(self, autoscaling_config: Optional[AutoscalingConfig]):
        """Dynamically update autoscaling config."""

        self._autoscaling_config = autoscaling_config

        if (
            not RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE
            and self._autoscaling_config
        ):
            self._metrics_pusher.start()

            # Push autoscaling metrics to the controller periodically.
            self._metrics_pusher.register_or_update_task(
                self.PUSH_METRICS_TO_CONTROLLER_TASK_NAME,
                self._push_autoscaling_metrics,
                self._autoscaling_config.metrics_interval_s,
            )
            # Collect autoscaling metrics locally periodically.
            self._metrics_pusher.register_or_update_task(
                self.RECORD_METRICS_TASK_NAME,
                self._add_autoscaling_metrics_point,
                min(
                    RAY_SERVE_REPLICA_AUTOSCALING_METRIC_RECORD_PERIOD_S,
                    self._autoscaling_config.metrics_interval_s,
                ),
            )

    def inc_num_ongoing_requests(self) -> int:
        """Increment the current total queue length of requests for this replica."""
        self._num_ongoing_requests += 1
        self._num_ongoing_requests_gauge.set(self._num_ongoing_requests)

    def dec_num_ongoing_requests(self) -> int:
        """Decrement the current total queue length of requests for this replica."""
        self._num_ongoing_requests -= 1
        self._num_ongoing_requests_gauge.set(self._num_ongoing_requests)

    def get_num_ongoing_requests(self) -> int:
        """Get current total queue length of requests for this replica."""
        return self._num_ongoing_requests

    def record_request_metrics(
        self, *, route: str, status_str: str, latency_ms: float, was_error: bool
    ):
        """Records per-request metrics."""
        self._processing_latency_tracker.observe(latency_ms, tags={"route": route})
        if was_error:
            self._error_counter.inc(tags={"route": route})
        else:
            self._request_counter.inc(tags={"route": route})

    def _push_autoscaling_metrics(self) -> Dict[str, Any]:
        look_back_period = self._autoscaling_config.look_back_period_s
        self._controller_handle.record_autoscaling_metrics.remote(
            replica_id=self._replica_id,
            window_avg=self._metrics_store.window_average(
                self._replica_id, time.time() - look_back_period
            ),
            send_timestamp=time.time(),
        )

    def _add_autoscaling_metrics_point(self) -> None:
        self._metrics_store.add_metrics_point(
            {self._replica_id: self._num_ongoing_requests},
            time.time(),
        )


class ReplicaActor:
    """Actor definition for replicas of Ray Serve deployments.

    This class defines the interface that the controller and deployment handles
    (i.e., from proxies and other replicas) use to interact with a replica.

    All interaction with the user-provided callable is done via the
    `UserCallableWrapper` class.
    """

    async def __init__(
        self,
        replica_id: ReplicaID,
        serialized_deployment_def: bytes,
        serialized_init_args: bytes,
        serialized_init_kwargs: bytes,
        deployment_config_proto_bytes: bytes,
        version: DeploymentVersion,
    ):
        self._version = version
        self._replica_id = replica_id
        self._deployment_id = replica_id.deployment_id
        self._deployment_config = DeploymentConfig.from_proto_bytes(
            deployment_config_proto_bytes
        )
        self._component_name = f"{self._deployment_id.name}"
        if self._deployment_id.app_name:
            self._component_name = (
                f"{self._deployment_id.app_name}_" + self._component_name
            )

        self._component_id = self._replica_id.unique_id
        self._configure_logger_and_profilers(self._deployment_config.logging_config)
        self._event_loop = get_or_create_event_loop()

        deployment_def = cloudpickle.loads(serialized_deployment_def)
        if isinstance(deployment_def, str):
            deployment_def = _load_deployment_def_from_import_path(deployment_def)

        self._user_callable_wrapper = UserCallableWrapper(
            deployment_def,
            cloudpickle.loads(serialized_init_args),
            cloudpickle.loads(serialized_init_kwargs),
            deployment_id=self._deployment_id,
        )

        # Guards against calling the user's callable constructor multiple times.
        self._user_callable_initialized = False
        self._user_callable_initialized_lock = asyncio.Lock()
        self._initialization_latency: Optional[float] = None

        # Set metadata for logs and metrics.
        # servable_object will be populated in `initialize_and_get_metadata`.
        self._set_internal_replica_context(servable_object=None)

        self._metrics_manager = ReplicaMetricsManager(
            replica_id,
            self._event_loop,
            self._deployment_config.autoscaling_config,
        )

    def _set_internal_replica_context(self, *, servable_object: Callable = None):
        ray.serve.context._set_internal_replica_context(
            replica_id=self._replica_id,
            servable_object=servable_object,
            _deployment_config=self._deployment_config,
        )

    def _configure_logger_and_profilers(
        self, logging_config: Union[None, Dict, LoggingConfig]
    ):
        if logging_config is None:
            logging_config = {}
        if isinstance(logging_config, dict):
            logging_config = LoggingConfig(**logging_config)

        configure_component_logger(
            component_type=ServeComponentType.REPLICA,
            component_name=self._component_name,
            component_id=self._component_id,
            logging_config=logging_config,
        )
        configure_component_memory_profiler(
            component_type=ServeComponentType.REPLICA,
            component_name=self._component_name,
            component_id=self._component_id,
        )
        self.cpu_profiler, self.cpu_profiler_log = configure_component_cpu_profiler(
            component_type=ServeComponentType.REPLICA,
            component_name=self._component_name,
            component_id=self._component_id,
        )

    def get_num_ongoing_requests(self) -> int:
        """Fetch the number of ongoing requests at this replica (queue length).

        This runs on a separate thread (using a Ray concurrency group) so it will
        not be blocked by user code.
        """
        return self._metrics_manager.get_num_ongoing_requests()

    @contextmanager
    def _wrap_user_method_call(self, request_metadata: RequestMetadata):
        """Context manager that wraps user method calls.

        1) Sets the request context var with appropriate metadata.
        2) Records the access log message (if not disabled).
        3) Records per-request metrics via the metrics manager.
        """
        ray.serve.context._serve_request_context.set(
            ray.serve.context._RequestContext(
                route=request_metadata.route,
                request_id=request_metadata.request_id,
                app_name=self._deployment_id.app_name,
                multiplexed_model_id=request_metadata.multiplexed_model_id,
                grpc_context=request_metadata.grpc_context,
            )
        )

        start_time = time.time()
        user_exception = None
        try:
            self._metrics_manager.inc_num_ongoing_requests()
            yield
        except asyncio.CancelledError as e:
            user_exception = e
        except Exception as e:
            user_exception = e
            logger.error(f"Request failed:\n{e}")
            if ray.util.pdb._is_ray_debugger_enabled():
                ray.util.pdb._post_mortem()
        finally:
            self._metrics_manager.dec_num_ongoing_requests()

        latency_ms = (time.time() - start_time) * 1000
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
        self._metrics_manager.record_request_metrics(
            route=request_metadata.route,
            status_str=status_str,
            latency_ms=latency_ms,
            was_error=user_exception is not None,
        )

        if user_exception is not None:
            raise user_exception from None

    async def handle_request(
        self,
        pickled_request_metadata: bytes,
        *request_args,
        **request_kwargs,
    ) -> Tuple[bytes, Any]:
        """Entrypoint for `stream=False` calls."""
        request_metadata = pickle.loads(pickled_request_metadata)
        with self._wrap_user_method_call(request_metadata):
            return await self._user_callable_wrapper.call_user_method(
                request_metadata, request_args, request_kwargs
            )

    async def _call_user_generator(
        self,
        request_metadata: RequestMetadata,
        request_args: Tuple[Any],
        request_kwargs: Dict[str, Any],
    ) -> AsyncGenerator[Any, None]:
        """Calls a user method for a streaming call and yields its results.

        The user method is called in an asyncio `Task` and places its results on a
        `result_queue`. This method pulls and yields from the `result_queue`.
        """
        call_user_method_future = None
        wait_for_message_task = None
        try:
            result_queue = MessageQueue()

            # `asyncio.Event`s are not thread safe, so `call_soon_threadsafe` must be
            # used to interact with the result queue from the user callable thread.
            async def _enqueue_thread_safe(item: Any):
                self._event_loop.call_soon_threadsafe(result_queue.put_nowait, item)

            call_user_method_future = self._user_callable_wrapper.call_user_method(
                request_metadata,
                request_args,
                request_kwargs,
                generator_result_callback=_enqueue_thread_safe,
            )

            while True:
                wait_for_message_task = self._event_loop.create_task(
                    result_queue.wait_for_message()
                )
                done, _ = await asyncio.wait(
                    [call_user_method_future, wait_for_message_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Consume and yield all available messages in the queue.
                messages = result_queue.get_messages_nowait()
                if messages:
                    # HTTP (ASGI) messages are only consumed by the proxy so batch them
                    # and use vanilla pickle (we know it's safe because these messages
                    # only contain primitive Python types).
                    if request_metadata.is_http_request:
                        yield pickle.dumps(messages)
                    else:
                        for msg in messages:
                            yield msg

                # Exit once `call_user_method` has finished. In this case, all
                # messages must have already been sent.
                if call_user_method_future in done:
                    break

            e = call_user_method_future.exception()
            if e is not None:
                raise e from None
        finally:
            if (
                call_user_method_future is not None
                and not call_user_method_future.done()
            ):
                call_user_method_future.cancel()

            if wait_for_message_task is not None and not wait_for_message_task.done():
                wait_for_message_task.cancel()

    async def handle_request_streaming(
        self,
        pickled_request_metadata: bytes,
        *request_args,
        **request_kwargs,
    ) -> AsyncGenerator[Any, None]:
        """Generator that is the entrypoint for all `stream=True` handle calls."""
        request_metadata = pickle.loads(pickled_request_metadata)
        with self._wrap_user_method_call(request_metadata):
            async for result in self._call_user_generator(
                request_metadata,
                request_args,
                request_kwargs,
            ):
                yield result

    async def handle_request_with_rejection(
        self,
        pickled_request_metadata: bytes,
        *request_args,
        **request_kwargs,
    ) -> AsyncGenerator[Any, None]:
        """Entrypoint for all requests with strict max_ongoing_requests enforcement.

        The first response from this generator is always a system message indicating
        if the request was accepted (the replica has capacity for the request) or
        rejected (the replica is already at max_ongoing_requests).

        For non-streaming requests, there will only be one more message, the unary
        result of the user request handler.

        For streaming requests, the subsequent messages will be the results of the
        user request handler (which must be a generator).
        """
        request_metadata = pickle.loads(pickled_request_metadata)
        limit = self._deployment_config.max_ongoing_requests
        num_ongoing_requests = self.get_num_ongoing_requests()
        if num_ongoing_requests >= limit:
            logger.warning(
                f"Replica at capacity of max_ongoing_requests={limit}, "
                f"rejecting request {request_metadata.request_id}.",
                extra={"log_to_stderr": False},
            )
            yield pickle.dumps(
                ReplicaQueueLengthInfo(
                    accepted=False, num_ongoing_requests=num_ongoing_requests
                )
            )
            return

        with self._wrap_user_method_call(request_metadata):
            yield pickle.dumps(
                ReplicaQueueLengthInfo(
                    accepted=True,
                    # NOTE(edoakes): `_wrap_user_method_call` will increment the number
                    # of ongoing requests to include this one, so re-fetch the value.
                    num_ongoing_requests=self.get_num_ongoing_requests(),
                )
            )

            if request_metadata.is_streaming:
                async for result in self._call_user_generator(
                    request_metadata,
                    request_args,
                    request_kwargs,
                ):
                    yield result
            else:
                yield await self._user_callable_wrapper.call_user_method(
                    request_metadata, request_args, request_kwargs
                )

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
            request_id=proto.request_id,
            internal_request_id=proto.internal_request_id,
            endpoint=proto.endpoint,
            call_method=proto.call_method,
            multiplexed_model_id=proto.multiplexed_model_id,
            route=proto.route,
        )
        with self._wrap_user_method_call(request_metadata):
            return await self._user_callable_wrapper.call_user_method(
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
    ) -> Tuple[DeploymentConfig, DeploymentVersion, Optional[float]]:
        """Handles initializing the replica.

        Returns: 3-tuple containing
            1. DeploymentConfig of the replica
            2. DeploymentVersion of the replica
            3. Initialization duration in seconds
        """
        # Unused `_after` argument is for scheduling: passing an ObjectRef
        # allows delaying this call until after the `_after` call has returned.
        try:
            # Ensure that initialization is only performed once.
            # When controller restarts, it will call this method again.
            async with self._user_callable_initialized_lock:
                initialization_start_time = time.time()
                if not self._user_callable_initialized:
                    await self._user_callable_wrapper.initialize_callable()
                    self._user_callable_initialized = True
                    self._set_internal_replica_context(
                        servable_object=self._user_callable_wrapper.user_callable
                    )
                if deployment_config:
                    await self._user_callable_wrapper.call_reconfigure(
                        deployment_config.user_config
                    )

            # A new replica should not be considered healthy until it passes
            # an initial health check. If an initial health check fails,
            # consider it an initialization failure.
            await self.check_health()

            # Save the initialization latency if the replica is initializing
            # for the first time.
            if self._initialization_latency is None:
                self._initialization_latency = time.time() - initialization_start_time

            return self._get_metadata()
        except Exception:
            raise RuntimeError(traceback.format_exc()) from None

    async def reconfigure(
        self,
        deployment_config: DeploymentConfig,
    ) -> Tuple[DeploymentConfig, DeploymentVersion, Optional[float]]:
        try:
            user_config_changed = (
                deployment_config.user_config != self._deployment_config.user_config
            )
            logging_config_changed = (
                deployment_config.logging_config
                != self._deployment_config.logging_config
            )
            self._deployment_config = deployment_config
            self._version = DeploymentVersion.from_deployment_version(
                self._version, deployment_config
            )

            self._metrics_manager.set_autoscaling_config(
                deployment_config.autoscaling_config
            )
            if logging_config_changed:
                self._configure_logger_and_profilers(deployment_config.logging_config)

            if user_config_changed:
                await self._user_callable_wrapper.call_reconfigure(
                    deployment_config.user_config
                )

            # We need to update internal replica context to reflect the new
            # deployment_config.
            self._set_internal_replica_context(
                servable_object=self._user_callable_wrapper.user_callable
            )

            return self._get_metadata()
        except Exception:
            raise RuntimeError(traceback.format_exc()) from None

    def _get_metadata(
        self,
    ) -> Tuple[DeploymentConfig, DeploymentVersion, Optional[float]]:
        return (
            self._version.deployment_config,
            self._version,
            self._initialization_latency,
        )

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

    async def _drain_ongoing_requests(self):
        """Wait for any ongoing requests to finish.

        Sleep for a grace period before the first time we check the number of ongoing
        requests to allow the notification to remove this replica to propagate to
        callers first.
        """
        wait_loop_period_s = self._deployment_config.graceful_shutdown_wait_loop_s
        while True:
            await asyncio.sleep(wait_loop_period_s)

            num_ongoing_requests = self._metrics_manager.get_num_ongoing_requests()
            if num_ongoing_requests > 0:
                logger.info(
                    f"Waiting for an additional {wait_loop_period_s}s to shut down "
                    f"because there are {num_ongoing_requests} ongoing requests."
                )
            else:
                logger.info(
                    "Graceful shutdown complete; replica exiting.",
                    extra={"log_to_stderr": False},
                )
                break

    async def perform_graceful_shutdown(self):
        # If the replica was never initialized it never served traffic, so we
        # can skip the wait period.
        if self._user_callable_initialized:
            await self._drain_ongoing_requests()

        try:
            await self._user_callable_wrapper.call_destructor()
        except:  # noqa: E722
            # We catch a blanket exception since the constructor may still be
            # running, so instance variables used by the destructor may not exist.
            if self._user_callable_initialized:
                logger.exception(
                    "__del__ ran before replica finished initializing, and "
                    "raised an exception."
                )
            else:
                logger.exception("__del__ raised an exception.")

        await self._metrics_manager.shutdown()

    async def check_health(self):
        await self._user_callable_wrapper.call_user_health_check()


class UserCallableWrapper:
    """Wraps a user-provided callable that is used to handle requests to a replica."""

    # All interactions with user code run on this loop to avoid blocking the replica's
    # main event loop.
    # NOTE(edoakes): this is a class variable rather than an instance variable to
    # enable writing the `_run_on_user_code_event_loop` decorator method (the decorator
    # doesn't have access to `self` at class definition time).
    _user_code_event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    _user_code_event_loop_thread: Optional[threading.Thread] = None

    def __init__(
        self,
        deployment_def: Callable,
        init_args: Tuple,
        init_kwargs: Dict,
        *,
        deployment_id: DeploymentID,
    ):
        if not (inspect.isfunction(deployment_def) or inspect.isclass(deployment_def)):
            raise TypeError(
                "deployment_def must be a function or class. Instead, its type was "
                f"{type(deployment_def)}."
            )

        self._deployment_def = deployment_def
        self._init_args = init_args
        self._init_kwargs = init_kwargs
        self._is_function = inspect.isfunction(deployment_def)
        self._deployment_id = deployment_id
        self._destructor_called = False

        # Will be populated in `initialize_callable`.
        self._callable = None

        # Start the `_user_code_event_loop_thread` singleton if needed.
        if self._user_code_event_loop_thread is None:

            def _run_user_code_event_loop():
                # Required so that calls to get the current running event loop work
                # properly in user code.
                asyncio.set_event_loop(self._user_code_event_loop)
                self._user_code_event_loop.run_forever()

            self._user_code_event_loop_thread = threading.Thread(
                daemon=True,
                target=_run_user_code_event_loop,
            )
            self._user_code_event_loop_thread.start()

    def _run_on_user_code_event_loop(f: Callable):
        """Decorator to run a coroutine method on the user code event loop.

        The method will be modified to be a sync function that returns an
        `asyncio.Future`.
        """
        assert inspect.iscoroutinefunction(
            f
        ), "_run_on_user_code_event_loop can only be used on coroutine functions."

        @wraps(f)
        def wrapper(*args, **kwargs) -> asyncio.Future:
            return asyncio.wrap_future(
                asyncio.run_coroutine_threadsafe(
                    f(*args, **kwargs),
                    UserCallableWrapper._user_code_event_loop,
                )
            )

        return wrapper

    def _get_user_callable_method(self, method_name: str) -> Callable:
        if self._is_function:
            return self._callable

        if not hasattr(self._callable, method_name):
            # Filter to methods that don't start with '__' prefix.
            def callable_method_filter(attr):
                if attr.startswith("__"):
                    return False
                elif not callable(getattr(self._callable, attr)):
                    return False

                return True

            methods = list(filter(callable_method_filter, dir(self._callable)))
            raise RayServeException(
                f"Tried to call a method '{method_name}' "
                "that does not exist. Available methods: "
                f"{methods}."
            )

        return getattr(self._callable, method_name)

    async def _send_user_result_over_asgi(
        self,
        result: Any,
        asgi_args: ASGIArgs,
    ):
        """Handle the result from user code and send it over the ASGI interface.

        If the result is already a Response type, it is sent directly. Otherwise, it
        is converted to a custom Response type that handles serialization for
        common Python objects.
        """
        scope, receive, send = asgi_args.to_args_tuple()
        if isinstance(result, starlette.responses.Response):
            await result(scope, receive, send)
        else:
            await Response(result).send(scope, receive, send)

    async def _call_func_or_gen(self, callable: Callable, *args, **kwargs) -> Any:
        """Call the callable with the provided arguments.

        This is a convenience wrapper that will work for `def`, `async def`,
        generator, and async generator functions.
        """
        result = callable(*args, **kwargs)
        if inspect.iscoroutine(result):
            result = await result

        return result

    @property
    def user_callable(self) -> Optional[Callable]:
        return self._callable

    @_run_on_user_code_event_loop
    async def initialize_callable(self):
        if self._callable is not None:
            raise RuntimeError("initialize_callable should only be called once.")

        # This closure initializes user code and finalizes replica
        # startup. By splitting the initialization step like this,
        # we can already access this actor before the user code
        # has finished initializing.
        # The supervising state manager can then wait
        # for allocation of this replica by using the `is_allocated`
        # method. After that, it calls `reconfigure` to trigger
        # user code initialization.
        logger.info(
            "Started initializing replica.",
            extra={"log_to_stderr": False},
        )

        if self._is_function:
            self._callable = self._deployment_def
        else:
            # This allows deployments to define an async __init__
            # method (mostly used for testing).
            self._callable = self._deployment_def.__new__(self._deployment_def)
            await self._call_func_or_gen(
                self._callable.__init__,
                *self._init_args,
                **self._init_kwargs,
            )

            if isinstance(self._callable, ASGIAppReplicaWrapper):
                await self._callable._run_asgi_lifespan_startup()

        self._user_health_check = getattr(self._callable, HEALTH_CHECK_METHOD, None)

        logger.info(
            "Finished initializing replica.",
            extra={"log_to_stderr": False},
        )

    @_run_on_user_code_event_loop
    async def _call_user_health_check(self):
        await self._call_func_or_gen(self._user_health_check)

    def _raise_if_not_initialized(self, method_name: str):
        if self._callable is None:
            raise RuntimeError(
                "`initialize_callable` must be called before `{method_name}`."
            )

    async def call_user_health_check(self):
        self._raise_if_not_initialized("call_user_health_check")

        # If the user provided a health check, call it on the user code thread. If user
        # code blocks the event loop the health check may time out.
        #
        # To avoid this issue for basic cases without a user-defined health check, skip
        # interacting with the user callable entirely.
        if self._user_health_check is not None:
            return await self._call_user_health_check()

    @_run_on_user_code_event_loop
    async def call_reconfigure(self, user_config: Any):
        self._raise_if_not_initialized("call_reconfigure")

        # NOTE(edoakes): there is the possibility of a race condition in user code if
        # they don't have any form of concurrency control between `reconfigure` and
        # other methods. See https://github.com/ray-project/ray/pull/42159.
        if user_config is not None:
            if self._is_function:
                raise ValueError("deployment_def must be a class to use user_config")
            elif not hasattr(self._callable, RECONFIGURE_METHOD):
                raise RayServeException(
                    "user_config specified but deployment "
                    + self._deployment_id
                    + " missing "
                    + RECONFIGURE_METHOD
                    + " method"
                )
            await self._call_func_or_gen(
                getattr(self._callable, RECONFIGURE_METHOD),
                user_config,
            )

    def _prepare_args_for_http_request(
        self,
        request: StreamingHTTPRequest,
        request_metadata: RequestMetadata,
        user_method_params: Dict[str, inspect.Parameter],
        *,
        is_asgi_app: bool,
        generator_result_callback: Optional[Callable] = None,
    ) -> Tuple[Tuple[Any], ASGIArgs, asyncio.Task]:
        """Prepare arguments for a user method handling an HTTP request.

        Returns (request_args, asgi_args, receive_task).

        The returned `receive_task` should be cancelled when the user method exits.
        """
        scope = pickle.loads(request.pickled_asgi_scope)
        receive = ASGIReceiveProxy(
            scope,
            request_metadata,
            request.receive_asgi_messages,
        )
        receive_task = self._user_code_event_loop.create_task(
            receive.fetch_until_disconnect()
        )
        asgi_args = ASGIArgs(
            scope=scope,
            receive=receive,
            send=generator_result_callback,
        )
        if is_asgi_app:
            request_args = asgi_args.to_args_tuple()
        elif len(user_method_params) == 0:
            # Edge case to support empty HTTP handlers: don't pass the Request
            # argument if the callable has no parameters.
            request_args = tuple()
        else:
            # Non-FastAPI HTTP handlers take only the starlette `Request`.
            request_args = (asgi_args.to_starlette_request(),)

        return request_args, asgi_args, receive_task

    def _prepare_args_for_grpc_request(
        self,
        request: gRPCRequest,
        request_metadata: RequestMetadata,
        user_method_params: Dict[str, inspect.Parameter],
    ) -> Tuple[Tuple[Any], Dict[str, Any]]:
        """Prepare arguments for a user method handling a gRPC request.

        Returns (request_args, request_kwargs).
        """
        request_args = (pickle.loads(request.grpc_user_request),)
        if GRPC_CONTEXT_ARG_NAME in user_method_params:
            request_kwargs = {GRPC_CONTEXT_ARG_NAME: request_metadata.grpc_context}
        else:
            request_kwargs = {}

        return request_args, request_kwargs

    async def _handle_user_method_result(
        self,
        result: Any,
        user_method_name: str,
        request_metadata: RequestMetadata,
        *,
        generator_result_callback: Optional[Callable],
        is_asgi_app: bool,
        asgi_args: Optional[ASGIArgs],
    ) -> Any:
        """Postprocess the result of a user method.

        User methods can be regular unary functions or return a sync or async generator.
        This method will raise an exception if the result is not of the expected type
        (e.g., non-generator for streaming requests or generator for unary requests).

        Generator outputs will be written to the `generator_result_callback`.

        Note that HTTP requests are an exception: they are *always* streaming requests,
        but for ASGI apps (like FastAPI), the actual method will be a regular function
        implementing the ASGI `__call__` protocol.
        """
        result_is_gen = inspect.isgenerator(result)
        result_is_async_gen = inspect.isasyncgen(result)
        if request_metadata.is_streaming:
            if result_is_gen:
                for r in result:
                    if request_metadata.is_grpc_request:
                        r = (request_metadata.grpc_context, r.SerializeToString())
                    await generator_result_callback(r)
            elif result_is_async_gen:
                async for r in result:
                    if request_metadata.is_grpc_request:
                        r = (request_metadata.grpc_context, r.SerializeToString())
                    await generator_result_callback(r)
            elif request_metadata.is_http_request and not is_asgi_app:
                # For the FastAPI codepath, the response has already been sent over
                # ASGI, but for the vanilla deployment codepath we need to send it.
                await self._send_user_result_over_asgi(result, asgi_args)
            elif not request_metadata.is_http_request:
                # If a unary method is called with stream=True for anything EXCEPT
                # an HTTP request, raise an error.
                # HTTP requests are always streaming regardless of if the method
                # returns a generator, because it's provided the result queue as its
                # ASGI `send` interface to stream back results.
                raise TypeError(
                    f"Called method '{user_method_name}' with "
                    "`handle.options(stream=True)` but it did not return a "
                    "generator."
                )
        else:
            assert (
                not request_metadata.is_http_request
            ), "All HTTP requests go through the streaming codepath."

            if result_is_gen or result_is_async_gen:
                raise TypeError(
                    f"Method '{user_method_name}' returned a generator. "
                    "You must use `handle.options(stream=True)` to call "
                    "generators on a deployment."
                )
            if request_metadata.is_grpc_request:
                result = (request_metadata.grpc_context, result.SerializeToString())

        return result

    @_run_on_user_code_event_loop
    async def call_user_method(
        self,
        request_metadata: RequestMetadata,
        request_args: Tuple[Any],
        request_kwargs: Dict[str, Any],
        *,
        generator_result_callback: Optional[Callable] = None,
    ) -> Any:
        """Call a user method (unary or generator).

        The `generator_result_callback` is used to communicate the results of generator
        methods.

        Raises any exception raised by the user code so it can be propagated as a
        `RayTaskError`.
        """
        self._raise_if_not_initialized("call_user_method")

        logger.info(
            f"Started executing request to method '{request_metadata.call_method}'.",
            extra={"log_to_stderr": False, "serve_access_log": True},
        )

        result = None
        asgi_args = None
        user_method = None
        receive_task = None
        user_method_name = "unknown"
        try:
            is_asgi_app = isinstance(self._callable, ASGIAppReplicaWrapper)
            user_method = self._get_user_callable_method(request_metadata.call_method)
            user_method_name = user_method.__name__
            user_method_params = inspect.signature(user_method).parameters
            if request_metadata.is_http_request:
                assert len(request_args) == 1 and isinstance(
                    request_args[0], StreamingHTTPRequest
                )
                (
                    request_args,
                    asgi_args,
                    receive_task,
                ) = self._prepare_args_for_http_request(
                    request_args[0],
                    request_metadata,
                    user_method_params,
                    is_asgi_app=is_asgi_app,
                    generator_result_callback=generator_result_callback,
                )
            elif request_metadata.is_grpc_request:
                # Ensure the request args are a single gRPCRequest object.
                assert len(request_args) == 1 and isinstance(
                    request_args[0], gRPCRequest
                )
                request_args, request_kwargs = self._prepare_args_for_grpc_request(
                    request_args[0], request_metadata, user_method_params
                )

            result = await self._handle_user_method_result(
                await self._call_func_or_gen(
                    user_method, *request_args, **request_kwargs
                ),
                user_method_name,
                request_metadata,
                generator_result_callback=generator_result_callback,
                is_asgi_app=is_asgi_app,
                asgi_args=asgi_args,
            )

        except Exception as e:
            e = wrap_to_ray_error(user_method_name, e)
            if request_metadata.is_http_request and asgi_args is not None:
                result = starlette.responses.Response(
                    f"Unexpected error, traceback: {e}.", status_code=500
                )
                await self._send_user_result_over_asgi(result, asgi_args)

            raise e from None
        finally:
            if receive_task is not None and not receive_task.done():
                receive_task.cancel()

        return result

    @_run_on_user_code_event_loop
    async def call_destructor(self):
        """Explicitly call the `__del__` method of the user callable.

        Calling this multiple times has no effect; only the first call will
        actually call the destructor.
        """
        if self._callable is None:
            logger.info(
                "This replica has not yet started running user code. "
                "Skipping __del__."
            )
            return

        # Only run the destructor once. This is safe because there is no `await` between
        # checking the flag here and flipping it to `True` below.
        if self._destructor_called:
            return

        self._destructor_called = True
        try:
            if hasattr(self._callable, "__del__"):
                # Make sure to accept `async def __del__(self)` as well.
                await self._call_func_or_gen(self._callable.__del__)

            if hasattr(self._callable, "__serve_multiplex_wrapper"):
                await getattr(self._callable, "__serve_multiplex_wrapper").shutdown()

        except Exception as e:
            logger.exception(f"Exception during graceful shutdown of replica: {e}")
