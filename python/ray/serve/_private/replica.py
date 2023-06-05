import aiorwlock
import asyncio
from importlib import import_module
import inspect
import logging
import os
import pickle
import time
from typing import Any, AsyncGenerator, Callable, Optional, Tuple, Dict
import traceback

import starlette.responses
from starlette.types import Send

import ray
from ray import cloudpickle
from ray.actor import ActorClass, ActorHandle
from ray.remote_function import RemoteFunction
from ray._private.async_compat import sync_to_async
from ray._private.utils import get_or_create_event_loop

from ray.serve import metrics
from ray.serve._private.common import (
    HEALTH_CHECK_CONCURRENCY_GROUP,
    ReplicaTag,
    ServeComponentType,
)
from ray.serve.config import DeploymentConfig
from ray.serve._private.constants import (
    HEALTH_CHECK_METHOD,
    RECONFIGURE_METHOD,
    DEFAULT_LATENCY_BUCKET_MS,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
    DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S,
)
from ray.serve.deployment import Deployment
from ray.serve.exceptions import RayServeException
from ray.serve._private.http_util import (
    ASGIHTTPSender,
    ASGIHTTPQueueSender,
    RawASGIResponse,
    Response,
)
from ray.serve._private.logging_utils import (
    access_log_msg,
    configure_component_logger,
    get_component_logger_file_path,
)
from ray.serve._private.router import Query, RequestMetadata
from ray.serve._private.utils import (
    parse_import_path,
    parse_request_item,
    wrap_to_ray_error,
    merge_dict,
    MetricsPusher,
)
from ray.serve._private.version import DeploymentVersion


logger = logging.getLogger(SERVE_LOGGER_NAME)


async def mock_asgi_receive():
    # This is called in a tight loop in responses just to check
    # for an HTTP disconnect. So rather than returning immediately
    # we should suspend execution to avoid wasting CPU cycles.
    never_set_event = asyncio.Event()
    await never_set_event.wait()


def _format_replica_actor_name(deployment_name: str):
    return f"ServeReplica:{deployment_name}"


def create_replica_wrapper(name: str):
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
            detached: bool,
            app_name: str = None,
        ):
            self._replica_tag = replica_tag
            configure_component_logger(
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

            deployment_config = DeploymentConfig.from_proto_bytes(
                deployment_config_proto_bytes
            )

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
                deployment_name,
                replica_tag,
                controller_name,
                servable_object=None,
                app_name=app_name,
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
                    # method (required for FastAPI).
                    _callable = deployment_def.__new__(deployment_def)
                    await sync_to_async(_callable.__init__)(*init_args, **init_kwargs)

                # Setting the context again to update the servable_object.
                ray.serve.context._set_internal_replica_context(
                    deployment_name,
                    replica_tag,
                    controller_name,
                    servable_object=_callable,
                    app_name=app_name,
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

        @ray.method(num_returns=2)
        async def handle_request(
            self,
            pickled_request_metadata: bytes,
            *request_args,
            **request_kwargs,
        ):
            query = Query(
                request_args,
                request_kwargs,
                pickle.loads(pickled_request_metadata),
            )
            return await self.replica.handle_request(query)

        async def handle_request_streaming(
            self,
            pickled_request_metadata: bytes,
            *request_args,
            **request_kwargs,
        ) -> AsyncGenerator[Dict[str, Any], None]:
            """Handle a request and stream the results to the caller.

            This is used by the HTTP proxy for experimental StreamingResponse support.

            This generator yields ASGI-compliant messages sent via an ASGI sender
            interface. This allows us to return the messages back to the HTTP proxy as
            they're sent by user code (e.g., the FastAPI wrapper).
            """
            query = Query(
                request_args,
                request_kwargs,
                pickle.loads(pickled_request_metadata),
            )

            # Handle the request in a background asyncio.Task. It's expected that this
            # task will use the provided ASGI sender interface to send its HTTP
            # response. We will poll for the sent messages and yield them back to the
            # caller.
            asgi_queue_sender = ASGIHTTPQueueSender()
            handle_request_task = self._event_loop.create_task(
                self.replica.handle_request(query, asgi_sender=asgi_queue_sender)
            )

            done = []
            while handle_request_task not in done:
                done, _ = await asyncio.wait(
                    [handle_request_task, asgi_queue_sender.wait_for_message()],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                # Consume and yield all available messages in the queue.
                # The messages are batched into a list to avoid unnecessary RPCs and
                # we use vanilla pickle because it's faster than cloudpickle and we
                # know it's safe for these messages containing primitive types.
                yield pickle.dumps(asgi_queue_sender.get_messages_nowait())

            e = handle_request_task.exception()
            if e is not None:
                raise e from None

        async def handle_request_from_java(
            self,
            proto_request_metadata: bytes,
            *request_args,
            **request_kwargs,
        ):
            from ray.serve.generated.serve_pb2 import (
                RequestMetadata as RequestMetadataProto,
            )

            proto = RequestMetadataProto.FromString(proto_request_metadata)
            request_metadata: RequestMetadata = RequestMetadata(
                proto.request_id, proto.endpoint, call_method=proto.call_method
            )
            request_args = request_args[0]
            query = Query(request_args, request_kwargs, request_metadata, return_num=1)
            return await self.replica.handle_request(query)

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
                ray._private.worker.global_worker.worker_id.hex(),
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
            # allows delaying reconfiguration until after this call has returned.
            try:
                # Ensure that initialization is only performed once.
                # When controller restarts, it will call this method again.
                async with self._replica_init_lock:
                    if not self._initialized:
                        await self._initialize_replica()
                    if deployment_config:
                        await self.reconfigure(deployment_config)
                metadata = await self._get_metadata()

                # A new replica should not be considered healthy until it passes an
                # initial health check. If an initial health check fails, consider
                # it an initialization failure.
                await self.check_health()
                return metadata
            except Exception:
                raise RuntimeError(traceback.format_exc()) from None

        async def reconfigure(
            self, deployment_config: DeploymentConfig
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

        async def prepare_for_shutdown(self):
            if self.replica is not None:
                return await self.replica.prepare_for_shutdown()

        @ray.method(concurrency_group=HEALTH_CHECK_CONCURRENCY_GROUP)
        async def check_health(self):
            await self.replica.check_health()

    # Dynamically create a new class with custom name here so Ray picks it up
    # correctly in actor metadata table and observability stack.
    return type(
        _format_replica_actor_name(name),
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
        self.deployment_name = deployment_name
        self.replica_tag = replica_tag
        self.callable = _callable
        self.is_function = is_function
        self.version = version
        self.deployment_config = None
        self.rwlock = aiorwlock.RWLock()
        self.app_name = app_name

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

        self.restart_counter.inc()

        if autoscaling_config:
            process_remote_func = controller_handle.record_autoscaling_metrics.remote
            config = autoscaling_config
            self.metrics_pusher = MetricsPusher(
                process_remote_func,
                config.metrics_interval_s,
                self._collect_autoscaling_metrics,
            )
            self.metrics_pusher.start()

    async def check_health(self):
        await self.user_health_check()

    def _get_handle_request_stats(self) -> Optional[Dict[str, int]]:
        replica_actor_name = _format_replica_actor_name(self.deployment_name)
        actor_stats = ray.runtime_context.get_runtime_context()._get_actor_call_stats()
        method_stat = actor_stats.get(f"{replica_actor_name}.handle_request")
        streaming_method_stat = actor_stats.get(
            f"{replica_actor_name}.handle_request_streaming"
        )
        method_stat_java = actor_stats.get(
            f"{replica_actor_name}.handle_request_from_java"
        )
        return merge_dict(
            merge_dict(method_stat, streaming_method_stat), method_stat_java
        )

    def _collect_autoscaling_metrics(self):
        method_stat = self._get_handle_request_stats()

        num_inflight_requests = 0
        if method_stat is not None:
            num_inflight_requests = method_stat["pending"] + method_stat["running"]

        return {self.replica_tag: num_inflight_requests}

    def get_runner_method(self, request_item: Query) -> Callable:
        method_name = request_item.metadata.call_method
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

    async def send_user_result_over_asgi(self, result: Any, asgi_sender: Send):
        """Handle the result from user code and send it over the ASGI interface.

        If the result is already a Response type, it is sent directly. Otherwise, it
        is converted to a custom Response type that handles serialization for
        common Python objects.
        """
        if not isinstance(result, (starlette.responses.Response, RawASGIResponse)):
            await Response(result).send(
                scope=None, receive=mock_asgi_receive, send=asgi_sender
            )
        else:
            await result(scope=None, receive=mock_asgi_receive, send=asgi_sender)

    async def convert_streaming_response_to_unary(
        self, response: starlette.responses.StreamingResponse
    ) -> RawASGIResponse:
        """Convert a StreamingResponse to a custom buffered unary response.

        This is used on the legacy non-streaming codepath because we cannot serialize
        and return a StreamingResponse.
        """
        sender = ASGIHTTPSender()
        await response(scope=None, receive=mock_asgi_receive, send=sender)
        return sender.build_asgi_response()

    async def invoke_single(
        self, request_item: Query, *, asgi_sender: Optional[Send] = None
    ) -> Tuple[Any, bool]:
        """Executes the provided request on this replica.

        Returns the user-provided output and a boolean indicating if the
        request succeeded (user code didn't raise an exception).

        If asgi_sender is provided, then the result is always `None`
        because the response is sent over that interface instead.
        """
        logger.info(
            f"Started executing request {request_item.metadata.request_id}",
            extra={"log_to_stderr": False},
        )

        args, kwargs = parse_request_item(request_item)

        # Check if the callable is our ASGI wrapper (i.e., the user used
        # `@serve.ingress`).
        callable_is_asgi_wrapper = hasattr(self.callable, "_is_serve_asgi_wrapper")
        if asgi_sender is not None and callable_is_asgi_wrapper:
            kwargs["asgi_sender"] = asgi_sender

        method_to_call = None
        success = True
        try:
            runner_method = self.get_runner_method(request_item)
            method_to_call = sync_to_async(runner_method)
            result = None
            if len(inspect.signature(runner_method).parameters) > 0:
                result = await method_to_call(*args, **kwargs)
            else:
                # When access via http http_arg_is_pickled with no args:
                # args = (<starlette.requests.Request object at 0x7fe900694cc0>,)
                # When access via python with no args:
                # args = ()
                if len(args) == 1 and isinstance(args[0], starlette.requests.Request):
                    # The method doesn't take in anything, including the request
                    # information, so we pass nothing into it
                    result = await method_to_call()
                else:
                    # Will throw due to signature mismatch if user attempts to
                    # call with non-empty args
                    result = await method_to_call(*args, **kwargs)

            # Streaming HTTP codepath: always send response over ASGI interface.
            if asgi_sender is not None:
                # For the FastAPI codepath, the response has already been sent over the
                # ASGI interace and result should always be `None`.
                if callable_is_asgi_wrapper:
                    assert result is None
                # For the vanilla deployment codepath, always send the result over ASGI.
                else:
                    result = await self.send_user_result_over_asgi(result, asgi_sender)

            # Legacy codepath: always return the result, so ensure it can be serialized.
            elif isinstance(result, starlette.responses.StreamingResponse):
                result = await self.convert_streaming_response_to_unary(result)

            self.request_counter.inc(tags={"route": request_item.metadata.route})
        except Exception as e:
            logger.exception(f"Request failed due to {type(e).__name__}:")
            success = False

            # If the debugger is enabled, drop into the remote pdb here.
            if ray.util.pdb._is_ray_debugger_enabled():
                ray.util.pdb._post_mortem()

            function_name = "unknown"
            if method_to_call is not None:
                function_name = method_to_call.__name__
            result = wrap_to_ray_error(function_name, e)
            self.error_counter.inc(tags={"route": request_item.metadata.route})

        return result, success

    async def reconfigure(self, deployment_config: DeploymentConfig):
        async with self.rwlock.writer_lock:
            user_config_changed = False
            if (
                self.deployment_config is None
                or self.deployment_config.user_config != deployment_config.user_config
            ):
                user_config_changed = True
            self.deployment_config = deployment_config
            self.version = DeploymentVersion.from_deployment_version(
                self.version, self.deployment_config
            )

            if self.deployment_config.user_config is not None and user_config_changed:
                if self.is_function:
                    raise ValueError(
                        "deployment_def must be a class to use user_config"
                    )
                elif not hasattr(self.callable, RECONFIGURE_METHOD):
                    raise RayServeException(
                        "user_config specified but deployment "
                        + self.deployment_name
                        + " missing "
                        + RECONFIGURE_METHOD
                        + " method"
                    )
                reconfigure_method = sync_to_async(
                    getattr(self.callable, RECONFIGURE_METHOD)
                )
                await reconfigure_method(self.deployment_config.user_config)

    async def handle_request(
        self, request: Query, *, asgi_sender: Optional[Send] = None
    ) -> asyncio.Future:
        async with self.rwlock.reader_lock:
            num_running_requests = self._get_handle_request_stats()["running"]
            self.num_processing_items.set(num_running_requests)

            # Set request context variables for subsequent handle so that
            # handle can pass the correct request context to subsequent replicas.
            ray.serve.context._serve_request_context.set(
                ray.serve.context.RequestContext(
                    request.metadata.route,
                    request.metadata.request_id,
                    self.app_name,
                    request.metadata.multiplexed_model_id,
                )
            )

            start_time = time.time()
            result, success = await self.invoke_single(
                request,
                asgi_sender=asgi_sender,
            )
            latency_ms = (time.time() - start_time) * 1000
            self.processing_latency_tracker.observe(
                latency_ms, tags={"route": request.metadata.route}
            )
            logger.info(
                access_log_msg(
                    method=request.metadata.call_method,
                    status="OK" if success else "ERROR",
                    latency_ms=latency_ms,
                )
            )
            if request.return_num == 1:
                return result
            else:
                # Returns a small object for router to track request status.
                return b"", result

    async def prepare_for_shutdown(self):
        """Perform graceful shutdown.

        Trigger a graceful shutdown protocol that will wait for all the queued
        tasks to be completed and return to the controller.
        """
        while True:
            # Sleep first because we want to make sure all the routers receive
            # the notification to remove this replica first.
            if self.deployment_config:
                await asyncio.sleep(
                    self.deployment_config.graceful_shutdown_wait_loop_s
                )
            else:
                await asyncio.sleep(DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S)
            method_stat = self._get_handle_request_stats()
            # The handle_request method wasn't even invoked.
            if method_stat is None:
                break
            num_ongoing_requests = method_stat["running"] + method_stat["pending"]
            # The handle_request method has 0 inflight requests.
            if num_ongoing_requests == 0:
                break
            else:
                logger.info(
                    "Waiting for an additional "
                    f"{self.deployment_config.graceful_shutdown_wait_loop_s}s to shut "
                    f"down because there are {num_ongoing_requests} ongoing "
                    "requests."
                )

        # Explicitly call the del method to trigger clean up.
        # We set the del method to noop after successfully calling it so the
        # destructor is called only once.
        try:
            if hasattr(self.callable, "__del__"):
                # Make sure to accept `async def __del__(self)` as well.
                await sync_to_async(self.callable.__del__)()
                setattr(self.callable, "__del__", lambda _: None)
        except Exception as e:
            logger.exception(f"Exception during graceful shutdown of replica: {e}")
        finally:
            if hasattr(self.callable, "__del__"):
                del self.callable
