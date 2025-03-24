import asyncio
import inspect
import logging
import pickle
import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, AsyncGenerator, Callable, Generator, Optional, Tuple

import grpc
from starlette.types import Receive, Scope, Send

import ray
from ray import cloudpickle
from ray.anyscale.serve._private.constants import (
    ANYSCALE_RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH,
    ANYSCALE_RAY_SERVE_ENABLE_DIRECT_INGRESS,
)
from ray.anyscale.serve._private.tracing_utils import (
    TraceContextManager,
    extract_propagated_context,
    set_http_span_attributes,
    set_rpc_span_attributes,
    set_span_attributes,
    set_span_exception,
    setup_tracing,
)
from ray.serve.generated.serve_pb2 import HealthzResponse, ListApplicationsResponse
from ray.serve._private.grpc_util import start_grpc_server
from ray.serve._private.http_util import (
    convert_object_to_asgi_messages,
    start_asgi_http_server,
    MessageQueue,
)
from ray.anyscale.serve.context import _get_in_flight_requests
from ray.anyscale.serve.utils import asyncio_grpc_exception_handler
from ray.serve._private.common import (
    RequestProtocol,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    ServeComponentType,
    gRPCRequest,
    StreamingHTTPRequest,
)
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
    SERVE_CONTROLLER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.replica import ReplicaBase, StatusCodeCallback
from ray.serve._private.utils import generate_request_id
from ray.serve.generated import serve_proprietary_pb2, serve_proprietary_pb2_grpc
from ray.serve.grpc_util import RayServegRPCContext


logger = logging.getLogger(SERVE_LOGGER_NAME)


def _wrap_grpc_call(f):
    """Decorator that processes grpc methods."""

    def serialize(result, metadata):
        if metadata.is_streaming and metadata.is_http_request:
            return result
        else:
            return cloudpickle.dumps(result)

    @wraps(f)
    async def wrapper(
        self,
        request: serve_proprietary_pb2.ASGIRequest,
        context: grpc.aio.ServicerContext,
    ):
        request_metadata = pickle.loads(request.pickled_request_metadata)
        request_args = cloudpickle.loads(request.request_args)
        request_kwargs = cloudpickle.loads(request.request_kwargs)

        if request_metadata.is_http_request or request_metadata.is_grpc_request:
            request_args = (pickle.loads(request_args[0]),)

        try:
            result = await f(
                self, context, request_metadata, *request_args, **request_kwargs
            )
            return serve_proprietary_pb2.ASGIResponse(
                serialized_message=serialize(result, request_metadata)
            )
        except (Exception, asyncio.CancelledError) as e:
            return serve_proprietary_pb2.ASGIResponse(
                serialized_message=cloudpickle.dumps(e),
                is_error=True,
            )

    @wraps(f)
    async def gen_wrapper(
        self,
        request: serve_proprietary_pb2.ASGIRequest,
        context: grpc.aio.ServicerContext,
    ):
        request_metadata = pickle.loads(request.pickled_request_metadata)
        request_args = cloudpickle.loads(request.request_args)
        request_kwargs = cloudpickle.loads(request.request_kwargs)

        if request_metadata.is_http_request or request_metadata.is_grpc_request:
            request_args = (pickle.loads(request_args[0]),)

        try:
            async for result in f(
                self, context, request_metadata, *request_args, **request_kwargs
            ):
                yield serve_proprietary_pb2.ASGIResponse(
                    serialized_message=serialize(result, request_metadata)
                )
        except (Exception, asyncio.CancelledError) as e:
            yield serve_proprietary_pb2.ASGIResponse(
                serialized_message=cloudpickle.dumps(e),
                is_error=True,
            )

    if inspect.isasyncgenfunction(f):
        return gen_wrapper
    else:
        return wrapper


class AnyscaleReplica(ReplicaBase):
    def __init__(self, **kwargs):
        self._server = grpc.aio.server(
            options=[
                (
                    "grpc.max_receive_message_length",
                    ANYSCALE_RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH,
                )
            ]
        )

        self._http_direct_ingress_server_task: Optional[asyncio.Task] = None
        self._grpc_direct_ingress_server_task: Optional[asyncio.Task] = None

        super().__init__(**kwargs)

        # Silence spammy false positive errors from gRPC Python
        self._event_loop.set_exception_handler(asyncio_grpc_exception_handler)

        # Set up tracing
        try:
            is_tracing_setup_successful = setup_tracing(
                component_type=ServeComponentType.REPLICA,
                component_name=self._component_name,
                component_id=self._component_id,
            )
            if is_tracing_setup_successful:
                logger.info("Successfully set up tracing for replica")
        except Exception as e:
            logger.warning(
                f"Failed to set up tracing: {e}. "
                "The replica will continue running, but traces will not be exported."
            )

    async def _maybe_start_direct_ingress_servers(self):
        if not ANYSCALE_RAY_SERVE_ENABLE_DIRECT_INGRESS:
            return

        controller_handle = ray.get_actor(
            SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE
        )
        http_options, grpc_options = ray.get(
            [
                controller_handle.get_http_config.remote(),
                controller_handle.get_grpc_config.remote(),
            ]
        )
        grpc_enabled = (
            grpc_options.port > 0 and len(grpc_options.grpc_servicer_functions) > 0
        )
        logger.info(
            f"Starting HTTP server on port {http_options.port}"
            + (
                f" and gRPC server on port {grpc_options.port}."
                if grpc_enabled
                else "."
            )
        )

        self._direct_ingress_http_server_task = await start_asgi_http_server(
            self._direct_ingress_asgi,
            http_options,
            event_loop=self._event_loop,
            enable_so_reuseport=True,
        )
        if grpc_enabled:
            self._direct_ingress_grpc_server_task = await start_grpc_server(
                self._direct_ingress_service_handler_factory,
                grpc_options,
                event_loop=self._event_loop,
                enable_so_reuseport=True,
            )

    async def _on_initialized(self):
        serve_proprietary_pb2_grpc.add_ASGIServiceServicer_to_server(self, self._server)
        self._port = self._server.add_insecure_port("[::]:0")
        await self._server.start()

        await self._maybe_start_direct_ingress_servers()

        self._set_internal_replica_context(
            servable_object=self._user_callable_wrapper.user_callable
        )

        # Save the initialization latency if the replica is initializing
        # for the first time.
        if self._initialization_latency is None:
            self._initialization_latency = time.time() - self._initialization_start_time

    def _on_request_cancelled(
        self, metadata: RequestMetadata, e: asyncio.CancelledError
    ):
        """Recursively cancel child requests.

        This includes all requests that are pending assignment, and gRPC
        requests that have already been assigned.
        """
        # Cancel child requests pending assignment
        requests_pending_assignment = (
            ray.serve.context._get_requests_pending_assignment(
                metadata.internal_request_id
            )
        )
        for task in requests_pending_assignment.values():
            task.cancel()

        # Cancel child gRPC requests that have already been assigned
        in_flight_requests = _get_in_flight_requests(metadata.internal_request_id)
        for call in in_flight_requests.values():
            call.cancel()

    def _on_request_failed(self, request_metadata: RequestMetadata, e: Exception):
        if ray.util.pdb._is_ray_debugger_post_mortem_enabled():
            ray.util.pdb._post_mortem()

    @contextmanager
    def _wrap_user_method_call(
        self, request_metadata: RequestMetadata, request_args: Tuple[Any]
    ) -> Generator[StatusCodeCallback, None, None]:
        """Context manager that wraps user method calls.

        1) Sets the request context var with appropriate metadata.
        2) Records the access log message (if not disabled).
        3) Records per-request metrics via the metrics manager.
        """
        # TODO (abrar): for http requests on ASGI, request_metadata.call_method is __call__
        #   this is not nice, figure out a better way to map this to method name.
        call_method = request_metadata.call_method
        trace_context = extract_propagated_context(request_metadata.tracing_context)
        trace_manager = TraceContextManager(
            trace_name=f"replica_handle_request {self._deployment_id.name} {call_method}",
            trace_context=trace_context,
        )
        with trace_manager:
            request_metadata.route = self._maybe_get_http_route(
                request_metadata, request_args
            )

            trace_attributes = {
                "request_id": request_metadata.request_id,
                "replica_id": self._replica_id.unique_id,
                "deployment": self._deployment_id.name,
                "app": self._deployment_id.app_name,
                "call_method": request_metadata.call_method,
                "route": request_metadata.route,
                "multiplexed_model_id": request_metadata.multiplexed_model_id,
                "is_streaming": request_metadata.is_streaming,
            }
            set_span_attributes(trace_attributes)

            ray.serve.context._serve_request_context.set(
                ray.serve.context._RequestContext(
                    route=request_metadata.route,
                    request_id=request_metadata.request_id,
                    _internal_request_id=request_metadata.internal_request_id,
                    app_name=self._deployment_id.app_name,
                    multiplexed_model_id=request_metadata.multiplexed_model_id,
                    grpc_context=request_metadata.grpc_context,
                )
            )

            with self._handle_errors_and_metrics(
                request_metadata, request_args
            ) as status_code_callback:
                yield status_code_callback

    def _record_errors_and_metrics(
        self,
        user_exception: Optional[BaseException],
        status_code: Optional[str],
        latency_ms: float,
        request_metadata: RequestMetadata,
        request_args: Tuple[Any],
    ):
        super()._record_errors_and_metrics(
            user_exception, status_code, latency_ms, request_metadata, request_args
        )
        http_method = self._maybe_get_http_method(request_metadata, request_args)
        http_route = request_metadata.route
        call_method = request_metadata.call_method
        if request_metadata.is_http_request:
            set_http_span_attributes(
                method=http_method,
                status_code=status_code,
                route=http_route,
            )
        else:
            # in this case we are either in grpc or undefined. I think
            # undefined is the case where we call the user method as ray
            # tasks. Treating it as grpc for now from POV of tracing.
            set_rpc_span_attributes(
                system=request_metadata._request_protocol,
                method=call_method,
                status_code=status_code,
                service=self._deployment_id.name,
            )

        if user_exception is not None:
            set_span_exception(user_exception, escaped=False)

    @_wrap_grpc_call
    async def HandleRequest(
        self,
        context: grpc.aio.ServicerContext,
        request_metadata: RequestMetadata,
        *request_args,
        **request_kwargs,
    ):
        result = await self.handle_request(
            request_metadata, *request_args, **request_kwargs
        )
        if request_metadata.is_grpc_request:
            result = (request_metadata.grpc_context, result.SerializeToString())

        return result

    @_wrap_grpc_call
    async def HandleRequestStreaming(
        self,
        context: grpc.aio.ServicerContext,
        request_metadata: RequestMetadata,
        *request_args,
        **request_kwargs,
    ):
        async for result in self.handle_request_streaming(
            request_metadata, *request_args, **request_kwargs
        ):
            if request_metadata.is_grpc_request:
                result = (request_metadata.grpc_context, result.SerializeToString())

            yield result

    @_wrap_grpc_call
    async def HandleRequestWithRejection(
        self,
        context: grpc.aio.ServicerContext,
        request_metadata: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> AsyncGenerator[Any, None]:
        """gRPC entrypoint for all requests with strict max_ongoing_requests enforcement

        This generator yields a system message indicating if the request was accepted,
        then the actual response(s).

        If an exception occurred while processing the request, whether it's a user
        exception or an error intentionally raised by Serve, it will be returned as
        a gRPC response instead of raised directly.
        """
        result_gen = self.handle_request_with_rejection(
            request_metadata, *request_args, **request_kwargs
        )
        queue_len_info: ReplicaQueueLengthInfo = await result_gen.__anext__()
        await context.send_initial_metadata(
            [
                ("accepted", str(int(queue_len_info.accepted))),
                ("num_ongoing_requests", str(queue_len_info.num_ongoing_requests)),
            ]
        )
        if not queue_len_info.accepted:
            # NOTE(edoakes): in gRPC, it's not guaranteed that the initial metadata sent
            # by the server will be delivered for a stream with no messages. Therefore,
            # we send a dummy message here to ensure it is populated in every case.
            yield b""
            return

        async for result in result_gen:
            if request_metadata.is_grpc_request:
                result = (request_metadata.grpc_context, result.SerializeToString())

            yield result

    async def _dataplane_health_check(self) -> Tuple[bool, str]:
        healthy, message = True, "OK"
        if self._shutting_down:
            healthy = False
            message = "DRAINING"
        elif not self._healthy:
            healthy = False
            message = "UNHEALTHY"

        return healthy, message

    async def _direct_ingress_unary_unary(
        self,
        service_method: str,
        request_proto: Any,
        context: grpc._cython.cygrpc._ServicerContext,
    ) -> bytes:
        if service_method == "/ray.serve.RayServeAPIService/Healthz":
            healthy, message = await self._dataplane_health_check()
            context.set_code(
                grpc.StatusCode.OK if healthy else grpc.StatusCode.UNAVAILABLE
            )
            context.set_details(message)
            return HealthzResponse(message=message).SerializeToString()

        if service_method == "/ray.serve.RayServeAPIService/ListApplications":
            # NOTE(edoakes): ListApplications may currently be used by Anyscale for
            # health checking. We should clean this up in the future.
            healthy, message = await self._dataplane_health_check()
            context.set_code(
                grpc.StatusCode.OK if healthy else grpc.StatusCode.UNAVAILABLE
            )
            context.set_details(message)
            return ListApplicationsResponse(application_names=[]).SerializeToString()

        c = RayServegRPCContext(context)
        request_metadata = RequestMetadata(
            # TODO: pick up the request ID from gRPC initial metadata.
            request_id=generate_request_id(),
            internal_request_id=generate_request_id(),
            call_method=service_method.split("/")[-1],
            _request_protocol=RequestProtocol.GRPC,
            grpc_context=c,
            app_name=self._deployment_id.app_name,
            # TODO(edoakes): populate this.
            multiplexed_model_id="",
        )
        result_gen = self.handle_request_with_rejection(
            request_metadata, gRPCRequest(request_proto)
        )
        queue_len_info: ReplicaQueueLengthInfo = await result_gen.__anext__()
        # TODO(edoakes): update the behavior to more closely mimic the existing path:
        # add an internal queue and drop requests based on max_queued_requests.
        if not queue_len_info.accepted:
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details("Replica exceeded capacity of max_ongoing_requests.")
            return

        result = await result_gen.__anext__()
        c._set_on_grpc_context(context)

        # NOTE(edoakes): we need to fully consume the generator otherwise the
        # finalizers that run after the `yield` statement won't run. There might
        # be a cleaner way to structure this.
        try:
            await result_gen.__anext__()
        except StopAsyncIteration:
            pass

        return result.SerializeToString()

    async def _direct_ingress_unary_stream(
        self,
        service_method: str,
        request: Any,
        context: grpc._cython.cygrpc._ServicerContext,
    ):
        raise NotImplementedError("unary_stream not implemented.")

    def _direct_ingress_service_handler_factory(
        self, service_method: str, stream: bool
    ) -> Callable:
        if stream:

            async def handler(*args, **kwargs):
                return await self._direct_ingress_unary_stream(
                    service_method, *args, **kwargs
                )

        else:

            async def handler(*args, **kwargs):
                return await self._direct_ingress_unary_unary(
                    service_method, *args, **kwargs
                )

        return handler

    async def _proxy_asgi_receive(
        self, receive: Receive, queue: MessageQueue
    ) -> Optional[int]:
        """Proxies the `receive` interface, placing its messages into the queue.

        Once a disconnect message is received, the call exits and `receive` is no longer
        called.
        For HTTP messages, `None` is always returned.
        For websocket messages, the disconnect code is returned if a disconnect code is
        received.
        """
        try:
            while True:
                msg = await receive()
                await queue(msg)

                if msg["type"] == "http.disconnect":
                    return None

                if msg["type"] == "websocket.disconnect":
                    return msg["code"]
        finally:
            # Close the queue so any subsequent calls to fetch messages return
            # immediately: https://github.com/ray-project/ray/issues/38368.
            queue.close()

    async def _direct_ingress_asgi(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ):
        # NOTE(edoakes): it's important to only start the replica server after the
        # constructor runs because we are using SO_REUSEPORT. We don't want a new
        # replica to start handling connections until it's ready to serve traffic.
        #
        # This can be loosened to listen on the port but fail health checks once we no
        # longer rely on SO_REUSEPORT.
        assert (
            self._user_callable_initialized
        ), "Replica server should only be started *after* the replica is initialized."

        if scope.get("path", "") in ["/-/healthz", "/-/routes"]:
            healthy, message = await self._dataplane_health_check()
            for msg in convert_object_to_asgi_messages(
                message,
                status_code=200 if healthy else 503,
            ):
                await send(msg)
            return

        receive_queue = MessageQueue()
        proxy_asgi_receive_task = self._event_loop.create_task(
            self._proxy_asgi_receive(receive, receive_queue)
        )

        async def receive_thread_safe(*args):
            return await asyncio.wrap_future(
                asyncio.run_coroutine_threadsafe(
                    receive_queue.get_one_message(),
                    self._event_loop,
                )
            )

        request_metadata = RequestMetadata(
            # TODO: pick up from header.
            request_id=generate_request_id(),
            internal_request_id=generate_request_id(),
            call_method="__call__",
            # TODO(edoakes): populate this.
            route=scope.get("path", ""),
            app_name=self._deployment_id.app_name,
            # TODO(edoakes): populate the multiplexed model ID.
            multiplexed_model_id="",
            is_streaming=True,
            _request_protocol=RequestProtocol.HTTP,
        )
        http_request = StreamingHTTPRequest(
            asgi_scope=scope,
            receive_asgi_messages=receive_thread_safe,
        )

        try:
            result_gen = self.handle_request_with_rejection(
                request_metadata, http_request
            )
            queue_len_info: ReplicaQueueLengthInfo = await result_gen.__anext__()
            # TODO(edoakes): update the behavior to more closely mimic the existing path:
            # add an internal queue and drop requests based on max_queued_requests.
            if not queue_len_info.accepted:
                for msg in convert_object_to_asgi_messages(
                    "Replica exceeded capacity of max_ongoing_requests.",
                    status_code=503,
                ):
                    await send(msg)

                return

            async for result in result_gen:
                # TODO(edoakes): we should avoid serializing and deserializing the ASGI
                # messages here. This requires some upstream refactoring.
                for msg in pickle.loads(result):
                    await send(msg)
        finally:
            if not proxy_asgi_receive_task.done():
                proxy_asgi_receive_task.cancel()
