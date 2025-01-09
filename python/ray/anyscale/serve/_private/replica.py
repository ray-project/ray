import asyncio
import inspect
import logging
import pickle
import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, AsyncGenerator, Generator, Tuple

import grpc

import ray
from ray import cloudpickle
from ray.anyscale.serve._private.constants import (
    ANYSCALE_RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH,
)
from ray.anyscale.serve._private.tracing_utils import (
    TraceContextManager,
    extract_propagated_context,
    set_span_attributes,
    setup_tracing,
)
from ray.anyscale.serve.context import _get_in_flight_requests
from ray.anyscale.serve.utils import asyncio_grpc_exception_handler
from ray.serve._private.common import (
    ReplicaQueueLengthInfo,
    RequestMetadata,
    ServeComponentType,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.replica import ReplicaBase, StatusCodeCallback
from ray.serve.generated import serve_proprietary_pb2, serve_proprietary_pb2_grpc

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

    async def _on_initialized(self):
        serve_proprietary_pb2_grpc.add_ASGIServiceServicer_to_server(self, self._server)
        self._port = self._server.add_insecure_port("[::]:0")
        await self._server.start()

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
        trace_context = extract_propagated_context(request_metadata.tracing_context)
        trace_manager = TraceContextManager(
            trace_name="replica_handle_request",
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

    @_wrap_grpc_call
    async def HandleRequest(
        self,
        context: grpc.aio.ServicerContext,
        request_metadata: RequestMetadata,
        *request_args,
        **request_kwargs,
    ):
        return await self.handle_request(
            request_metadata, *request_args, **request_kwargs
        )

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
            yield result
