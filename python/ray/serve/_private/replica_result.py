import asyncio
import inspect
import logging
import pickle
import threading
import time
from abc import ABC, abstractmethod
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Optional, Union

import grpc

import ray

if TYPE_CHECKING:
    from ray.serve._private.serialization import RPCSerializer
from ray.exceptions import TaskCancelledError
from ray.serve._private.common import ReplicaQueueLengthInfo, RequestMetadata
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import calculate_remaining_timeout, generate_request_id
from ray.serve.exceptions import RequestCancelledError

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ReplicaResult(ABC):
    @abstractmethod
    async def get_rejection_response(self) -> Optional[ReplicaQueueLengthInfo]:
        raise NotImplementedError

    @abstractmethod
    def get(self, timeout_s: Optional[float]):
        raise NotImplementedError

    @abstractmethod
    async def get_async(self):
        raise NotImplementedError

    @abstractmethod
    def __next__(self):
        raise NotImplementedError

    @abstractmethod
    async def __anext__(self):
        raise NotImplementedError

    @abstractmethod
    def add_done_callback(self, callback: Callable):
        raise NotImplementedError

    @abstractmethod
    def cancel(self):
        raise NotImplementedError

    @abstractmethod
    def to_object_ref(self, timeout_s: Optional[float]) -> ray.ObjectRef:
        raise NotImplementedError

    @abstractmethod
    async def to_object_ref_async(self) -> ray.ObjectRef:
        raise NotImplementedError

    @abstractmethod
    def to_object_ref_gen(self) -> ray.ObjectRefGenerator:
        # NOTE(edoakes): there is only a sync version of this method because it
        # does not block like `to_object_ref` (so there's also no timeout argument).
        raise NotImplementedError


class ActorReplicaResult(ReplicaResult):
    def __init__(
        self,
        obj_ref_or_gen: Union[ray.ObjectRef, ray.ObjectRefGenerator],
        metadata: RequestMetadata,
        *,
        with_rejection: bool = False,
    ):
        self._obj_ref: Optional[ray.ObjectRef] = None
        self._obj_ref_gen: Optional[ray.ObjectRefGenerator] = None
        self._is_streaming: bool = metadata.is_streaming
        self._request_id: str = metadata.request_id
        self._object_ref_or_gen_sync_lock = threading.Lock()
        self._with_rejection = with_rejection
        self._rejection_response = None

        if isinstance(obj_ref_or_gen, ray.ObjectRefGenerator):
            self._obj_ref_gen = obj_ref_or_gen
        else:
            self._obj_ref = obj_ref_or_gen

        if self._is_streaming:
            assert (
                self._obj_ref_gen is not None
            ), "An ObjectRefGenerator must be passed for streaming requests."

        request_context = ray.serve.context._get_serve_request_context()
        if request_context.cancel_on_parent_request_cancel:
            # Keep track of in-flight requests.
            self._response_id = generate_request_id()
            ray.serve.context._add_in_flight_request(
                request_context._internal_request_id, self._response_id, self
            )
            self.add_done_callback(
                lambda _: ray.serve.context._remove_in_flight_request(
                    request_context._internal_request_id, self._response_id
                )
            )

    def _process_response(f: Union[Callable, Coroutine]):
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            try:
                return f(self, *args, **kwargs)
            except ray.exceptions.TaskCancelledError:
                raise RequestCancelledError(self._request_id)

        @wraps(f)
        async def async_wrapper(self, *args, **kwargs):
            try:
                return await f(self, *args, **kwargs)
            except ray.exceptions.TaskCancelledError:
                raise asyncio.CancelledError()

        if inspect.iscoroutinefunction(f):
            return async_wrapper
        else:
            return wrapper

    @_process_response
    async def get_rejection_response(self) -> Optional[ReplicaQueueLengthInfo]:
        """Get the queue length info from the replica to handle rejection."""
        assert (
            self._with_rejection and self._obj_ref_gen is not None
        ), "get_rejection_response() can only be called when request rejection is enabled."

        try:
            if self._rejection_response is None:
                response = await (await self._obj_ref_gen.__anext__())
                self._rejection_response = pickle.loads(response)

            return self._rejection_response
        except asyncio.CancelledError as e:
            # HTTP client disconnected or request was explicitly canceled.
            logger.info(
                "Cancelling request that has already been assigned to a replica."
            )
            self.cancel()
            raise e from None
        except TaskCancelledError:
            raise asyncio.CancelledError()

    @_process_response
    def get(self, timeout_s: Optional[float]):
        assert (
            not self._is_streaming
        ), "get() can only be called on a unary ActorReplicaResult."

        start_time_s = time.time()
        object_ref = self.to_object_ref(timeout_s=timeout_s)
        remaining_timeout_s = calculate_remaining_timeout(
            timeout_s=timeout_s,
            start_time_s=start_time_s,
            curr_time_s=time.time(),
        )
        return ray.get(object_ref, timeout=remaining_timeout_s)

    @_process_response
    async def get_async(self):
        assert (
            not self._is_streaming
        ), "get_async() can only be called on a unary ActorReplicaResult."

        return await (await self.to_object_ref_async())

    @_process_response
    def __next__(self):
        assert (
            self._is_streaming
        ), "next() can only be called on a streaming ActorReplicaResult."

        next_obj_ref = self._obj_ref_gen.__next__()
        return ray.get(next_obj_ref)

    @_process_response
    async def __anext__(self):
        assert (
            self._is_streaming
        ), "__anext__() can only be called on a streaming ActorReplicaResult."

        next_obj_ref = await self._obj_ref_gen.__anext__()
        return await next_obj_ref

    def add_done_callback(self, callback: Callable):
        if self._obj_ref_gen is not None:
            self._obj_ref_gen.completed()._on_completed(callback)
        else:
            self._obj_ref._on_completed(callback)

    def cancel(self):
        if self._obj_ref_gen is not None:
            ray.cancel(self._obj_ref_gen)
        else:
            ray.cancel(self._obj_ref)

    def to_object_ref(self, *, timeout_s: Optional[float] = None) -> ray.ObjectRef:
        assert (
            not self._is_streaming
        ), "to_object_ref can only be called on a unary ReplicaActorResult."

        # NOTE(edoakes): this section needs to be guarded with a lock and the resulting
        # object ref cached in order to avoid calling `__next__()` to
        # resolve to the underlying object ref more than once.
        # See: https://github.com/ray-project/ray/issues/43879.
        with self._object_ref_or_gen_sync_lock:
            if self._obj_ref is None:
                obj_ref = self._obj_ref_gen._next_sync(timeout_s=timeout_s)
                if obj_ref.is_nil():
                    raise TimeoutError("Timed out resolving to ObjectRef.")

                self._obj_ref = obj_ref

        return self._obj_ref

    async def to_object_ref_async(self) -> ray.ObjectRef:
        assert (
            not self._is_streaming
        ), "to_object_ref_async can only be called on a unary ReplicaActorResult."

        # NOTE(edoakes): this section needs to be guarded with a lock and the resulting
        # object ref cached in order to avoid calling `__anext__()` to
        # resolve to the underlying object ref more than once.
        # See: https://github.com/ray-project/ray/issues/43879.
        #
        # IMPORTANT: We use a threading lock instead of asyncio.Lock because this method
        # can be called from multiple event loops concurrently:
        # 1. From the user's code (on the replica's event loop) when awaiting a response
        # 2. From the router's event loop when resolving a DeploymentResponse argument
        # asyncio.Lock is NOT thread-safe and NOT designed for cross-loop usage, which
        # causes deadlocks.
        #
        # We use a non-blocking acquire pattern to avoid blocking the event loop:
        # - Try to acquire the lock without blocking
        # - If already held, yield and retry (allows other async tasks to run)
        # - Once acquired, check if result is already available (double-check pattern)
        while True:
            # Fast path: already computed
            if self._obj_ref is not None:
                return self._obj_ref

            acquired = self._object_ref_or_gen_sync_lock.acquire(blocking=False)
            if acquired:
                try:
                    # Double-check under lock
                    if self._obj_ref is None:
                        self._obj_ref = await self._obj_ref_gen.__anext__()
                    return self._obj_ref
                finally:
                    self._object_ref_or_gen_sync_lock.release()
            else:
                # Lock is held by another task/thread, yield and retry
                await asyncio.sleep(0)

    def to_object_ref_gen(self) -> ray.ObjectRefGenerator:
        assert (
            self._is_streaming
        ), "to_object_ref_gen can only be called on a streaming ReplicaActorResult."

        return self._obj_ref_gen


class gRPCReplicaResult(ReplicaResult):
    """ReplicaResult for gRPC inter-deployment communication."""

    def __init__(
        self,
        call: Union[grpc.aio.UnaryUnaryCall, grpc.aio.UnaryStreamCall],
        metadata: RequestMetadata,
        serializer: "RPCSerializer",
        *,
        with_rejection: bool = False,
    ):
        # Import here to avoid circular import
        from ray.serve._private.serialization import RPCSerializer

        self._call = call
        self._is_streaming: bool = metadata.is_streaming
        self._request_id: str = metadata.request_id
        self._serializer: RPCSerializer = serializer
        self._with_rejection = with_rejection
        self._rejection_response: Optional[ReplicaQueueLengthInfo] = None
        self._cached_result: Optional[Any] = None
        self._result_lock = threading.Lock()
        self._done_callbacks = []
        self._is_done = False
        self._stream_iterator = None

        # Set up done callback to track completion
        if hasattr(call, "add_done_callback"):
            call.add_done_callback(self._on_call_done)

    def _on_call_done(self, call):
        """Called when the gRPC call completes."""
        self._is_done = True
        for callback in self._done_callbacks:
            try:
                callback(self)
            except Exception:
                logger.exception("Error in done callback")

    def _deserialize_response(self, response) -> Any:
        """Deserialize a gRPC response."""
        if response.is_error:
            # Deserialize and raise the exception
            exception = self._serializer.loads_response(response.serialized_message)
            raise exception
        return self._serializer.loads_response(response.serialized_message)

    async def get_rejection_response(self) -> Optional[ReplicaQueueLengthInfo]:
        """Get the queue length info from the replica to handle rejection."""
        if not self._with_rejection:
            return None

        try:
            if self._rejection_response is None:
                # For gRPC, rejection info is sent in initial metadata
                initial_metadata = await self._call.initial_metadata()
                metadata_dict = dict(initial_metadata)

                accepted = metadata_dict.get("accepted", "1") == "1"
                num_ongoing = int(metadata_dict.get("num_ongoing_requests", "0"))

                self._rejection_response = ReplicaQueueLengthInfo(
                    accepted=accepted,
                    num_ongoing_requests=num_ongoing,
                )

            return self._rejection_response
        except asyncio.CancelledError as e:
            logger.info(
                "Cancelling gRPC request that has already been assigned to a replica."
            )
            self.cancel()
            raise e from None
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.CANCELLED:
                raise asyncio.CancelledError()
            raise

    def get(self, timeout_s: Optional[float]):
        """Synchronously get the result (blocking)."""
        assert (
            not self._is_streaming
        ), "get() can only be called on a unary gRPCReplicaResult."

        # Run the async get in an event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're already in an async context, can't use run_until_complete
                raise RuntimeError(
                    "Cannot call get() from an async context. Use get_async() instead."
                )
            return loop.run_until_complete(
                asyncio.wait_for(self.get_async(), timeout=timeout_s)
            )
        except asyncio.TimeoutError:
            raise TimeoutError(f"gRPC call timed out after {timeout_s}s")

    async def get_async(self):
        """Asynchronously get the result."""
        assert (
            not self._is_streaming
        ), "get_async() can only be called on a unary gRPCReplicaResult."

        try:
            with self._result_lock:
                if self._cached_result is not None:
                    return self._cached_result

            response = await self._call
            result = self._deserialize_response(response)

            with self._result_lock:
                self._cached_result = result

            return result
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.CANCELLED:
                raise asyncio.CancelledError()
            raise
        except asyncio.CancelledError:
            raise RequestCancelledError(self._request_id)

    def __next__(self):
        """Synchronously get the next streaming result."""
        assert (
            self._is_streaming
        ), "next() can only be called on a streaming gRPCReplicaResult."

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError(
                    "Cannot call __next__() from an async context. "
                    "Use __anext__() instead."
                )
            return loop.run_until_complete(self.__anext__())
        except StopAsyncIteration:
            raise StopIteration

    async def __anext__(self):
        """Asynchronously get the next streaming result."""
        assert (
            self._is_streaming
        ), "__anext__() can only be called on a streaming gRPCReplicaResult."

        try:
            if self._stream_iterator is None:
                self._stream_iterator = self._call.__aiter__()

            response = await self._stream_iterator.__anext__()
            return self._deserialize_response(response)
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.CANCELLED:
                raise asyncio.CancelledError()
            raise
        except StopAsyncIteration:
            raise

    def add_done_callback(self, callback: Callable):
        """Add a callback to be called when the request completes."""
        if self._is_done:
            callback(self)
        else:
            self._done_callbacks.append(callback)

    def cancel(self):
        """Cancel the gRPC request."""
        self._call.cancel()

    def to_object_ref(self, *, timeout_s: Optional[float] = None) -> ray.ObjectRef:
        """Convert to ObjectRef (not supported for gRPC results)."""
        raise NotImplementedError(
            "gRPCReplicaResult does not support conversion to ObjectRef. "
            "Use get() or get_async() instead."
        )

    async def to_object_ref_async(self) -> ray.ObjectRef:
        """Convert to ObjectRef async (not supported for gRPC results)."""
        raise NotImplementedError(
            "gRPCReplicaResult does not support conversion to ObjectRef. "
            "Use get_async() instead."
        )

    def to_object_ref_gen(self) -> ray.ObjectRefGenerator:
        """Convert to ObjectRefGenerator (not supported for gRPC results)."""
        raise NotImplementedError(
            "gRPCReplicaResult does not support conversion to ObjectRefGenerator. "
            "Use async iteration instead."
        )
