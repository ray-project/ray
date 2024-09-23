import asyncio
from asyncio import run_coroutine_threadsafe
from typing import Any, AsyncIterator, Callable, Iterator, Optional

import grpc

import ray
from ray import cloudpickle
from ray.serve._private.http_util import MessageQueue
from ray.serve._private.replica_result import ReplicaResult
from ray.serve.generated import serve_proprietary_pb2


def is_running_in_asyncio_loop() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


class gRPCReplicaResult(ReplicaResult):
    def __init__(
        self,
        call: grpc.aio.Call,
        is_streaming: bool,
        loop: asyncio.AbstractEventLoop,
        on_separate_loop: bool,
    ):
        self._call: grpc.aio.Call = call
        self._result_queue: MessageQueue = MessageQueue()
        # This is the asyncio event loop that the gRPC Call object is attached to
        self._grpc_call_loop = loop
        self._is_streaming = is_streaming
        # NOTE(zcin): for now, these two concepts will be synonymous.
        # In other words, using a queue means the router is running on
        # a separate thread/event loop, and vice versa not using a queue
        # means the router is running on the main event loop, where the
        # DeploymentHandle lives.
        self._use_queue = False
        self._calling_from_same_loop = not on_separate_loop

        self._gen = None
        self._fut = None
        self._consume_task = None

        if hasattr(self._call, "__aiter__"):
            self._gen = self._call.__aiter__()
            # If the grpc call IS streaming, AND it was created on a
            # a separate loop, then use a queue to fetch the objects
            self._use_queue = on_separate_loop

        if self._use_queue:
            self._consume_task = self._grpc_call_loop.create_task(
                self.consume_messages_from_gen()
            )

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    def __iter__(self) -> Iterator[Any]:
        return self

    async def consume_messages_from_gen(self):
        try:
            async for resp in self._gen:
                self._result_queue.put_nowait(resp)
        except BaseException as e:
            self._result_queue.set_error(e)
        finally:
            self._result_queue.close()

    async def _get_internal(self):
        """Gets the result from the gRPC call object.

        If the call object is a UnaryUnaryCall, we await the call.
        Otherwise the call object is a UnaryStreamCall.
          - If the request was sent on a separate loop, then the
          streamed results are being consumed and put onto the in-memory
          queue, so we read from that queue.
          - Otherwise the request was sent on the current loop, so we
          fetch the next object from the async generator.
        """

        if self._gen is None:
            return await self._call
        elif self._use_queue:
            return await self._result_queue.get_one_message()
        else:
            return await self._gen.__anext__()

    def get(self, timeout_s: Optional[float]):
        if is_running_in_asyncio_loop():
            raise RuntimeError(
                "Sync method `get()` should not be called from within an `asyncio` "
                "event loop. Use `get_async()` instead."
            )

        if self._fut is None:
            self._fut = run_coroutine_threadsafe(
                self._get_internal(), self._grpc_call_loop
            )

        grpc_response: serve_proprietary_pb2.ASGIResponse = self._fut.result(
            timeout=timeout_s
        )
        return cloudpickle.loads(grpc_response.msg)

    async def get_async(self):
        if self._fut is None:
            if self._calling_from_same_loop:
                self._fut = asyncio.create_task(self._get_internal())
            else:
                self._fut = run_coroutine_threadsafe(
                    self._get_internal(), self._grpc_call_loop
                )

        grpc_response: serve_proprietary_pb2.ASGIResponse = await asyncio.wrap_future(
            self._fut
        )
        return cloudpickle.loads(grpc_response.msg)

    def __next__(self):
        if is_running_in_asyncio_loop():
            raise RuntimeError(
                "Sync method `__next__()` should not be called from within an "
                "`asyncio` event loop. Use `__anext__()` instead."
            )

        fut = run_coroutine_threadsafe(self._get_internal(), loop=self._grpc_call_loop)
        try:
            grpc_response: serve_proprietary_pb2.ASGIResponse = fut.result()
            return cloudpickle.loads(grpc_response.msg)
        except StopAsyncIteration:
            # We need to raise the synchronous version, StopIteration
            raise StopIteration

    async def __anext__(self):
        if self._calling_from_same_loop:
            grpc_response: serve_proprietary_pb2.ASGIResponse = (
                await self._get_internal()
            )
        else:
            fut = run_coroutine_threadsafe(
                self._get_internal(), loop=self._grpc_call_loop
            )
            grpc_response: serve_proprietary_pb2.ASGIResponse = (
                await asyncio.wrap_future(fut)
            )

        # If it's an HTTP request, then the proxy response generator is
        # expecting a pickled dictionary, so we return result directly
        # without deserializing. Otherwise, we deserialize the result.
        if ray.serve.context._serve_request_context.get().is_http_request:
            return grpc_response.msg
        else:
            return cloudpickle.loads(grpc_response.msg)

    def add_callback(self, callback: Callable):
        self._call.add_done_callback(callback)

    def cancel(self):
        self._call.cancel()
