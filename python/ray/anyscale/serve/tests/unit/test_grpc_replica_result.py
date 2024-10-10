import asyncio
import sys
import threading

import pytest

from ray import cloudpickle
from ray._private.test_utils import wait_for_condition
from ray.anyscale.serve._private.replica_result import gRPCReplicaResult
from ray.serve.generated import serve_proprietary_pb2


class FakegRPCUnaryCall:
    def __init__(self, item):
        self._loop = asyncio.get_running_loop()
        self._item = item

    def __await__(self):
        if asyncio.get_running_loop() != self._loop:
            raise RuntimeError("Tried to fetch from a different loop!")

        yield
        return serve_proprietary_pb2.ASGIResponse(
            serialized_message=cloudpickle.dumps(self._item)
        )


class FakegRPCStreamCall:
    def __init__(self, items, event: threading.Event = None):
        self._loop = asyncio.get_running_loop()
        self._items = items
        self._event = event

    def is_empty(self) -> bool:
        assert len(self._items) == 0
        return True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if asyncio.get_running_loop() != self._loop:
            raise RuntimeError("Tried to fetch from a different loop!")

        if not self._items:
            raise StopAsyncIteration

        if self._event:
            await self._loop.run_in_executor(None, self._event.wait)

        return serve_proprietary_pb2.ASGIResponse(
            serialized_message=cloudpickle.dumps(self._items.pop(0))
        )


@pytest.fixture
def create_asyncio_event_loop_in_thread():
    async_loop = asyncio.new_event_loop()
    thread = threading.Thread(daemon=True, target=async_loop.run_forever)
    thread.start()
    event = threading.Event()

    yield async_loop, event

    # Unblock event in case it's blocking shutdown
    event.set()


@pytest.mark.asyncio
class TestSameLoop:
    async def test_unary(self):
        fake_call = FakegRPCUnaryCall("hello")
        replica_result = gRPCReplicaResult(
            fake_call,
            is_streaming=False,
            loop=asyncio.get_running_loop(),
            on_separate_loop=False,
        )

        assert await replica_result.get_async() == "hello"

    async def test_streaming(self):
        fake_call = FakegRPCStreamCall([1, 2, 3, 4])
        replica_result = gRPCReplicaResult(
            fake_call,
            is_streaming=True,
            loop=asyncio.get_running_loop(),
            on_separate_loop=False,
        )
        assert [r async for r in replica_result] == [1, 2, 3, 4]

    async def test_unary_with_gen(self):
        fake_call = FakegRPCStreamCall(["hello"])
        replica_result = gRPCReplicaResult(
            fake_call,
            is_streaming=False,
            loop=asyncio.get_running_loop(),
            on_separate_loop=False,
        )
        assert await replica_result.get_async() == "hello"


class TestSeparateLoop:
    async def make_fake_unary_request(self, data, loop: asyncio.AbstractEventLoop):
        fake_call = FakegRPCUnaryCall(data)
        replica_result = gRPCReplicaResult(
            fake_call, is_streaming=False, loop=loop, on_separate_loop=True
        )
        return replica_result

    async def make_fake_streaming_request(
        self,
        data,
        loop: asyncio.AbstractEventLoop,
        on_separate_loop: bool,
        *,
        is_streaming: bool = True,
        event: threading.Event = None,
    ):
        fake_call = FakegRPCStreamCall(data, event=event)
        return gRPCReplicaResult(
            fake_call,
            is_streaming=is_streaming,
            loop=loop,
            on_separate_loop=on_separate_loop,
        )

    def test_unary_sync(self, create_asyncio_event_loop_in_thread):
        loop, _ = create_asyncio_event_loop_in_thread

        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_unary_request("hello", loop), loop=loop
        )
        replica_result = fut.result()

        assert replica_result.get(None) == "hello"

    @pytest.mark.asyncio
    async def test_unary_async(self, create_asyncio_event_loop_in_thread):
        loop, _ = create_asyncio_event_loop_in_thread

        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_unary_request("hello", loop), loop=loop
        )
        replica_result = fut.result()

        assert await replica_result.get_async() == "hello"

    def test_streaming_sync(self, create_asyncio_event_loop_in_thread):
        loop, _ = create_asyncio_event_loop_in_thread

        # Instantiate gRPCReplicaResult with FakegRPCStreamCall. This needs
        # to be run on the "other loop"
        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request([1, 2, 3, 4], loop, on_separate_loop=True),
            loop=loop,
        )
        replica_result = fut.result()

        # The async generator should be consumed even if we don't fetch
        # the items explicitly through the ReplicaResult object
        wait_for_condition(replica_result._call.is_empty, retry_interval_ms=10)

        # Finally, check results given by gRPCReplicaResult fetched from
        # the queue are correct
        assert list(replica_result) == [1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_streaming_async(self, create_asyncio_event_loop_in_thread):
        loop, _ = create_asyncio_event_loop_in_thread

        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request([1, 2, 3, 4], loop, on_separate_loop=True),
            loop=loop,
        )
        replica_result = fut.result()

        # Check async generator is consumed on its own
        wait_for_condition(replica_result._call.is_empty, retry_interval_ms=10)
        assert [r async for r in replica_result] == [1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_streaming_blocked(self, create_asyncio_event_loop_in_thread):
        """Use threading event to block async generator, check everything works"""

        loop, event = create_asyncio_event_loop_in_thread

        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request(
                [1, 2, 3, 4], loop, on_separate_loop=True, event=event
            ),
            loop=loop,
        )
        replica_result = fut.result()

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(replica_result.__anext__(), 0.01)

        event.set()
        assert [r async for r in replica_result] == [1, 2, 3, 4]

    def test_unary_with_gen_sync(self, create_asyncio_event_loop_in_thread):
        loop, _ = create_asyncio_event_loop_in_thread

        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request(
                ["hello"], loop, on_separate_loop=True, is_streaming=False
            ),
            loop=loop,
        )
        replica_result = fut.result()

        # Check async generator is consumed on its own
        wait_for_condition(replica_result._call.is_empty, retry_interval_ms=10)
        assert replica_result.get(None) == "hello"

    @pytest.mark.asyncio
    async def test_unary_with_gen_async(self, create_asyncio_event_loop_in_thread):
        loop, _ = create_asyncio_event_loop_in_thread

        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request(
                ["hello"], loop, on_separate_loop=True, is_streaming=False
            ),
            loop=loop,
        )
        replica_result = fut.result()

        # Check async generator is consumed on its own
        wait_for_condition(replica_result._call.is_empty, retry_interval_ms=10)
        assert await replica_result.get_async() == "hello"

    @pytest.mark.asyncio
    async def test_unary_with_gen_blocked(self, create_asyncio_event_loop_in_thread):
        """Use threading event to block async generator, check everything works"""

        loop, event = create_asyncio_event_loop_in_thread

        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request(
                ["hello"], loop, on_separate_loop=True, event=event
            ),
            loop=loop,
        )
        replica_result = fut.result()

        t = asyncio.create_task(replica_result.get_async())
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(t), 0.01)

        event.set()
        assert await t == "hello"

    def test_unary_with_timeout(self, create_asyncio_event_loop_in_thread):
        """Test get() with timeout."""

        loop, event = create_asyncio_event_loop_in_thread

        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request(
                ["hello"], loop, on_separate_loop=True, event=event
            ),
            loop=loop,
        )
        replica_result = fut.result()

        with pytest.raises(TimeoutError):
            replica_result.get(timeout_s=0.01)

        event.set()
        assert replica_result.get(timeout_s=0.01) == "hello"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
