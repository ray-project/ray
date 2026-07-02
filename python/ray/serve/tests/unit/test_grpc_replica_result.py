import asyncio
import sys
import threading

import grpc
import pytest

from ray import ActorID, cloudpickle
from ray._common.test_utils import wait_for_condition
from ray.exceptions import ActorUnavailableError
from ray.serve._private.common import RequestMetadata
from ray.serve._private.replica_result import gRPCReplicaResult
from ray.serve.generated import serve_pb2


class FakegRPCUnaryCall:
    def __init__(self, item, is_error: bool = False):
        self._loop = asyncio.get_running_loop()
        self._item = item
        self._is_error = is_error

    def __await__(self):
        if asyncio.get_running_loop() != self._loop:
            raise RuntimeError("Tried to fetch from a different loop!")

        yield
        return serve_pb2.ASGIResponse(
            serialized_message=cloudpickle.dumps(self._item), is_error=self._is_error
        )

    def add_done_callback(self, cb):
        pass


class FakegRPCStreamCall:
    def __init__(self, items, *, event: threading.Event = None):
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

        item, is_error = self._items.pop(0)
        return serve_pb2.ASGIResponse(
            serialized_message=cloudpickle.dumps(item),
            is_error=is_error,
        )

    def add_done_callback(self, cb):
        pass


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
    def make_fake_call(self, is_streaming: bool, *, data=None, error=None):
        if is_streaming:
            fake_call = FakegRPCStreamCall(data)
        else:
            if error:
                fake_call = FakegRPCUnaryCall(error, is_error=True)
            else:
                fake_call = FakegRPCUnaryCall(data, is_error=False)

        return gRPCReplicaResult(
            fake_call,
            metadata=RequestMetadata(
                request_id="",
                internal_request_id="",
                is_streaming=False,
                _on_separate_loop=False,
            ),
            actor_id=ActorID(b"2" * 16),
            loop=asyncio.get_running_loop(),
        )

    async def test_unary(self):
        replica_result = self.make_fake_call(is_streaming=False, data="hello")
        assert await replica_result.get_async() == "hello"

    async def test_streaming(self):
        replica_result = self.make_fake_call(
            is_streaming=True, data=[(1, False), (2, False), (3, False), (4, False)]
        )
        assert [r async for r in replica_result] == [1, 2, 3, 4]

    async def test_unary_with_gen(self):
        replica_result = self.make_fake_call(is_streaming=True, data=[("hello", False)])
        assert await replica_result.get_async() == "hello"

    async def test_unary_error(self):
        """Test error is raised correctly."""

        replica_result = self.make_fake_call(
            is_streaming=False, error=RuntimeError("oh no!")
        )
        with pytest.raises(RuntimeError, match="oh no!"):
            await replica_result.get_async()

    async def test_streaming_error(self):
        """Test error is raised correctly."""

        replica_result = self.make_fake_call(
            is_streaming=True, data=[(RuntimeError("oh no!"), True)]
        )
        with pytest.raises(RuntimeError, match="oh no!"):
            await replica_result.__anext__()


class TestSeparateLoop:
    async def make_fake_unary_request(self, data, loop: asyncio.AbstractEventLoop):
        fake_call = FakegRPCUnaryCall(data)
        replica_result = gRPCReplicaResult(
            fake_call,
            metadata=RequestMetadata(
                request_id="",
                internal_request_id="",
                is_streaming=False,
                _on_separate_loop=True,
            ),
            actor_id=ActorID(b"2" * 16),
            loop=loop,
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
        error=None,
    ):
        if error:
            fake_call = FakegRPCStreamCall([(error, True)], event=event)
        else:
            fake_call = FakegRPCStreamCall([(d, False) for d in data], event=event)
        return gRPCReplicaResult(
            fake_call,
            metadata=RequestMetadata(
                request_id="",
                internal_request_id="",
                is_streaming=is_streaming,
                _on_separate_loop=on_separate_loop,
            ),
            actor_id=ActorID(b"2" * 16),
            loop=loop,
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

        async def fetch():
            return [r async for r in replica_result]

        t = asyncio.create_task(fetch())

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(t), 0.01)

        event.set()
        assert await t == [1, 2, 3, 4]

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

    def test_unary_error_sync(self, create_asyncio_event_loop_in_thread):
        """Test error is raised correctly."""

        loop, _ = create_asyncio_event_loop_in_thread
        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request(
                None, loop, on_separate_loop=True, error=RuntimeError("oh no!")
            ),
            loop=loop,
        )
        replica_result = fut.result()

        with pytest.raises(RuntimeError, match="oh no!"):
            replica_result.get(None)

    @pytest.mark.asyncio
    async def test_unary_error_async(self, create_asyncio_event_loop_in_thread):
        """Test error is raised correctly."""

        loop, _ = create_asyncio_event_loop_in_thread
        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request(
                None, loop, on_separate_loop=True, error=RuntimeError("oh no!")
            ),
            loop=loop,
        )
        replica_result = fut.result()

        with pytest.raises(RuntimeError, match="oh no!"):
            await replica_result.get_async()

    def test_streaming_error_sync(self, create_asyncio_event_loop_in_thread):
        """Test error is raised correctly."""

        loop, _ = create_asyncio_event_loop_in_thread
        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request(
                None, loop, on_separate_loop=True, error=RuntimeError("oh no!")
            ),
            loop=loop,
        )
        replica_result = fut.result()

        with pytest.raises(RuntimeError, match="oh no!"):
            replica_result.__next__()

    @pytest.mark.asyncio
    async def test_streaming_error_async(self, create_asyncio_event_loop_in_thread):
        """Test error is raised correctly."""

        loop, _ = create_asyncio_event_loop_in_thread
        fut = asyncio.run_coroutine_threadsafe(
            self.make_fake_streaming_request(
                None, loop, on_separate_loop=True, error=RuntimeError("oh no!")
            ),
            loop=loop,
        )
        replica_result = fut.result()

        with pytest.raises(RuntimeError, match="oh no!"):
            await replica_result.__anext__()


class FakegRPCCallWithStatus:
    """Minimal fake of a completed ``grpc.aio.Call`` for done-callback tests.

    ``grpc.aio`` invokes done-callbacks with the call object itself and exposes
    the final status only via the async ``code()`` method, so we mirror that.
    """

    def __init__(self, code: grpc.StatusCode):
        self._loop = asyncio.get_running_loop()
        self._code = code
        self._done_callbacks = []

    def add_done_callback(self, cb):
        self._done_callbacks.append(cb)

    async def code(self) -> grpc.StatusCode:
        return self._code

    def complete(self):
        # grpc invokes done-callbacks with the call object itself.
        for cb in self._done_callbacks:
            cb(self)


@pytest.mark.asyncio
class TestDoneCallbackNormalization:
    """gRPCReplicaResult.add_done_callback must normalize a failed call into the
    same shape the actor transport delivers, so the router's completion handler
    can invalidate its queue-length cache for the dead replica. See
    https://github.com/ray-project/ray/issues/63261.
    """

    def make_result(self, code: grpc.StatusCode):
        fake_call = FakegRPCCallWithStatus(code)
        result = gRPCReplicaResult(
            fake_call,
            metadata=RequestMetadata(
                request_id="",
                internal_request_id="",
                is_streaming=False,
                _on_separate_loop=False,
            ),
            actor_id=ActorID(b"2" * 16),
            loop=asyncio.get_running_loop(),
        )
        return result, fake_call

    async def _fire_and_capture(self, code: grpc.StatusCode):
        result, fake_call = self.make_result(code)
        event = asyncio.Event()
        received = []

        def callback(r):
            received.append(r)
            event.set()

        result.add_done_callback(callback)
        fake_call.complete()
        # Normalization resolves the status code asynchronously on the call's
        # loop; wait deterministically for the callback to be invoked.
        try:
            await asyncio.wait_for(event.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pytest.fail("done-callback was never invoked")
        return received[0], fake_call

    async def test_unavailable_normalized_to_actor_unavailable(self):
        """A failed (UNAVAILABLE) call is surfaced as ActorUnavailableError so the
        router invalidates its cache instead of silently ignoring the failure."""
        received, _ = await self._fire_and_capture(grpc.StatusCode.UNAVAILABLE)
        assert isinstance(received, ActorUnavailableError)

    async def test_ok_passes_through_call(self):
        """A successful call preserves the previous behavior: the callback
        receives the call object, not a synthesized error."""
        received, fake_call = await self._fire_and_capture(grpc.StatusCode.OK)
        assert received is fake_call

    async def test_cancelled_not_treated_as_failure(self):
        """CANCELLED (typically a client-initiated cancellation) must NOT be
        converted into a retryable ActorUnavailableError."""
        received, fake_call = await self._fire_and_capture(grpc.StatusCode.CANCELLED)
        assert not isinstance(received, ActorUnavailableError)
        assert received is fake_call


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
