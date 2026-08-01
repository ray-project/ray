import asyncio
from typing import Any, AsyncIterator, Optional, TypeVar

from ray._common.utils import get_or_create_event_loop

T = TypeVar("T")

# Sentinel placed on the queue to signal that the request stream has ended.
_STREAM_END = object()


class gRPCDIReceiveStream(AsyncIterator[T]):
    """Exposes a native gRPC request iterator to user code for direct ingress.

    The native iterator is bound to the replica's server event loop, but user code
    may run on a separate event loop. When the loops differ, a fetch task drains the
    iterator on the server loop and hands messages to user code across loops; when
    they're the same, user code iterates the native iterator directly.
    """

    def __init__(
        self,
        request_iterator: AsyncIterator[T],
        user_event_loop: asyncio.AbstractEventLoop,
        *,
        cancel_event: Optional[asyncio.Event] = None,
    ):
        self._request_iterator = request_iterator
        self._user_event_loop = user_event_loop
        self._same_event_loop = user_event_loop is get_or_create_event_loop()
        # Set when the client disconnects/errors mid-stream so the consumer's
        # gRPCInputStream reports is_cancelled() and ends gracefully.
        self._cancel_event = cancel_event
        # Lazily created so it binds to the user-code event loop (where it is
        # always accessed), never the server loop.
        self._queue: Optional[asyncio.Queue] = None
        self._fetch_task: Optional[asyncio.Task] = None

    @property
    def _message_queue(self) -> asyncio.Queue:
        if self._queue is None:
            self._queue = asyncio.Queue()
        return self._queue

    def _put(self, item: Any):
        self._message_queue.put_nowait(item)

    def start(self) -> Optional[asyncio.Task]:
        """Start draining the native iterator. Must be called on the server loop."""
        # We don't create another task to consume the input stream if we run on the
        # same event loop.
        if self._same_event_loop:
            return None

        self._fetch_task = asyncio.ensure_future(self._fetch_until_done())
        return self._fetch_task

    def cancel(self):
        """Stop draining the native iterator (e.g. if the consumer finished)."""
        if self._fetch_task is not None:
            self._fetch_task.cancel()

    async def _fetch_until_done(self):
        """Drive the native iterator on the server loop, forwarding each message.

        A stream error (e.g. client disconnect) is treated as cancellation: the
        cancel event is set and the stream ends gracefully -- mirroring the proxy
        path (`receive_grpc_messages`) -- rather than surfacing the raw gRPC error
        to user code. A sentinel is always enqueued at the end so the consumer
        terminates.
        """
        try:
            async for message in self._request_iterator:
                # Stop draining if the consumer cancelled (e.g. user code called
                # input_stream.cancel()).
                if self._cancel_event is not None and self._cancel_event.is_set():
                    break
                self._user_event_loop.call_soon_threadsafe(self._put, message)
        except asyncio.CancelledError:
            raise
        except Exception:
            if self._cancel_event is not None:
                self._user_event_loop.call_soon_threadsafe(self._cancel_event.set)
        finally:
            self._user_event_loop.call_soon_threadsafe(self._put, _STREAM_END)

    def __aiter__(self) -> "gRPCDIReceiveStream":
        return self

    async def __anext__(self) -> T:
        """Return the next request message. Runs on the user-code loop."""
        if self._same_event_loop:
            try:
                return await anext(self._request_iterator)
            except (asyncio.CancelledError, StopAsyncIteration):
                raise
            except Exception:
                if self._cancel_event is not None:
                    self._cancel_event.set()
                raise StopAsyncIteration

        item = await self._message_queue.get()
        if item is _STREAM_END:
            raise StopAsyncIteration
        return item
