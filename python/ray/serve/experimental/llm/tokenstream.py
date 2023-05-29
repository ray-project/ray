import time
import asyncio
from threading import RLock, Condition


class _EndOfStream:
    pass


EOS = _EndOfStream()

class Event_ts(asyncio.Event):
    #TODO: clear() method
    def set(self):
        #FIXME: The _loop attribute is not documented as public api!
        self._loop.call_soon_threadsafe(super().set)


class FakeTokenStream:
    def __init__(self, loop=None, event=None):
        self._lock = RLock()
        self._cv = Condition(self._lock)
        self._num_tokens = 0
        self._end = False
        self._data = []
        self._event = event

    def end(self):
        with self._lock:
            self._end = True
            self._cv.notify_all()

        if self._event:
            self._event.set()

    def put(self, item):
        with self._lock:
            self._data.append(item)

    def num_tokens(self):
        with self._lock:
            return len(self._data)

    def last(self):
        with self._lock:
            return self._data[-1]

    def finished(self):
        with self._lock:
            return self._end

    def wait_until_finished(self, timeout=None):
        start = time.time()
        with self._cv:
            while not self._end:
                self._cv.wait(timeout)
                if timeout is not None and time.time() - start >= timeout:
                    return


class TokenStream:
    """A stream of tokens that can be iterated over asynchronously."""

    def __init__(self, loop=asyncio.get_event_loop()):
        self._queue = asyncio.Queue()
        self._loop = loop

    async def end(self):
        await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(self._queue.put(EOS), self._loop)
        )

    async def put(self, item):
        await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(self._queue.put(item), self._loop)
        )

    def __aiter__(self):
        return self

    async def __anext__(self):
        result = await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(self._queue.get(), self._loop)
        )
        if result == EOS:
            raise StopAsyncIteration
        return result
