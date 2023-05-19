import asyncio


class _EndOfStream:
    pass


EOS = _EndOfStream()


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
