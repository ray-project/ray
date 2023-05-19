import asyncio


class _EndOfStream:
    pass


EOS = _EndOfStream()


class TokenStream:
    def __init__(self):
        self._queue = asyncio.Queue()
        self._lock = asyncio.Lock()

    async def end(self):
        await self._queue.put(EOS)

    async def put(self, item):
        async with self._lock:
            await self._queue.put(item)

    async def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            async with self._lock:
                result = await self._queue.get()
            if result == EOS:
                raise StopAsyncIteration
            yield result
