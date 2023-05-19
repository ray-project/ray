import pytest
import asyncio

from ray.serve.experimental.llm.tokenstream import TokenStream
from threading import Thread, Lock


@pytest.mark.asyncio
async def test_token_stream(event_loop):
    token_stream = TokenStream(loop=event_loop)
    await token_stream.put(1)
    await token_stream.put(2)
    await token_stream.end()
    results = []
    async for entry in token_stream:
        results.append(entry)
    assert results == [1, 2]


@pytest.mark.asyncio
async def test_token_stream_concurrent(event_loop):
    token_stream = TokenStream(loop=event_loop)
    lock = Lock()
    results = []

    async def read_tokens():
        tmp = []
        async for entry in token_stream:
            tmp.append(entry)
        with lock:
            results.extend(tmp)

    loop1 = asyncio.new_event_loop()

    def run_loop(loop):
        asyncio.set_event_loop(loop)
        loop.run_until_complete(read_tokens())

    thread = Thread(target=run_loop, args=(loop1,))
    thread.start()

    for i in range(10):
        await token_stream.put(i)
        await asyncio.sleep(0.1)
    await token_stream.end()

    thread.join()

    with lock:
        assert results == list(range(10))
