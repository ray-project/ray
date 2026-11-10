"""The KV router's ZMQ request/reply primitive: ZmqRpcClient and ZmqRpcServer."""

import asyncio
import sys

import pytest

from ray.llm._internal.serve.routing_policies.kv_aware.network_utils import (
    ZmqRpcClient,
    ZmqRpcError,
    ZmqRpcServer,
)


class _Handler:
    """Echoes payloads. ``b"slow"`` and ``b"hang"`` wait; ``b"fail"`` raises."""

    def __init__(self):
        self.release = asyncio.Event()
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def __call__(self, payload: bytes) -> bytes:
        if payload == b"fail":
            raise ValueError("bad request")
        if payload in (b"slow", b"hang"):
            self.started.set()
            try:
                await (self.release.wait() if payload == b"slow" else asyncio.Future())
            except asyncio.CancelledError:
                self.cancelled.set()
                raise
        return payload.upper()


async def _start_server(handler):
    server = ZmqRpcServer(handler)
    port = await server.start()
    return server, f"tcp://127.0.0.1:{port}"


@pytest.mark.asyncio
async def test_concurrent_calls_reply_independently():
    """A slow call does not delay a fast one on the same connection."""
    handler = _Handler()
    server, endpoint = await _start_server(handler)
    client = ZmqRpcClient()
    try:
        slow = asyncio.create_task(client.call(endpoint, b"slow", timeout_s=10))
        await handler.started.wait()
        assert await client.call(endpoint, b"fast", timeout_s=10) == b"FAST"
        assert not slow.done()
        handler.release.set()
        assert await slow == b"SLOW"
    finally:
        await client.close()
        await server.close()


@pytest.mark.asyncio
async def test_handler_error_is_raised_to_the_caller():
    server, endpoint = await _start_server(_Handler())
    client = ZmqRpcClient()
    try:
        with pytest.raises(ZmqRpcError, match="ValueError: bad request"):
            await client.call(endpoint, b"fail", timeout_s=10)
        # The connection survives a failed call.
        assert await client.call(endpoint, b"ok", timeout_s=10) == b"OK"
    finally:
        await client.close()
        await server.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("give_up", ["cancel", "timeout"])
async def test_abandoned_call_cancels_the_handler(give_up):
    handler = _Handler()
    server, endpoint = await _start_server(handler)
    client = ZmqRpcClient()
    try:
        if give_up == "cancel":
            call = asyncio.create_task(client.call(endpoint, b"hang", timeout_s=10))
            await handler.started.wait()
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call
        else:
            with pytest.raises(asyncio.TimeoutError):
                await client.call(endpoint, b"hang", timeout_s=0.5)
        await asyncio.wait_for(handler.cancelled.wait(), 5)
    finally:
        await client.close()
        await server.close()


@pytest.mark.asyncio
async def test_server_loss_fails_calls_in_flight():
    """A call fails fast when its server goes away, instead of waiting out its
    timeout. The next call reconnects."""
    handler = _Handler()
    server, endpoint = await _start_server(handler)
    client = ZmqRpcClient()
    try:
        call = asyncio.create_task(client.call(endpoint, b"hang", timeout_s=60))
        await handler.started.wait()
        await server.close()
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(call, 5)

        server, endpoint = await _start_server(_Handler())
        assert await client.call(endpoint, b"again", timeout_s=10) == b"AGAIN"
    finally:
        await client.close()
        await server.close()


@pytest.mark.asyncio
async def test_send_on_a_closed_connection_is_a_connection_error():
    server, endpoint = await _start_server(_Handler())
    client = ZmqRpcClient()
    try:
        channel = client._get_channel(endpoint)
        channel._socket.close(linger=0)
        with pytest.raises(ConnectionError):
            await channel.call(b"1", b"x")
    finally:
        await client.close()
        await server.close()


@pytest.mark.asyncio
async def test_idle_connections_are_evicted_past_the_limit():
    servers = [await _start_server(_Handler()) for _ in range(3)]
    client = ZmqRpcClient(max_channels=2)
    try:
        for _, endpoint in servers:
            assert await client.call(endpoint, b"x", timeout_s=10) == b"X"
        assert list(client._channels) == [endpoint for _, endpoint in servers[1:]]
        # An evicted server is reachable again on the next call.
        assert await client.call(servers[0][1], b"y", timeout_s=10) == b"Y"
    finally:
        await client.close()
        for server, _ in servers:
            await server.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
