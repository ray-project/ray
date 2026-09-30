import asyncio
import sys

import pytest

from ray.llm._internal.serve.core.ingress.byte_relay import (
    HIGH_WATER,
    TERMINATOR,
    ByteRelay,
    _ChunkDecoder,
    _find_transport,
    _Upstream,
)

HEAD = (
    b"HTTP/1.1 200 OK\r\n"
    b"content-type: text/event-stream\r\n"
    b"transfer-encoding: chunked\r\n"
    b"connection: keep-alive\r\n\r\n"
)


def _chunk(payload: bytes) -> bytes:
    return b"%x\r\n%s\r\n" % (len(payload), payload)


def _events(n: int):
    return [_chunk(b'data: {"i": %d}\n\n' % i) for i in range(n)]


class _Transport:
    def __init__(self):
        self.written = bytearray()
        self.closing = False
        self.reading = True
        self.buffered = 0

    def write(self, data):
        self.written += data

    def is_closing(self):
        return self.closing

    def close(self):
        self.closing = True

    def pause_reading(self):
        self.reading = False

    def resume_reading(self):
        self.reading = True

    def get_write_buffer_size(self):
        return self.buffered


def _upstream() -> _Upstream:
    up = _Upstream()
    up.connection_made(_Transport())
    return up


def _relay_without_handle() -> ByteRelay:
    # Skips __init__, which would look up the backend's deployment handle.
    relay = ByteRelay.__new__(ByteRelay)
    relay._endpoints, relay._misses, relay._in_flight, relay._rr = [], {}, {}, 0
    relay._idle = {}
    return relay


def test_streamed_body_passes_through_without_terminator():
    async def run():
        up = _upstream()
        events = _events(3)
        up.data_received(HEAD + events[0][:7])
        assert up.head_ready.done() and up.chunked and up.status == 200
        names = [name for name, _ in up.headers]
        assert b"transfer-encoding" not in names and b"connection" not in names
        client = _Transport()
        up.attach(client)
        rest = events[0][7:] + b"".join(events[1:]) + TERMINATOR
        # 3-byte reads split events and the terminator across callbacks.
        for i in range(0, len(rest), 3):
            up.data_received(rest[i : i + 3])
        assert up.complete and up.done.done()
        assert bytes(client.written) == b"".join(events)

    asyncio.run(run())


def test_response_that_ends_before_attach_completes_on_flush():
    async def run():
        up = _upstream()
        events = _events(2)
        up.data_received(HEAD + b"".join(events) + TERMINATOR)
        client = _Transport()
        up.attach(client)
        assert up.complete and up.out is None
        assert bytes(client.written) == b"".join(events)

    asyncio.run(run())


def test_whole_response_completes_on_content_length():
    async def run():
        up = _upstream()
        body = b'{"object": "list"}'
        head = b"HTTP/1.1 200 OK\r\ncontent-length: %d\r\n\r\n" % len(body)
        up.data_received(head + body[:5])
        assert up.head_ready.done() and not up.chunked and not up.complete
        up.data_received(body[5:])
        assert up.complete and bytes(up.buf) == body

    asyncio.run(run())


def test_backpressure_pauses_reading_until_the_client_drains():
    async def run():
        up = _upstream()
        up.data_received(HEAD)
        client = _Transport()
        up.attach(client)
        client.buffered = HIGH_WATER + 1
        up.data_received(_events(1)[0])
        assert up.paused and not up.transport.reading
        client.buffered = 0
        await asyncio.sleep(0.01)
        assert not up.paused and up.transport.reading

    asyncio.run(run())


def test_client_disconnect_closes_the_upstream():
    async def run():
        up = _upstream()
        up.data_received(HEAD)
        client = _Transport()
        up.attach(client)
        client.closing = True
        up.data_received(_events(1)[0])
        assert up.transport.closing and not client.written

    asyncio.run(run())


def test_connection_lost_before_the_head_fails_head_ready():
    async def run():
        up = _upstream()
        up.connection_lost(None)
        with pytest.raises(ConnectionError):
            await up.head_ready
        assert up.closed and up.done.done()

    asyncio.run(run())


class RequestResponseCycle:
    # Stands in for uvicorn's per-request cycle, the only owner _find_transport accepts.
    __module__ = "uvicorn.protocols.http.httptools_impl"

    def __init__(self):
        self.transport = _Transport()

    async def send(self, message):
        pass


def _wrap(inner):
    async def send(message):
        await inner(message)

    return send


def test_find_transport_through_send_wrappers():
    cycle = RequestResponseCycle()
    assert _find_transport(_wrap(_wrap(cycle.send))) is cycle.transport
    assert _find_transport(_wrap(lambda message: None)) is None


def test_find_transport_ignores_sockets_uvicorn_does_not_own():
    async def run():
        # The relay's own upstream socket must never be mistaken for the client's.
        up = _upstream()
        assert _find_transport(_wrap(up.pause)) is None

    asyncio.run(run())


def test_chunk_decoder_unframes_split_chunks():
    decoder = _ChunkDecoder()
    events = _events(3)
    stream = b"".join(events)
    payloads = []
    for i in range(0, len(stream), 4):
        payloads += decoder.feed(stream[i : i + 4])
    assert payloads == [b'data: {"i": %d}\n\n' % i for i in range(3)]


def test_fallback_relays_through_asgi_messages():
    async def run():
        relay = _relay_without_handle()
        up = _upstream()
        up.data_received(HEAD)
        messages = []

        async def send(message):
            messages.append(message)

        events = _events(3)
        loop = asyncio.get_running_loop()
        for data in events + [TERMINATOR]:
            loop.call_soon(up.data_received, data)
        await asyncio.wait_for(relay._relay_through_asgi(up, send), timeout=5)
        assert up.complete
        assert b"".join(m["body"] for m in messages) == b"".join(
            b'data: {"i": %d}\n\n' % i for i in range(3)
        )
        assert all(m["more_body"] for m in messages)

    asyncio.run(run())


def test_pick_prefers_the_endpoint_with_fewest_streams():
    async def run():
        relay = _relay_without_handle()
        relay._endpoints = [("10.0.0.1", 1), ("10.0.0.2", 2), ("10.0.0.3", 3)]
        relay._in_flight = {("10.0.0.1", 1): 4, ("10.0.0.2", 2): 1, ("10.0.0.3", 3): 4}
        for _ in range(3):
            assert await relay._pick() == ("10.0.0.2", 2)

    asyncio.run(run())


def test_refresh_keeps_a_replica_until_repeated_misses():
    relay = _relay_without_handle()
    a, b = ("10.0.0.1", 1), ("10.0.0.2", 2)
    relay._merge_endpoints({a, b})
    relay._merge_endpoints({a})
    relay._merge_endpoints({a})
    assert relay._endpoints == [a, b]
    relay._merge_endpoints({a})
    assert relay._endpoints == [a]
    relay._merge_endpoints({a, b})
    assert relay._endpoints == [a, b]


def test_open_routes_around_an_unreachable_replica():
    async def run():
        relay = _relay_without_handle()
        gone, alive = ("10.0.0.1", 1), ("10.0.0.2", 2)
        relay._endpoints = [gone, alive]
        relay._in_flight = {gone: 0, alive: 5}

        async def connect(endpoint):
            if endpoint == gone:
                raise OSError("connection refused")
            up = _upstream()
            up.head_ready.set_result(None)
            return up, False

        relay._connect = connect
        endpoint, up = await relay._open(b"GET / HTTP/1.1\r\n\r\n")
        assert endpoint == alive and relay._endpoints == [alive]
        assert relay._in_flight == {gone: 0, alive: 6}
        assert bytes(up.transport.written) == b"GET / HTTP/1.1\r\n\r\n"

    asyncio.run(run())


def test_release_pools_only_completed_connections():
    async def run():
        relay = _relay_without_handle()
        endpoint = ("10.0.0.1", 1)
        done, broken = _upstream(), _upstream()
        done.data_received(HEAD)
        done.attach(_Transport())
        done.data_received(TERMINATOR)
        relay._release(endpoint, done)
        relay._release(endpoint, broken)
        assert relay._idle[endpoint] == [done] and done.out is None
        assert broken.transport.closing
        assert await relay._connect(endpoint) == (done, True)

    asyncio.run(run())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
