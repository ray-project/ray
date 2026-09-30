"""Byte relay: an OpenAI ingress that stays in the response path without per-token Python objects.

Tokens never cross a DeploymentHandle. Each request picks an LLMServer replica, and the replica's
chunk-framed HTTP stream is written straight to the client socket, one write per socket read.
"""

import asyncio
import json
from typing import Dict, List, Optional, Tuple

from starlette.applications import Starlette
from starlette.routing import Mount

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.builder import (
    _build_direct_streaming_llm_deployment,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve._private.constants import RAY_SERVE_ENABLE_DIRECT_INGRESS
from ray.serve.config import RequestRouterConfig
from ray.serve.deployment import Application

logger = get_logger(__name__)

# The replica's last chunk; it is held back so uvicorn ends the client response itself.
TERMINATOR = b"0\r\n\r\n"
HOP_BY_HOP = {
    b"connection",
    b"keep-alive",
    b"transfer-encoding",
    b"content-length",
    b"host",
    b"te",
    b"upgrade",
}
# Past HIGH_WATER bytes buffered for a slow client, the relay stops reading from the replica.
HIGH_WATER, LOW_WATER = 256 * 1024, 64 * 1024
# choose_replica returns one replica per call, so sample enough calls to see them all.
ENDPOINT_SAMPLES, ENDPOINT_REFRESH_S = 48, 5.0
# Refreshes in a row that must miss a replica before it leaves the rotation.
ENDPOINT_MAX_MISSES = 3
# Replicas to try per request before answering 502.
CONNECT_ATTEMPTS = 3


def _find_transport(send):
    """uvicorn's client socket behind Starlette's and Serve's send wrappers, or None.

    None means the request came through the Serve proxy rather than direct ingress.
    """
    stack, seen = [send], set()
    while stack:
        fn = stack.pop()
        if id(fn) in seen:
            continue
        seen.add(id(fn))
        owner = getattr(fn, "__self__", None)
        # Only uvicorn's per-request cycle holds the client socket; nothing else qualifies.
        if type(owner).__name__ == "RequestResponseCycle" and type(
            owner
        ).__module__.startswith("uvicorn."):
            return owner.transport
        for cell in getattr(fn, "__closure__", None) or ():
            try:
                content = cell.cell_contents
            except ValueError:
                continue
            if callable(content):
                stack.append(content)
    return None


class _ChunkDecoder:
    """Unframes chunked bytes for the fallback path, where ASGI frames the body again."""

    def __init__(self):
        self.buf = bytearray()
        self.need: Optional[int] = None

    def feed(self, data: bytes) -> List[bytes]:
        self.buf += data
        payloads = []
        while True:
            if self.need is None:
                end = self.buf.find(b"\r\n")
                if end < 0:
                    break
                size = int(bytes(self.buf[:end]).split(b";")[0], 16)
                del self.buf[: end + 2]
                self.need = size + 2
            if len(self.buf) < self.need:
                break
            if self.need > 2:
                payloads.append(bytes(self.buf[: self.need - 2]))
            del self.buf[: self.need]
            self.need = None
        return payloads


class _QueueOut:
    """Stands in for the client socket when there is none; the relay drains it through ASGI."""

    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.size = 0

    def write(self, data: bytes):
        self.size += len(data)
        self.queue.put_nowait(data)

    def is_closing(self) -> bool:
        return False

    def get_write_buffer_size(self) -> int:
        return self.size


class _Upstream(asyncio.Protocol):
    """One kept-alive replica connection; each response's body bytes pass through unparsed."""

    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.transport = None
        self.closed = False
        self.begin()

    def begin(self):
        # A pooled connection carries many responses, so per-response state starts here.
        self.buf = bytearray()
        self.head_ready = self.loop.create_future()
        self.done = self.loop.create_future()
        self.status = 502
        self.headers: List[Tuple[bytes, bytes]] = []
        self.chunked = False
        self.length: Optional[int] = None
        self.out = None
        self.tail = b""
        self.paused = False
        self.complete = False

    def connection_made(self, transport):
        self.transport = transport

    def attach(self, out):
        """Flush what arrived before the client socket was known, then write reads straight to it."""
        pending = bytes(self.buf)
        self.buf = None
        self.tail = pending[-5:]
        if len(pending) > 5:
            out.write(pending[:-5])
        if self.tail == TERMINATOR:
            self.finish()
        else:
            # No await since the flush above, so no read can slip in between.
            self.out = out

    def finish(self):
        self.complete = True
        if not self.done.done():
            self.done.set_result(None)

    def data_received(self, data: bytes):
        out = self.out
        if out is not None:
            # Hot path: one callback per socket read, however many tokens it holds.
            if out.is_closing():
                # Closing the upstream is what tells the replica to stop generating.
                self.transport.close()
                return
            data = self.tail + data
            self.tail = data[-5:]
            if len(data) > 5:
                out.write(data[:-5])
            if self.tail == TERMINATOR:
                self.finish()
            elif out.get_write_buffer_size() > HIGH_WATER:
                self.pause()
            return
        self.buf += data
        if not self.head_ready.done():
            end = self.buf.find(b"\r\n\r\n")
            if end < 0:
                return
            head = bytes(self.buf[:end]).split(b"\r\n")
            del self.buf[: end + 4]
            self.status = int(head[0].split(b" ", 2)[1])
            for line in head[1:]:
                name, _, value = line.partition(b":")
                name, value = name.strip().lower(), value.strip()
                if name == b"transfer-encoding" and value.lower() == b"chunked":
                    self.chunked = True
                elif name == b"content-length":
                    self.length = int(value)
                elif name not in HOP_BY_HOP:
                    self.headers.append((name, value))
            self.head_ready.set_result(None)
        if self.length is not None and len(self.buf) >= self.length:
            self.finish()

    def pause(self):
        if not self.paused:
            self.paused = True
            self.transport.pause_reading()
            self.loop.call_later(0.001, self.resume_when_drained)

    def resume_when_drained(self):
        if self.out is None or self.closed:
            return
        if self.out.is_closing():
            self.transport.close()
        elif self.out.get_write_buffer_size() > LOW_WATER:
            self.loop.call_later(0.001, self.resume_when_drained)
        else:
            self.paused = False
            self.transport.resume_reading()

    def connection_lost(self, exc):
        self.closed = True
        if not self.head_ready.done():
            self.head_ready.set_exception(
                exc or ConnectionError("replica closed before the response head")
            )
        if not self.done.done():
            self.done.set_result(None)


async def _send_error(send, status: int, message: str):
    body = json.dumps({"error": {"message": message, "code": status}}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": body})


class ByteRelay:
    """Relays each request to an LLMServer replica's own HTTP port and streams back its bytes."""

    def __init__(
        self,
        backend_app_name: str,
        backend_deployment_name: str,
        backend_route_prefix: str,
    ):
        self._handle = serve.get_deployment_handle(
            backend_deployment_name, app_name=backend_app_name
        )
        self._prefix = backend_route_prefix.rstrip("/").encode()
        self._host = backend_app_name.encode()
        self._endpoints: List[Tuple[str, int]] = []
        self._misses: Dict[Tuple[str, int], int] = {}
        self._in_flight: Dict[Tuple[str, int], int] = {}
        self._rr = 0
        self._idle: Dict[Tuple[str, int], List[_Upstream]] = {}
        self._started = False

    async def __serve_build_asgi_app__(self):
        return Starlette(routes=[Mount("/", app=self._relay)])

    def _start_background(self):
        # The replica's event loop is only reachable from a request, so background work starts there.
        self._started = True
        asyncio.get_running_loop().create_task(self._refresh_endpoints())

    async def _refresh_endpoints(self):
        while True:
            seen = set()
            try:
                for _ in range(ENDPOINT_SAMPLES):
                    async with self._handle.choose_replica(_reserve=False) as selection:
                        seen.add(tuple(selection._replica.backend_http_endpoint))
            except Exception:
                logger.exception(
                    "Refreshing LLMServer endpoints failed; keeping the last list."
                )
            if seen:
                self._merge_endpoints(seen)
            await asyncio.sleep(ENDPOINT_REFRESH_S)

    def _merge_endpoints(self, seen):
        # One sweep can miss a live replica, since power-of-two routing steers away from busy ones.
        for endpoint in seen:
            self._in_flight.setdefault(endpoint, 0)
            self._misses[endpoint] = 0
        known = set(self._endpoints) | set(seen)
        for endpoint in known - set(seen):
            self._misses[endpoint] = self._misses.get(endpoint, 0) + 1
        self._endpoints = sorted(
            e for e in known if self._misses[e] < ENDPOINT_MAX_MISSES
        )

    def _forget(self, endpoint: Tuple[str, int]):
        self._endpoints = [e for e in self._endpoints if e != endpoint]
        for up in self._idle.pop(endpoint, []):
            up.transport.close()

    async def _pick(self) -> Tuple[str, int]:
        endpoints = self._endpoints
        if endpoints:
            # The relay sees every stream it opened, so its counts are exact; rotate to split ties.
            n = len(endpoints)
            start = self._rr = (self._rr + 1) % n
            best = endpoints[start]
            for i in range(1, n):
                candidate = endpoints[(start + i) % n]
                if self._in_flight[candidate] < self._in_flight[best]:
                    best = candidate
            return best
        async with self._handle.choose_replica(_reserve=False) as selection:
            return tuple(selection._replica.backend_http_endpoint)

    async def _connect(self, endpoint: Tuple[str, int]) -> Tuple[_Upstream, bool]:
        idle = self._idle.get(endpoint)
        while idle:
            up = idle.pop()
            if not up.closed and not up.transport.is_closing():
                return up, True
        loop = asyncio.get_running_loop()
        _, up = await loop.create_connection(_Upstream, *endpoint)
        return up, False

    async def _open(self, request: bytes) -> Tuple[Tuple[str, int], _Upstream]:
        """Send the request and wait for the response head, routing around replicas that are gone."""
        error: Optional[Exception] = None
        for _ in range(CONNECT_ATTEMPTS):
            endpoint = await self._pick()
            # Count the stream before awaiting, so concurrent picks see it.
            self._in_flight[endpoint] = self._in_flight.get(endpoint, 0) + 1
            try:
                up, reused = await self._connect(endpoint)
            except OSError as e:
                self._in_flight[endpoint] -= 1
                self._forget(endpoint)
                error = e
                continue
            up.transport.write(request)
            try:
                await up.head_ready
                return endpoint, up
            except ConnectionError as e:
                # A pooled connection the replica closed while idle, or a replica that just died.
                self._in_flight[endpoint] -= 1
                up.transport.close()
                if not reused:
                    self._forget(endpoint)
                error = e
        raise ConnectionError(f"No LLMServer replica reachable: {error}")

    def _release(self, endpoint: Tuple[str, int], up: _Upstream):
        if up.complete and not up.closed:
            if up.paused:
                up.transport.resume_reading()
            up.begin()
            self._idle.setdefault(endpoint, []).append(up)
        else:
            up.transport.close()

    async def _relay_through_asgi(self, up: _Upstream, send):
        # Slower, but correct when the request came through the Serve proxy: no socket to take over.
        out, decoder = _QueueOut(), _ChunkDecoder()
        up.done.add_done_callback(lambda _: out.queue.put_nowait(None))
        up.attach(out)
        while (data := await out.queue.get()) is not None:
            out.size -= len(data)
            for payload in decoder.feed(data):
                await send(
                    {"type": "http.response.body", "body": payload, "more_body": True}
                )

    async def _relay(self, scope, receive, send):
        if scope["type"] != "http":
            return
        if not self._started:
            self._start_background()
        body = bytearray()
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        path = self._prefix + (scope.get("raw_path") or scope["path"].encode())
        if scope.get("query_string"):
            path += b"?" + scope["query_string"]
        lines = [
            b"%s %s HTTP/1.1" % (scope["method"].encode(), path),
            # uvicorn and Serve do not route by Host, and the replica is chosen later.
            b"host: " + self._host,
            b"content-length: %d" % len(body),
        ]
        lines += [
            name + b": " + value
            for name, value in scope["headers"]
            if name.lower() not in HOP_BY_HOP
        ]
        request = b"\r\n".join(lines) + b"\r\n\r\n" + bytes(body)
        try:
            endpoint, up = await self._open(request)
        except ConnectionError as e:
            await _send_error(send, 502, str(e))
            return
        try:
            if not up.chunked:
                # Whole responses (/v1/models, errors) are small; send them through ASGI.
                await up.done
                content = bytes(up.buf if up.length is None else up.buf[: up.length])
                await send(
                    {
                        "type": "http.response.start",
                        "status": up.status,
                        "headers": up.headers,
                    }
                )
                await send({"type": "http.response.body", "body": content})
                return
            await send(
                {
                    "type": "http.response.start",
                    "status": up.status,
                    "headers": up.headers + [(b"transfer-encoding", b"chunked")],
                }
            )
            out = _find_transport(send)
            if out is not None:
                up.attach(out)
                await up.done
            else:
                await self._relay_through_asgi(up, send)
            if up.tail != TERMINATOR:
                logger.warning("LLMServer response ended without its final chunk.")
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        finally:
            self._in_flight[endpoint] -= 1
            self._release(endpoint, up)


def build_byte_relay_apps(
    llm_config: LLMConfig,
    *,
    backend_app_name: str = "llm-backend",
    backend_route_prefix: str = "/llm-backend",
    relay_replicas: int = 1,
) -> Tuple[Application, Application]:
    """The LLMServer backend app and the relay app in front of it; run both with serve.run.

    The backend serves its own HTTP port without LLMRouter, so HAProxy sends every request to the relay.
    """
    if not RAY_SERVE_ENABLE_DIRECT_INGRESS:
        raise ValueError(
            "The byte relay reads from LLMServer's own HTTP port, which needs direct "
            "ingress (for example RAY_SERVE_THROUGHPUT_OPTIMIZED=1)."
        )
    # Without LLMRouter the backend is a plain ingress, which HAProxy mode allows only
    # with the default request router.
    backend = _build_direct_streaming_llm_deployment(
        llm_config,
        override_serve_options={"request_router_config": RequestRouterConfig()},
    )
    # One relay replica saturates one core; add replicas to scale the relay tier.
    relay = serve.deployment(
        num_replicas=relay_replicas,
        max_ongoing_requests=8192,
        ray_actor_options={"num_cpus": 1},
    )(serve.ingress()(ByteRelay)).bind(
        backend_app_name,
        backend._bound_deployment.name,
        backend_route_prefix,
    )
    return backend, relay
