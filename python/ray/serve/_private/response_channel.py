"""Leaf-side ResponseChannel: stream a response straight back to HAProxy.

A deployment whose app opted into the ResponseChannel (see
``Application._with_response_channel``) streams its response chunks to HAProxy's
``/internal/response/{response_id}`` over a single streamed POST, keyed by the
per-request id HAProxy minted and forwarded as the ``x-response-id`` header.
HAProxy drains the per-id queue to the waiting client, so parent deployments stay
on the request path only; the response bytes never traverse them.

General to any Serve deployment: ``response_channel(request)`` returns a channel
when the incoming request carries the header, else ``None``.
"""

import asyncio
import json
import re
from typing import Optional

import httpx

from ray.serve._private.constants import (
    RAY_SERVE_RESPONSE_CHANNEL_INTERNAL_PORT_OFFSET,
)

RESPONSE_ID_HEADER = "x-response-id"

# The response id HAProxy mints is "t<thread>-<sec>-<usec>-<seq>". The leaf posts
# back to the internal ingest port pinned to that thread so the push and the
# client's stream stay on one HAProxy thread (per-thread queue, no shared lock).
_THREAD_RE = re.compile(r"^t(\d+)-")


def internal_port(frontend_port: int, thread: int) -> int:
    """The internal ingest port pinned to ``thread`` (1-based).

    HAProxy binds one port per thread at a fixed offset above the frontend port;
    the leaf posts to the port for its request's thread so push and stream share
    a per-thread queue. HAProxy config generation and the leaf derive the port
    from this one formula, so they cannot drift.
    """
    return frontend_port + RAY_SERVE_RESPONSE_CHANNEL_INTERNAL_PORT_OFFSET + thread - 1


def _internal_url(response_id: str, haproxy_base: str) -> str:
    """Rewrite ``haproxy_base`` to the per-thread internal ingest port for this id.

    Falls back to ``haproxy_base`` if the id carries no thread (a caller that
    minted its own id rather than HAProxy's thread-tagged one).
    """
    m = _THREAD_RE.match(response_id)
    if not m:
        return f"{haproxy_base}/internal/response/{response_id}"
    scheme_host, _, port = haproxy_base.rpartition(":")
    dst = internal_port(int(port), int(m.group(1)))
    return f"{scheme_host}:{dst}/internal/response/{response_id}"


class ResponseChannel:
    """A pipe back to HAProxy for one request.

    ``write`` enqueues a chunk; a single background sender streams the queued
    chunks as the body of one POST, so generation is never blocked on a per-chunk
    round-trip and the bounded queue applies backpressure. Each chunk is one
    newline-delimited JSON line; HAProxy wraps it as a Server-Sent Event.
    """

    def __init__(self, response_id: str, haproxy_base: str, client: httpx.AsyncClient):
        self._url = _internal_url(response_id, haproxy_base)
        self._client = client
        self._q: asyncio.Queue = asyncio.Queue(maxsize=2048)
        self._task = asyncio.create_task(self._run())

    async def _body(self):
        while True:
            item = await self._q.get()
            if item is None:
                return
            yield (item + "\n").encode()

    async def _run(self) -> None:
        try:
            await self._client.post(self._url, content=self._body())
        except Exception:  # noqa: BLE001
            # A drain-side failure (client gone, HAProxy reload) must not crash
            # the producer; the caller aborts its own work on close().
            pass

    async def write(self, chunk) -> None:
        """Enqueue one chunk. Accepts a pydantic model, a dict, or a raw JSON
        string (any SSE ``data:`` framing is stripped; HAProxy re-adds it)."""
        await self._q.put(_to_json_line(chunk))

    async def close(self) -> None:
        await self._q.put(None)
        await self._task


def _to_json_line(chunk) -> str:
    """Normalize a chunk to a single JSON line with no SSE framing."""
    if hasattr(chunk, "model_dump_json"):
        return chunk.model_dump_json()
    if isinstance(chunk, str):
        s = chunk.strip()
        if s.startswith("data:"):
            s = s[len("data:") :].strip()
        return s
    return json.dumps(chunk)


def haproxy_base_for_leaf(port: Optional[int] = None) -> str:
    """The HAProxy frontend the leaf posts its response channel to.

    HAProxy fronts the Serve HTTP port on the local node, so a co-located leaf
    reaches it on loopback. Multi-node delivery (leaf and client-holding HAProxy
    on different nodes) is not yet handled.
    """
    from ray.serve._private.constants import DEFAULT_HTTP_PORT

    return f"http://127.0.0.1:{port or DEFAULT_HTTP_PORT}"


def response_channel(
    request, client: httpx.AsyncClient, port: Optional[int] = None
) -> Optional[ResponseChannel]:
    """Return a ResponseChannel for ``request`` if it carries the channel header.

    A deployment reached over HAProxy for an opted-in app receives the
    ``x-response-id`` header; it writes its response to the returned channel
    instead of returning it up the DAG. Returns ``None`` for requests without the
    header (the app did not opt in), so the caller falls back to a normal
    response.
    """
    response_id = request.headers.get(RESPONSE_ID_HEADER)
    if not response_id:
        return None
    return ResponseChannel(response_id, haproxy_base_for_leaf(port), client)
