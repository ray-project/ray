"""A direct call path from API replicas to sandbox node hosts.

Every call a replica makes on a hosted sandbox (``describe``, ``start_exec``,
``get_exec_by_key``...) is otherwise a Ray actor task on the sandbox's
``SandboxNodeHost``. An asyncio actor starts its tasks one by one on a
single thread that takes the GIL for each, and that dispatch is what limits
a busy host: under a burst of ~650 calls/s per node host, calls waited
~100 ms between arriving and starting while the host's event loop itself
stayed idle (measured, Ray 2.58).

A ``HostChannel`` carries those calls over one TCP connection per replica
and host instead, served on the host's own event loop. The server listens on
the node's IP, which other processes on the network can reach, sandboxes
with network access among them. A connection's first frame must be the
host's token, raw bytes compared before anything else is read; callers get
the token through a Ray call (``channel_endpoint``), so the channel admits
exactly who can already call the host through Ray. Calls and replies after
that are msgpack (plain data only), never pickle, so even a caller with the
token can only call the listed methods. A call whose connection breaks
fails with ``HostChannelError`` and the caller repeats it as a Ray actor
task; the methods served here are idempotent (reads, or keyed by the
caller's ids).
"""

import asyncio
import builtins
import hmac
import logging
import secrets
import struct
from typing import Any, Dict, FrozenSet, Optional, Set

import msgpack

logger = logging.getLogger(__name__)

_HEADER = struct.Struct("!I")
# Largest frame either side accepts; file writes travel as call arguments.
_MAX_FRAME_BYTES = 1 << 30
# Largest first frame (the token) a connection may send.
_MAX_HELLO_BYTES = 256
# How long a new connection may take to present its token.
_HELLO_SECONDS = 10.0
# Above this much unsent data, a call's reply waits for the connection to
# drain first, so a burst of large calls can't buffer without bound.
_DRAIN_ABOVE_BYTES = 4 << 20


class HostChannelError(ConnectionError):
    """The connection to a node host's channel failed; retry through Ray."""


def _frame(data: bytes) -> bytes:
    return _HEADER.pack(len(data)) + data


def _pack(obj: Any) -> bytes:
    return _frame(msgpack.packb(obj, use_bin_type=True))


async def _read_bytes(reader: asyncio.StreamReader, limit: int) -> bytes:
    (size,) = _HEADER.unpack(await reader.readexactly(_HEADER.size))
    if size > limit:
        raise HostChannelError(f"frame of {size} bytes exceeds the limit")
    return await reader.readexactly(size)


async def _read_frame(reader: asyncio.StreamReader) -> Any:
    data = await _read_bytes(reader, _MAX_FRAME_BYTES)
    return msgpack.unpackb(data, raw=False, strict_map_key=False)


def _error(exc: BaseException) -> Dict[str, str]:
    return {"type": type(exc).__name__, "message": str(exc)}


def _exception(error: Dict[str, str]) -> Exception:
    """The exception a host raised: a builtin type as itself, else RuntimeError."""
    kind = getattr(builtins, error.get("type", ""), None)
    message = error.get("message", "")
    if isinstance(kind, type) and issubclass(kind, Exception):
        return kind(message)
    return RuntimeError(f"{error.get('type')}: {message}")


class HostChannelServer:
    """Serves ``methods`` of one object over token-checked connections.

    Args:
        target: The object whose (async or plain) methods are called.
        methods: Names of the methods connections may call.
    """

    def __init__(self, target: Any, methods: FrozenSet[str]) -> None:
        self._target = target
        self._methods = methods
        self._token = secrets.token_hex(32)
        self._server: Optional[asyncio.AbstractServer] = None
        self._start_lock = asyncio.Lock()
        self._address = ""
        self._writers: Set[asyncio.StreamWriter] = set()
        # The loop keeps only weak references to tasks.
        self._calls: Set[asyncio.Task] = set()

    async def start(self, address: str) -> Dict[str, Any]:
        """Listen on ``address`` (once) and return how to connect."""
        async with self._start_lock:  # concurrent first calls share one server
            if self._server is None:
                self._server = await asyncio.start_server(
                    self._serve, host=address, port=0
                )
                self._address = address
        port = self._server.sockets[0].getsockname()[1]
        return {"address": self._address, "port": port, "token": self._token}

    def close(self) -> None:
        """Stop listening and drop every connection (callers fall back to Ray)."""
        if self._server is not None:
            self._server.close()
        for writer in list(self._writers):
            writer.close()

    async def _serve(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writers.add(writer)
        try:
            # Raw bytes, compared before this peer's input is decoded at all.
            token = await asyncio.wait_for(
                _read_bytes(reader, _MAX_HELLO_BYTES), _HELLO_SECONDS
            )
            if not hmac.compare_digest(token, self._token.encode()):
                return
            while True:
                call_id, method, args, kwargs = await _read_frame(reader)
                task = asyncio.ensure_future(
                    self._call(writer, call_id, method, args, kwargs)
                )
                self._calls.add(task)
                task.add_done_callback(self._calls.discard)
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError):
            pass
        except Exception as exc:  # noqa: BLE001 - a bad peer must not kill the host
            logger.warning("Closing a host channel connection: %s", exc)
        finally:
            self._writers.discard(writer)
            writer.close()

    async def _call(
        self,
        writer: asyncio.StreamWriter,
        call_id: int,
        method: str,
        args: list,
        kwargs: dict,
    ) -> None:
        try:
            if method not in self._methods:
                raise AttributeError(f"{method!r} is not served over the host channel")
            result = getattr(self._target, method)(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            reply = [call_id, True, result]
        except Exception as exc:  # noqa: BLE001 - sent to the caller
            reply = [call_id, False, _error(exc)]
        try:
            data = _pack(reply)
        except Exception as exc:  # noqa: BLE001 - a result msgpack can't carry
            # The caller repeats the call through Ray, which can.
            data = _pack([call_id, False, _error(HostChannelError(repr(exc)))])
        if writer.is_closing():
            return
        writer.write(data)
        try:
            await writer.drain()
        except ConnectionError:
            pass


class HostChannel:
    """One connection to a node host's ``HostChannelServer``.

    Bound to the event loop it was created on; ``call`` must run there.
    """

    def __init__(self, endpoint: Dict[str, Any]) -> None:
        self._endpoint = endpoint
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._pending: Dict[int, asyncio.Future] = {}
        self._next_id = 0
        self._drain_lock = asyncio.Lock()
        self.closed = False

    async def connect(self) -> None:
        reader, writer = await asyncio.open_connection(
            self._endpoint["address"], self._endpoint["port"]
        )
        writer.write(_frame(self._endpoint["token"].encode()))
        self._writer = writer
        self._reader_task = asyncio.ensure_future(self._read_replies(reader))

    @property
    def ready(self) -> bool:
        return self._writer is not None and not self.closed

    def call(self, method: str, args: tuple, kwargs: dict) -> "asyncio.Future[Any]":
        """Send a call now; the future resolves to its result or exception.

        Arguments must be plain data (msgpack); others raise
        ``HostChannelError``, so the caller uses Ray instead.
        """
        if not self.ready:
            raise HostChannelError("the host channel is not connected")
        self._next_id += 1
        call_id = self._next_id
        try:
            data = _pack([call_id, method, list(args), kwargs])
        except Exception as exc:
            raise HostChannelError(f"arguments msgpack can't carry: {exc}") from exc
        future = asyncio.get_running_loop().create_future()
        self._pending[call_id] = future
        try:
            self._writer.write(data)
        except Exception as exc:
            self._pending.pop(call_id, None)
            self._fail(HostChannelError(f"host channel write failed: {exc}"))
            raise HostChannelError(str(exc)) from exc
        if self._writer.transport.get_write_buffer_size() > _DRAIN_ABOVE_BYTES:
            return asyncio.ensure_future(self._drained(future))
        return future

    async def _drained(self, future: "asyncio.Future[Any]") -> Any:
        try:
            async with self._drain_lock:
                await self._writer.drain()
        except ConnectionError as exc:
            self._fail(HostChannelError(f"host channel lost: {exc!r}"))
        return await future

    async def _read_replies(self, reader: asyncio.StreamReader) -> None:
        error: BaseException = HostChannelError("the host closed the channel")
        try:
            while True:
                call_id, ok, value = await _read_frame(reader)
                future = self._pending.pop(call_id, None)
                if future is None or future.done():
                    continue
                if ok:
                    future.set_result(value)
                elif value.get("type") == HostChannelError.__name__:
                    future.set_exception(HostChannelError(value.get("message", "")))
                else:
                    future.set_exception(_exception(value))
        except (asyncio.IncompleteReadError, ConnectionError) as exc:
            error = HostChannelError(f"host channel lost: {exc!r}")
        except asyncio.CancelledError:
            error = HostChannelError("host channel closed")
            raise
        except Exception as exc:  # noqa: BLE001
            error = HostChannelError(f"host channel failed: {exc!r}")
        finally:
            self._fail(error)

    def _fail(self, error: BaseException) -> None:
        self.closed = True
        pending, self._pending = self._pending, {}
        for future in pending.values():
            if not future.done():
                future.set_exception(error)
        if self._writer is not None:
            self._writer.close()

    def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
        self._fail(HostChannelError("host channel closed"))
