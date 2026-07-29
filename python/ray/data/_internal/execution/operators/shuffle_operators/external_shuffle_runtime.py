"""External-shuffle file-transport runtime: TCP wire protocol, per-node
ShuffleManager actor, prefetch layout, error hierarchy. Imported by the
map/reduce task bodies in ``external_shuffle_tasks``."""

import errno
import ipaddress
import logging
import os
import socket
import socketserver
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    TypedDict,
    Union,
)

import pyarrow as pa

import ray
from ray.exceptions import (
    ActorDiedError,
    ActorUnavailableError,
    ActorUnschedulableError,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Wire protocol.
#
# Each TCP connection has a one-time handshake, then any number of FETCH
# requests (or CLOSE), then teardown. Keep-alive is the default: clients amortize
# TCP handshake cost across many FETCHes, and a single FETCH can request ranges
# from MULTIPLE source files at once (so a reducer needing data from N sources
# colocated on one ShuffleManager pays one network round-trip total, not N).
#
# All multi-byte integers are big-endian. Lengths use the smallest type that
# fits the field's natural bound (u8 / u16 / u32 / u64).
#
#   ──────────────── Handshake (once per TCP connection) ────────────────
#     client → server:
#       u16     token_len
#       bytes   token       (UTF-8, token_len bytes)
#     server → client:
#       u8      status      (_STATUS_OK or error)
#
#   ──────────────── Request frames (any number, after handshake) ────────
#     client → server:
#       u8      opcode      (_OPCODE_FETCH | _OPCODE_CLOSE)
#       if FETCH:
#         u16   num_sources
#         repeat num_sources times:
#           u16    path_len
#           bytes  path (UTF-8)            # must resolve under server base_dir
#           u32    num_ranges
#           repeat num_ranges times:
#             u64  offset
#             u64  length
#         if CLOSE: (no body)
#
#   ──────────────── Response frame (one per FETCH) ──────────────────────
#     server → client:
#       u8      status      (_STATUS_OK or error)
#       if OK:
#         repeat num_sources times (in request order):
#           u32    num_ranges          # == request num_ranges
#           repeat num_ranges times (in request order):
#             u32  data_len            # == requested length unless EOF clamp
#             bytes data
#       else:
#         u32   msg_len
#         bytes msg (UTF-8)
# =============================================================================

# Opcodes
_OPCODE_FETCH = 0x01
_OPCODE_CLOSE = 0x00

# Status codes
_STATUS_OK = 0x00
_STATUS_AUTH_FAIL = 0x01
_STATUS_PATH_DENIED = 0x02  # path resolves outside server's base_dir
_STATUS_NOT_FOUND = 0x03  # path doesn't exist on disk
_STATUS_READ_ERR = 0x04  # IO error reading file content

# The response frame encodes each range's payload length as u32,
# so no single range/IPC frame may exceed 4 GiB - 1.
# Checked at mapper write time so an oversized IPC buffer fails
# at the mapper task
_MAX_RANGE_BYTES: int = (1 << 32) - 1


# ----------------------------------------------------------------- Arrow IPC
# Shard wire format: [u64 uncompressed_size][zstd(whole IPC stream)] -- ONE zstd
# frame per shard, vs Arrow's per-buffer IPC compression (~40 zstd blobs/shard).
# One decompress + one alloc per shard at the reducer; inner IPC is uncompressed
# so buffers read zero-copy.
_WF_HEADER = struct.Struct("<Q")


def _codec_for(compression: Optional[str]) -> Optional["pa.Codec"]:
    """Codec name -> pa.Codec; None/"none" -> None (pyarrow has no "none" codec)."""
    if not compression or compression == "none":
        return None
    return pa.Codec(compression)


def _encode_shard(table: pa.Table, compression: Optional[str] = "zstd") -> pa.Buffer:
    """Encode a partition shard as a whole-frame blob (one codec frame per shard,
    vs Arrow's per-buffer IPC compression). ``compression`` comes from
    ``data_context.hash_shuffle_compression``."""
    if table.num_columns > 0:
        table = table.combine_chunks()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as w:  # uncompressed inner IPC
        for batch in table.to_batches():
            w.write_batch(batch)
    raw = sink.getvalue()
    codec = _codec_for(compression)
    out = pa.BufferOutputStream()
    out.write(_WF_HEADER.pack(raw.size))
    out.write(raw if codec is None else codec.compress(raw))
    return out.getvalue()


def _read_ipc(
    buf: Union[bytes, "pa.Buffer", memoryview], compression: Optional[str] = "zstd"
) -> pa.Table:
    """Decode a whole-frame shard: read the u64 size header, one decompress of
    the rest (per ``compression``, same value the map encoded with), then read
    the (uncompressed) inner IPC stream."""
    src = buf if isinstance(buf, pa.Buffer) else pa.py_buffer(buf)
    n = _WF_HEADER.unpack_from(memoryview(src))[0]
    body = src.slice(_WF_HEADER.size)
    codec = _codec_for(compression)
    raw = body if codec is None else codec.decompress(body, decompressed_size=n)
    with pa.ipc.open_stream(raw) as r:
        return r.read_all()


# wire framing
def _tune_shuffle_socket(sock: socket.socket) -> None:
    """Configure a shuffle TCP socket for our usage pattern.

    * ``TCP_NODELAY`` disables Nagle. Our wire protocol sends many small
      frames in sequence (token, opcode + header, per-range headers),
      Nagle's default coalesce would inject ~40ms latency between them.
    * ``SO_KEEPALIVE`` lets the kernel detect dead peers via TCP-level
      probes on long-idle connections.

    Applied to both the reducer's client socket and each connection the
    ShuffleManager server accepts.  Silently ignores failures: not every
    socket family supports both options (Unix domain etc.); for our
    AF_INET / AF_INET6 use they always succeed.
    """
    for level, opt in (
        (socket.IPPROTO_TCP, socket.TCP_NODELAY),
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE),
    ):
        try:
            sock.setsockopt(level, opt, 1)
        except OSError:
            pass

    # Tune keepalive to detect a silently dead peer (VM strand / raylet
    # crash without RST) in ~5 minutes: 240s idle before first probe, then
    # 3 probes 20s apart. Default Linux is ~2 hours, which lets a reducer
    # hang for that long on a lost node. 5min is a magic number — bounded
    # detection window without over-eagerly killing slow/paused links.
    # Linux-only ``TCP_KEEP*``; other platforms silently skip.
    for opt, val in (
        (getattr(socket, "TCP_KEEPIDLE", None), 240),
        (getattr(socket, "TCP_KEEPINTVL", None), 20),
        (getattr(socket, "TCP_KEEPCNT", None), 3),
    ):
        if opt is None:
            continue
        try:
            sock.setsockopt(socket.IPPROTO_TCP, opt, val)
        except OSError:
            pass


def _recvall(sock: socket.socket, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise ConnectionError("peer closed mid-frame")
        out.extend(chunk)
    return bytes(out)


def _recv_u32(sock) -> int:
    return struct.unpack(">I", _recvall(sock, 4))[0]


def _recv_u8(sock) -> int:
    return _recvall(sock, 1)[0]


def _recv_u16(sock) -> int:
    return struct.unpack(">H", _recvall(sock, 2))[0]


def _recv_u64(sock) -> int:
    return struct.unpack(">Q", _recvall(sock, 8))[0]


# Linux-only; macOS lacks POSIX_FADV_DONTNEED. Probed once at import time so
# the hot path is a constant-time attribute check, not a try/except per range.
_HAS_FADV_DONTNEED = hasattr(os, "posix_fadvise") and hasattr(os, "POSIX_FADV_DONTNEED")


def _drop_pagecache(fd: int, offset: int, length: int) -> None:
    """Hint the kernel to drop ``[offset, offset+length)`` of ``fd`` from the
    page cache. Effective on clean pages; on dirty pages the hint is recorded
    and eviction happens on next writeback."""
    if not _HAS_FADV_DONTNEED or length <= 0:
        return
    try:
        os.posix_fadvise(fd, offset, length, os.POSIX_FADV_DONTNEED)
    except OSError:
        # Best-effort: any failure is silently ignored, cause the worst case is the
        # kernel keeps the pages longer.
        pass


# Fetch helper class used by file server actor
class _FetchHandler(socketserver.BaseRequestHandler):
    """Lifecycle: one handshake → loop of FETCH requests → CLOSE (or peer close).
    Each FETCH can carry multiple source paths, so a reducer with N sources on
    this node pays only one TCP round-trip handshake/setup overhead.
    """

    def handle(self):
        srv = self.server
        sock = self.request
        _tune_shuffle_socket(sock)
        sock.settimeout(300.0)
        try:
            if not self._handshake(sock, srv):
                return
            self._serve_loop(sock, srv)
        except (ConnectionError, OSError):
            # Peer closed mid-read, socket dead, or idle timeout.
            pass
        except Exception:
            logger.exception(
                f"Unexpected error handling connection from {self.client_address}"
            )

    # ── handshake ───────────────────────────────────────────────────────
    @staticmethod
    def _handshake(sock, srv) -> bool:
        """Read token, send status. Returns True on success. Non-ShuffleManager
        peers will fail either the recv (garbage bytes) or the auth check."""
        token_len = _recv_u16(sock)
        token = _recvall(sock, token_len).decode("utf-8")
        if token != srv.token:
            sock.sendall(struct.pack(">B", _STATUS_AUTH_FAIL))
            return False
        sock.sendall(struct.pack(">B", _STATUS_OK))
        return True

    # ── main request loop ───────────────────────────────────────────────
    def _serve_loop(self, sock, srv):
        while True:
            opcode = _recv_u8(sock)
            if opcode == _OPCODE_CLOSE:
                return
            if opcode != _OPCODE_FETCH:
                # Client sent something we don't know how to handle.
                # Drop the connection.
                return
            self._handle_fetch(sock, srv)

    # ── FETCH ────────────────────────────────────────────────────────────
    def _handle_fetch(self, sock, srv):
        """Parse a FETCH frame, validate, serve, write response."""
        # Parse the request body.
        num_sources = _recv_u16(sock)
        requests: List[Tuple[str, List[Tuple[int, int]]]] = []
        for _ in range(num_sources):
            path_len = _recv_u16(sock)
            path = _recvall(sock, path_len).decode("utf-8")
            # Path-traversal guard: resolved path must be strictly inside
            # ``srv.base_dir``. ``commonpath`` compares component-wise;
            # ``startswith`` would let sibling prefixes through.
            real = os.path.realpath(path)
            try:
                is_inside = os.path.commonpath([srv.base_dir, real]) == srv.base_dir
            except ValueError:
                is_inside = False
            if not is_inside:
                # Drain the rest of the request before answering so the socket
                # remains in a well-defined state for future FETCHes on this
                # connection
                self._drain_remaining_sources(
                    sock, remaining_source_count=num_sources - len(requests)
                )
                self._send_error(
                    sock, _STATUS_PATH_DENIED, f"path outside base_dir: {path}"
                )
                return

            num_ranges = _recv_u32(sock)
            ranges = []
            for _ in range(num_ranges):
                offset = _recv_u64(sock)
                length = _recv_u64(sock)
                ranges.append((offset, length))
            requests.append((real, ranges))

        # Zero-copy serve via sendfile: validate every file + range FIRST so
        # a missing file / bad range still produces an error status before we
        # commit _STATUS_OK; then stream each range in REQUEST order.
        files = []
        try:
            for path, ranges in requests:
                f = open(path, "rb")
                files.append(f)
                sz = os.fstat(f.fileno()).st_size
                for off, length in ranges:
                    if off < 0 or length < 0 or off + length > sz:
                        raise OSError(
                            f"range {off}+{length} outside {path} (size {sz})"
                        )
        except FileNotFoundError as e:
            for f in files:
                f.close()
            self._send_error(sock, _STATUS_NOT_FOUND, str(e))
            return
        except OSError as e:
            for f in files:
                f.close()
            self._send_error(sock, _STATUS_READ_ERR, str(e))
            return
        try:
            sock.sendall(struct.pack(">B", _STATUS_OK))
            for (path, ranges), f in zip(requests, files):
                fd = f.fileno()
                sock.sendall(struct.pack(">I", len(ranges)))
                for off, length in ranges:
                    sock.sendall(struct.pack(">I", length))
                    sent = sock.sendfile(f, offset=off, count=length)
                    if sent != length:
                        raise ConnectionError(
                            f"peer closed mid-sendfile: sent {sent} of {length}"
                        )
                    # Drop these pages from the page cache: hash-shuffle
                    # ranges are typically read once per reducer.
                    _drop_pagecache(fd, off, length)
        finally:
            for f in files:
                f.close()

    @staticmethod
    def _drain_remaining_sources(sock, remaining_source_count: int) -> None:
        """Read (and discard) the current source's ranges + all subsequent
        source frames. Called after we've committed to failing this FETCH
        so the socket is left at the start of the next request opcode,
        matching the client's "send full request then read full response"
        contract."""
        # Current source: its ranges header + range bytes (u64 off + u64 len each).
        num_ranges = _recv_u32(sock)
        _recvall(sock, num_ranges * 16)
        # Then any sources we hadn't started yet.
        for _ in range(remaining_source_count - 1):
            pl = _recv_u16(sock)
            _recvall(sock, pl)
            nr = _recv_u32(sock)
            _recvall(sock, nr * 16)

    @staticmethod
    def _send_error(sock, status: int, msg: str):
        payload = msg.encode("utf-8")
        try:
            sock.sendall(struct.pack(">B", status))
            sock.sendall(struct.pack(">I", len(payload)))
            sock.sendall(payload)
        except OSError:
            pass


class _ThreadingServer(socketserver.ThreadingTCPServer):
    # Default family is AF_INET; ``_threading_server_for(ip)`` dynamically
    # picks the V6 subclass below when the node's advertised address is IPv6
    allow_reuse_address = True
    daemon_threads = True
    # socket.listen() backlog. Default (5) is well below the SYN burst
    # when all reducers fan-out to every manager at once, causing silent
    # SYN drops → ETIMEDOUT. Kernel clamps to ``somaxconn`` if lower.
    request_queue_size = 256


class _ThreadingServerV6(_ThreadingServer):
    address_family = socket.AF_INET6


def _threading_server_for(ip: str) -> type:
    """Pick the ThreadingServer subclass matching ``ip``'s address family.
    Falls back to V4 for hostnames or unparseable strings — bind will fail
    fast if the family truly doesn't match."""
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return _ThreadingServer
    return (
        _ThreadingServerV6
        if isinstance(addr, ipaddress.IPv6Address)
        else _ThreadingServer
    )


# ShuffleManager actor identity. Name is deterministic in (shuffle_id, node_id)
_SHUFFLE_MANAGER_NAMESPACE = "ray_data_shuffle_external"


def _manager_name(shuffle_id: str, node_id: str) -> str:
    return f"shuffle_mgr:{shuffle_id}:{node_id}"


def _lookup_manager(shuffle_id: str, node_id: str) -> "ray.actor.ActorHandle":
    """Locate the ShuffleManager for this shuffle+node in the named-actor
    namespace. Raises ``ValueError`` if the actor has never been registered
    (or has been terminated and garbage-collected)."""
    return ray.get_actor(
        _manager_name(shuffle_id, node_id),
        namespace=_SHUFFLE_MANAGER_NAMESPACE,
    )


@ray.remote
class ShuffleManager:
    """Per-node file fetch service: owns a socket server that serves byte-ranges
    of local shuffle files to remote reducers. Survives individual map/reduce
    workers; in a real cluster, one per node (NodeAffinity)."""

    def __init__(
        self,
        base_dir: str,
        token: str,
    ):
        self.base_dir = os.path.realpath(base_dir)
        os.makedirs(self.base_dir, exist_ok=True)
        self.token = token
        ip = ray.util.get_node_ip_address()
        self._server = _threading_server_for(ip)((ip, 0), _FetchHandler)
        self._server.token = token
        self._server.base_dir = self.base_dir
        # IPv6 server_address is (host, port, flowinfo, scopeid); take only host+port.
        self._host, self._port = self._server.server_address[:2]
        t = threading.Thread(target=self._run_server, daemon=True)
        t.start()

    def _run_server(self) -> None:
        # If serve_forever ever returns/raises, the TCP endpoint is dead but
        # the actor process would keep answering RPCs — a false-positive that
        # breaks the "actor alive ⇒ server alive" invariant reducers rely on.
        # Kill the process so Ray restarts the actor (max_restarts=-1).
        try:
            self._server.serve_forever()
        except BaseException:
            logger.exception("ShuffleManager serve_forever crashed; exiting actor")
        else:
            logger.error("ShuffleManager serve_forever returned; exiting actor")
        os._exit(1)

    def endpoint(self) -> Tuple[str, int]:
        return (self._host, self._port)


@ray.remote(num_cpus=0)
def _cleanup_shuffle_dir(map_dir: str, reduce_dir: str, expected_node_id: str) -> None:
    """Best-effort ``rmtree`` of this shuffle's map + reduce staging dirs.
    NodeAffinity(soft=True) may land us off-target; no-op then."""
    if ray.get_runtime_context().get_node_id() != expected_node_id:
        return
    import shutil

    shutil.rmtree(map_dir, ignore_errors=True)
    shutil.rmtree(reduce_dir, ignore_errors=True)


# =============================================================================
# Client side: ``_ShuffleConnection`` is an open, handshake'd TCP connection
# to one ShuffleManager. Use within a ``with`` block; multiple ``fetch(...)``
# / ``fetch_into(...)`` calls are amortized over the same connection.
# =============================================================================


class _ShuffleConnection:
    """A keep-alive client connection to one ShuffleManager.

    Wraps a TCP socket that has already completed the handshake. Each
    ``fetch*`` call sends a single FETCH frame and reads the matching response
    frame. The socket can carry many such fetches before being closed, so the
    cost of TCP handshake + auth is amortized across all of them.
    """

    __slots__ = ("_sock", "_closed", "_endpoint")

    def __init__(self, sock: socket.socket, endpoint: Tuple[str, int]):
        self._sock = sock
        self._endpoint = endpoint
        self._closed = False

    # ── lifecycle ────────────────────────────────────────────────────────
    def __enter__(self) -> "_ShuffleConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        # Best-effort: send the CLOSE opcode so the server can exit its handler
        # cleanly without seeing a peer-reset.
        try:
            self._sock.sendall(struct.pack(">B", _OPCODE_CLOSE))
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass
        self._closed = True

    # ── FETCH (streams response APPENDED into one open file) ────────────
    def fetch_into(
        self,
        sources: List[Tuple[str, List[Tuple[int, int]]]],
        out_file_obj: "_PwriteSink",
        chunk_size: int = 4 * 1024 * 1024,
    ) -> None:
        """Single FETCH whose entire response streams into ``out_file_obj``
        as a flat sequence of ``(u32 len + ipc_bytes)*`` records, one record
        per shard across all sources.

        The reader walks the file as a simple stream:
            while file.tell() < file_size:
                length = u32; data = file.read(length); decode(data)

        Source ordering across calls is preserved (sources within a FETCH
        are written in request order; calls happen sequentially), so a
        """
        self._send_fetch_request(sources)
        status = _recv_u8(self._sock)
        if status != _STATUS_OK:
            self._raise_error_response(status)

        chunk = bytearray(chunk_size)
        view = memoryview(chunk)
        for path, ranges in sources:
            num_ranges = _recv_u32(self._sock)
            if num_ranges != len(ranges):
                raise RuntimeError(
                    f"protocol error: server returned {num_ranges} ranges "
                    f"for {path!r}, expected {len(ranges)}"
                )
            for _ in range(num_ranges):
                length = _recv_u32(self._sock)
                out_file_obj.write(struct.pack(">I", length))
                remaining = length
                while remaining > 0:
                    want = min(remaining, chunk_size)
                    n = self._sock.recv_into(view[:want], want)
                    if n == 0:
                        raise ConnectionError("peer closed mid-fetch")
                    out_file_obj.write(view[:n])
                    remaining -= n

    # internals
    def _send_fetch_request(
        self, sources: List[Tuple[str, List[Tuple[int, int]]]]
    ) -> None:
        """Serialize a FETCH frame over the wire (see protocol comment above)."""
        sock = self._sock
        sock.sendall(struct.pack(">B", _OPCODE_FETCH))
        sock.sendall(struct.pack(">H", len(sources)))
        for path, ranges in sources:
            path_bytes = path.encode("utf-8")
            sock.sendall(struct.pack(">H", len(path_bytes)))
            sock.sendall(path_bytes)
            sock.sendall(struct.pack(">I", len(ranges)))
            for offset, length in ranges:
                sock.sendall(struct.pack(">QQ", offset, length))

    def _raise_error_response(self, status: int) -> None:
        """Read error message body and raise an appropriate Python exception."""
        msg_len = _recv_u32(self._sock)
        msg = _recvall(self._sock, msg_len).decode("utf-8", errors="replace")
        if status == _STATUS_AUTH_FAIL:
            raise PermissionError(f"ShuffleManager auth failed: {msg}")
        if status == _STATUS_PATH_DENIED:
            raise PermissionError(f"path denied by ShuffleManager: {msg}")
        if status == _STATUS_NOT_FOUND:
            raise FileNotFoundError(msg)
        if status == _STATUS_READ_ERR:
            raise OSError(msg)
        raise RuntimeError(f"ShuffleManager error status={status}: {msg}")


def open_shuffle_connection(
    endpoint: Tuple[str, int],
    token: str,
    timeout: float = 60.0,
) -> _ShuffleConnection:
    """Open a TCP connection to ``endpoint`` and complete the handshake.

    Args:
        endpoint: (host, port) of the target ShuffleManager.
        token: Per-shuffle auth token; must match the server's stored token.
        timeout: Per-syscall socket timeout in seconds, applied to connect
            and all subsequent recv / send calls.

    Returns:
        A live, handshake'd :class:`_ShuffleConnection` ready for FETCHes.

    Raises:
        PermissionError: auth token is wrong.
        ShuffleManagerAnomalyError: server returned an unknown status byte.
        TimeoutError: connect or recv exceeded ``timeout`` seconds.
    """
    sock = socket.create_connection(endpoint, timeout=timeout)
    _tune_shuffle_socket(sock)
    sock.settimeout(timeout)
    try:
        token_bytes = token.encode("utf-8")
        sock.sendall(struct.pack(">H", len(token_bytes)))
        sock.sendall(token_bytes)
        status = _recv_u8(sock)
        if status == _STATUS_OK:
            return _ShuffleConnection(sock, endpoint)
        if status == _STATUS_AUTH_FAIL:
            raise PermissionError("ShuffleManager handshake: bad token")
        raise ShuffleManagerAnomalyError(
            f"ShuffleManager handshake: unexpected status {status}, "
            f"this should never happen."
        )
    except Exception:
        try:
            sock.close()
        except OSError:
            pass
        raise


class ShuffleHandle(TypedDict, total=False):
    """Handle written by each mapper task, consumed by reducer.

    Only the fields the runtime consumes are declared; the mapper task can
    add producer-side bookkeeping (byte counts, schema, etc.) as extra keys.
    """

    path: str
    # CSR per-partition range index (3 int64 arrays; see _index_to_csr).
    # Partition ``p``: ``zip(index_off[a:b], index_len[a:b])``,
    # ``a=index_part_start[p]``, ``b=index_part_start[p+1]``.
    index_off: "np.ndarray"
    index_len: "np.ndarray"
    index_part_start: "np.ndarray"
    shuffle_id: str
    node_id: str
    token: str
    schema: Optional["pa.Schema"]


class ShuffleDiskError(RuntimeError):
    """Terminal: reducer's local disk exhausted (ENOSPC / EDQUOT).
    Retrying doesn't reclaim space."""


# errno values that indicate the reducer's local disk is exhausted.
# EDQUOT is glibc's quota-exceeded error; not all platforms expose it.
_DISK_EXHAUSTED_ERRNOS = frozenset(
    e
    for e in (
        errno.ENOSPC,
        getattr(errno, "EDQUOT", None),
    )
    if e is not None
)


def _is_disk_exhausted(exc: BaseException) -> bool:
    return isinstance(exc, OSError) and exc.errno in _DISK_EXHAUSTED_ERRNOS


class ShuffleManagerAnomalyError(RuntimeError):
    """Terminal shuffle-manager failure: a driver-level retry cannot fix it.

    We run with ``max_restarts=-1``, ``lifetime="detached"``,
    ``NodeAffinitySchedulingStrategy(node_id, soft=False)``, this means Ray auto-restarts
    the actor on any mid-life crash (os.exit) and surfaces ``ActorUnavailableError``
    during the restart window. So the anomalous states we key off of are:

    - ``ActorDiedError``          -> ``__init__`` raised, or external
                                    ``ray.kill(actor, no_restart=True)``.
    - ``ActorUnschedulableError`` -> pinned node is gone; ``soft=False`` blocks
                                    relocation. Terminal at the shuffle layer;
                                    an upstream lineage layer must re-execute
                                    the mapper on healthy capacity.
    - ``ValueError`` (from ``ray.get_actor``) -> actor name is not registered
                                                (never created or gc'd).
    - Handshake replied with an unknown status byte.
    - TCP failed but the endpoint is unchanged and Ray RPC still works, which is
      often a network-configuration problem (``NetworkPolicy``, firewall, routing).
    """


# Process-global cache of ShuffleManager endpoints: {actor_name: (ip, port)}.
# Stale entries are popped by ``_prefetch_node_into`` on TCP failure; the next
# ``_resolve()`` call re-queries the actor. The lock guards concurrent access
# from reducer fetch threads.
_ENDPOINT_CACHE: Dict[str, Tuple[str, int]] = {}
_ENDPOINT_CACHE_LOCK = threading.Lock()

# --------------------------------------------------------- fetch routing types
# Named containers for reducer fetch orchestration. ``slots=True`` keeps
# per-instance memory small since we allocate one per source/group/member.


@dataclass(slots=True, frozen=True)
class _SourceRef:
    """One (mapper file, partition slice) the reducer needs to pull.

    Built once per input handle for a given partition_id at reducer start.
    The (shuffle_id, node_id) pair is the manager's named-actor identity;
    """

    shuffle_id: str
    node_id: str
    token: str
    path: str
    ranges: List[Tuple[int, int]]


@dataclass(slots=True, frozen=True)
class _NodeMember:
    """A source's ranges as they appear within a per-node fetch group."""

    path: str
    ranges: List[Tuple[int, int]]


@dataclass(slots=True)
class _NodeGroup:
    """All sources on one ShuffleManager, grouped so we open ONE TCP
    connection per manager. Sources collapse to the same group when their
    ``(shuffle_id, node_id)`` (i.e., the manager's named-actor identity) matches.
    """

    shuffle_id: str
    node_id: str
    token: str
    members: List[_NodeMember] = field(default_factory=list)


class _PwriteSink:
    """A write-only file-like that ``os.pwrite``s sequentially from a
    fixed base offset on a shared fd. Multiple sinks (one per fetch thread) at
    DISJOINT base regions write the same fd concurrently without lock.

    ``reset()`` rewinds ``_pos`` to the base offset so a fetch attempt that
    partially wrote can be retried in-place and subsequent ``os.pwrite``s
    overwrite the partial data (same offsets, idempotent write)."""

    __slots__ = ("_fd", "_base_offset", "_pos")

    def __init__(self, fd: int, base_offset: int):
        self._fd = fd
        self._base_offset = base_offset
        self._pos = base_offset

    def reset(self) -> None:
        self._pos = self._base_offset

    def write(self, data) -> int:
        mv = memoryview(data)
        total = 0
        n_total = len(mv)
        while total < n_total:
            n = os.pwrite(self._fd, mv[total:], self._pos + total)
            if n <= 0:
                # POSIX pwrite of a nonzero buffer should return >0 or raise;
                # 0 can happen on some FUSE / network filesystems and would
                # spin this loop forever.
                raise OSError(
                    f"pwrite returned {n} on fd {self._fd} "
                    f"(offset={self._pos + total}, remaining={n_total - total})"
                )
            total += n
        self._pos += total
        return total


def _prefetch_node_into(
    out_file_obj: "_PwriteSink",
    shuffle_id: str,
    node_id: str,
    token: str,
    members: List[_NodeMember],
    max_bytes_per_fetch: int,
) -> None:
    """Stream every member's shards into ``out_file_obj`` over ONE keep-alive
    connection, chunked into multi-source FETCH frames of
    ≤ ``max_bytes_per_fetch``.

    Actor state drives the recovery policy:
      * Dead (init fail/ray.kill)     -> ``ShuffleManagerAnomalyError`` (terminal)
      * Unschedulable (node lost)     -> ``ShuffleManagerAnomalyError`` (terminal)
      * Unavailable (restarting)      -> poll until Ray resolves
      * TCP dead, endpoint changed    -> reset sink, reopen, retry in-place
      * TCP dead, endpoint unchanged  -> ``ShuffleManagerAnomalyError`` (network
                                           config problem, terminal)
    """
    key = _manager_name(shuffle_id, node_id)

    def _resolve() -> Tuple[str, int]:
        # Endpoint cache avoids a blocking Ray RPC per fetch. On miss, ask
        # Ray for actor state and route by outcome (see docstring above).
        with _ENDPOINT_CACHE_LOCK:
            ep = _ENDPOINT_CACHE.get(key)
        if ep is not None:
            return ep
        try:
            manager = _lookup_manager(shuffle_id, node_id)
        except ValueError as e:
            # Actor name isn't registered (never created or cleaned up)
            raise ShuffleManagerAnomalyError(
                f"ShuffleManager on node {node_id} not found in namespace: {e}"
            ) from e
        poll_count = 0
        while True:
            try:
                ep = ray.get(manager.endpoint.remote())
                break
            except ActorUnavailableError:
                poll_count += 1
                # Surface a warning every ~30s so a stuck fetch is visible.
                if poll_count % 15 == 0:
                    logger.warning(
                        f"ShuffleManager on node {node_id} unavailable "
                        f"(~{poll_count * 2}s), still polling..."
                    )
                time.sleep(2.0)
            except ActorDiedError as e:
                # With max_restarts=-1, Ray auto-restarts on mid-life death and
                # surfaces ActorUnavailableError during the restart. So an
                # ActorDiedError reaching us means init failure or external
                # ray.kill
                raise ShuffleManagerAnomalyError(
                    f"ShuffleManager on node {node_id} is dead: {e}"
                ) from e
            except ActorUnschedulableError as e:
                # Pinned node is gone; soft=False can't relocate the actor.
                # Ray transitions from ActorUnavailableError to this ~10s after
                # heartbeat loss.
                raise ShuffleManagerAnomalyError(
                    f"ShuffleManager on node {node_id} is unschedulable "
                    f"(node likely dead): {e}"
                ) from e
        with _ENDPOINT_CACHE_LOCK:
            _ENDPOINT_CACHE[key] = ep
        return ep

    while True:
        try:
            endpoint = _resolve()
            with open_shuffle_connection(endpoint, token) as conn:
                for batch in _chunk_members_by_bytes(members, max_bytes_per_fetch):
                    sources = [(m.path, m.ranges) for m in batch]
                    conn.fetch_into(sources, out_file_obj)
            return
        except PermissionError:
            raise
        except (ConnectionError, TimeoutError) as e:
            # If _resolve() returns, the actor is alive; endpoint compare tells us
            # whether the manager restarted (retry in-place) or the reducer-manager
            # TCP path is broken (terminal).
            out_file_obj.reset()
            with _ENDPOINT_CACHE_LOCK:
                _ENDPOINT_CACHE.pop(key, None)
            fresh = _resolve()
            if fresh == endpoint:
                # Endpoint unchanged: actor is alive but TCP is blocked. Most
                # likely a network configuration issue (NetworkPolicy, firewall,
                # routing); retrying to the same manager won't help.
                raise ShuffleManagerAnomalyError(
                    f"TCP fetch from node {node_id} failed ({e}) but "
                    f"ShuffleManager at {fresh} is still reachable via Ray. "
                    f"Likely a network configuration issue (NetworkPolicy, "
                    f"firewall, routing) between reducer and manager. "
                    f"Check the network config."
                ) from e
            logger.warning(
                f"TCP fetch from node {node_id} failed ({e}); ShuffleManager "
                f"restarted (endpoint {endpoint} → {fresh}). Retrying in place."
            )
            continue
        except OSError as e:
            if _is_disk_exhausted(e):
                raise ShuffleDiskError(
                    f"Disk exhausted writing prefetch for node {node_id}: {e}"
                ) from e
            raise


def _chunk_members_by_bytes(
    members: List[_NodeMember],
    max_bytes: int,
) -> Iterable[List[_NodeMember]]:
    """Yield sub-batches of members whose total requested bytes ≤ ``max_bytes``.

    A source's ranges MAY be split across batches: the source appears as
    multiple pseudo-members with the same ``path`` but disjoint range
    subsets, in the original range order. Individual ranges are NEVER
    split as each range is one Arrow IPC frame at the mapper, so a
    sub-range cut would break the reducer's decode.
    """
    batch: List[_NodeMember] = []
    batch_bytes = 0
    for member in members:
        pending: List[Tuple[int, int]] = []
        for off, length in member.ranges:
            if (batch or pending) and batch_bytes + length > max_bytes:
                if pending:
                    batch.append(_NodeMember(path=member.path, ranges=pending))
                    pending = []
                yield batch
                batch, batch_bytes = [], 0
            pending.append((off, length))
            batch_bytes += length
        if pending:
            batch.append(_NodeMember(path=member.path, ranges=pending))
    if batch:
        yield batch


# fetch helpers
def _handle_batch_size(handles, batch_bytes):
    """#handles to resolve per batch so materialized metadata stays ≈ batch_bytes.

    Peeks one handle for ``num_partitions`` (each handle ≈ num_partitions × 16
    bytes of CSR arrays), so in-flight handle memory stays constant regardless
    of #mappers/#partitions.
    """
    if not handles:
        return 1
    probe = handles[0]
    if not isinstance(probe, dict):
        probe = ray.get(probe)
    try:
        npart = int(probe.get("num_partitions") or len(probe["index_part_start"]) - 1)
    except Exception:
        npart = 1
    per_handle = max(1, npart * 16)
    return max(1, min(len(handles), batch_bytes // per_handle))


def _handles_to_sources(
    handles: List["ShuffleHandle"],
    partition_id: int,
    batch_bytes: int = 64 * 1024 * 1024,
) -> Tuple[List[_SourceRef], Optional[pa.Schema]]:
    """Extract per-partition source refs from a reducer's input handles.

    Resolves handle refs in ≈``batch_bytes`` batches, slices this partition's
    ranges out of each handle's CSR arrays, then frees the batch — so in-flight
    handle memory stays constant, not O(maps × partitions). Skips handles with
    zero bytes for this partition; picks the first non-None schema.
    """
    sources: List[_SourceRef] = []
    output_schema: Optional[pa.Schema] = None
    if not handles:
        return sources, output_schema

    k = _handle_batch_size(handles, batch_bytes)
    for start in range(0, len(handles), k):
        batch = handles[start : start + k]
        refs = [h for h in batch if not isinstance(h, dict)]
        vals = iter(ray.get(refs)) if refs else iter(())
        resolved = [h if isinstance(h, dict) else next(vals) for h in batch]
        for h in resolved:
            if output_schema is None:
                output_schema = h.get("schema")
            # CSR slice: only this partition's ranges materialize as Python
            # objects; the rest stay as (zero-copy) numpy buffers.
            ps = h["index_part_start"]
            s = int(ps[partition_id])
            e = int(ps[partition_id + 1])
            if e > s:
                offs = h["index_off"][s:e].tolist()
                lens = h["index_len"][s:e].tolist()
                sources.append(
                    _SourceRef(
                        shuffle_id=h["shuffle_id"],
                        node_id=h["node_id"],
                        token=h["token"],
                        path=h["path"],
                        ranges=list(zip(offs, lens)),
                    )
                )
        # Free this batch's resolved handles before resolving the next, so
        # in-flight handle memory stays ≈ batch_bytes regardless of #mappers.
        del resolved, vals
    return sources, output_schema


def _group_by_manager(sources: List[_SourceRef]) -> List[_NodeGroup]:
    """Collapse sources by manager so each manager gets ONE TCP connection.

    Sources on the same manager share a ``(shuffle_id, node_id)`` which is
    used as the collapse key.
    """
    by_key: Dict[Tuple[str, str], _NodeGroup] = {}
    for s in sources:
        key = (s.shuffle_id, s.node_id)
        group = by_key.get(key)
        if group is None:
            group = _NodeGroup(
                shuffle_id=s.shuffle_id,
                node_id=s.node_id,
                token=s.token,
                members=[],
            )
            by_key[key] = group
        group.members.append(_NodeMember(path=s.path, ranges=s.ranges))
    return list(by_key.values())


def _compute_prefetch_layout(
    groups: List[_NodeGroup],
) -> Tuple[int, List[int], List[int]]:
    """Assign each group a contiguous byte region in the reducer's prefetch file.

    Returns ``(total_size, base_offsets, per_group_sizes)`` where sizes are the
    ``4 + length`` framed byte totals (u32 len prefix + IPC bytes per range),
    base offsets are running cumulative sums. Fetch threads then pwrite each
    group's response frames at DISJOINT offsets
    """
    sizes = [
        sum(4 + length for m in g.members for (_off, length) in m.ranges)
        for g in groups
    ]
    base_offsets: List[int] = []
    acc = 0
    for sz in sizes:
        base_offsets.append(acc)
        acc += sz
    return acc, base_offsets, sizes
