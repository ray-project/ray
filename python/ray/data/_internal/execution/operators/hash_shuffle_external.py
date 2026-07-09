"""hash_shuffle_external — file-transport hash shuffle with an out-of-band side-channel.

Design: design/hash-shuffle-bypass.md.

vs v2 (N×M plasma ObjectRefs + reducer ``ray.get``):
- each MAP task writes ONE file (all its partitions, Arrow IPC) and returns ONE
  small handle (path + per-partition offset index + the source node's fetch
  endpoint + a per-shuffle auth token). Driver tracks O(N) handles, not O(N×M)
  refs; bulk never enters plasma.
- a per-node ``ShuffleManager`` Ray actor runs its OWN socket server (§3.4/§3.5)
  that ``pread``s requested byte-ranges and streams them back. The REDUCE task is
  a client of that server: bytes arrive in its **user space** (NOT plasma →
  preserves 1×, §4.6) and are consumed inline. Cross-node this is the real
  out-of-band transport; single-node it is a loopback socket (still the real
  code path, not a direct ``open``).

Reuses v2's PartitionFn / ReduceFn / MapBlockTransformer contracts verbatim, so
group-by / sort / aggregate / join factories compose unchanged.

Scope: works single-node (one actor) and is structured for multi-node (one actor
per node). No planner/ShuffleStrategy wiring yet (driven by a harness).
"""

# todo: pre-check same-key skew in mapphase would be smart
import atexit
import os
import pickle
import shutil
import socket
import socketserver
import struct
import tempfile
import threading
import time
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa

import ray
from ray._raylet import (
    StreamingGeneratorStats,  # pyrefly: ignore[missing-module-attribute]
)
from ray.exceptions import ActorDiedError
from ray.data._internal.output_buffer import (
    BlockOutputBuffer,
    OutputBlockSizeOption,
)
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    TaskExecWorkerStats,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

PartitionFn = Callable[[pa.Table], Dict[int, pa.Table]]
ReduceFn = Callable[[int, List[pa.Table]], Iterable[pa.Table]]
MapBlockTransformer = Callable[[pa.Table], pa.Table]

# Per-RecordBatch IPC compression. None preserves zero-copy mmap into Arrow
# (uncompressed bytes are directly the on-wire IPC buffer). LZ4 is the
# recommended default for cross-node clusters: ~2-5x smaller for tabular data
# with sub-µs/MB decompression. ZSTD trades CPU for higher ratio on slow links.
ShuffleCompression = Optional[Literal["lz4", "zstd"]]

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
#       u8[4]   magic       = b'EXSH'
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
#       if CLOSE: (no body)
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

# EXSH for external-shuffle, application-level handshake magic
_PROTO_MAGIC = b"EXSH"

# Opcodes
_OPCODE_FETCH = 0x01
_OPCODE_CLOSE = 0x00

# Status codes
_STATUS_OK = 0x00
_STATUS_AUTH_FAIL = 0x01
_STATUS_PATH_DENIED = 0x02  # path resolves outside server's base_dir
_STATUS_NOT_FOUND = 0x03  # path doesn't exist on disk
_STATUS_READ_ERR = 0x04  # IO error reading file content
_STATUS_PROTOCOL_ERR = 0x05  # malformed frame / unknown opcode / etc.

# The response frame encodes each range's payload length as u32 (see
# "Response frame" block above), so no single range/IPC frame may exceed
# 4 GiB - 1. Checked at mapper write time so an oversized IPC buffer fails
# the mapper task at its origin, not deep in a reducer fetch.
_MAX_RANGE_BYTES: int = (1 << 32) - 1


# ----------------------------------------------------------------- Arrow IPC
def _ipc_buffer(table: pa.Table, compression: ShuffleCompression = None) -> pa.Buffer:
    """Serialize an Arrow ``Table`` to an IPC stream and return the result as
    a zero-copy ``pa.Buffer``.

    combine_chunks() helps compression

    Compression is applied per RecordBatch via Arrow IPC's built-in codec
    flag; the resulting stream still has the standard continuation + schema
    + EOS framing, so format-level corruption detection (``_read_ipc``
    raising ``ArrowInvalid``) is preserved. The reader auto-detects
    compression from stream metadata — no caller coordination needed
    beyond the writer.
    """
    if table.num_columns > 0:
        table = table.combine_chunks()
    sink = pa.BufferOutputStream()
    write_opts = (
        pa.ipc.IpcWriteOptions(compression=compression) if compression else None
    )
    with pa.ipc.new_stream(sink, table.schema, options=write_opts) as w:
        w.write_table(table)
    return sink.getvalue()


def _read_ipc(buf: Union[bytes, "pa.Buffer", memoryview]) -> pa.Table:
    """Decode an IPC stream from bytes or a pa.Buffer view (e.g. mmap).

    pa.ipc.open_stream transparently handles compressed payloads, so this
    works for both uncompressed and lz4/zstd IPC streams.
    """
    if isinstance(buf, (bytes, bytearray)):
        source = pa.py_buffer(buf)
    else:
        source = buf
    with pa.ipc.open_stream(source) as r:
        return r.read_all()


# wire framing
def _tune_shuffle_socket(sock: socket.socket) -> None:
    """Configure a shuffle TCP socket for our usage pattern.

    * ``TCP_NODELAY`` disables Nagle. Our wire protocol sends many small
      frames in sequence (magic + token, opcode + header, per-range hdrs),
      Nagle's default coalesce would inject ~40ms latency between them.
    * ``SO_KEEPALIVE`` lets the kernel detect dead peers via TCP-level
      probes on long-idle connections — important when intermediate
      NAT / firewall middleboxes silently drop idle flows.

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


def _sendfile_all(sock, in_fd: int, offset: int, count: int) -> None:
    """Send exactly ``count`` bytes of file ``in_fd`` starting at ``offset``
    straight to ``sock`` via kernel zero-copy. ``os.sendfile`` may transfer
    fewer bytes per call (socket buffer pressure), so loop until done. Blocking
    socket -> a short write means EOF/peer-gone, not EAGAIN."""
    out_fd = sock.fileno()
    sent = 0
    while sent < count:
        n = os.sendfile(out_fd, in_fd, offset + sent, count - sent)
        if n == 0:
            raise ConnectionError("peer closed mid-sendfile")
        sent += n


# Linux-only; macOS lacks POSIX_FADV_DONTNEED. Probed once at import time so
# the hot path is a constant-time attribute check, not a try/except per range.
_HAS_FADV_DONTNEED = hasattr(os, "posix_fadvise") and hasattr(os, "POSIX_FADV_DONTNEED")


def _drop_pagecache(fd: int, offset: int, length: int) -> None:
    """Hint the kernel to drop ``[offset, offset+length)`` of ``fd`` from the
    page cache. Called after sendfile to keep the server's page-cache footprint
    bounded to in-flight bytes."""
    if not _HAS_FADV_DONTNEED or length <= 0:
        return
    try:
        os.posix_fadvise(fd, offset, length, os.POSIX_FADV_DONTNEED)
    except OSError:
        # Best-effort: any failure is silently ignored (the file is read-only, so
        # the worst case is the kernel keeps the pages a bit longer).
        pass


# Fetch helper class used by reducer tasks
class _FetchHandler(socketserver.StreamRequestHandler):
    """Per-connection handler implementing the v1 wire protocol.

    Lifecycle: one handshake → loop of FETCH requests → CLOSE (or peer close).
    Each FETCH can carry multiple source paths, so a reducer with N sources on
    this node pays only one TCP round-trip's worth of handshake/setup overhead
    """
    def handle(self):
        srv = self.server
        sock = self.connection
        _tune_shuffle_socket(sock)
        try:
            if not self._handshake(sock, srv):
                return
            self._serve_loop(sock, srv)
        except (ConnectionError, OSError):
            # Peer closed mid-read, or socket dead — normal teardown.
            pass

    # ── handshake ───────────────────────────────────────────────────────
    @staticmethod
    def _handshake(sock, srv) -> bool:
        """Read magic + token, send status. Returns True on success."""
        magic = _recvall(sock, 4)
        if magic != _PROTO_MAGIC:
            # Send PROTOCOL_ERR even though the peer probably isn't speaking our
            # protocol — best-effort, then bail.
            try:
                sock.sendall(struct.pack(">B", _STATUS_PROTOCOL_ERR))
            except OSError:
                pass
            return False
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
                self._send_error(sock, _STATUS_PROTOCOL_ERR, f"unknown opcode {opcode}")
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
            # path-traversal guard: realpath must be inside the server base_dir.
            real = os.path.realpath(path)
            if not real.startswith(srv.base_dir):
                # Drain the rest of the request before answering so the socket
                # remains in a well-defined state for future FETCHes on this
                # connection (the client's protocol contract is "send full
                # request, then read full response").
                num_ranges = _recv_u32(sock)
                _recvall(sock, num_ranges * 16)  # u64 offset + u64 length
                for _ in range(num_sources - len(requests) - 1):
                    pl = _recv_u16(sock)
                    _recvall(sock, pl)
                    nr = _recv_u32(sock)
                    _recvall(sock, nr * 16)
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

        # Zero-copy serve via os.sendfile: validate every file + range FIRST so
        # a missing file / bad range still produces an error status before we
        # commit _STATUS_OK; then os.sendfile each range in REQUEST order
        # (client maps positionally).
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
                    _sendfile_all(sock, fd, off, length)
                    # Drop these pages from the page cache: hash-shuffle
                    # ranges are typically read once per reducer, and same-
                    # node reducers concurrently pwrite their own
                    # ``prefetch.bin`` -- without this hint the just-served
                    # (hot in LRU) ranges would evict the reducer's
                    # incoming data. See _drop_pagecache.
                    _drop_pagecache(fd, off, length)
                    srv.bytes_served += length
        finally:
            for f in files:
                f.close()

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
    allow_reuse_address = True
    daemon_threads = True
    # socket.listen() backlog. socketserver's default (5) is far below the
    # SYN burst at shuffle start (all reducers fan-out TCP dial to each
    # manager roughly at once). On server's overflow, Linux by default silently drops
    # SYNs (tcp_abort_on_overflow=0), then clients see slow connects, eventually ETIMEDOUT.
    # kernel silently clamps to somaxconn if lower.
    request_queue_size = 256


# -- ShuffleManager identity -------------------------------------------------
#
# The actor's real identity is its ``(name, namespace)`` GCS entry; ActorHandle
# is a cache. We construct the name deterministically from ``shuffle_id`` +
# ``node_id`` so any process can rebuild the identity from handle-dict fields
# and look the actor up via ``ray.get_actor`` — no ActorHandle needs to
# travel through the map→reduce plumbing.
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
        self._server = _ThreadingServer((ip, 0), _FetchHandler)
        self._server.token = token
        self._server.base_dir = self.base_dir
        self._server.bytes_served = 0
        self._host, self._port = self._server.server_address
        t = threading.Thread(target=self._server.serve_forever, daemon=True)
        t.start()

        # Cleanup fires on two paths:
        #   1. Graceful actor shutdown via __ray_terminate__ (driver-side),
        #      which Ray triggers ``__ray_shutdown__`` on after draining
        #      pending tasks and before exiting the actor.
        #   2. atexit hook covers the "Ray runtime torn down without going
        #      through __ray_terminate__" fallback (interpreter shutdown).
        # Both call the same idempotent method.
        self._cleaned = False
        atexit.register(self.__ray_shutdown__)

    def __ray_shutdown__(self) -> None:
        """Stop the socket server + rmtree ``base_dir``. Idempotent.
        Best-effort throughout: a cleanup failure must never propagate."""
        if self._cleaned:
            return
        self._cleaned = True
        try:
            self._server.shutdown()
        except Exception:
            pass
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def endpoint(self) -> Tuple[str, int]:
        return (self._host, self._port)

    def base(self) -> str:
        return self.base_dir

    def bytes_served(self) -> int:
        return self._server.bytes_served


# =============================================================================
# Client side
#
# ``_ShuffleConnection`` is the primitive: an open, handshake'd TCP connection
# to one ShuffleManager. Once open, you can ``fetch(...)`` or
# ``fetch_to_files(...)`` any number of times, each with potentially many
# source paths and many ranges per source. Use within a ``with`` block to make
# sure the CLOSE opcode is sent and the socket is torn down properly.
#
# The two convenience wrappers ``_fetch_ranges`` and ``_fetch_ranges_to_file``
# preserve the single-source API for tests and ad-hoc callers; they internally
# open a connection, do one FETCH, and close.
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

    # ── FETCH (returns bytes per source per range) ───────────────────────
    def fetch(
        self,
        sources: List[Tuple[str, List[Tuple[int, int]]]],
    ) -> List[List[bytes]]:
        """Single FETCH carrying ``sources`` and returning the matching bytes.

        ``sources`` is a list of ``(path, [(offset, length), ...])`` tuples;
        the returned list is parallel — index ``i`` corresponds to
        ``sources[i]``, and each inner list is parallel to that source's
        ``ranges`` argument. Useful for in-memory consumers (tests, direct
        ``_read_ipc(buf)`` callers); production reduce path uses
        :meth:`fetch_to_files` instead to avoid full per-range bytes.
        """
        self._send_fetch_request(sources)
        status = _recv_u8(self._sock)
        if status != _STATUS_OK:
            self._raise_error_response(status)
        out: List[List[bytes]] = []
        for path, ranges in sources:
            n = _recv_u32(self._sock)
            if n != len(ranges):
                # Protocol contract violation: server's per-source range count
                # must match what we sent. Raise instead of ``assert`` so the
                # check survives ``python -O`` (where ``assert`` is stripped
                # and a mismatch would silently corrupt the read stream).
                raise RuntimeError(
                    f"protocol error: server returned {n} ranges for "
                    f"{path!r}, expected {len(ranges)}"
                )
            buf: List[bytes] = []
            for _ in range(n):
                length = _recv_u32(self._sock)
                buf.append(_recvall(self._sock, length))
            out.append(buf)
        return out

    # ── FETCH (streams response into files) ──────────────────────────────
    def fetch_to_files(
        self,
        sources: List[Tuple[str, List[Tuple[int, int]]]],
        out_files: List[str],
        chunk_size: int = 64 * 1024,
    ) -> None:
        """Single FETCH whose per-source response streams into ``out_files[i]``.

        Each output file gets self-contained framing (``u32 num_ranges`` then
        per range ``u32 len`` + raw bytes). Kept around for tests and for any
        caller that wants per-source files (one mmap per source).

        User-space peak is ``chunk_size`` regardless of how big any range is.
        """
        if len(sources) != len(out_files):
            # Caller bug, not a protocol violation — but still must be a real
            # raise so ``python -O`` doesn't swallow it.
            raise ValueError(
                f"sources/out_files length mismatch: "
                f"{len(sources)} vs {len(out_files)}"
            )
        self._send_fetch_request(sources)
        status = _recv_u8(self._sock)
        if status != _STATUS_OK:
            self._raise_error_response(status)

        chunk = bytearray(chunk_size)
        view = memoryview(chunk)
        opened_files: List[str] = []
        try:
            for (path, ranges), out_path in zip(sources, out_files):
                num_ranges = _recv_u32(self._sock)
                if num_ranges != len(ranges):
                    raise RuntimeError(
                        f"protocol error: server returned {num_ranges} "
                        f"ranges for {path!r}, expected {len(ranges)}"
                    )
                with open(out_path, "wb") as f:
                    opened_files.append(out_path)
                    f.write(struct.pack(">I", num_ranges))
                    for _ in range(num_ranges):
                        length = _recv_u32(self._sock)
                        f.write(struct.pack(">I", length))
                        remaining = length
                        while remaining > 0:
                            want = min(remaining, chunk_size)
                            n = self._sock.recv_into(view[:want], want)
                            if n == 0:
                                raise ConnectionError("peer closed mid-fetch")
                            f.write(view[:n])
                            remaining -= n
        except Exception:
            # Best-effort cleanup so failed multi-source fetches don't leak
            # partial files (the connection itself is unusable past this point;
            # caller's ``with`` block will close it).
            for p in opened_files:
                try:
                    os.unlink(p)
                except OSError:
                    pass
            raise

    # ── FETCH (streams response APPENDED into one open file) ────────────
    def fetch_into(
        self,
        sources: List[Tuple[str, List[Tuple[int, int]]]],
        out_file_obj,
        chunk_size: int = 64 * 1024,
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
) -> _ShuffleConnection:
    """Open a TCP connection to ``endpoint`` and complete the v1 handshake.

    Raises ``PermissionError`` on auth failure, ``ConnectionError`` if the
    server isn't reachable, ``RuntimeError`` on protocol errors.
    """
    sock = socket.create_connection(endpoint)
    _tune_shuffle_socket(sock)
    try:
        token_bytes = token.encode("utf-8")
        sock.sendall(_PROTO_MAGIC)
        sock.sendall(struct.pack(">H", len(token_bytes)))
        sock.sendall(token_bytes)
        status = _recv_u8(sock)
        if status == _STATUS_OK:
            return _ShuffleConnection(sock, endpoint)
        if status == _STATUS_AUTH_FAIL:
            sock.close()
            raise PermissionError("ShuffleManager handshake: bad token")
        sock.close()
        raise RuntimeError(f"ShuffleManager handshake: unexpected status {status}")
    except Exception:
        try:
            sock.close()
        except OSError:
            pass
        raise


# map / reduce task body
ShuffleHandle = dict  # {path, index:{pid:[(off,len)]}, endpoint:(host,port), token, node_id}


@ray.remote
def external_hash_shuffle_map_task(
    *blocks: Block,
    partition_fn: PartitionFn,
    num_partitions: int,
    out_dir: str,
    map_id: int,
    shuffle_id: str,
    token: str,
    transformer: MapBlockTransformer = None,
    map_op_name: str = "ExternalHashShuffleMap",
    pool_budget_bytes: int = 16 * 1024 * 1024,
    compression: ShuffleCompression = None,
    fsync_on_close: bool = True,
) -> ShuffleHandle:
    """Streaming write with a shared, byte-accounted staging pool, sealed
    via atomic ``rename``.

    ``pool_budget_bytes`` bounds both ends of the map:

    - **Output**: all post-hash partition buckets share one pool of that
      size. On overflow the LARGEST bucket is spilled — total staging is
      bounded independent of the partition count M.
    - **Input**: v2's ``PartitionFn(Table → Dict[pid, Table])`` materializes
      all M shards at once (~2× copy of its input), so we feed it in
      row-batches sized ``pool_budget_bytes / avg_row_bytes`` to keep the
      transient spike ~pool-bounded. If the whole block already fits the
      pool, we skip batching.

    Net: **map peak ≈ input block + O(pool_budget_bytes)**. Blocking
    ``f.write`` gives natural OS backpressure on slow disks.

    Sealed via atomic ``rename``: writes go to ``map_{i}.shf.tmp``; after a
    final ``f.flush()`` + optional ``os.fsync`` + size sanity check against
    the index, we ``os.rename`` to the published path. Readers therefore
    see either no file or a complete, size-validated one — catches
    truncated files earlier than Arrow IPC's per-shard magic would.

    ``fsync_on_close=True`` gives durability against node crash. Readers go
    through the manager's page-cache-backed sendfile serve, so this sync
    only matters if node-reboot recovery is later added to the FT model.
    """
    node_id = ray.get_runtime_context().get_node_id()
    # Ensure the manager actor exists (get_if_exists=True → reuse across
    # mappers on the same node). We don't need to keep the handle: reducers
    # will look the manager up by name via ``_lookup_manager``.
    ShuffleManager.options(
        name=_manager_name(shuffle_id, node_id),
        namespace=_SHUFFLE_MANAGER_NAMESPACE,
        get_if_exists=True,
        lifetime="detached",
        max_restarts=-1,
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
        num_cpus=0,
    ).remote(out_dir, token)

    os.makedirs(out_dir, exist_ok=True)
    final_path = os.path.join(out_dir, f"map_{map_id}.shf")
    # Write to a temp file first; only ``rename`` once we've verified the full file.
    tmp_path = final_path + ".tmp"
    # index = {partition id, {offset, length}}
    index: Dict[int, List[Tuple[int, int]]] = {}
    staging: Dict[int, List[pa.Table]] = {}
    staging_bytes: Dict[int, int] = {}
    peak_inflight = 0  # max bytes of partition output held at once (excludes input)
    decoded_bytes_per_partition: Dict[int, int] = {} # Decoded bytes per partition
    output_schema: Optional[pa.Schema] = None

    def _partition_units(blk):
        """Yield (pid, shard).
        yield whole-block when the block already fits the pool (no overhead),
        else split into pool-sized row-batches (which bounds the 2×S partition
        spike to S + O(pool))."""
        if blk.num_rows == 0:
            return
        avg_row = max(1, blk.nbytes // blk.num_rows)
        batch_rows = max(1, pool_budget_bytes // avg_row)
        if blk.num_rows <= batch_rows:
            # block's partition spike is already ≤ pool → whole-block, no overhead
            for pid, shard in partition_fn(blk).items():
                yield pid, shard
        else:
            for batch in blk.to_batches(max_chunksize=batch_rows):
                bt = pa.Table.from_batches([batch], schema=blk.schema)
                for pid, shard in partition_fn(bt).items():
                    yield pid, shard

    final_size_on_close = -1
    try:
        with open(tmp_path, "wb") as f:

            def flush(pid: int):
                shards = staging.get(pid)
                if not shards:
                    return
                tbl = pa.concat_tables(shards) if len(shards) > 1 else shards[0]
                # ``tbl.nbytes`` is the decoded (pre-IPC, pre-compression) byte
                # count of this shard
                decoded_bytes_per_partition[pid] = (
                    decoded_bytes_per_partition.get(pid, 0) + tbl.nbytes
                )

                buf = _ipc_buffer(tbl, compression=compression)
                # Refuse frames the u32 response-wire encoding can't
                # represent (see top-of-file spec).
                if buf.size > _MAX_RANGE_BYTES:
                    raise RuntimeError(
                        f"map_{map_id}.shf partition {pid}: IPC frame is "
                        f"{buf.size} bytes, exceeding the u32 wire-protocol "
                        f"per-range limit ({_MAX_RANGE_BYTES}). Reduce "
                        f"``pool_budget_bytes`` or the upstream block size."
                    )
                off = f.tell()
                f.write(memoryview(buf))
                index.setdefault(pid, []).append((off, buf.size))
                staging[pid] = []
                staging_bytes[pid] = 0

            def pool_size() -> int:
                return sum(staging_bytes.values())

            for blk in blocks:
                # Match v2's boundary convention: accept any Ray Data Block
                # (Arrow / pandas / ...) and normalize to ``pa.Table`` here.
                # Downstream (partition_fn, transformer, IPC serialize) is
                # Arrow-only and stays that way. No-op when already Arrow.
                if not isinstance(blk, pa.Table):
                    blk = BlockAccessor.for_block(blk).to_arrow()
                if transformer is not None:
                    blk = transformer(blk)
                if output_schema is None:
                    # First-seen schema; reducer uses it to type empty
                    # partitions (ShuffleHandle["schema"]).
                    output_schema = getattr(blk, "schema", None)
                for pid, shard in _partition_units(blk):
                    if not shard.num_rows:
                        continue
                    staging.setdefault(pid, []).append(shard)
                    staging_bytes[pid] = staging_bytes.get(pid, 0) + shard.nbytes
                    peak_inflight = max(peak_inflight, pool_size())
                    # Spill LARGEST bucket(s) on overflow to bound total
                    # staging to pool_budget_bytes.
                    while pool_size() >= pool_budget_bytes:
                        victim = max(staging_bytes, key=staging_bytes.get)
                        if staging_bytes[victim] == 0:
                            break
                        flush(victim)
            for pid in list(staging.keys()):
                flush(pid)

            # userspace --flush-→ page cache --fsync-→ disk, then sanity-check the file
            # size matches the index. Mismatch = logic bug or silent short
            # write; refuse to publish (the except below unlinks tmp).
            f.flush()
            if fsync_on_close:
                os.fsync(f.fileno())
            final_size_on_close = f.tell()
            if index:
                expected_size = max(
                    off + length for ranges in index.values() for off, length in ranges
                )
            else:
                expected_size = 0
            if final_size_on_close != expected_size:
                raise RuntimeError(
                    f"external_hash_shuffle_map_task: file size mismatch — wrote "
                    f"{final_size_on_close} bytes, index implies "
                    f"{expected_size}. Refusing to publish corrupt file."
                )

        # Atomic publish: .tmp → .shf.
        os.rename(tmp_path, final_path)
    except Exception:
        # Don't leak a half-written .tmp in out_dir; Ray retries the task.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return {
        "path": os.path.realpath(final_path),
        "index": index,
        # ShuffleManager identity: reducers rebuild the actor name from
        # (shuffle_id, node_id) and call ``_lookup_manager`` when they need
        # the handle. No ActorHandle in the wire format.
        "shuffle_id": shuffle_id,
        "node_id": node_id,
        "token": token,
        "num_partitions": num_partitions,
        "peak_inflight_bytes": peak_inflight,
        # Total bytes written to the output file, post-seal.
        "total_bytes": final_size_on_close,
        "compression": compression,
        "decoded_bytes": decoded_bytes_per_partition,
        "schema": output_schema,
    }


class ShuffleFetchError(RuntimeError):
    """Raised when a side-channel fetch fails (source gone / file lost). Surfaced
    so the executor/lineage can re-run the producer mapper (§4.10/§11 Q1)."""


class ShuffleNodeLostError(ShuffleFetchError):
    """Raised when a source ShuffleManager's node is confirmed dead
    (via ``ray.nodes()``).

    External-shuffle materializes mapper output on the mapper node's local
    disk. If that node dies, the on-disk output is gone with it and Ray
    lineage cannot reconstruct the mapper (the handle-refs are aggregated
    driver-side, so mapper return-refs are not observed as lost). Retrying
    the reducer against the same dead node would just hang or fail again.

    Under the current FT design (no mapper re-execution yet) this is
    surfaced as fatal so the user knows the job is not recoverable — no
    silent hang against a permanently-pending (soft=False, max_restarts=-1)
    actor. Phase 3 of the FT rollout will convert this into a signal
    that triggers upstream mapper re-execution to a live node.
    """


class ShuffleManagerAnomalyError(ShuffleFetchError):
    """Raised when a ShuffleManager reports as dead but its node is still alive.

    Under our configuration (``max_restarts=-1``, ``lifetime="detached"``,
    ``NodeAffinitySchedulingStrategy(node_id, soft=False)``) an actor should
    NEVER be permanently dead while its node is alive: Ray restarts it on
    every crash, and a dead node produces PENDING (``ActorUnavailableError``)
    not ``ActorDiedError``. If this fires, one of the following unusual
    things has happened:

    - External ``ray.kill(actor, no_restart=True)`` from user code.
    - An unhandled exception in ``ShuffleManager.__init__`` that Ray cannot
      recover from within its restart budget.
    - A Ray-internal state inconsistency (rare GCS races).

    We fail loudly (rather than trying to recover) so the underlying cause
    is visible in the traceback instead of being masked by opaque
    retry-and-still-broken behavior.
    """


def _classify_and_raise(
    node_id: str,
    *,
    exc: Optional[BaseException],
    context: str,
    num_sources: int,
) -> "NoReturn":
    """Diagnose a manager RPC failure and raise the appropriate typed error.

    - Node dead → ``ShuffleNodeLostError`` (recoverable via mapper re-execution
      in Phase 3; fatal today).
    - Node alive → ``ShuffleManagerAnomalyError`` (unexpected under our
      configuration; fail loudly, no auto-recovery).
    - Node liveness inconclusive (GCS lag) → ``ShuffleFetchError`` (treated
      as transient).
    """
    alive = _is_node_alive(node_id)
    detail = f"context={context!r}; sources={num_sources}"
    if alive is False:
        raise ShuffleNodeLostError(
            f"ShuffleManager node {node_id} is dead — on-disk shuffle output "
            f"lost with the node. External-shuffle does not yet reconstruct "
            f"across node loss. ({detail})"
        ) from exc
    if alive is True:
        raise ShuffleManagerAnomalyError(
            f"ShuffleManager for node {node_id} is dead but the node is "
            f"still alive. This is not expected under max_restarts=-1 + "
            f"detached lifetime; failing loudly so the underlying cause is "
            f"visible. ({detail})"
        ) from exc
    # alive is None: GCS inconclusive. Treat as transient — Ray task retry
    # or a resubmit will get a fresh view.
    raise ShuffleFetchError(
        f"ShuffleManager RPC failed and node liveness is inconclusive "
        f"(node_id={node_id!r}, likely transient GCS lag). ({detail})"
    ) from exc


def _is_node_alive(node_id: str) -> Optional[bool]:
    """Return True/False from ``ray.nodes()``, or None if the node isn't in the
    snapshot at all (transient GCS lag — treat as inconclusive, don't upgrade
    a fetch error to a fatal ShuffleNodeLostError on a maybe)."""
    if not node_id:
        return None
    try:
        for n in ray.nodes():
            if n.get("NodeID") == node_id:
                return bool(n.get("Alive"))
    except Exception:
        # GCS query itself failed — inconclusive.
        return None
    return None


_DEFAULT_MAX_BYTES_PER_FETCH = 256 * 1024 * 1024  # 256 MiB per FETCH frame

# Process-global cache of ShuffleManager endpoints: {actor_id_bytes: (ip, port)}.
# When an file server actor is respawned (due to failure), the CACHE will be re-populated
_ENDPOINT_CACHE: Dict[str, Tuple[str, int]] = {}

# --------------------------------------------------------- fetch routing types
# Named containers replace the anonymous tuples the reducer used to thread
# through fetch orchestration. Read via attribute access (``group.manager``)
# instead of positional indexing (``group[0]``). ``slots=True`` keeps memory
# footprint on par with the tuples they replace.


@dataclass(slots=True, frozen=True)
class _SourceRef:
    """One (mapper file, partition slice) the reducer needs to pull.

    Built once per input handle for a given partition_id at reducer start.
    The (shuffle_id, node_id) pair is the manager's named-actor identity;
    the reducer calls ``_lookup_manager(shuffle_id, node_id)`` when it
    actually needs an ActorHandle for the fetch RPC.
    """

    shuffle_id: str
    node_id: str
    token: str
    path: str
    ranges: List[Tuple[int, int]]


@dataclass(slots=True, frozen=True)
class _NodeMember:
    """A source's ranges as they appear within a per-node fetch group.

    ``idx`` is the source's original position across all reducer sources —
    preserved so ``_PwriteSink``'s sequential pwrite layout stays consistent
    across FETCH batches (each range lands at its expected offset).
    """

    idx: int
    path: str
    ranges: List[Tuple[int, int]]


@dataclass(slots=True)
class _NodeGroup:
    """All sources on one ShuffleManager, grouped so we open ONE TCP
    connection per manager. Sources collapse to the same group when their
    ``(shuffle_id, node_id)`` — the manager's named-actor identity — matches.
    """

    shuffle_id: str
    node_id: str
    token: str
    members: List[_NodeMember] = field(default_factory=list)


class _PwriteSink:
    """A write-only file-like that ``os.pwrite``s sequentially from a
    fixed base offset on a shared fd. Multiple sinks (one per fetch thread) at
    DISJOINT base regions write the same fd concurrently without lock"""

    __slots__ = ("_fd", "_pos")

    def __init__(self, fd: int, base_offset: int):
        self._fd = fd
        self._pos = base_offset

    def write(self, data) -> int:
        mv = memoryview(data)
        total = 0
        n_total = len(mv)
        while total < n_total:
            total += os.pwrite(self._fd, mv[total:], self._pos + total)
        self._pos += total
        return total


def _prefetch_node_into(
    out_file_obj,
    shuffle_id: str,
    node_id: str,
    token: str,
    members: List[_NodeMember],
    max_bytes_per_fetch: int,
) -> None:
    """Open ONE keep-alive connection to the manager's endpoint and stream
    every member's shards into ``out_file_obj`` via 1+ multi-source FETCH
    frames, each bounded by ``max_bytes_per_fetch``.

    Manager is located via named-actor lookup (``_lookup_manager``); its
    endpoint is resolved at call-time via ``manager.endpoint.remote()`` —
    survives ShuffleManager restart on a new port. If the first connect
    fails with a transient error (typical sign of mid-restart), re-resolve
    once and retry; persistent failure surfaces as ShuffleFetchError so the
    operator layer can decide what to do.
    """

    def _resolve() -> Tuple[str, int]:
        # Process-global cache: a manager's (ip, port) is stable for its
        # lifetime, so this avoids a blocking ``ray.get`` actor round-trip per
        # node per task. That round-trip also released the task's CPU (Ray frees
        # the slot during a blocking get), which oversubscribed nodes; caching
        # removes both costs. No force-refresh path: if the manager restarts
        # on a new port mid-task, the cached endpoint goes stale and the
        # connect fails downstream; recovery is via Ray task retry (which
        # starts a fresh worker with an empty cache), not in-task re-resolve.
        key = _manager_name(shuffle_id, node_id)
        ep = _ENDPOINT_CACHE.get(key)
        if ep is not None:
            return ep
        manager = _lookup_manager(shuffle_id, node_id)
        # Bounded wait: manager is NodeAffinity-pinned (soft=False) with
        # max_restarts=-1, so if the node dies the actor stays PENDING forever
        # and a naked ``ray.get`` hangs indefinitely. ``ray.wait`` bails out
        # after 60s so we can probe node liveness. 60s comfortably absorbs a
        # same-node actor restart (seconds) yet still converts a node-death
        # hang into a typed error in bounded time.
        ref = manager.endpoint.remote()
        ready, _ = ray.wait([ref], timeout=60)
        if not ready:
            _classify_and_raise(
                node_id,
                exc=None,
                context="Timed out resolving ShuffleManager endpoint after 60s",
                num_sources=len(members),
            )
        try:
            ep = ray.get(ready[0])
        except ActorDiedError as e:
            _classify_and_raise(
                node_id, exc=e, context="ActorDiedError resolving endpoint",
                num_sources=len(members),
            )
        _ENDPOINT_CACHE[key] = ep
        return ep

    endpoint = _resolve()
    try:
        with open_shuffle_connection(endpoint, token) as conn:
            for batch in _chunk_members_by_bytes(members, max_bytes_per_fetch):
                sources = [(m.path, m.ranges) for m in batch]
                conn.fetch_into(sources, out_file_obj)
    except ShuffleFetchError:
        # Already the right type (ShuffleNodeLostError / ShuffleManagerAnomalyError
        # from _resolve, or a re-raised one from the connection code).
        raise
    except ActorDiedError as e:
        # Actor died mid-fetch — could be node loss or an anomaly on a
        # live node. Classify.
        _classify_and_raise(
            node_id, exc=e, context="ActorDiedError mid-fetch",
            num_sources=len(members),
        )
    except Exception as e:
        # TCP / network / other transient. Still cross-check node liveness:
        # if the node died mid-fetch, upgrade to ShuffleNodeLostError so the
        # user gets an actionable message instead of an opaque
        # ShuffleFetchError chain. Node alive → the underlying cause is
        # transient / local (e.g. mid-restart TCP refuse); keep the existing
        # ShuffleFetchError semantics for now.
        if _is_node_alive(node_id) is False:
            raise ShuffleNodeLostError(
                f"ShuffleManager node {node_id} died mid-fetch; "
                f"lost on-disk output for {len(members)} source(s). External-"
                f"shuffle does not yet reconstruct across node loss."
            ) from e
        raise ShuffleFetchError(
            f"fetch from {endpoint} (sources={len(members)}) failed: {e}"
        ) from e


def _chunk_members_by_bytes(
    members: List[_NodeMember],
    max_bytes: int,
) -> Iterable[List[_NodeMember]]:
    """Yield sub-batches of members whose total requested bytes ≤ ``max_bytes``.

    A source's ranges MAY be split across batches: the source appears as
    multiple pseudo-members with the same ``idx``/``path`` but disjoint
    range subsets, in the original range order. Individual ranges are
    NEVER split — each range is one Arrow IPC frame at the mapper, so a
    sub-range cut would break the reducer's decode. A single range larger
    than ``max_bytes`` therefore still gets its own oversized batch.
    """
    batch: List[_NodeMember] = []
    batch_bytes = 0
    for member in members:
        pending: List[Tuple[int, int]] = []
        for off, length in member.ranges:
            if (batch or pending) and batch_bytes + length > max_bytes:
                if pending:
                    batch.append(
                        _NodeMember(
                            idx=member.idx, path=member.path, ranges=pending
                        )
                    )
                    pending = []
                yield batch
                batch, batch_bytes = [], 0
            pending.append((off, length))
            batch_bytes += length
        if pending:
            batch.append(
                _NodeMember(idx=member.idx, path=member.path, ranges=pending)
            )
    if batch:
        yield batch


# fetch helpers
def _handles_to_sources(
    handles: List["ShuffleHandle"],
    partition_id: int,
) -> Tuple[List[_SourceRef], Optional[pa.Schema]]:
    """Extract per-partition source refs from a reducer's input handles.

    Skips handles that produced zero bytes for this partition. Also picks
    the first non-None output schema (for the empty-partition fallback).

    ``node_id`` may be missing on handles produced before that field was
    added — fall back to "" so ``_is_node_alive`` returns None
    ("inconclusive"), preserving pre-existing ShuffleFetchError semantics
    instead of ever raising a false-positive node-lost error.
    """
    sources: List[_SourceRef] = []
    output_schema: Optional[pa.Schema] = None
    for h in handles:
        if not isinstance(h, dict):
            h = ray.get(h)
        if output_schema is None:
            output_schema = h.get("schema")
        ranges = h["index"].get(partition_id) or []
        if ranges:
            sources.append(
                _SourceRef(
                    shuffle_id=h["shuffle_id"],
                    node_id=h.get("node_id", ""),
                    token=h["token"],
                    path=h["path"],
                    ranges=ranges,
                )
            )
    return sources, output_schema


def _group_by_manager(sources: List[_SourceRef]) -> List[_NodeGroup]:
    """Collapse sources by manager so each manager gets ONE TCP connection.

    Sources on the same manager share a ``(shuffle_id, node_id)`` — the
    manager's named-actor identity — which is used as the collapse key.
    ``idx`` on each member preserves the source's original position across
    all reducer sources — used to keep the sequential pwrite layout stable
    across FETCH batches.
    """
    by_key: Dict[Tuple[str, str], _NodeGroup] = {}
    for idx, s in enumerate(sources):
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
        group.members.append(_NodeMember(idx=idx, path=s.path, ranges=s.ranges))
    return list(by_key.values())


def _compute_prefetch_layout(
    groups: List[_NodeGroup],
) -> Tuple[int, List[int], List[int]]:
    """Assign each group a contiguous byte region in the shared prefetch.bin.

    Returns ``(total_size, base_offsets, per_group_sizes)`` — sizes are the
    ``4 + length`` framed byte totals (u32 len prefix + IPC bytes per range),
    base offsets are running cumulative sums. Fetch threads then pwrite each
    group's response frames at DISJOINT offsets → lock-free concurrent writes
    to one fd.
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


@ray.remote
def external_hash_shuffle_reduce_task(
    handles: List[ShuffleHandle],
    partition_id: int,
    reduce_fn: ReduceFn,
    prefetch_dir: Optional[str] = None,
    max_bytes_per_fetch: int = _DEFAULT_MAX_BYTES_PER_FETCH,
    target_max_block_size: Optional[int] = None,
    downstream_map_transformer: Optional[Any] = None,
    reduce_op_name: str = "ExternalHashShuffleReduce",
    downstream_map_task_kwargs: Optional[Dict[str, Any]] = None,
    downstream_map_target_max_block_size_override: Optional[int] = None,
) -> Generator[Union[Block, bytes], None, None]:
    """Fetch one partition's shards and stream ``reduce_fn`` output as
    ``(block, pickled metadata)`` pairs. Bytes stay out of plasma.

    Fetch + decode are pipelined: one thread per ShuffleManager pwrites
    response frames into a shared ``prefetch.bin`` at pre-assigned offsets,
    and this generator mmap-decodes each region as its future completes.

    The reducer always runs in blocking mode — accumulate the partition,
    reduce once, then finalize (mirrors v2's decision in #64481: repartition
    needs "one partition = one block", so incremental flush was dead code).
    Output is reshaped to ``target_max_block_size`` via ``BlockOutputBuffer``
    (a no-op passthrough when ``target_max_block_size`` is None).

    ``downstream_map_transformer`` runs a fused downstream map (typically
    Write) inline on each emitted block before yielding.
    """
    start_time_s = time.perf_counter()

    # Pull per-partition source refs + an output schema for the empty-partition
    # fallback path (so the N-block contract still emits a typed 0-row block
    # when no mapper produced any data for this partition_id).
    sources, output_schema = _handles_to_sources(handles, partition_id)

    def _yield_with_stats(block: Block):
        """Yield ``block`` then its pickled metadata. The two-yield protocol
        lets the executor slot ``StreamingGeneratorStats`` in between for
        accurate ``block_ser_time_s``."""
        exec_stats_builder = BlockExecStats.builder()
        exec_stats_builder.finish()
        gen_stats: StreamingGeneratorStats = yield block
        exec_stats = exec_stats_builder.build(
            block_ser_time_s=(gen_stats.object_creation_dur_s if gen_stats else None),
        )
        yield pickle.dumps(
            BlockMetadataWithSchema.from_block(
                block,
                block_exec_stats=exec_stats,
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )
        )

    def _emit(block: Block):
        if downstream_map_transformer is None:
            yield from _yield_with_stats(block)
            return
        from ray.data._internal.execution.interfaces import TaskContext

        for out_block in downstream_map_transformer.apply_transform(
            iter([block]),
            TaskContext(
                task_idx=partition_id,
                op_name=reduce_op_name,
                kwargs=downstream_map_task_kwargs or {},
                target_max_block_size_override=(
                    downstream_map_target_max_block_size_override
                ),
            ),
        ):
            yield from _yield_with_stats(out_block)

    # Empty-input shortcut: no shards for this partition, hand [] to
    # reduce_fn and emit whatever it yields (may be nothing — same as v2).
    if not sources:
        for block in reduce_fn(partition_id, []):
            yield from _emit(block)
        return

    # Decide where the prefetch file lives, and whether we own the cleanup.
    owns_dir = prefetch_dir is None
    if owns_dir:
        prefetch_dir = tempfile.mkdtemp(prefix=f"ray_shuffle_p{partition_id}_")
    else:
        os.makedirs(prefetch_dir, exist_ok=True)
    assert prefetch_dir is not None
    staging_dir: str = prefetch_dir
    prefetch_file = os.path.join(staging_dir, "prefetch.bin")

    groups = _group_by_manager(sources)

    try:
        # Fetch each source region in parallel, pwrite at pre-assigned offsets
        # into one prefetch.bin (disjoint → lock-free). Buffered pwrite lands
        # in page cache so the decode-side mmap reads hit cache.
        _fetch_threads = int(os.environ.get("RAY_DATA_SHUFFLE_FETCH_THREADS", "32"))
        total_size, base_offsets, node_sizes = _compute_prefetch_layout(groups)

        # Accumulator for the final reduce.
        accum_tables: List[pa.Table] = []
        accum_bytes: int = 0
        output_buffer: Optional[BlockOutputBuffer] = None

        def _flush(tables: List[pa.Table]):
            """Call reduce_fn on ``tables`` and yield reshaped output."""
            nonlocal output_buffer
            if output_buffer is None and target_max_block_size is not None:
                output_buffer = BlockOutputBuffer(
                    OutputBlockSizeOption.of(
                        target_max_block_size=target_max_block_size,
                    )
                )
            for block in reduce_fn(partition_id, tables):
                if output_buffer is None:
                    # target_max_block_size=None: emit blocks as-is.
                    yield from _emit(block)
                else:
                    output_buffer.add_block(block)
                    while output_buffer.has_next():
                        yield from _emit(output_buffer.next())

        # O_RDWR: same fd serves ``os.pwrite`` from fetch threads AND
        # ``os.pread`` from decode. Avoids a long-lived ``pa.memory_map``,
        # which would pin all touched pages resident and defeat
        # ``POSIX_FADV_DONTNEED``. With no mmap in the picture, the
        # per-region drop below can actually release memory.
        fd = os.open(prefetch_file, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            if total_size > 0:
                try:
                    os.posix_fallocate(fd, 0, total_size)  # Linux only
                except (AttributeError, OSError):
                    os.ftruncate(fd, total_size)

            def _fetch_one(args):
                base, size, group = args
                _prefetch_node_into(
                    _PwriteSink(fd, base),
                    group.shuffle_id,
                    group.node_id,
                    group.token,
                    group.members,
                    max_bytes_per_fetch,
                )
                return base, size

            n_threads = min(len(groups), max(1, _fetch_threads))
            work = list(zip(base_offsets, node_sizes, groups))
            # Rotate submission order by partition_id to spread simultaneous
            # fan-in across all managers (avoids every reducer hitting the same
            # first N managers when n_threads < #managers).
            if work:
                _rot = partition_id % len(work)
                work = work[_rot:] + work[:_rot]

            def _decode_region(base: int, size: int):
                """Walk frames in [base, base+size), accumulate for the
                final reduce. Hint the kernel to drop the region's pages
                at end so peak page cache is bounded by the currently-
                decoding region + the accumulator."""
                nonlocal accum_tables, accum_bytes
                pos = base
                end = base + size
                while pos < end:
                    length = struct.unpack(">I", os.pread(fd, 4, pos))[0]
                    ipc_buf = os.pread(fd, length, pos + 4)
                    pos += 4 + length
                    table = _read_ipc(ipc_buf)
                    accum_tables.append(table)
                    accum_bytes += table.nbytes
                # Region consumed — evict from page cache. Dirty pages
                # from the earlier pwrite are writeback-then-evicted;
                # clean pages drop immediately.
                _drop_pagecache(fd, base, size)

            with ThreadPoolExecutor(max_workers=n_threads) as ex:
                futs = [ex.submit(_fetch_one, w) for w in work]
                for fut in as_completed(futs):
                    base, size = fut.result()
                    if size > 0:
                        _decode_region(base, size)

            # Drain the accumulator tail.
            if accum_tables:
                yield from _flush(accum_tables)
                accum_tables = []
            if output_buffer is not None:
                output_buffer.finalize()
                while output_buffer.has_next():
                    yield from _emit(output_buffer.next())
        finally:
            os.close(fd)
    finally:
        # One file, one unlink. Idempotent on partial-failure paths.
        try:
            os.unlink(prefetch_file)
        except OSError:
            pass
        if owns_dir:
            try:
                os.rmdir(prefetch_dir)
            except OSError:
                pass
