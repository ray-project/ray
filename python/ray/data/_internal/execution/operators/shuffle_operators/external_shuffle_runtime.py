"""External-shuffle file-transport runtime: TCP wire protocol, per-node
ShuffleManager actor, prefetch layout, error hierarchy. Imported by the
map/reduce task bodies in ``external_shuffle_tasks``."""

import errno
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
    NoReturn,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa

import ray
from ray.exceptions import ActorDiedError

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

# The response frame encodes each range's payload length as u32,
# so no single range/IPC frame may exceed 4 GiB - 1.
# Checked at mapper write time so an oversized IPC buffer fails
# at the mapper task
_MAX_RANGE_BYTES: int = (1 << 32) - 1


# ----------------------------------------------------------------- Arrow IPC
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
    page cache. Effective on clean pages; on dirty pages the hint is recorded
    and eviction happens on next writeback."""
    if not _HAS_FADV_DONTNEED or length <= 0:
        return
    try:
        os.posix_fadvise(fd, offset, length, os.POSIX_FADV_DONTNEED)
    except OSError:
        # Best-effort: any failure is silently ignored — worst case is the
        # kernel keeps the pages a bit longer.
        pass


# Fetch helper class used by file server actor
class _FetchHandler(socketserver.StreamRequestHandler):
    """ Lifecycle: one handshake → loop of FETCH requests → CLOSE (or peer close).
    Each FETCH can carry multiple source paths, so a reducer with N sources on
    this node pays only one TCP round-trip handshake/setup overhead
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

        # Zero-copy serve via os.sendfile: validate every file + range FIRST so
        # a missing file / bad range still produces an error status before we
        # commit _STATUS_OK; then os.sendfile each range in REQUEST order
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
                    # ranges are typically read once per reducer
                    _drop_pagecache(fd, off, length)
                    srv.bytes_served += length
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
    allow_reuse_address = True
    daemon_threads = True
    # socket.listen() backlog. Default (5) is well below the SYN burst
    # when all reducers fan-out to every manager at once, causing silent
    # SYN drops → ETIMEDOUT. Kernel clamps to ``somaxconn`` if lower.
    request_queue_size = 256


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
        self._server = _ThreadingServer((ip, 0), _FetchHandler)
        self._server.token = token
        self._server.base_dir = self.base_dir
        self._server.bytes_served = 0
        self._host, self._port = self._server.server_address
        t = threading.Thread(target=self._server.serve_forever, daemon=True)
        t.start()

    def endpoint(self) -> Tuple[str, int]:
        return (self._host, self._port)

    def base(self) -> str:
        return self.base_dir

    def bytes_served(self) -> int:
        return self._server.bytes_served


# Import ``shutil`` lazily in the remote task body — module-level import
# would drag it into every driver / worker process that touches this
# module, and it's only needed here.
@ray.remote(num_cpus=0)
def _cleanup_shuffle_dir(base_dir: str) -> None:
    """Best-effort ``rmtree`` of a per-shuffle ``base_dir`` on the target
    node. Driver submits one of these per source node at end-of-shuffle
    via NodeAffinity, decoupling file cleanup from actor lifetime.
    Failure never propagates — OS tmpwatch is the fallback."""
    import shutil
    shutil.rmtree(base_dir, ignore_errors=True)


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
        out_file_obj: "_PwriteSink",
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
    """Open a TCP connection to ``endpoint`` and complete the handshake.

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


class ShuffleFetchError(RuntimeError):
    """Raised when a side-channel fetch fails (source gone / file lost).
    Surfaced so the executor/lineage can re-run the producer mapper."""


# Not a ShuffleFetchError subclass — reducer options list
# ``retry_exceptions=[ShuffleFetchError]``, and we don't want Ray to
# retry a disk-exhausted task (the disk stays full across retries).
class ShuffleDiskError(RuntimeError):
    """Raised when the reducer's local disk can't accommodate more
    prefetched bytes (ENOSPC / EDQUOT / similar terminal filesystem
    errors). Not retriable — Ray retries won't reclaim disk space."""


# errno values that indicate the reducer's local disk is exhausted.
# EDQUOT is glibc's quota-exceeded error; not all platforms expose it.
_DISK_EXHAUSTED_ERRNOS = frozenset(
    e for e in (
        errno.ENOSPC,
        getattr(errno, "EDQUOT", None),
    )
    if e is not None
)


def _is_disk_exhausted(exc: BaseException) -> bool:
    return isinstance(exc, OSError) and exc.errno in _DISK_EXHAUSTED_ERRNOS


class ShuffleNodeLostError(ShuffleFetchError):
    """Raised when a source ShuffleManager's node is confirmed dead
    (via ``ray.nodes()``).

    Recovery story: the mapper's return handle is a Ray ObjectRef;
    when its owning node dies, Ray Core marks the ref as lost, and on
    the reducer's next retry (see ``max_retries`` on
    ``_external_shuffle_reduce_task.options``) Ray Core's lineage
    kicks in — the mapper task is re-executed on a live node, the
    ObjectRef re-materializes with a fresh handle (new node_id, new
    path), and the retried reducer transparently reads from the new
    location. No app-level orchestration is needed.

    If this error still propagates past the reducer's retry budget, it
    means either (a) the same or another mapper's node kept dying across
    retries, or (b) lineage reconstruction failed (rare — e.g. Ray Core
    still has a stale "live" copy record). Either way we fail loudly
    instead of hanging on a permanently-pending
    (soft=False, max_restarts=-1) actor — the job should be re-run.
    """


class ShuffleManagerAnomalyError(ShuffleFetchError):
    """Raised when a ShuffleManager is unreachable while its node is alive.

    Under our configuration (``max_restarts=-1``, ``lifetime="detached"``,
    ``NodeAffinitySchedulingStrategy(node_id, soft=False)``) Ray restarts
    the actor on any crash, and a dead node surfaces as PENDING /
    ``ActorUnavailableError``, not ``ActorDiedError``. So an
    ``ActorDiedError`` on a live node means one of:

    - External ``ray.kill(actor, no_restart=True)`` from user code.
    - An unrecoverable initialization error keeping Ray from restarting.
    - A rare Ray-internal state race.

    Recovery is not automatic: the mapper's return ObjectRef is still
    "live" (Ray Core won't trigger lineage), files on disk have no server
    to serve them, and re-running the mapper requires app-level
    coordination we intentionally don't do. Retrying the job is the
    normal remedy.
    """


def _classify_and_raise(
    node_id: str,
    *,
    exc: Optional[BaseException],
    context: str,
    num_sources: int,
) -> NoReturn:
    """Diagnose a manager RPC failure and raise the appropriate typed error.

    Prefers structured hints from ``ActorDiedError`` (``.preempted``,
    ``.actor_init_failed``) — set by Ray Core itself and more reliable than
    a delayed ``ray.nodes()`` snapshot. Falls back to ``_is_node_alive``
    when Ray hasn't told us the cause structurally.

    - Node dead → ``ShuffleNodeLostError`` (Ray-Core lineage handles most
      cases via reducer retry; propagation past that budget = fatal).
    - Actor unreachable on live node → ``ShuffleManagerAnomalyError``
      (retry-the-job situation, no auto-recovery).
    - Node liveness inconclusive (GCS lag) → ``ShuffleFetchError``
      (transient; caller's retry loop can wait it out).
    """
    detail = f"context={context!r}; sources={num_sources}"

    # Ray Core's own hints on ``ActorDiedError`` are authoritative — no
    # ``ray.nodes()`` lag, and set at exception-construction time.
    if isinstance(exc, ActorDiedError):
        if exc.preempted:
            raise ShuffleNodeLostError(
                f"ShuffleManager node {node_id} preempted (autoscaler "
                f"drain / SIGTERM). Ray-Core lineage will re-execute "
                f"affected mappers on reducer retry. ({detail}) "
                f"[cause: {exc}]"
            ) from exc
        if exc.actor_init_failed:
            raise ShuffleManagerAnomalyError(
                f"ShuffleManager on node {node_id} failed to initialize; "
                f"Ray gave up restarting it. Not automatically recoverable — "
                f"retry the job. ({detail}) [cause: {exc}]"
            ) from exc

    # Fall back to ``ray.nodes()`` for anything not structurally tagged.
    alive = _is_node_alive(node_id)
    if alive is False:
        raise ShuffleNodeLostError(
            f"ShuffleManager node {node_id} is dead — on-disk shuffle "
            f"output lost with the node. Ray-Core lineage will re-execute "
            f"the affected mapper on the reducer's next retry; if this "
            f"error propagates past that budget, the job needs to be "
            f"re-run. ({detail})"
        ) from exc
    if alive is True:
        raise ShuffleManagerAnomalyError(
            f"ShuffleManager on node {node_id} is unreachable (actor "
            f"terminated, node still alive). Possible causes: external "
            f"ray.kill(), an unrecoverable actor initialization error, or "
            f"a rare Ray-internal state race. Not automatically recoverable "
            f"— the mapper's return ObjectRef is still 'live' (so Ray-Core "
            f"lineage won't re-run it) but no manager exists to serve it. "
            f"Retry the job. ({detail})"
        ) from exc
    # alive is None: GCS inconclusive. Treat as transient — the caller's
    # in-place retry loop can wait for GCS to catch up.
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


# Process-global cache of ShuffleManager endpoints: {actor_name: (ip, port)}.
# Stale entries are popped by ``_prefetch_node_into`` on retry. The lock
# guards concurrent access from reducer fetch threads.
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
    DISJOINT base regions write the same fd concurrently without lock.

    ``reset()`` rewinds ``_pos`` to the base offset so a fetch attempt that
    partially wrote can be retried in-place — subsequent ``os.pwrite``s
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
            total += os.pwrite(self._fd, mv[total:], self._pos + total)
        self._pos += total
        return total


# In-place retry when the manager is transiently unreachable (actor
# restart on the same node, network hiccup). Keep trying at a fixed
# interval while the node is alive; give up only when node liveness
# returns False or the total deadline expires.
_FETCH_RETRY_DEADLINE_S = 300.0
_FETCH_RETRY_INTERVAL_S = 5.0


def _handle_transient_fetch_error(
    exc: BaseException,
    *,
    shuffle_id: str,
    node_id: str,
    out_file_obj: "_PwriteSink",
    members: List[_NodeMember],
    deadline: float,
    attempts: int,
) -> None:
    """Classify a mid-fetch exception and either raise a terminal typed
    error or return to let the caller sleep + retry.

    Returning means "transient — try again". Raising ends the retry loop
    with one of ``ShuffleDiskError`` (disk full), ``ShuffleNodeLostError``
    (source node confirmed dead), or ``ShuffleFetchError`` (retry deadline
    exhausted). Handles the shared side-effects on every retry path:
    sink rewind (idempotent overwrite on retry) and endpoint-cache
    eviction (force re-resolve in case the manager restarted on a new
    port).
    """
    # Disk exhausted is terminal — no amount of retry frees space.
    if _is_disk_exhausted(exc):
        raise ShuffleDiskError(
            f"Disk exhausted while writing prefetch.bin for node "
            f"{node_id} (sources={len(members)}): {exc}"
        ) from exc

    # Any pwrites we did on this attempt land at the same offsets on the
    # next attempt (server re-sends the same bytes), so rewind the sink
    # so subsequent writes don't overrun into the next fetch group's region.
    out_file_obj.reset()
    with _ENDPOINT_CACHE_LOCK:
        _ENDPOINT_CACHE.pop(_manager_name(shuffle_id, node_id), None)

    if _is_node_alive(node_id) is False:
        raise ShuffleNodeLostError(
            f"ShuffleManager node {node_id} died mid-fetch; "
            f"lost on-disk output for {len(members)} source(s). "
            f"Ray-Core lineage will re-execute affected mappers on "
            f"reducer retry."
        ) from exc

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ShuffleFetchError(
            f"Fetch from node {node_id} did not succeed after "
            f"{attempts} attempts in "
            f"{_FETCH_RETRY_DEADLINE_S:.0f}s (sources="
            f"{len(members)}). Last error: {exc}"
        ) from exc

    # Node alive (or inconclusive) — wait, then retry in place.
    time.sleep(min(_FETCH_RETRY_INTERVAL_S, remaining))


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

    Retry policy: transient failures (TCP break, actor restarting) sleep
    ``_FETCH_RETRY_INTERVAL_S`` and retry in place, up to
    ``_FETCH_RETRY_DEADLINE_S``. Escalation only on a hard verdict:
    - Source node confirmed dead → ``ShuffleNodeLostError``.
    - Actor confirmed dead on a live node → ``ShuffleManagerAnomalyError``.
    - Retry deadline exhausted → ``ShuffleFetchError``.
    """

    def _resolve() -> Tuple[str, int]:
        # Process-global cache: a manager's (ip, port) is stable for its
        # lifetime, so we avoid a blocking ``ray.get`` per node per task.
        # Staleness (manager restarted on a new port) is caught downstream
        # at connect time and cleared by ``_handle_transient_fetch_error``.
        key = _manager_name(shuffle_id, node_id)
        with _ENDPOINT_CACHE_LOCK:
            ep = _ENDPOINT_CACHE.get(key)
        if ep is not None:
            return ep
        manager = _lookup_manager(shuffle_id, node_id)
        # Bounded wait: manager is NodeAffinity-pinned (soft=False) with
        # max_restarts=-1, so if the node dies the actor stays PENDING forever
        # and a naked ``ray.get`` hangs indefinitely.
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
        with _ENDPOINT_CACHE_LOCK:
            _ENDPOINT_CACHE[key] = ep
        return ep

    deadline = time.monotonic() + _FETCH_RETRY_DEADLINE_S
    attempts = 0

    while True:
        attempts += 1
        try:
            endpoint = _resolve()
            with open_shuffle_connection(endpoint, token) as conn:
                for batch in _chunk_members_by_bytes(
                    members, max_bytes_per_fetch
                ):
                    sources = [(m.path, m.ranges) for m in batch]
                    conn.fetch_into(sources, out_file_obj)
            return
        except ShuffleFetchError:
            # Terminal typed error from _resolve / classify — propagate.
            raise
        except ActorDiedError as e:
            _classify_and_raise(
                node_id, exc=e, context="ActorDiedError mid-fetch",
                num_sources=len(members),
            )
        except Exception as e:
            _handle_transient_fetch_error(
                e,
                shuffle_id=shuffle_id,
                node_id=node_id,
                out_file_obj=out_file_obj,
                members=members,
                deadline=deadline,
                attempts=attempts,
            )


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
