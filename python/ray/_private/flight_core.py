"""Shared Flight server and transfer primitives.

Used by both the Arrow Flight RDT backend
(python/ray/experimental/rdt/arrow_flight_transport.py) and the legacy
"native" Flight store (python/ray/_private/flight_object_store.py). One
Flight server per worker process hosts both flows.
"""

import errno
import os
import select
import socket
import struct
import sys
import threading
from typing import Any, Dict, List, Optional


class _RecordingSink:
    """Sink that captures write pointers instead of copying bytes.

    Used with pa.PythonFile so the IPC writer can "write" into it without
    actually serializing. Large column buffers (pa.Buffer) have their
    address/size recorded directly. Small metadata chunks are copied into
    a py_buffer that's kept alive for the subsequent scatter-write.
    """

    def __init__(self):
        self._chunks: List[tuple] = []
        self._refs: List[Any] = []
        self._offset = 0

    def write(self, data):
        import pyarrow as pa

        if isinstance(data, pa.Buffer):
            self._chunks.append((data.address, data.size))
            self._refs.append(data)
            self._offset += data.size
            return data.size
        b = bytes(data) if not isinstance(data, bytes) else data
        buf = pa.py_buffer(b)
        self._chunks.append((buf.address, len(b)))
        self._refs.append(buf)
        self._offset += len(b)
        return len(b)

    def tell(self):
        return self._offset

    def writable(self):
        return True

    @property
    def closed(self):
        return False

    def flush(self):
        pass

    @property
    def scatter_list(self):
        return self._chunks


def _serialize_to_recording_sink(table) -> "_RecordingSink":
    """Walk the table once via the recording sink, producing both the IPC
    stream size (sink.tell()) and the scatter-list of (addr, size) tuples
    that the producer will hand to process_vm_writev. Buffer references are
    held in sink._refs so the underlying memory stays alive while the sink
    is cached.
    """
    import pyarrow as pa
    import pyarrow.ipc as ipc

    sink = _RecordingSink()
    pf = pa.PythonFile(sink, mode="w")
    writer = ipc.new_stream(pf, table.schema)
    writer.write_table(table)
    writer.close()
    return sink


def _dataplane() -> str:
    """Same-node dataplane backend: "vm" (process_vm_writev, Linux-only) or
    "shm" (anonymous shared memory + SCM_RIGHTS fd passing, Linux + macOS).
    Defaults to "vm" to preserve existing behavior."""
    return os.environ.get("RAY_FLIGHT_DATAPLANE", "vm").lower()


def _transport() -> str:
    """Cross-node transport: "flight" (Arrow Flight / gRPC DoGet) or "tcp"
    (raw length-prefixed Arrow IPC stream over a plain socket). "tcp" drops the
    HTTP/2 + protobuf stack, so the consumer worker spends far less CPU on the
    receive path. Defaults to "flight"."""
    return os.environ.get("RAY_FLIGHT_TRANSPORT", "flight").lower()


def _close_shm_entry(entry) -> None:
    """Close the mmap and fd of a `self._shm` entry (fd, mm, size)."""
    if entry is None:
        return
    fd, mm, _size = entry
    try:
        mm.close()
    except Exception:
        pass
    try:
        os.close(fd)
    except OSError:
        pass


def _serialize_table_bytes(table):
    """Serialize a table to a contiguous Arrow IPC stream (a pa.Buffer)."""
    import pyarrow as pa
    import pyarrow.ipc as ipc

    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
    return sink.getvalue()


def _write_shm_region(buf):
    """Copy a buffer-like into a fresh anonymous shm region; (fd, mm, size)."""
    import mmap

    from ray._raylet import shm_create_buffer

    src = memoryview(buf).cast("B")
    size = src.nbytes
    fd = shm_create_buffer(size)
    try:
        mm = mmap.mmap(fd, size)
    except Exception:
        os.close(fd)
        raise
    dst = memoryview(mm).cast("B")
    try:
        if size:
            dst[:] = src
    finally:
        dst.release()
    return (fd, mm, size)


class ReceivedManifest:
    """Segments a consumer received for a manifest table, holding their fds open
    so they can be forwarded (re-exported) to a downstream stage.

    Each segment is a dict {kind, field, fd, mm, size}. If the caller is not
    forwarding (final consumer), call close() to drop the fds — the mmaps stay
    valid and are held alive by the reconstructed table's buffers.
    """

    def __init__(self, segments):
        self.segments = segments
        self._transferred = False

    def close(self) -> None:
        if self._transferred:
            return
        for seg in self.segments:
            try:
                os.close(seg["fd"])
            except OSError:
                pass
        self.segments = []


def _close_quietly(sock) -> None:
    """Close a socket, ignoring errors (already-closed / broken pipe)."""
    try:
        sock.close()
    except OSError:
        pass


def _recv_exact(conn, n: int) -> Optional[bytes]:
    """Read exactly `n` bytes from `conn`, or None if the peer closed early."""
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = conn.recv(remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ---------------------------------------------------- TCP zero-copy transmit
#
# SO_ZEROCOPY (Linux >= 4.14) lets the kernel DMA straight from the producer's
# table buffers instead of copying them into the socket send buffer, cutting
# producer-side CPU on the raw-TCP transport. It is send-side only; the consumer
# receive path is unaffected (RX zero-copy is a separate mmap-based mechanism).
#
# The stdlib `socket` module only exposes these constants on newer interpreters
# (SO_ZEROCOPY / MSG_ZEROCOPY landed in Python 3.13), so on older Pythons they
# are missing even when the running kernel supports the feature. Set them from
# the well-known Linux uapi values so we don't silently lose zero-copy just
# because the interpreter is old. Values are from the kernel uapi headers
# (asm-generic/socket.h, bits/socket.h) and are correct for the x86_64 and
# aarch64 architectures Ray targets.
_IS_LINUX = sys.platform.startswith("linux")


def _linux_socket_const(name: str, linux_value: int) -> Optional[int]:
    """Return socket.<name> if the stdlib exposes it, else the known Linux uapi
    value on Linux, else None (the constant is meaningless off Linux)."""
    val = getattr(socket, name, None)
    if val is not None:
        return val
    return linux_value if _IS_LINUX else None


_SO_ZEROCOPY = _linux_socket_const("SO_ZEROCOPY", 60)
_MSG_ZEROCOPY = _linux_socket_const("MSG_ZEROCOPY", 0x4000000)
_MSG_ERRQUEUE = _linux_socket_const("MSG_ERRQUEUE", 0x2000)
# Below this size the page-pinning + completion-notification overhead of
# MSG_ZEROCOPY outweighs the copy it avoids (the kernel docs peg the crossover
# near ~10 KiB), so small chunks use a plain copying send.
_ZEROCOPY_MIN_BYTES = 16 * 1024
# struct sock_extended_err.ee_origin value for a zero-copy completion.
_SO_EE_ORIGIN_ZEROCOPY = 5


class ZeroCopyUnsupportedError(RuntimeError):
    """Raised when RAY_FLIGHT_TRANSPORT=tcp is requested but the platform can't
    provide the SO_ZEROCOPY send path the raw-TCP transport relies on."""


def _tcp_zerocopy_supported() -> bool:
    return (
        _SO_ZEROCOPY is not None
        and _MSG_ZEROCOPY is not None
        and _MSG_ERRQUEUE is not None
    )


def _require_tcp_zerocopy() -> None:
    """Fail loudly if the raw-TCP transport can't use SO_ZEROCOPY on this host.

    Called when transport="tcp" is selected so an unsupported platform errors
    out at server-start time instead of silently degrading to copying sends.
    Verifies both that the constants are known and that the running kernel
    actually accepts SO_ZEROCOPY (setsockopt succeeds), which catches kernels
    older than 4.14.
    """
    if not _IS_LINUX:
        raise ZeroCopyUnsupportedError(
            "RAY_FLIGHT_TRANSPORT=tcp requires Linux SO_ZEROCOPY support, but "
            f"this process is running on {sys.platform!r}."
        )
    if not _tcp_zerocopy_supported():
        raise ZeroCopyUnsupportedError(
            "RAY_FLIGHT_TRANSPORT=tcp requires SO_ZEROCOPY/MSG_ZEROCOPY, which "
            "could not be resolved on this platform."
        )
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.setsockopt(socket.SOL_SOCKET, _SO_ZEROCOPY, 1)
    except OSError as e:
        raise ZeroCopyUnsupportedError(
            "RAY_FLIGHT_TRANSPORT=tcp requires SO_ZEROCOPY but the kernel "
            f"rejected it ({e}); a Linux kernel >= 4.14 is required."
        ) from e
    finally:
        probe.close()


def _reap_zerocopy(conn, outstanding: int) -> None:
    """Drain `outstanding` MSG_ZEROCOPY completion notifications from the
    socket's error queue.

    Each flagged send() gets a sequential id; completions arrive as inclusive
    [lo, hi] id ranges in a struct sock_extended_err on the error queue. Reaping
    keeps the queue from filling (a full queue silently makes the kernel fall
    back to copying) and confirms the buffers are free to reuse. Bounded by a
    short poll budget so a coalesced/lost notification can't hang the handler.
    """
    if outstanding <= 0:
        return
    poller = select.poll()
    poller.register(conn.fileno(), select.POLLERR)
    ancbytes = socket.CMSG_SPACE(128)
    reaped = 0
    stalls = 0
    while reaped < outstanding and stalls < 4:
        if not poller.poll(50):  # ms
            stalls += 1
            continue
        try:
            _data, ancdata, _flags, _addr = conn.recvmsg(0, ancbytes, _MSG_ERRQUEUE)
        except OSError:
            break
        before = reaped
        for _level, _type, cdata in ancdata:
            if len(cdata) < 16:
                continue
            # ee_errno(I) ee_origin(B) ee_type(B) ee_code(B) ee_pad(B)
            # ee_info(I)=lo  ee_data(I)=hi
            fields = struct.unpack_from("=IBBBBII", cdata, 0)
            ee_origin, lo, hi = fields[1], fields[5], fields[6]
            if ee_origin == _SO_EE_ORIGIN_ZEROCOPY:
                reaped += hi - lo + 1
        if reaped == before:
            stalls += 1


def _send_ipc_payload(conn, refs) -> None:
    """Send the ordered IPC-stream chunks in `refs` over `conn`.

    Chunks at or above the zero-copy threshold are sent with MSG_ZEROCOPY
    (kernel DMAs from the table's own buffers); sub-threshold chunks use a plain
    copying send since the pinning overhead isn't worth it below the crossover.
    Zero-copy support is guaranteed by the _require_tcp_zerocopy() check at
    server start. `refs` are the producer's live table buffers, so they stay
    valid until the completions are reaped.
    """
    # Zero-copy support is validated up front in _require_tcp_zerocopy() when
    # the TCP server starts, so enabling it here should always succeed; a
    # failure means the host changed underneath us and we surface it loudly
    # rather than quietly reverting to copying sends.
    try:
        conn.setsockopt(socket.SOL_SOCKET, _SO_ZEROCOPY, 1)
    except OSError as e:
        raise ZeroCopyUnsupportedError(
            f"failed to enable SO_ZEROCOPY on the TCP transport socket ({e})"
        ) from e

    outstanding = 0
    for ref in refs:
        mv = memoryview(ref).cast("B")
        n = mv.nbytes
        if not n:
            continue
        if n < _ZEROCOPY_MIN_BYTES:
            conn.sendall(mv)
            continue
        off = 0
        while off < n:
            try:
                sent = conn.send(mv[off:], _MSG_ZEROCOPY)
            except OSError as e:
                if e.errno == errno.ENOBUFS:
                    # Pinned-memory budget hit; drain in-flight then copy-send.
                    _reap_zerocopy(conn, outstanding)
                    outstanding = 0
                    conn.sendall(mv[off:])
                    break
                raise
            off += sent
            outstanding += 1
    _reap_zerocopy(conn, outstanding)


class FlightCore:
    """Per-process Flight server + table storage + transfer primitives.

    Thread-safe. Instantiated once per worker (see get_flight_core()) and
    shared across all code paths that need Flight-based transfer.
    """

    def __init__(self):
        self._tables: Dict[str, Any] = {}  # key -> pa.Table
        # key -> _RecordingSink. Built once at put time. Holds the
        # scatter-list (addresses + sizes) the producer hands to
        # process_vm_writev on every fetch, plus a refs list that keeps
        # the underlying buffers alive even if the table dict were to
        # drop the table independently.
        self._sinks: Dict[str, Any] = {}
        # key -> (fd: int, mm: mmap.mmap, size: int). Populated at put time when
        # the shm dataplane is active: the IPC stream is copied once into an
        # anonymous shared-memory buffer, whose fd is then handed to same-node
        # consumers over an AF_UNIX socket (SCM_RIGHTS) for zero-copy reads.
        self._shm: Dict[str, Any] = {}
        # Multi-region "manifest" transfer (append-without-recopy). A logical
        # table is a base region (full-table IPC stream) plus zero or more
        # single-column regions appended downstream. `_segments` maps a
        # per-segment key -> (fd, mm, size) that the fd server can hand out;
        # `_manifests` maps an object key -> the ordered list of segment
        # descriptors describing how to reconstruct it. A stage that appends a
        # column re-registers (forwards) the segments it received plus its new
        # column, so the whole chain's regions are served from this process.
        self._segments: Dict[str, Any] = {}
        self._manifests: Dict[str, list] = {}
        self._lock = threading.Lock()
        self._server = None
        self._server_thread = None
        self._uri: Optional[str] = None
        self._clients: Dict[str, Any] = {}
        # AF_UNIX server for handing shared-memory fds to same-node consumers.
        self._fd_sock = None
        self._fd_sock_path: Optional[str] = None
        self._fd_server_thread = None
        # Raw-TCP transport server (alternative to Flight for cross-node fetch).
        # Streams the length-prefixed Arrow IPC bytes over a plain socket,
        # bypassing gRPC/HTTP-2 to cut consumer-side receive CPU.
        self._tcp_sock = None
        self._tcp_addr: Optional[str] = None
        self._tcp_server_thread = None
        # Consumer-side pool of persistent TCP connections, keyed by producer
        # "host:port". Reusing a connection across fetches avoids a TCP
        # handshake (and a producer-side thread spawn) per transfer. Each active
        # fetch checks out one idle socket (or opens a new one) and checks it
        # back in on success; a fetch never shares a socket concurrently.
        self._tcp_client_pool: Dict[str, List[Any]] = {}
        self._tcp_pool_lock = threading.Lock()

    # ------------------------------------------------------------ public API

    def ensure_server(self) -> str:
        """Start the Flight server on first call; return its URI."""
        if self._uri is not None:
            return self._uri
        with self._lock:
            if self._uri is not None:
                return self._uri
            self._start_server_locked()
            return self._uri

    @property
    def uri(self) -> Optional[str]:
        return self._uri

    def ensure_fd_server(self) -> str:
        """Start the AF_UNIX shared-memory fd server on first call; return its
        socket path. Used by the shm dataplane to hand fds to consumers."""
        if self._fd_sock_path is not None:
            return self._fd_sock_path
        with self._lock:
            if self._fd_sock_path is not None:
                return self._fd_sock_path
            self._start_fd_server_locked()
            return self._fd_sock_path

    def ensure_tcp_server(self) -> str:
        """Start the raw-TCP transport server on first call; return its
        "host:port" address. Used by the tcp cross-node transport."""
        if self._tcp_addr is not None:
            return self._tcp_addr
        with self._lock:
            if self._tcp_addr is not None:
                return self._tcp_addr
            self._start_tcp_server_locked()
            return self._tcp_addr

    def put(self, key: str, table) -> int:
        """Store `table` under `key`; return its IPC stream size.

        Walks the table exactly once via the recording sink — this captures
        the scatter-list that subsequent fetches will use, so
        _handle_scatter_write doesn't have to re-serialize on every fetch.
        """
        sink = _serialize_to_recording_sink(table)
        size = sink.tell()
        with self._lock:
            self._tables[key] = table
            self._sinks[key] = sink
        if _dataplane() == "shm":
            self._materialize_shm(key, sink, size)
        return size

    def get(self, key: str):
        """Look up a table by key (does not pop)."""
        with self._lock:
            return self._tables.get(key)

    def pop(self, key: str):
        """Remove and return a table by key."""
        with self._lock:
            self._sinks.pop(key, None)
            shm_entry = self._shm.pop(key, None)
            table = self._tables.pop(key, None)
        _close_shm_entry(shm_entry)
        return table

    def delete(self, key: str) -> None:
        with self._lock:
            self._tables.pop(key, None)
            self._sinks.pop(key, None)
            shm_entry = self._shm.pop(key, None)
        _close_shm_entry(shm_entry)

    def fetch_via_vm(self, flight_uri: str, key: str, size: int):
        """Same-node consumer path: allocate a local buffer, ask the producer
        to scatter-write the IPC stream into it via process_vm_writev, then
        reassemble the pa.Table with zero-copy column views.
        """
        import pyarrow as pa
        import pyarrow.flight as flight
        import pyarrow.ipc as ipc

        local_buf = pa.allocate_buffer(size)

        key_bytes = key.encode("utf-8")
        body = struct.pack("<I", len(key_bytes))
        body += key_bytes
        body += struct.pack("<i", os.getpid())
        body += struct.pack("<Q", local_buf.address)
        body += struct.pack("<q", size)

        client = self._get_client(flight_uri)
        action = flight.Action("scatter_write_vm", body)
        list(client.do_action(action))

        return ipc.open_stream(local_buf).read_all()

    def fetch_via_shm(self, fd_sock_path: str, key: str, size: int):
        """Same-node consumer path (Linux + macOS): connect to the producer's
        AF_UNIX fd server, receive the shared-memory fd for `key` via SCM_RIGHTS,
        mmap it read-only, and reassemble the pa.Table with zero-copy views.

        The mmap is kept alive by the returned table (pa.py_buffer holds a
        reference), so the fd can be closed immediately after mapping.
        """
        import mmap

        import pyarrow as pa
        import pyarrow.ipc as ipc

        key_bytes = key.encode("utf-8")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(fd_sock_path)
        try:
            sock.sendall(struct.pack("<I", len(key_bytes)) + key_bytes)
            # Producer replies with an 8-byte size and (on success) one fd.
            msg, fds, _flags, _addr = socket.recv_fds(sock, 8, 1)
            if len(msg) < 8:
                raise OSError(f"short fd-server reply for key {key}")
            recv_size = struct.unpack("<q", msg)[0]
            if recv_size < 0 or not fds:
                raise KeyError(f"shm object not found: {key}")
            fd = fds[0]
            try:
                mm = mmap.mmap(fd, recv_size, prot=mmap.PROT_READ)
            finally:
                # The mapping stays valid after the fd is closed.
                os.close(fd)
            buf = pa.py_buffer(mm)
            return ipc.open_stream(buf).read_all()
        finally:
            sock.close()

    def fetch_via_flight(self, flight_uri: str, key: str):
        """Cross-node consumer path: plain Flight DoGet RPC."""
        import pyarrow.flight as flight

        client = self._get_client(flight_uri)
        ticket = flight.Ticket(key.encode("utf-8"))
        return client.do_get(ticket).read_all()

    def fetch_via_tcp(self, tcp_addr: str, key: str, size: int):
        """Cross-node consumer path over a raw socket (no gRPC).

        Sends the key, receives an 8-byte length then exactly that many bytes of
        the Arrow IPC stream into a single buffer, and reconstructs the table
        zero-copy (arrays view the received buffer, which the table keeps alive).
        The only payload copy is the kernel->buffer recv; there is no HTTP/2
        framing, flow-control, or protobuf work on the consumer worker.

        The connection is drawn from a per-producer pool and returned on
        success, so steady-state fetches skip the TCP handshake. A reused socket
        that turns out to be stale is transparently retried once on a fresh one.
        """
        import pyarrow as pa
        import pyarrow.ipc as ipc

        key_bytes = key.encode("utf-8")
        req = struct.pack("<I", len(key_bytes)) + key_bytes

        last_err: Optional[OSError] = None
        for _attempt in range(2):
            sock, reused = self._tcp_checkout(tcp_addr)
            try:
                sock.sendall(req)
                hdr = _recv_exact(sock, 8)
                if hdr is None:
                    raise OSError(f"short tcp reply for key {key}")
                (recv_size,) = struct.unpack("<q", hdr)
                if recv_size < 0:
                    # Miss: the connection is still healthy, so keep it pooled.
                    self._tcp_checkin(tcp_addr, sock)
                    raise KeyError(f"object not found: {key}")

                # Uninitialized Arrow buffer (like fetch_via_vm) rather than
                # bytearray(recv_size): the latter zero-fills the whole buffer,
                # which recv_into immediately overwrites -- a wasted full memset
                # (and page-fault storm) over every byte received. allocate_buffer
                # leaves it uninitialized, so we only pay for the recv copy.
                buf = pa.allocate_buffer(recv_size)
                view = memoryview(buf)
                got = 0
                while got < recv_size:
                    n = sock.recv_into(view[got:], recv_size - got)
                    if n == 0:
                        raise OSError(
                            f"tcp stream truncated for key {key}: {got}/{recv_size}"
                        )
                    got += n
                self._tcp_checkin(tcp_addr, sock)
                return ipc.open_stream(buf).read_all()
            except OSError as e:
                # A broken socket must not go back in the pool.
                _close_quietly(sock)
                last_err = e
                # Only a *reused* socket is worth retrying (the producer may
                # have reaped an idle connection); a fresh one failing is real.
                if reused:
                    continue
                raise
        raise last_err

    def _tcp_checkout(self, addr: str):
        """Return (sock, reused): an idle pooled connection to `addr` if one is
        available, else a freshly connected socket. `reused` distinguishes the
        two so the caller can retry a stale pooled socket."""
        with self._tcp_pool_lock:
            pool = self._tcp_client_pool.get(addr)
            if pool:
                return pool.pop(), True
        host, port = addr.rsplit(":", 1)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.connect((host, int(port)))
        return sock, False

    def _tcp_checkin(self, addr: str, sock) -> None:
        """Return a healthy connection to the pool for the next fetch."""
        with self._tcp_pool_lock:
            self._tcp_client_pool.setdefault(addr, []).append(sock)

    def send_delete_rpc(self, flight_uri: str, key: str) -> None:
        """Native path helper: ask producer to drop a key."""
        import pyarrow.flight as flight

        try:
            client = self._get_client(flight_uri)
            action = flight.Action("delete", key.encode("utf-8"))
            list(client.do_action(action))
        except Exception:
            pass

    # ------------------------------------------------- multi-region manifests

    def store_shared(self, key: str, table) -> dict:
        """Store `table` as a single-segment manifest; return its locator dict.

        The locator is a small dict (embeddable in the Ray object store) of
        {fd_sock_path, segments:[descriptor...]} that a same-node consumer
        passes to fetch_shared."""
        self.ensure_fd_server()
        fd, mm, size = _write_shm_region(_serialize_table_bytes(table))
        seg_key = f"{key}::0"
        seg = {"seg_key": seg_key, "size": size, "kind": "base", "field": None}
        with self._lock:
            self._segments[seg_key] = (fd, mm, size)
            self._manifests[key] = [seg]
        return {"fd_sock_path": self._fd_sock_path, "segments": [dict(seg)]}

    def fetch_shared(self, manifest: dict):
        """Reconstruct a manifest table zero-copy across all its shm regions.

        Returns (table, ReceivedManifest). The handle keeps the segment fds open
        for forwarding via store_shared_append; a final consumer should call
        handle.close() (the table keeps the mmaps alive on its own)."""
        import mmap

        import pyarrow as pa
        import pyarrow.ipc as ipc

        sock_path = manifest["fd_sock_path"]
        received = []
        table = None
        for seg in manifest["segments"]:
            fd, size = self._recv_segment_fd(sock_path, seg["seg_key"])
            mm = mmap.mmap(fd, size, prot=mmap.PROT_READ)
            seg_table = ipc.open_stream(pa.py_buffer(mm)).read_all()
            received.append(
                {
                    "kind": seg["kind"],
                    "field": seg["field"],
                    "fd": fd,
                    "mm": mm,
                    "size": size,
                }
            )
            if seg["kind"] == "base":
                table = seg_table
            else:
                table = table.append_column(seg["field"], seg_table.column(0))
        return table, ReceivedManifest(received)

    def store_shared_append(
        self, new_key: str, handle: ReceivedManifest, field, column
    ) -> dict:
        """Produce a new manifest = the segments in `handle` + one appended
        column, materializing ONLY the new column into shm. The handle's fds are
        re-exported from this process (ownership transfers to this manifest), so
        the base/upstream columns are never re-copied."""
        self.ensure_fd_server()
        import pyarrow as pa

        col_table = pa.table({field: column})
        fd, mm, size = _write_shm_region(_serialize_table_bytes(col_table))
        out = []
        with self._lock:
            for i, seg in enumerate(handle.segments):
                seg_key = f"{new_key}::{i}"
                self._segments[seg_key] = (seg["fd"], seg["mm"], seg["size"])
                out.append(
                    {
                        "seg_key": seg_key,
                        "size": seg["size"],
                        "kind": seg["kind"],
                        "field": seg["field"],
                    }
                )
            col_seg_key = f"{new_key}::{len(handle.segments)}"
            self._segments[col_seg_key] = (fd, mm, size)
            out.append(
                {"seg_key": col_seg_key, "size": size, "kind": "column", "field": field}
            )
            self._manifests[new_key] = out
        handle._transferred = True  # fds now owned by self._segments
        return {"fd_sock_path": self._fd_sock_path, "segments": [dict(s) for s in out]}

    def delete_shared(self, key: str) -> None:
        with self._lock:
            segs = self._manifests.pop(key, None)
            entries = []
            if segs:
                for seg in segs:
                    entry = self._segments.pop(seg["seg_key"], None)
                    if entry is not None:
                        entries.append(entry)
        for entry in entries:
            _close_shm_entry(entry)

    def _recv_segment_fd(self, sock_path: str, seg_key: str):
        """Connect to an fd server and receive one segment's fd + size."""
        key_bytes = seg_key.encode("utf-8")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(sock_path)
        try:
            sock.sendall(struct.pack("<I", len(key_bytes)) + key_bytes)
            msg, fds, _flags, _addr = socket.recv_fds(sock, 8, 1)
            if len(msg) < 8:
                raise OSError(f"short fd-server reply for segment {seg_key}")
            (size,) = struct.unpack("<q", msg)
            if size < 0 or not fds:
                raise KeyError(f"shm segment not found: {seg_key}")
            return fds[0], size
        finally:
            sock.close()

    # ----------------------------------------------------------- internals

    def _materialize_shm(self, key: str, sink: "_RecordingSink", size: int) -> None:
        """Copy the IPC stream captured by `sink` once into an anonymous
        shared-memory buffer and record it under `key`. Idempotent per key."""
        import mmap

        from ray._raylet import shm_create_buffer

        with self._lock:
            if key in self._shm:
                return

        fd = shm_create_buffer(size)
        try:
            mm = mmap.mmap(fd, size)
        except Exception:
            os.close(fd)
            raise
        # Copy each captured chunk contiguously into the shared buffer. The
        # sink's `_refs` are buffer-protocol objects (pa.Buffer / py_buffer)
        # parallel to its scatter-list, so this is a straight memcpy per chunk.
        # Cast both sides to unsigned bytes so slice assignment matches formats
        # (mmap is "B"; a pa.Buffer memoryview may report a different format).
        view = memoryview(mm).cast("B")
        offset = 0
        try:
            for ref in sink._refs:
                mv = memoryview(ref).cast("B")
                nbytes = mv.nbytes
                if nbytes:
                    view[offset : offset + nbytes] = mv
                offset += nbytes
        finally:
            view.release()

        entry = (fd, mm, size)
        drop = None
        with self._lock:
            if key in self._shm:
                drop = entry  # Lost a race; discard ours.
            else:
                self._shm[key] = entry
        if drop is not None:
            _close_shm_entry(drop)
        # Consumers need the fd server running to fetch.
        self.ensure_fd_server()

    def _start_fd_server_locked(self) -> None:
        import tempfile

        # Keep the path short: AF_UNIX sun_path is capped (~104 on macOS).
        path = os.path.join(tempfile.gettempdir(), f"ray_flt_{os.getpid()}.sock")
        try:
            os.unlink(path)
        except OSError:
            pass
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(path)
        srv.listen(128)
        self._fd_sock = srv
        self._fd_sock_path = path
        t = threading.Thread(target=self._serve_fds, args=(srv,), daemon=True)
        t.start()
        self._fd_server_thread = t

    def _serve_fds(self, srv) -> None:
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            threading.Thread(
                target=self._handle_fd_conn, args=(conn,), daemon=True
            ).start()

    def _handle_fd_conn(self, conn) -> None:
        """Producer-side handler: read a key, reply with its shm fd (SCM_RIGHTS).

        Wire format: request is key_len(4) + key; reply is size(8) plus, on a
        hit, the fd as ancillary data. On a miss, size is -1 and no fd is sent.
        """
        try:
            hdr = _recv_exact(conn, 4)
            if hdr is None:
                return
            (key_len,) = struct.unpack("<I", hdr)
            key_bytes = _recv_exact(conn, key_len)
            if key_bytes is None:
                return
            key = key_bytes.decode("utf-8")
            with self._lock:
                # Single-blob shm objects and manifest segments share one
                # namespace on the wire; segment keys are prefixed to avoid
                # collision with object keys.
                entry = self._segments.get(key) or self._shm.get(key)
            if entry is None:
                conn.sendall(struct.pack("<q", -1))
                return
            fd, _mm, size = entry
            socket.send_fds(conn, [struct.pack("<q", size)], [fd])
        except OSError:
            pass
        finally:
            conn.close()

    def _start_tcp_server_locked(self) -> None:
        # The raw-TCP transport streams table buffers with SO_ZEROCOPY; refuse
        # to start (rather than silently fall back to copying sends) if this
        # host can't provide it.
        _require_tcp_zerocopy()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", 0))
        srv.listen(128)
        port = srv.getsockname()[1]
        self._tcp_sock = srv
        self._tcp_addr = f"{_get_local_ip()}:{port}"
        t = threading.Thread(target=self._serve_tcp, args=(srv,), daemon=True)
        t.start()
        self._tcp_server_thread = t

    def _serve_tcp(self, srv) -> None:
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            threading.Thread(
                target=self._handle_tcp_conn, args=(conn,), daemon=True
            ).start()

    def _handle_tcp_conn(self, conn) -> None:
        """Producer-side handler: serve fetch requests until the peer hangs up.

        Wire format per request: request is key_len(4) + key; reply is size(8)
        followed by that many IPC-stream bytes on a hit, or size=-1 with no
        payload on a miss. The connection is kept open and looped so a pooled
        client can issue many fetches over it (see fetch_via_tcp); the loop ends
        when the peer closes (_recv_exact returns None). The payload is sent
        straight from the scatter-list captured by put() (sink._refs), so the
        producer never re-serializes the table, and large chunks go out via
        MSG_ZEROCOPY (see _send_ipc_payload).
        """
        try:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            while True:
                hdr = _recv_exact(conn, 4)
                if hdr is None:
                    return  # peer closed the (pooled) connection
                (key_len,) = struct.unpack("<I", hdr)
                key_bytes = _recv_exact(conn, key_len)
                if key_bytes is None:
                    return
                key = key_bytes.decode("utf-8")
                with self._lock:
                    sink = self._sinks.get(key)
                if sink is None:
                    # Miss: report it but keep the connection open for the next
                    # request rather than tearing down the pooled socket.
                    conn.sendall(struct.pack("<q", -1))
                    continue
                conn.sendall(struct.pack("<q", sink.tell()))
                _send_ipc_payload(conn, sink._refs)
        except OSError:
            pass
        finally:
            conn.close()

    def _get_client(self, uri: str):
        import pyarrow.flight as flight

        with self._lock:
            client = self._clients.get(uri)
            if client is None:
                client = flight.connect(uri)
                self._clients[uri] = client
            return client

    def _handle_scatter_write(self, body: bytes) -> None:
        """Producer-side Flight do_action handler for scatter-write transfer.

        Uses the scatter-list cached by put(); no per-fetch table walk.

        Body format: key_len(4) + key + pid(4) + addr(8) + size(8).
        """
        import pyarrow.flight as flight

        from ray._raylet import vm_scatter_write

        offset = 0
        key_len = struct.unpack_from("<I", body, offset)[0]
        offset += 4
        key = body[offset : offset + key_len].decode("utf-8")
        offset += key_len
        consumer_pid = struct.unpack_from("<i", body, offset)[0]
        offset += 4
        consumer_addr = struct.unpack_from("<Q", body, offset)[0]
        offset += 8
        buf_size = struct.unpack_from("<q", body, offset)[0]

        with self._lock:
            sink = self._sinks.get(key)
        if sink is None:
            raise flight.FlightError(f"Object not found: {key}")

        vm_scatter_write(consumer_pid, consumer_addr, buf_size, sink.scatter_list)

    def _start_server_locked(self):
        import pyarrow.flight as flight

        core = self

        class _Server(flight.FlightServerBase):
            def do_get(self, context, ticket):
                key = ticket.ticket.decode("utf-8")
                table = core.get(key)
                if table is None:
                    raise flight.FlightError(f"Object not found: {key}")
                return flight.RecordBatchStream(table)

            def do_action(self, context, action):
                if action.type == "scatter_write_vm":
                    core._handle_scatter_write(action.body.to_pybytes())
                    return []
                if action.type == "delete":
                    core.delete(action.body.to_pybytes().decode("utf-8"))
                    return []
                raise flight.FlightError(f"Unknown action: {action.type}")

        location = flight.Location.for_grpc_tcp("0.0.0.0", 0)
        self._server = _Server(location)
        self._uri = f"grpc://{_get_local_ip()}:{self._server.port}"
        self._server_thread = threading.Thread(target=self._server.serve, daemon=True)
        self._server_thread.start()


_core: Optional[FlightCore] = None
_core_lock = threading.Lock()


def get_flight_core() -> FlightCore:
    """Return the per-process FlightCore, creating it lazily."""
    global _core
    if _core is not None:
        return _core
    with _core_lock:
        if _core is None:
            _core = FlightCore()
    return _core
