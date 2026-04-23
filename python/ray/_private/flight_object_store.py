"""Pure-Python Arrow Flight object store.

Uses PyArrow Flight for the server/client and Ray's C++ vm_transfer
(process_vm_readv/writev) for same-node zero-gRPC transfers.

Two IPC copy modes (set via ARROW_IPC_COPY_MODE env var):
  - "eager" (default): Pre-serialize IPC on put. Consumer reads via
    process_vm_readv. 2 copies total (put: table→IPC, read: vm_readv).
  - "lazy": No serialization on put. Consumer allocates buffer and sends
    DoAction to producer, which serializes IPC via a recording sink and
    scatter-writes directly into consumer's buffer via process_vm_writev.
    1 copy total (the scatter write IS the serialization).
"""

import os
import sys
import threading

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.ipc as ipc

_COPY_MODE = os.environ.get("ARROW_IPC_COPY_MODE", "eager")


class _RecordingSink(pa.NativeFile):
    """A sink that records write addresses and sizes without copying.

    Used in lazy mode: the IPC writer calls write() with pointers to
    Arrow column buffers and metadata. We record those pointers and
    flush them all in a single process_vm_writev scatter call.

    IMPORTANT: The buffers passed to write() must remain valid until
    flush() is called. Arrow's IPC writer holds references to column
    buffers during write_table(), and metadata buffers are valid for
    the duration of each WriteRecordBatch call. We flush immediately
    after each write() to avoid holding stale pointers.
    """

    def __init__(self):
        self._chunks = []  # list of (address, size) for Arrow buffers
        self._copied_chunks = []  # small metadata chunks we had to copy
        self._offset = 0

    def write(self, data):
        """Record a write. For large buffers (column data), record the pointer.
        For small buffers (metadata), copy to keep alive.
        """
        if isinstance(data, pa.Buffer):
            size = data.size
            self._chunks.append((data.address, size))
            self._offset += size
            # Keep a reference to prevent GC.
            self._copied_chunks.append(data)
        else:
            # bytes or memoryview — copy to keep alive.
            b = bytes(data)
            buf = pa.py_buffer(b)
            self._chunks.append((buf.address, len(b)))
            self._offset += len(b)
            self._copied_chunks.append(buf)
        return len(data) if not isinstance(data, pa.Buffer) else data.size

    def tell(self):
        return self._offset

    @property
    def total_size(self):
        return self._offset

    @property
    def scatter_list(self):
        """Return list of (address, size) tuples for scatter write."""
        return self._chunks


class _FlightServer(flight.FlightServerBase):
    """Flight server that stores Arrow tables and serves them via DoGet."""

    def __init__(self, location, store):
        super().__init__(location)
        self._store = store

    def do_get(self, context, ticket):
        key = ticket.ticket.decode("utf-8")
        table = self._store._pop(key)
        if table is None:
            raise flight.FlightError(f"Object not found: {key}")
        return flight.RecordBatchStream(table)

    def do_action(self, context, action):
        if action.type == "delete":
            key = action.body.to_pybytes().decode("utf-8")
            self._store.delete(key)
            return []
        if action.type == "scatter_write_vm":
            return self._handle_scatter_write(action)
        raise flight.FlightError(f"Unknown action: {action.type}")

    def _handle_scatter_write(self, action):
        """Lazy mode: consumer requests producer to serialize and scatter-write.

        Body format: key_len(4) + key + pid(4) + address(8) + size(8)
        """
        import struct

        body = action.body.to_pybytes()
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

        table = self._store.get_local(key)
        if table is None:
            raise flight.FlightError(f"Object not found: {key}")

        # Serialize IPC through a recording sink (no copy — just records
        # pointers to Arrow column buffers and metadata).
        sink = _RecordingSink()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()

        # Scatter-write all recorded chunks into the consumer's buffer
        # in a single process_vm_writev syscall.
        from ray._raylet import vm_scatter_write

        vm_scatter_write(consumer_pid, consumer_addr, buf_size,
                         sink.scatter_list)

        # Clean up the table after writing.
        self._store.delete(key)

        return []


class FlightObjectStore:
    """Arrow Flight-based object store."""

    def __init__(self):
        self._tables = {}
        self._ipc_cache = {}  # key -> Arrow buffer (eager mode only)
        self._ipc_sizes = {}  # key -> int (lazy mode: just the size)
        self._lock = threading.Lock()
        self._server = None
        self._server_thread = None
        self._port = None
        self._uri = None
        self._clients = {}
        self._copy_mode = _COPY_MODE

    def start_server(self):
        """Start the Flight server on a random port."""
        location = flight.Location.for_grpc_tcp("0.0.0.0", 0)
        self._server = _FlightServer(location, self)
        self._port = self._server.port
        ip = _get_local_ip()
        self._uri = f"grpc://{ip}:{self._port}"
        self._server_thread = threading.Thread(
            target=self._server.serve, daemon=True
        )
        self._server_thread.start()
        return self._port

    def stop_server(self):
        if self._server:
            self._server.shutdown()
            self._server = None

    def get_uri(self):
        return self._uri

    def put(self, key, table):
        """Store a table by key."""
        with self._lock:
            self._tables[key] = table

    def put_and_get_transfer_info(self, key, table):
        """Store a table and return transfer info dict."""
        if self._copy_mode == "lazy":
            return self._put_lazy(key, table)
        return self._put_eager(key, table)

    def _put_eager(self, key, table):
        """Eager mode: pre-serialize IPC into an Arrow buffer on put."""
        # Step 1: Compute exact IPC size without copying.
        mock_sink = pa.MockOutputStream()
        mock_writer = ipc.new_stream(mock_sink, table.schema)
        mock_writer.write_table(table)
        mock_writer.close()
        ipc_size = mock_sink.size()

        # Step 2: Serialize into an Arrow buffer (single copy).
        buf = pa.allocate_buffer(ipc_size)
        stream = pa.FixedSizeBufferWriter(buf)
        real_writer = ipc.new_stream(stream, table.schema)
        real_writer.write_table(table)
        real_writer.close()

        with self._lock:
            self._tables[key] = table
            self._ipc_cache[key] = buf

        return {
            "flight_uri": self._uri,
            "key": key,
            "pid": os.getpid(),
            "ipc_address": buf.address,
            "ipc_size": ipc_size,
            "copy_mode": "eager",
        }

    def _put_lazy(self, key, table):
        """Lazy mode: just store the table and compute IPC size (no copy)."""
        mock_sink = pa.MockOutputStream()
        mock_writer = ipc.new_stream(mock_sink, table.schema)
        mock_writer.write_table(table)
        mock_writer.close()
        ipc_size = mock_sink.size()

        with self._lock:
            self._tables[key] = table
            self._ipc_sizes[key] = ipc_size

        return {
            "flight_uri": self._uri,
            "key": key,
            "pid": os.getpid(),
            "ipc_address": 0,  # not used in lazy mode
            "ipc_size": ipc_size,
            "copy_mode": "lazy",
        }

    def get_local(self, key):
        """Get a locally stored table (does not auto-delete)."""
        with self._lock:
            return self._tables.get(key)

    def _pop(self, key):
        """Pop a table (used by Flight DoGet for move semantics)."""
        with self._lock:
            self._ipc_cache.pop(key, None)
            self._ipc_sizes.pop(key, None)
            return self._tables.pop(key, None)

    def fetch(self, uri, key):
        """Fetch a table from a remote Flight server."""
        client = self._get_client(uri)
        ticket = flight.Ticket(key.encode("utf-8"))
        reader = client.do_get(ticket)
        return reader.read_all()

    def fetch_via_vm_eager(self, pid, ipc_address, ipc_size,
                           flight_uri=None, key=None):
        """Eager mode: process_vm_readv from producer's pre-serialized buffer."""
        from ray._raylet import vm_read_into_arrow_buffer

        arrow_buf = vm_read_into_arrow_buffer(pid, ipc_address, ipc_size)
        table = ipc.open_stream(arrow_buf).read_all()
        if flight_uri and key:
            try:
                client = self._get_client(flight_uri)
                action = flight.Action("delete", key.encode("utf-8"))
                list(client.do_action(action))
            except Exception:
                pass
        return table

    def fetch_via_vm_lazy(self, flight_uri, key, ipc_size):
        """Lazy mode: allocate buffer, ask producer to scatter-write into it."""
        import struct

        from ray._raylet import vm_read_into_arrow_buffer

        # Allocate local Arrow buffer for producer to write into.
        local_buf = pa.allocate_buffer(ipc_size)

        # Build DoAction body: key_len(4) + key + pid(4) + address(8) + size(8)
        key_bytes = key.encode("utf-8")
        body = struct.pack("<I", len(key_bytes))
        body += key_bytes
        body += struct.pack("<i", os.getpid())
        body += struct.pack("<Q", local_buf.address)
        body += struct.pack("<q", ipc_size)

        # Ask producer to scatter-write IPC into our buffer.
        client = self._get_client(flight_uri)
        action = flight.Action("scatter_write_vm", body)
        list(client.do_action(action))

        # Deserialize from local buffer (zero-copy — column buffers are
        # slices of local_buf).
        return ipc.open_stream(local_buf).read_all()

    def delete(self, key):
        with self._lock:
            self._tables.pop(key, None)
            self._ipc_cache.pop(key, None)
            self._ipc_sizes.pop(key, None)

    def size(self):
        with self._lock:
            return len(self._tables)

    def _get_client(self, uri):
        if uri not in self._clients:
            self._clients[uri] = flight.connect(uri)
        return self._clients[uri]


def _get_local_ip():
    """Get the node's IP address."""
    import socket

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ---------------------------------------------------------------------------
# Per-worker singleton
# ---------------------------------------------------------------------------

_global_store = None


def get_flight_store():
    """Get or create the per-worker Flight store singleton."""
    global _global_store
    if _global_store is None:
        _global_store = FlightObjectStore()
        _global_store.start_server()
    return _global_store


def is_flight_store_enabled():
    return os.environ.get("RAY_USE_FLIGHT_STORE", "0") == "1"
