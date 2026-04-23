"""Pure-Python Arrow Flight object store.

Uses PyArrow Flight for the server/client and Ray's C++ vm_transfer
(process_vm_readv/writev) for same-node zero-gRPC transfers.
"""

import os
import threading

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.ipc as ipc


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
        raise flight.FlightError(f"Unknown action: {action.type}")


class FlightObjectStore:
    """Arrow Flight-based object store.

    - Tables stored in Python heap (no plasma, no shared memory).
    - Cross-process transfer via Flight RPC.
    - Same-node transfer via process_vm_writev (Linux only).
    - Move semantics: auto-delete after first read.
    """

    def __init__(self):
        self._tables = {}
        self._ipc_cache = {}  # key -> (bytes, buffer_address)
        self._lock = threading.Lock()
        self._server = None
        self._server_thread = None
        self._port = None
        self._uri = None
        self._clients = {}

    def start_server(self):
        """Start the Flight server on a random port."""
        location = flight.Location.for_grpc_tcp("0.0.0.0", 0)
        self._server = _FlightServer(location, self)
        self._port = self._server.port
        ip = _get_local_ip()
        self._uri = f"grpc://{ip}:{self._port}"
        self._server_thread = threading.Thread(target=self._server.serve, daemon=True)
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
        """Store a table and return transfer info dict.

        Also pre-serializes to IPC and caches the buffer address
        for same-node process_vm_readv transfers.
        """
        # Serialize to IPC bytes.
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        ipc_buf = sink.getvalue()
        ipc_bytes = ipc_buf.to_pybytes()

        with self._lock:
            self._tables[key] = table
            # Cache IPC bytes and their buffer address for VM read.
            self._ipc_cache[key] = ipc_bytes

        return {
            "flight_uri": self._uri,
            "key": key,
            "pid": os.getpid(),
            "ipc_address": _get_bytes_address(ipc_bytes),
            "ipc_size": len(ipc_bytes),
        }

    def get_local(self, key):
        """Get a locally stored table (does not auto-delete)."""
        with self._lock:
            return self._tables.get(key)

    def _pop(self, key):
        """Pop a table (used by Flight DoGet for move semantics)."""
        with self._lock:
            self._ipc_cache.pop(key, None)
            return self._tables.pop(key, None)

    def fetch(self, uri, key):
        """Fetch a table from a remote Flight server."""
        client = self._get_client(uri)
        ticket = flight.Ticket(key.encode("utf-8"))
        reader = client.do_get(ticket)
        return reader.read_all()

    def fetch_via_vm(self, pid, ipc_address, ipc_size, flight_uri=None, key=None):
        """Fetch via process_vm_readv (same-node, Linux only).

        Single copy: process_vm_readv reads directly into an Arrow buffer,
        then ipc.open_stream reads from that buffer without copying.
        """
        from ray._raylet import vm_read_into_arrow_buffer

        arrow_buf = vm_read_into_arrow_buffer(pid, ipc_address, ipc_size)
        table = ipc.open_stream(arrow_buf).read_all()
        # Notify producer to clean up.
        if flight_uri and key:
            try:
                client = self._get_client(flight_uri)
                action = flight.Action("delete", key.encode("utf-8"))
                list(client.do_action(action))
            except Exception:
                pass  # Best-effort cleanup.
        return table

    def delete(self, key):
        with self._lock:
            self._tables.pop(key, None)
            self._ipc_cache.pop(key, None)

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


def _get_bytes_address(b):
    """Get the memory address of a Python bytes object's data buffer."""
    import ctypes

    buf = ctypes.c_char_p(b)
    return ctypes.cast(buf, ctypes.c_void_p).value


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
