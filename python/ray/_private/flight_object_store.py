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

        Serializes to IPC into an Arrow buffer (single copy, no Python bytes).
        The buffer's address is exposed for same-node process_vm_readv.
        """
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
            # Keep Arrow buffer alive — its address is used by process_vm_readv.
            self._ipc_cache[key] = buf

        return {
            "flight_uri": self._uri,
            "key": key,
            "pid": os.getpid(),
            "ipc_address": buf.address,
            "ipc_size": ipc_size,
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
