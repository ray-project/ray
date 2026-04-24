"""Shared Flight server and transfer primitives.

Used by both the Arrow Flight RDT backend
(python/ray/experimental/rdt/arrow_flight_transport.py) and the legacy
"native" Flight store (python/ray/_private/flight_object_store.py). One
Flight server per worker process hosts both flows.
"""

import os
import socket
import struct
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


def ipc_size(table) -> int:
    """Compute the exact IPC stream size without allocating the bytes."""
    import pyarrow as pa
    import pyarrow.ipc as ipc

    mock = pa.MockOutputStream()
    writer = ipc.new_stream(mock, table.schema)
    writer.write_table(table)
    writer.close()
    return mock.size()


def _get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


class FlightCore:
    """Per-process Flight server + table storage + transfer primitives.

    Thread-safe. Instantiated once per worker (see get_flight_core()) and
    shared across all code paths that need Flight-based transfer.
    """

    def __init__(self):
        self._tables: Dict[str, Any] = {}  # key -> pa.Table
        self._lock = threading.Lock()
        self._server = None
        self._server_thread = None
        self._uri: Optional[str] = None
        self._clients: Dict[str, Any] = {}

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

    def put(self, key: str, table) -> int:
        """Store `table` under `key`; return its IPC size."""
        size = ipc_size(table)
        with self._lock:
            self._tables[key] = table
        return size

    def get(self, key: str):
        """Look up a table by key (does not pop)."""
        with self._lock:
            return self._tables.get(key)

    def pop(self, key: str):
        """Remove and return a table by key."""
        with self._lock:
            return self._tables.pop(key, None)

    def delete(self, key: str) -> None:
        with self._lock:
            self._tables.pop(key, None)

    def fetch_via_vm(
        self, flight_uri: str, key: str, size: int
    ):
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

    def fetch_via_flight(self, flight_uri: str, key: str):
        """Cross-node consumer path: plain Flight DoGet RPC."""
        import pyarrow.flight as flight

        client = self._get_client(flight_uri)
        ticket = flight.Ticket(key.encode("utf-8"))
        return client.do_get(ticket).read_all()

    def send_delete_rpc(self, flight_uri: str, key: str) -> None:
        """Native path helper: ask producer to drop a key."""
        import pyarrow.flight as flight

        try:
            client = self._get_client(flight_uri)
            action = flight.Action("delete", key.encode("utf-8"))
            list(client.do_action(action))
        except Exception:
            pass

    # ----------------------------------------------------------- internals

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

        Body format: key_len(4) + key + pid(4) + addr(8) + size(8).
        """
        import pyarrow as pa
        import pyarrow.flight as flight
        import pyarrow.ipc as ipc

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

        table = self.get(key)
        if table is None:
            raise flight.FlightError(f"Object not found: {key}")

        # The recording sink captures pointers to the table's column buffers
        # (no copy) plus small metadata bytes. process_vm_writev then delivers
        # everything to the consumer's buffer in one syscall.
        sink = _RecordingSink()
        pf = pa.PythonFile(sink, mode="w")
        writer = ipc.new_stream(pf, table.schema)
        writer.write_table(table)
        writer.close()

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
        self._server_thread = threading.Thread(
            target=self._server.serve, daemon=True
        )
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
