"""Arrow Flight RDT backend.

One-sided transport for pyarrow.Table objects:
  - Source actor runs a Flight server and holds tables indexed by obj_id.
  - Destination actor allocates a local Arrow buffer and asks the source,
    via a Flight DoAction, to serialize the table's IPC stream directly
    into that buffer using process_vm_writev (Linux only).
  - Cross-node destinations fall back to Flight DoGet (regular RPC).

Registered with RDT as `ARROW_FLIGHT`, `["cpu"]`, data_type=pyarrow.Table.
"""

import os
import socket
import struct
import sys
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass
class ArrowFlightCommunicatorMetadata(CommunicatorMetadata):
    """Empty — Arrow Flight is one-sided, no coordination state needed."""


@dataclass
class ArrowFlightTransportMetadata(TensorTransportMetadata):
    flight_uri: Optional[str] = None
    key: Optional[str] = None
    pid: Optional[int] = None
    node_id: Optional[str] = None
    ipc_size: Optional[int] = None


class _RecordingSink:
    """Sink that captures write pointers instead of copying bytes.

    Used with pa.PythonFile so the IPC writer can "write" into it without
    actually serializing. Large column buffers (pa.Buffer) have their
    address/size recorded directly. Small metadata chunks are copied into
    a py_buffer that's kept alive for the subsequent scatter-write.
    """

    def __init__(self):
        self._chunks: List[tuple] = []  # (address, size)
        self._refs: List[Any] = []  # keep buffers alive
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


def _ipc_size(table) -> int:
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


class _FlightServer:
    """Thin wrapper around pa.flight.FlightServerBase bound to a transport."""

    def __init__(self, transport: "ArrowFlightTransport"):
        import pyarrow.flight as flight

        class _Server(flight.FlightServerBase):
            def __init__(inner, location, t):
                super().__init__(location)
                inner._t = t

            def do_get(inner, context, ticket):
                obj_id = ticket.ticket.decode("utf-8")
                table = inner._t._get_table(obj_id)
                if table is None:
                    raise flight.FlightError(f"Object not found: {obj_id}")
                return flight.RecordBatchStream(table)

            def do_action(inner, context, action):
                if action.type == "scatter_write_vm":
                    inner._t._handle_scatter_write(action.body.to_pybytes())
                    return []
                raise flight.FlightError(f"Unknown action: {action.type}")

        location = flight.Location.for_grpc_tcp("0.0.0.0", 0)
        self._server = _Server(location, transport)
        self._port = self._server.port
        self._uri = f"grpc://{_get_local_ip()}:{self._port}"
        self._thread = threading.Thread(target=self._server.serve, daemon=True)
        self._thread.start()

    @property
    def uri(self) -> str:
        return self._uri

    def shutdown(self):
        try:
            self._server.shutdown()
        except Exception:
            pass


class ArrowFlightTransport(TensorTransportManager):
    def __init__(self):
        self._tables: Dict[str, "pa.Table"] = {}
        self._lock = threading.Lock()
        self._server: Optional[_FlightServer] = None
        self._uri: Optional[str] = None
        self._clients: Dict[str, Any] = {}

    # ------------------------------------------------------------------ RDT API

    def tensor_transport_backend(self) -> str:
        return "ARROW_FLIGHT"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor) -> bool:
        return True

    def extract_tensor_transport_metadata(
        self, obj_id: str, rdt_object: List["pa.Table"]
    ) -> ArrowFlightTransportMetadata:
        self._ensure_server()

        if not rdt_object:
            return ArrowFlightTransportMetadata(
                tensor_meta=[],
                tensor_device="cpu",
                flight_uri=self._uri,
                key=obj_id,
                pid=os.getpid(),
                node_id=ray.get_runtime_context().get_node_id(),
                ipc_size=0,
            )

        # We only support a single table per RDT object. Ray's serializer
        # walks the return value and extracts every registered-type instance
        # into the rdt_object list, so a list of >1 would mean the user's
        # return value contained multiple tables — not supported by this
        # transport.
        if len(rdt_object) != 1:
            raise ValueError(
                "ArrowFlightTransport only supports a single pa.Table per "
                f"RDT object; got {len(rdt_object)}"
            )
        [table] = rdt_object

        size = _ipc_size(table)
        with self._lock:
            self._tables[obj_id] = table

        return ArrowFlightTransportMetadata(
            tensor_meta=[((table.num_rows,), str(table.schema))],
            tensor_device="cpu",
            flight_uri=self._uri,
            key=obj_id,
            pid=os.getpid(),
            node_id=ray.get_runtime_context().get_node_id(),
            ipc_size=size,
        )

    def get_communicator_metadata(
        self, src_actor, dst_actor, backend: Optional[str] = None
    ) -> ArrowFlightCommunicatorMetadata:
        return ArrowFlightCommunicatorMetadata()

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: ArrowFlightTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List[Any]] = None,
    ) -> List["pa.Table"]:
        meta = tensor_transport_metadata
        if not meta.ipc_size:
            # Empty RDT object.
            return []

        my_node = ray.get_runtime_context().get_node_id()
        same_node = meta.node_id == my_node and sys.platform == "linux"
        if same_node:
            table = self._fetch_via_vm(meta)
        else:
            table = self._fetch_via_flight(meta)
        return [table]

    def send_multiple_tensors(self, tensors, meta, comm):
        raise NotImplementedError(
            "ArrowFlightTransport is one-sided; send_multiple_tensors is unused"
        )

    def garbage_collect(self, obj_id: str, meta, tensors):
        with self._lock:
            self._tables.pop(obj_id, None)

    def abort_transport(self, obj_id: str, communicator_metadata):
        pass

    # --------------------------------------------------------------- internals

    def _ensure_server(self):
        if self._server is not None:
            return
        with self._lock:
            if self._server is not None:
                return
            self._server = _FlightServer(self)
            self._uri = self._server.uri

    def _get_table(self, obj_id: str) -> Optional["pa.Table"]:
        with self._lock:
            return self._tables.get(obj_id)

    def _get_client(self, uri: str):
        import pyarrow.flight as flight

        with self._lock:
            client = self._clients.get(uri)
            if client is None:
                client = flight.connect(uri)
                self._clients[uri] = client
            return client

    def _handle_scatter_write(self, body: bytes):
        """Producer-side Flight do_action handler for lazy-mode transfer.

        Body format: key_len(4) + key + pid(4) + addr(8) + size(8).
        """
        from ray._raylet import vm_scatter_write
        import pyarrow as pa
        import pyarrow.ipc as ipc

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

        table = self._get_table(key)
        if table is None:
            import pyarrow.flight as flight

            raise flight.FlightError(f"Object not found: {key}")

        # Recording sink captures pointers to column buffers (no copy) plus
        # small metadata bytes. The scatter-write syscall then copies them
        # all into the consumer's buffer in a single process_vm_writev call.
        sink = _RecordingSink()
        pf = pa.PythonFile(sink, mode="w")
        writer = ipc.new_stream(pf, table.schema)
        writer.write_table(table)
        writer.close()

        vm_scatter_write(consumer_pid, consumer_addr, buf_size, sink.scatter_list)
        # Keep sink._refs alive until the syscall returns — they're buffers
        # we handed addresses for. sink stays in scope here; nothing to do.

    def _fetch_via_vm(self, meta: ArrowFlightTransportMetadata) -> "pa.Table":
        """Same-node recv: ask producer to scatter-write into our buffer."""
        import pyarrow as pa
        import pyarrow.flight as flight
        import pyarrow.ipc as ipc

        local_buf = pa.allocate_buffer(meta.ipc_size)

        key_bytes = meta.key.encode("utf-8")
        body = struct.pack("<I", len(key_bytes))
        body += key_bytes
        body += struct.pack("<i", os.getpid())
        body += struct.pack("<Q", local_buf.address)
        body += struct.pack("<q", meta.ipc_size)

        client = self._get_client(meta.flight_uri)
        action = flight.Action("scatter_write_vm", body)
        list(client.do_action(action))

        return ipc.open_stream(local_buf).read_all()

    def _fetch_via_flight(self, meta: ArrowFlightTransportMetadata) -> "pa.Table":
        """Cross-node recv: plain Flight DoGet RPC."""
        import pyarrow.flight as flight

        client = self._get_client(meta.flight_uri)
        ticket = flight.Ticket(meta.key.encode("utf-8"))
        return client.do_get(ticket).read_all()
