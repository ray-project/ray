import multiprocessing.shared_memory as shm
import struct
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.ipc as ipc
from pyarrow.lib import BufferOutputStream

import ray
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)


@dataclass
class ArrowFlightTransportMetadata(TensorTransportMetadata):
    """Metadata for Arrow Flight + SharedMemory transport."""

    flight_address: Optional[str] = None
    shm_name: Optional[str] = None
    shm_size: Optional[int] = None
    sender_node_id: Optional[str] = None


@dataclass
class ArrowFlightCommunicatorMetadata(CommunicatorMetadata):
    pass


def _serialize_tables_to_bytes(tables: List[pa.Table]) -> bytes:
    """Serialize a list of Arrow tables into a flat bytes buffer using IPC format.

    Layout: [num_tables (4B)] then per table: [ipc_size (8B)] [ipc_bytes...]
    """
    parts = []
    parts.append(struct.pack("<I", len(tables)))
    for table in tables:
        out = BufferOutputStream()
        with ipc.RecordBatchStreamWriter(out, schema=table.schema) as wr:
            wr.write_table(table)
        ipc_bytes = out.getvalue().to_pybytes()
        parts.append(struct.pack("<Q", len(ipc_bytes)))
        parts.append(ipc_bytes)
    return b"".join(parts)


def _deserialize_tables_from_buffer(
    buf: Union[bytes, memoryview],
) -> List[pa.Table]:
    """Deserialize Arrow tables from a buffer. Zero-copy when backed by SharedMemory."""
    offset = 0
    (num_tables,) = struct.unpack_from("<I", buf, offset)
    offset += 4

    tables = []
    for _ in range(num_tables):
        (ipc_size,) = struct.unpack_from("<Q", buf, offset)
        offset += 8
        ipc_buf = pa.py_buffer(buf[offset : offset + ipc_size])
        reader = ipc.open_stream(ipc_buf)
        tables.append(reader.read_all())
        offset += ipc_size
    return tables


class _FlightHandler(flight.FlightServerBase):
    """Arrow Flight server that serves tables by obj_id ticket."""

    def __init__(self, location, data_store, store_lock):
        super().__init__(location)
        self._data_store = data_store
        self._store_lock = store_lock

    def do_get(self, context, ticket):
        key = ticket.ticket.decode()
        with self._store_lock:
            tables = self._data_store.get(key)
        if tables is None:
            raise flight.FlightUnavailableError(
                f"Object {key} not found. It may have been garbage collected."
            )
        return flight.RecordBatchStream(tables)


class ArrowFlightSenderServer:
    """Background thread running an Arrow Flight server to serve tables to receivers."""

    def __init__(self):
        self._data_store: Dict[str, Any] = {}
        self._store_lock = threading.Lock()
        self._ready_event = threading.Event()
        self.flight_address: Optional[str] = None
        self._server: Optional[_FlightHandler] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._server = _FlightHandler(
            "grpc://0.0.0.0:0", self._data_store, self._store_lock
        )
        port = self._server.port
        node_ip = ray.util.get_node_ip_address()
        self.flight_address = f"grpc://{node_ip}:{port}"
        self._ready_event.set()

        self._thread = threading.Thread(target=self._server.serve, daemon=True)
        self._thread.start()

    def register(self, obj_id: str, tables: List[pa.Table]):
        with self._store_lock:
            if len(tables) == 1:
                self._data_store[obj_id] = tables[0]
            else:
                for i, table in enumerate(tables):
                    self._data_store[f"{obj_id}:{i}"] = table
                # Store count so receiver knows how many to fetch
                self._data_store[f"{obj_id}:count"] = len(tables)

    def unregister(self, obj_id: str):
        with self._store_lock:
            if obj_id in self._data_store:
                del self._data_store[obj_id]
            # Clean up multi-table entries
            count = self._data_store.pop(f"{obj_id}:count", None)
            if count is not None:
                for i in range(count):
                    self._data_store.pop(f"{obj_id}:{i}", None)

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=5.0)


class ArrowFlightTransport(TensorTransportManager):
    """
    One-sided transport for PyArrow tables.

    Same-node: Arrow IPC serialization into SharedMemory, zero-copy read on receiver.
    Cross-node: Arrow Flight RPC server on the sender, receiver fetches via Flight client.
    """

    def __init__(self):
        self._server: Optional[ArrowFlightSenderServer] = None
        self._server_lock = threading.Lock()
        self._shared_memory_objects: Dict[str, shm.SharedMemory] = {}
        self._receiver_shm_blocks: Dict[str, shm.SharedMemory] = {}
        self._flight_clients: Dict[str, flight.FlightClient] = {}
        self._flight_clients_lock = threading.Lock()

    def _get_flight_client(self, address: str) -> flight.FlightClient:
        client = self._flight_clients.get(address)
        if client is None:
            with self._flight_clients_lock:
                client = self._flight_clients.get(address)
                if client is None:
                    client = flight.connect(address)
                    self._flight_clients[address] = client
        return client

    def _ensure_server(self) -> ArrowFlightSenderServer:
        if self._server is None:
            with self._server_lock:
                if self._server is None:
                    server = ArrowFlightSenderServer()
                    server.start()
                    self._server = server
        return self._server

    def tensor_transport_backend(self) -> str:
        return "ARROW_FLIGHT"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return True

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        try:
            import pyarrow  # noqa: F401
            import pyarrow.flight  # noqa: F401

            return True
        except ImportError:
            return False

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List[pa.Table],
    ) -> ArrowFlightTransportMetadata:
        tensor_meta = []
        for table in rdt_object:
            tensor_meta.append(((table.num_rows, table.num_columns), "arrow_table"))

        serialized = _serialize_tables_to_bytes(rdt_object)
        size = len(serialized)

        name = obj_id[:20]
        shm_obj = shm.SharedMemory(name=name, create=True, size=size)
        shm_obj.buf[:size] = serialized
        self._shared_memory_objects[obj_id] = shm_obj

        server = self._ensure_server()
        server.register(obj_id, rdt_object)

        sender_node_id = ray.get_runtime_context().get_node_id()

        return ArrowFlightTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device="cpu",
            flight_address=server.flight_address,
            shm_name=name,
            shm_size=size,
            sender_node_id=sender_node_id,
        )

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> CommunicatorMetadata:
        return ArrowFlightCommunicatorMetadata()

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: ArrowFlightTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List[Any]] = None,
    ) -> List[Any]:
        my_node_id = ray.get_runtime_context().get_node_id()
        num_tables = len(tensor_transport_metadata.tensor_meta)

        if my_node_id == tensor_transport_metadata.sender_node_id:
            # Same node: zero-copy read from shared memory via Arrow IPC
            shm_block = shm.SharedMemory(name=tensor_transport_metadata.shm_name)
            try:
                tables = _deserialize_tables_from_buffer(
                    shm_block.buf[: tensor_transport_metadata.shm_size]
                )
            except Exception:
                shm_block.close()
                raise
            self._receiver_shm_blocks[obj_id] = shm_block
            return tables
        else:
            # Cross node: fetch via Arrow Flight
            client = self._get_flight_client(tensor_transport_metadata.flight_address)
            if num_tables == 1:
                reader = client.do_get(flight.Ticket(obj_id.encode()))
                return [reader.read_all()]
            else:
                tables = []
                for i in range(num_tables):
                    ticket = flight.Ticket(f"{obj_id}:{i}".encode())
                    reader = client.do_get(ticket)
                    tables.append(reader.read_all())
                return tables

    def send_multiple_tensors(
        self,
        tensors: List[pa.Table],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        raise NotImplementedError(
            "Arrow Flight transport is one-sided; send is handled by the Flight server."
        )

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: ArrowFlightTransportMetadata,
        tensors: List[pa.Table],
    ):
        if self._server is not None:
            self._server.unregister(obj_id)

        shm_obj = self._shared_memory_objects.pop(obj_id, None)
        if shm_obj is not None:
            shm_obj.close()
            shm_obj.unlink()

        recv_shm = self._receiver_shm_blocks.pop(obj_id, None)
        if recv_shm is not None:
            recv_shm.close()

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        if self._server is not None:
            self._server.unregister(obj_id)

        shm_obj = self._shared_memory_objects.pop(obj_id, None)
        if shm_obj is not None:
            try:
                shm_obj.close()
                shm_obj.unlink()
            except Exception:
                pass

        recv_shm = self._receiver_shm_blocks.pop(obj_id, None)
        if recv_shm is not None:
            try:
                recv_shm.close()
            except Exception:
                pass
