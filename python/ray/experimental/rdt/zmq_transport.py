import multiprocessing.shared_memory as shm
import os
import struct
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np

import ray
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)


@dataclass
class ZmqNumpyTransportMetadata(TensorTransportMetadata):
    """Metadata for ZMQ + SharedMemory numpy transport."""

    tcp_address: Optional[str] = None
    ipc_address: Optional[str] = None
    shm_name: Optional[str] = None
    shm_size: Optional[int] = None
    sender_node_id: Optional[str] = None


@dataclass
class ZmqNumpyCommunicatorMetadata(CommunicatorMetadata):
    pass


def _serialize_arrays_to_bytes(arrays: List[np.ndarray]) -> bytes:
    """Serialize a list of numpy arrays into a flat bytes buffer.

    Layout: [num_arrays (4B)] then for each array:
      [ndim (4B)] [shape... (ndim * 8B)] [dtype_len (4B)] [dtype_str] [data_len (8B)] [data...]
    """
    parts = []
    parts.append(struct.pack("<I", len(arrays)))
    for arr in arrays:
        arr = np.ascontiguousarray(arr)
        dtype_str = arr.dtype.str.encode()
        data = arr.tobytes()
        parts.append(struct.pack("<I", arr.ndim))
        for dim in arr.shape:
            parts.append(struct.pack("<Q", dim))
        parts.append(struct.pack("<I", len(dtype_str)))
        parts.append(dtype_str)
        parts.append(struct.pack("<Q", len(data)))
        parts.append(data)
    return b"".join(parts)


def _deserialize_arrays_from_buffer(
    buf: memoryview,
) -> List[np.ndarray]:
    """Deserialize numpy arrays from a buffer, zero-copy where possible.

    Each array is constructed as a view over the buffer memory.
    """
    offset = 0
    (num_arrays,) = struct.unpack_from("<I", buf, offset)
    offset += 4

    arrays = []
    for _ in range(num_arrays):
        (ndim,) = struct.unpack_from("<I", buf, offset)
        offset += 4
        shape = []
        for _ in range(ndim):
            (dim,) = struct.unpack_from("<Q", buf, offset)
            offset += 8
            shape.append(dim)
        shape = tuple(shape)
        (dtype_len,) = struct.unpack_from("<I", buf, offset)
        offset += 4
        dtype_str = bytes(buf[offset : offset + dtype_len]).decode()
        offset += dtype_len
        (data_len,) = struct.unpack_from("<Q", buf, offset)
        offset += 8
        # Zero-copy: construct numpy array directly from the buffer slice
        arr = np.frombuffer(buf[offset : offset + data_len], dtype=np.dtype(dtype_str))
        arr = arr.reshape(shape)
        arr.flags.writeable = False
        offset += data_len
        arrays.append(arr)
    return arrays


class ZmqSenderServer:
    """Background thread running a ZMQ ROUTER socket to serve tensor data to receivers."""

    def __init__(self):
        self._data_store: Dict[str, bytes] = {}
        self._store_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self.tcp_address: Optional[str] = None
        self.ipc_address: Optional[str] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready_event.wait(timeout=10.0):
            raise RuntimeError("ZMQ sender server failed to start within 10 seconds")

    def register(self, obj_id: str, data: bytes):
        with self._store_lock:
            self._data_store[obj_id] = data

    def unregister(self, obj_id: str):
        with self._store_lock:
            self._data_store.pop(obj_id, None)

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def _run(self):
        import zmq

        ctx = zmq.Context()
        sock = ctx.socket(zmq.ROUTER)

        # Bind TCP on a random port for cross-node communication
        tcp_port = sock.bind_to_random_port("tcp://*")
        node_ip = ray.util.get_node_ip_address()
        self.tcp_address = f"tcp://{node_ip}:{tcp_port}"

        # Bind IPC for same-node communication (included in metadata but
        # same-node receivers use shared memory directly instead)
        ipc_path = f"/tmp/ray_zmq_{os.getpid()}"
        self.ipc_address = f"ipc://{ipc_path}"
        try:
            sock.bind(self.ipc_address)
        except Exception:
            # IPC bind can fail if path already exists from a previous run
            try:
                os.unlink(ipc_path)
            except OSError:
                pass
            sock.bind(self.ipc_address)

        self._ready_event.set()

        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)

        while not self._stop_event.is_set():
            events = dict(poller.poll(timeout=100))
            if sock in events:
                frames = sock.recv_multipart()
                # ROUTER frames: [identity, empty delimiter, obj_id]
                if len(frames) >= 3:
                    identity = frames[0]
                    obj_id = frames[2].decode()
                    with self._store_lock:
                        data = self._data_store.get(obj_id)
                    if data is not None:
                        sock.send_multipart([identity, b"", data])
                    else:
                        sock.send_multipart([identity, b"", b"ERROR:NOT_FOUND"])

        sock.close(linger=0)
        ctx.term()
        # Clean up IPC socket file
        try:
            os.unlink(ipc_path)
        except OSError:
            pass


class ZmqNumpyTransport(TensorTransportManager):
    """
    One-sided tensor transport using ZMQ and multiprocessing SharedMemory.

    Same-node transfers: zero-copy read directly from shared memory. The numpy
    arrays returned are views backed by the shared memory buffer.
    Cross-node transfers: fetch data via ZMQ TCP from the sender's ROUTER server.
    """

    def __init__(self):
        self._server: Optional[ZmqSenderServer] = None
        self._server_lock = threading.Lock()
        self._shared_memory_objects: Dict[str, shm.SharedMemory] = {}
        # Keep receiver-side shm blocks alive so zero-copy array views remain valid
        self._receiver_shm_blocks: Dict[str, shm.SharedMemory] = {}

    def _ensure_server(self) -> ZmqSenderServer:
        if self._server is None:
            with self._server_lock:
                if self._server is None:
                    server = ZmqSenderServer()
                    server.start()
                    self._server = server
        return self._server

    def tensor_transport_backend(self) -> str:
        return "ZMQ_NUMPY"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return True

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        try:
            import zmq  # noqa: F401

            return True
        except ImportError:
            return False

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List[np.ndarray],
    ) -> ZmqNumpyTransportMetadata:
        tensor_meta = []
        if rdt_object:
            for tensor in rdt_object:
                tensor_meta.append((tensor.shape, tensor.dtype))

        serialized = _serialize_arrays_to_bytes(rdt_object)
        size = len(serialized)

        # Create shared memory segment for same-node transfers
        name = obj_id[:20]
        shm_obj = shm.SharedMemory(name=name, create=True, size=size)
        shm_obj.buf[:size] = serialized
        self._shared_memory_objects[obj_id] = shm_obj

        # Ensure ZMQ server is running and register data for cross-node transfers
        server = self._ensure_server()
        server.register(obj_id, serialized)

        sender_node_id = ray.get_runtime_context().get_node_id()

        return ZmqNumpyTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device="cpu",
            tcp_address=server.tcp_address,
            ipc_address=server.ipc_address,
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
        return ZmqNumpyCommunicatorMetadata()

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: ZmqNumpyTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List[Any]] = None,
    ) -> List[Any]:
        my_node_id = ray.get_runtime_context().get_node_id()

        if my_node_id == tensor_transport_metadata.sender_node_id:
            # Same node: zero-copy read directly from shared memory
            shm_block = shm.SharedMemory(name=tensor_transport_metadata.shm_name)
            arrays = _deserialize_arrays_from_buffer(
                shm_block.buf[: tensor_transport_metadata.shm_size]
            )
            # Keep shm_block alive so numpy array views remain valid
            self._receiver_shm_blocks[obj_id] = shm_block
            return arrays
        else:
            # Cross node: fetch via ZMQ TCP
            import zmq

            ctx = zmq.Context()
            sock = ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.RCVTIMEO, 30000)
            sock.setsockopt(zmq.SNDTIMEO, 5000)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(tensor_transport_metadata.tcp_address)
            try:
                sock.send(obj_id.encode())
                data = sock.recv()
                if data == b"ERROR:NOT_FOUND":
                    raise RuntimeError(
                        f"ZMQ transport: object {obj_id} not found on sender. "
                        "It may have been garbage collected."
                    )
                arrays = _deserialize_arrays_from_buffer(memoryview(data))
                # Cross-node arrays are backed by `data` bytes; make copies
                # so we don't hold the full zmq message buffer.
                arrays = [arr.copy() for arr in arrays]
            finally:
                sock.close()
                ctx.term()
            return arrays

    def send_multiple_tensors(
        self,
        tensors: List[np.ndarray],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        raise NotImplementedError(
            "ZMQ numpy transport is one-sided; send is handled by the background server."
        )

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: ZmqNumpyTransportMetadata,
        tensors: List[np.ndarray],
    ):
        # Remove from ZMQ server data store
        if self._server is not None:
            self._server.unregister(obj_id)

        # Clean up sender-side shared memory
        shm_obj = self._shared_memory_objects.pop(obj_id, None)
        if shm_obj is not None:
            shm_obj.close()
            shm_obj.unlink()

        # Clean up receiver-side shared memory reference
        recv_shm = self._receiver_shm_blocks.pop(obj_id, None)
        if recv_shm is not None:
            recv_shm.close()

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        # Best-effort cleanup
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
