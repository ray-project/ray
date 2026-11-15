# flake8: noqa

# __custom_metadata_start__
import multiprocessing.shared_memory as shm
import pickle
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy

import ray
from ray.experimental import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
    register_tensor_transport,
)


@dataclass
class ShmTransportMetadata(TensorTransportMetadata):
    """Custom metadata that stores the shared memory name and size."""

    shm_name: Optional[str] = None
    shm_size: Optional[int] = None


@dataclass
class ShmCommunicatorMetadata(CommunicatorMetadata):
    """No extra communicator metadata needed for shared memory."""

    pass


# __custom_metadata_end__


# __custom_properties_start__
class SharedMemoryTransport(TensorTransportManager):
    """A one-sided tensor transport that transfers numpy arrays through shared memory."""

    def __init__(self):
        self.shared_memory_objects: Dict[str, shm.SharedMemory] = {}

    def tensor_transport_backend(self) -> str:
        return "shared_memory"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        return True

    # __custom_properties_end__

    # __custom_extract_start__
    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List[numpy.ndarray],
    ) -> TensorTransportMetadata:
        # Record shapes and dtypes.
        tensor_meta = []
        if rdt_object:
            for tensor in rdt_object:
                tensor_meta.append((tensor.shape, tensor.dtype))

        # Serialize the tensors and store them in shared memory.
        serialized_rdt_object = pickle.dumps(rdt_object)
        size = len(serialized_rdt_object)
        name = obj_id[:20]
        shm_obj = shm.SharedMemory(name=name, create=True, size=size)
        shm_obj.buf[:size] = serialized_rdt_object
        self.shared_memory_objects[obj_id] = shm_obj

        return ShmTransportMetadata(
            tensor_meta=tensor_meta, tensor_device="cpu", shm_name=name, shm_size=size
        )

    # __custom_extract_end__

    # __custom_communicator_start__
    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> CommunicatorMetadata:
        return ShmCommunicatorMetadata()

    # __custom_communicator_end__

    # __custom_send_recv_start__
    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List[Any]] = None,
    ):
        # Open the shared memory block and deserialize.
        shm_name = tensor_transport_metadata.shm_name
        size = tensor_transport_metadata.shm_size
        shm_block = shm.SharedMemory(name=shm_name)
        recv_tensors = pickle.loads(shm_block.buf[:size])
        shm_block.close()
        return recv_tensors

    def send_multiple_tensors(
        self,
        tensors: List[numpy.ndarray],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        raise NotImplementedError("One-sided transport doesn't use send.")

    # __custom_send_recv_end__

    # __custom_cleanup_start__
    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List[numpy.ndarray],
    ):
        self.shared_memory_objects[obj_id].close()
        self.shared_memory_objects[obj_id].unlink()
        del self.shared_memory_objects[obj_id]

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        pass

    # __custom_cleanup_end__


# __custom_usage_start__
register_tensor_transport(
    "shared_memory",        # Transport name
    ["cpu"],                # Supported device types
    SharedMemoryTransport,  # TensorTransportManager class
    numpy.ndarray,          # Data type for this transport
)


@ray.remote
class MyActor:
    @ray.method(tensor_transport="shared_memory")
    def echo(self, data):
        return data

    def sum(self, data):
        return data.sum().item()


actors = [MyActor.remote() for _ in range(2)]
ref = actors[0].echo.remote(numpy.array([1, 2, 3]))
result = actors[1].sum.remote(ref)
print(ray.get(result))
# 6
# __custom_usage_end__
