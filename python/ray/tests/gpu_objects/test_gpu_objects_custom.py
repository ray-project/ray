import multiprocessing.shared_memory as shm
import pickle
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

import pytest
import torch

import ray
from ray.experimental import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
    register_tensor_transport,
)


@dataclass
class ShmTransportMetadata(TensorTransportMetadata):
    shm_name: Optional[str] = None
    shm_size: Optional[int] = None


@dataclass
class ShmCommunicatorMetadata(CommunicatorMetadata):
    pass


class SharedMemoryTransport(TensorTransportManager):
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

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        gpu_object: List[torch.Tensor],
    ) -> TensorTransportMetadata:

        tensor_meta = []
        device = None
        if gpu_object:
            device = gpu_object[0].device
            for tensor in gpu_object:
                if tensor.device.type != "cpu":
                    raise ValueError(
                        "Shared memory transport only supports CPU tensors"
                    )
                tensor_meta.append((tensor.shape, tensor.dtype))

        serialized_gpu_object = pickle.dumps(gpu_object)
        size = len(serialized_gpu_object)
        # Shm name can't be as long as the obj_id, so we truncate it.
        name = obj_id[:20]
        shm_obj = shm.SharedMemory(name=name, create=True, size=size)
        shm_obj.buf[:size] = serialized_gpu_object
        self.shared_memory_objects[obj_id] = shm_obj

        return ShmTransportMetadata(
            tensor_meta=tensor_meta, tensor_device=device, shm_name=name, shm_size=size
        )

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> CommunicatorMetadata:
        return ShmCommunicatorMetadata()

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        shm_name = tensor_transport_metadata.shm_name
        size = tensor_transport_metadata.shm_size
        shm_block = shm.SharedMemory(name=shm_name)
        recv_tensors = pickle.loads(shm_block.buf[:size])
        shm_block.close()
        return recv_tensors

    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        pass

    def garbage_collect(
        self, obj_id: str, tensor_transport_meta: TensorTransportMetadata
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


def test_register_and_use_custom_transport(ray_start_regular):
    register_tensor_transport("shared_memory", ["cpu"], SharedMemoryTransport)

    @ray.remote
    class Actor:
        @ray.method(tensor_transport="shared_memory")
        def echo(self, data):
            return data

        def sum(self, data):
            return data.sum().item()

    # Classes defined in test files get pickled by ref. So we need to
    # explicitly pickle the transport class in this module by value.
    # Note that this doesn't happen if you define the transport class on the
    # driver, something with pytest convinces cloudpickle to pickle by ref.
    from ray import cloudpickle

    cloudpickle.register_pickle_by_value(sys.modules[SharedMemoryTransport.__module__])

    actors = [Actor.remote() for _ in range(2)]
    ref = actors[0].echo.remote(torch.tensor([1, 2, 3]))
    result = actors[1].sum.remote(ref)
    assert ray.get(result) == 6


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
