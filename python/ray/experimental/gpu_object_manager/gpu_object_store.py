from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import ray.util.collective as collective
from ray._private.custom_types import TensorTransportEnum
from ray.util.collective.types import Backend


try:
    import torch
except ImportError:
    raise ImportError(
        "`tensor_transport` requires PyTorch. "
        "Please install torch with 'pip install torch' to use this feature."
    )

TENSOR_TRANSPORT_TO_COLLECTIVE_BACKEND = {
    TensorTransportEnum.NCCL: Backend.NCCL,
    TensorTransportEnum.GLOO: Backend.TORCH_GLOO,
}

COLLECTIVE_BACKEND_TO_TORCH_DEVICE = {
    Backend.NCCL: torch.device("cuda"),
    Backend.TORCH_GLOO: torch.device("cpu"),
}


def _tensor_transport_to_collective_backend(
    tensor_transport: TensorTransportEnum,
) -> Backend:
    try:
        return TENSOR_TRANSPORT_TO_COLLECTIVE_BACKEND[tensor_transport]
    except KeyError:
        raise ValueError(
            f"Invalid tensor transport {tensor_transport.name}, must be one of {list(TENSOR_TRANSPORT_TO_COLLECTIVE_BACKEND.keys())}."
        )


def __ray_send__(self, communicator_name: str, obj_id: str, dst_rank: int):
    """Helper function that runs on the src actor to send tensors to the dst actor."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    assert gpu_object_store.has_gpu_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    tensors = gpu_object_store.get_gpu_object(obj_id)

    backend = collective.get_group_handle(communicator_name).backend()
    device = COLLECTIVE_BACKEND_TO_TORCH_DEVICE[backend]

    for tensor in tensors:
        if tensor.device.type != device.type:
            # TODO(swang): Right now there is no way to catch this error
            # and the receiving Ray task will hang.
            raise ValueError(
                f"tensor device {tensor.device} does not match device {device}"
            )
        collective.send(tensor, dst_rank, group_name=communicator_name)


def __ray_recv__(
    self,
    communicator_name: str,
    obj_id: str,
    src_rank: int,
    tensor_meta: List[Tuple["torch.Size", "torch.dtype"]],
    num_readers: int,
):
    """Helper function that runs on the dst actor to receive tensors from the src actor."""
    from ray._private.worker import global_worker

    backend = collective.get_group_handle(communicator_name).backend()
    device = COLLECTIVE_BACKEND_TO_TORCH_DEVICE[backend]

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    tensors = []
    for meta in tensor_meta:
        shape, dtype = meta
        tensor = torch.zeros(shape, dtype=dtype, device=device)
        collective.recv(tensor, src_rank, group_name=communicator_name)
        tensors.append(tensor)
    gpu_object_store.add_gpu_object(obj_id, tensors, num_readers=num_readers)


def __ray_fetch_gpu_object__(self, obj_id: str):
    """Helper function that runs on the src actor to fetch tensors from the GPU object store via the object store."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    assert gpu_object_store.has_gpu_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    tensors = gpu_object_store.get_gpu_object(obj_id)
    return tensors


@dataclass
class GPUObject:
    # A list of tensors representing the GPU object.
    data: List["torch.Tensor"]
    # Whether the GPU object is the primary copy.
    is_primary: bool
    # The number of reads allowed to the GPU object before it will be GCed from this actor. Only used if is_primary=False.
    # This is used to implement garbage collection for receiver actors,
    # handling cases where the same GPU object reference is passed to the
    # same actor task multiple times. For sender actors, we still rely on
    # the object store's reference counting mechanism.
    num_readers: Optional[int] = None


class GPUObjectStore:
    def __init__(self):
        # A dictionary that maps from an object ID to a list of tensors.
        #
        # Note: Currently, `gpu_object_store` is only supported for Ray Actors.
        self.gpu_object_store: Dict[str, GPUObject] = {}

    def has_gpu_object(self, obj_id: str) -> bool:
        return obj_id in self.gpu_object_store

    def get_gpu_object(self, obj_id: str) -> Optional[List["torch.Tensor"]]:
        gpu_object_metadata = self.gpu_object_store[obj_id]
        # If the GPU object is the primary copy, it means the transfer
        # is intra-actor. In this case, we should not remove the GPU
        # object after it is consumed `num_readers` times, because the
        # GPU object reference may be used again. Instead, we should
        # wait for the GC callback to clean it up.
        if not gpu_object_metadata.is_primary:
            self._decrement_num_readers(obj_id)
        return gpu_object_metadata.data

    def add_gpu_object(
        self,
        obj_id: str,
        gpu_object: List["torch.Tensor"],
        num_readers: Optional[int] = None,
        is_primary: bool = False,
    ):
        """
        Add a GPU object to the GPU object store.

        Args:
            obj_id: The object ID of the GPU object.
            gpu_object: A list of tensors representing the GPU object.
            num_readers: The number of readers for the GPU object. The GPU
              object store will automatically clean up the object after
              `num_readers` calls to `decrement_num_readers()`.
            is_primary: Whether the GPU object is the primary copy.
        """
        self.gpu_object_store[obj_id] = GPUObject(
            gpu_object,
            is_primary,
            num_readers,
        )

    def is_primary_copy(self, obj_id: str) -> bool:
        return self.gpu_object_store[obj_id].is_primary

    def remove_gpu_object(self, obj_id: str):
        """
        Remove the GPU object from the GPU object store.

        Args:
            obj_id: The object ID of the GPU object.
        """
        assert (
            obj_id in self.gpu_object_store
        ), f"obj_id={obj_id} not found in GPU object store"
        del self.gpu_object_store[obj_id]

    def _decrement_num_readers(self, obj_id: str):
        """
        Decrement the number of readers for a GPU object.
        If `num_readers` reaches 0, the GPU object will be removed from the
        GPU object store.

        Args:
            obj_id: The object ID of the GPU object.
        """
        assert (
            obj_id in self.gpu_object_store
        ), f"obj_id={obj_id} not found in GPU object store"
        self.gpu_object_store[obj_id].num_readers -= 1
        if self.gpu_object_store[obj_id].num_readers == 0:
            del self.gpu_object_store[obj_id]
