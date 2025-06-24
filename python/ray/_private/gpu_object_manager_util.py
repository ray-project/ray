from typing import List, Tuple

try:
    import torch
except ImportError:
    raise ImportError(
        "`tensor_transport` requires PyTorch. "
        "Please install torch with 'pip install torch' to use this feature."
    )

import ray.util.collective as collective
from ray._private.custom_types import TensorTransportEnum
from ray._private.worker import global_worker
from ray.util.collective.types import Backend

TENSOR_TRANSPORT_TO_COLLECTIVE_BACKEND = {
    TensorTransportEnum.NCCL: Backend.NCCL,
    TensorTransportEnum.GLOO: Backend.TORCH_GLOO,
}

COLLECTIVE_BACKEND_TO_TORCH_DEVICE = {
    Backend.NCCL: torch.device("cuda"),
    Backend.TORCH_GLOO: torch.device("cpu"),
}


def tensor_transport_to_collective_backend(
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
    gpu_object_manager = global_worker.gpu_object_manager
    assert gpu_object_manager.has_gpu_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    tensors = gpu_object_manager.get_gpu_object(obj_id)

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
    # TODO(kevin85421): The current garbage collection implementation for the
    # in-actor object store is naive. We garbage collect each object after it
    # is consumed once.
    gpu_object_manager.remove_gpu_object(obj_id)


def __ray_recv__(
    self,
    communicator_name: str,
    obj_id: str,
    src_rank: int,
    tensor_meta: List[Tuple["torch.Size", "torch.dtype"]],
):
    """Helper function that runs on the dst actor to receive tensors from the src actor."""
    from ray._private.worker import global_worker

    backend = collective.get_group_handle(communicator_name).backend()
    device = COLLECTIVE_BACKEND_TO_TORCH_DEVICE[backend]

    gpu_object_manager = global_worker.gpu_object_manager
    tensors = []
    for meta in tensor_meta:
        shape, dtype = meta
        tensor = torch.zeros(shape, dtype=dtype, device=device)
        collective.recv(tensor, src_rank, group_name=communicator_name)
        tensors.append(tensor)
    gpu_object_manager.add_gpu_object(obj_id, tensors)


def __ray_fetch_gpu_object__(self, obj_id: str):
    """Helper function that runs on the src actor to fetch tensors from the GPU object store via the object store."""
    gpu_object_manager = global_worker.gpu_object_manager
    assert gpu_object_manager.has_gpu_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    tensors = gpu_object_manager.get_gpu_object(obj_id)
    # TODO(kevin85421): The current garbage collection implementation for the
    # in-actor object store is naive. We garbage collect each object after it
    # is consumed once.
    gpu_object_manager.remove_gpu_object(obj_id)
    return tensors
