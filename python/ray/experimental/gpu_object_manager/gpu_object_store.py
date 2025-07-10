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

    if (
        global_worker.gpu_object_manager.tensor_transport_backend
        == TensorTransportEnum.NIXL
    ):
        nixl_agent = global_worker.gpu_object_manager.init_nixl_agent()
        reg_descs = nixl_agent.register_memory(tensors)
        xfer_descs = reg_descs.trim()
        global_worker.gpu_object_manager.managed_gpu_object_metadata[
            obj_id
        ].nixl_serialized_descs = nixl_agent.get_serialized_descs(xfer_descs)
        global_worker.gpu_object_manager.managed_gpu_object_metadata[
            obj_id
        ].nixl_agent_meta = nixl_agent.get_agent_metadata()
        return

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
    gpu_object_store.remove_gpu_object(obj_id)


def __ray_recv__(
    self,
    communicator_name: str,
    obj_id: str,
    src_rank: int,
    tensor_meta: List[Tuple["torch.Size", "torch.dtype"]],
    nixl_serialized_descs: Optional[bytes] = None,
    nixl_agent_meta: Optional[bytes] = None,
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
        if (
            global_worker.gpu_object_manager.tensor_transport_backend
            != TensorTransportEnum.NIXL
        ):
            collective.recv(tensor, src_rank, group_name=communicator_name)
        tensors.append(tensor)

    if (
        global_worker.gpu_object_manager.tensor_transport_backend
        == TensorTransportEnum.NIXL
    ):
        nixl_agent = global_worker.gpu_object_manager.init_nixl_agent()
        remote_descs = nixl_agent.deserialize_descs(nixl_serialized_descs)
        local_descs = nixl_agent.register_memory(tensors)
        remote_name = nixl_agent.add_remote_agent(nixl_agent_meta)

        xfer_handle = nixl_agent.initialize_xfer(
            "READ", local_descs.trim(), remote_descs, remote_name, b"UUID1"
        )

        state = nixl_agent.transfer(xfer_handle)
        if state == "ERR":
            print("Posting transfer failed.")
            assert False
        while True:
            state = nixl_agent.check_xfer_state(xfer_handle)
            if state == "ERR":
                print("Transfer got to Error state.")
                assert False
            elif state == "DONE":
                break

    gpu_object_store.add_gpu_object(obj_id, tensors)


def __ray_fetch_gpu_object__(self, obj_id: str):
    """Helper function that runs on the src actor to fetch tensors from the GPU object store via the object store."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    assert gpu_object_store.has_gpu_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    tensors = gpu_object_store.get_gpu_object(obj_id)
    # TODO(kevin85421): The current garbage collection implementation for the
    # in-actor object store is naive. We garbage collect each object after it
    # is consumed once.
    gpu_object_store.remove_gpu_object(obj_id)
    return tensors


class GPUObjectStore:
    def __init__(self):
        # A dictionary that maps from an object ID to a list of tensors.
        #
        # Note: Currently, `gpu_object_store` is only supported for Ray Actors.
        self.gpu_object_store: Dict[str, List["torch.Tensor"]] = {}

    def has_gpu_object(self, obj_id: str) -> bool:
        return obj_id in self.gpu_object_store

    def get_gpu_object(self, obj_id: str) -> Optional[List["torch.Tensor"]]:
        return self.gpu_object_store[obj_id]

    def add_gpu_object(self, obj_id: str, gpu_object: List["torch.Tensor"]):
        self.gpu_object_store[obj_id] = gpu_object

    def remove_gpu_object(self, obj_id: str):
        del self.gpu_object_store[obj_id]
