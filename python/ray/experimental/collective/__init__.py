from ray.experimental.collective.operations import (
    allgather,
    allreduce,
    reducescatter,
)
from ray.experimental.collective.collective import (
    get_collective_groups,
    create_collective_group,
    destroy_collective_group,
    destroy_all_collective_groups,
)
from ray.util.collective.types import Backend
from ray.experimental.collective.tensor_transport_manager import TensorTransportManager
from ray.experimental.collective.nixl_tensor_transport import NixlTensorTransport
from ray.experimental.collective.collective_tensor_transport import (
    CollectiveTensorTransport,
)


def get_tensor_transport_manager(
    tensor_transport: Backend,
) -> "TensorTransportManager":
    """Get the tensor transport manager for the given tensor transport protocol.

    Args:
        tensor_transport: The tensor transport protocol to use for the GPU object.

    Returns:
        TensorTransportManager: The tensor transport manager for the given tensor transport protocol.
    """
    if tensor_transport == Backend.NIXL:
        return NixlTensorTransport()
    elif tensor_transport == Backend.TORCH_GLOO or tensor_transport == Backend.NCCL:
        return CollectiveTensorTransport()
    else:
        raise ValueError(f"Unsupported tensor transport protocol: {tensor_transport}")


__all__ = [
    "allgather",
    "allreduce",
    "reducescatter",
    "get_collective_groups",
    "create_collective_group",
    "destroy_collective_group",
    "destroy_all_collective_groups",
    "get_tensor_transport_manager",
]
