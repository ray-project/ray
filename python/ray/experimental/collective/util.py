import socket
from contextlib import closing
from typing import TYPE_CHECKING, Tuple

import ray
from ray.experimental.collective.collective_tensor_transport import (
    CollectiveTensorTransport,
)
from ray.experimental.collective.nixl_tensor_transport import NixlTensorTransport
from ray.experimental.collective.tensor_transport_manager import TensorTransportManager
from ray.util.collective.types import Backend

if TYPE_CHECKING:
    import torch

# Singleton instances for tensor transport managers
_nixl_tensor_transport_manager = None
_gloo_tensor_transport_manager = None
_nccl_tensor_transport_manager = None


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
        global _nixl_tensor_transport_manager
        if _nixl_tensor_transport_manager is None:
            _nixl_tensor_transport_manager = NixlTensorTransport()
        return _nixl_tensor_transport_manager
    elif tensor_transport == Backend.TORCH_GLOO:
        global _gloo_tensor_transport_manager
        if _gloo_tensor_transport_manager is None:
            _gloo_tensor_transport_manager = CollectiveTensorTransport(tensor_transport)
        return _gloo_tensor_transport_manager
    elif tensor_transport == Backend.NCCL:
        global _nccl_tensor_transport_manager
        if _nccl_tensor_transport_manager is None:
            _nccl_tensor_transport_manager = CollectiveTensorTransport(tensor_transport)
        return _nccl_tensor_transport_manager
    else:
        raise ValueError(f"Unsupported tensor transport protocol: {tensor_transport}")


def device_match_transport(device: "torch.device", tensor_transport: Backend) -> bool:
    """Check if the device matches the transport."""
    if tensor_transport == Backend.NIXL:
        return device.type == "cuda" or device.type == "cpu"
    elif tensor_transport == Backend.TORCH_GLOO:
        return device.type == "cpu"
    elif tensor_transport == Backend.NCCL:
        return device.type == "cuda"
    else:
        raise ValueError(f"Unsupported tensor transport protocol: {tensor_transport}")


def find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def get_address_and_port() -> Tuple[str, int]:
    """Returns the IP address and a free port on this node."""
    addr = ray.util.get_node_ip_address()
    port = find_free_port()

    return addr, port
