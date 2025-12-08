import threading
from typing import TYPE_CHECKING

from ray.experimental.gpu_object_manager.collective_tensor_transport import (
    CollectiveTensorTransport,
)
from ray.experimental.gpu_object_manager.nixl_tensor_transport import (
    NixlTensorTransport,
)
from ray.experimental.gpu_object_manager.tensor_transport_manager import (
    TensorTransportManager,
)

if TYPE_CHECKING:
    import torch


# Class definitions for transport managers
transport_manager_classes: dict[str, TensorTransportManager] = {
    "nixl": NixlTensorTransport,
    "torch_gloo": CollectiveTensorTransport,
    "nccl": CollectiveTensorTransport,
}

transport_devices = {
    "nixl": ["cuda", "cpu"],
    "torch_gloo": ["cpu"],
    "nccl": ["cuda"],
}


# Singleton instances of transport managers
transport_managers = {}

transport_managers_lock = threading.Lock()


def get_tensor_transport_manager(
    transport_name: str,
) -> "TensorTransportManager":
    """Get the tensor transport manager for the given tensor transport protocol.

    Args:
        transport_name: The tensor transport protocol to use for the GPU object.

    Returns:
        TensorTransportManager: The tensor transport manager for the given tensor transport protocol.
    """
    global transport_manager_classes
    global transport_managers
    global transport_managers_lock

    with transport_managers_lock:
        if transport_name in transport_managers:
            return transport_managers[transport_name]

        if transport_name not in transport_manager_classes:
            raise ValueError(f"Unsupported tensor transport protocol: {transport_name}")

        transport_managers[transport_name] = transport_manager_classes[transport_name](
            transport_name
        )
        return transport_managers[transport_name]


def device_match_transport(device: "torch.device", tensor_transport: str) -> bool:
    """Check if the device matches the transport."""

    if tensor_transport not in transport_devices:
        raise ValueError(f"Unsupported tensor transport protocol: {tensor_transport}")

    return device.type in transport_devices[tensor_transport]
