import threading
from typing import TYPE_CHECKING

from ray._private.custom_types import TensorTransportEnum
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
    "NIXL": NixlTensorTransport,
    "GLOO": CollectiveTensorTransport,
    "NCCL": CollectiveTensorTransport,
}

transport_devices = {
    "NIXL": ["cuda", "cpu"],
    "GLOO": ["cpu"],
    "NCCL": ["cuda"],
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


def normalize_and_validate_tensor_transport(tensor_transport: str) -> str:
    tensor_transport = tensor_transport.upper()

    if (
        tensor_transport != TensorTransportEnum.OBJECT_STORE.name
        and tensor_transport not in transport_manager_classes
    ):
        raise ValueError(f"Invalid tensor transport: {tensor_transport}")

    return tensor_transport


def validate_one_sided(tensor_transport: str, ray_usage_func: str):
    if (
        tensor_transport != TensorTransportEnum.OBJECT_STORE.name
        and not transport_manager_classes[tensor_transport].is_one_sided()
    ):
        raise ValueError(
            f"Trying to use two-sided tensor transport: {tensor_transport} for {ray_usage_func}. "
            "This is only supported for one-sided transports such as NIXL or the OBJECT_STORE."
        )
