import threading
from typing import TYPE_CHECKING, Dict, List

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
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch


# Class definitions for transport managers
transport_manager_classes: Dict[str, type[TensorTransportManager]] = {}

transport_devices: Dict[str, List[str]] = {}

# Singleton instances of transport managers
transport_managers: Dict[str, TensorTransportManager] = {}

transport_managers_lock = threading.Lock()


@PublicAPI(stability="alpha")
def register_tensor_transport(
    transport_name: str,
    devices: List[str],
    transport_manager_class: type[TensorTransportManager],
):
    """
    Register a new tensor transport for use in Ray.

    Args:
        transport_name: The name of the transport protocol.
        devices: List of device types supported by this transport (e.g., ["cuda", "cpu"]).
        transport_manager_class: A class that implements TensorTransportManager.

    Raises:
        ValueError: If transport_manager_class is not a class or does not subclass TensorTransportManager.
    """
    global transport_manager_classes
    global transport_devices

    if not issubclass(transport_manager_class, TensorTransportManager):
        raise ValueError(
            f"transport_manager_class {transport_manager_class.__name__} must be a subclass of TensorTransportManager."
        )

    transport_name = transport_name.upper()
    transport_manager_classes[transport_name] = transport_manager_class
    transport_devices[transport_name] = devices


register_tensor_transport("NIXL", ["cuda", "cpu"], NixlTensorTransport)
register_tensor_transport("GLOO", ["cpu"], CollectiveTensorTransport)
register_tensor_transport("NCCL", ["cuda"], CollectiveTensorTransport)


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
