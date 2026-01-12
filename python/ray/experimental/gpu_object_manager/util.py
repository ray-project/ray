import threading
from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional

import ray
from ray._raylet import ObjectRef
from ray.experimental.gpu_object_manager.collective_tensor_transport import (
    GLOOTensorTransport,
    NCCLTensorTransport,
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


class TransportManagerInfo(NamedTuple):
    transport_manager_class: type[TensorTransportManager]
    # list of support device types for the transport
    devices: List[str]


transport_manager_info: Dict[str, TransportManagerInfo] = {}

# Singleton instances of transport managers
transport_managers: Dict[str, TensorTransportManager] = {}

# To protect the singleton instances of transport managers
transport_managers_lock = threading.Lock()

# Flipped to True when the first custom transport is registered.
has_custom_transports = False


@PublicAPI(stability="alpha")
def register_tensor_transport(
    transport_name: str,
    devices: List[str],
    transport_manager_class: type[TensorTransportManager],
):
    """
    Register a new tensor transport for use in Ray. Note that this needs to be called
    before you create the actors that will use the transport. The actors also
    need to be created in the same process from which you call this function.

    Args:
        transport_name: The name of the transport protocol.
        devices: List of PyTorch device types supported by this transport (e.g., ["cuda", "cpu"]).
        transport_manager_class: A class that implements TensorTransportManager.
    Raises:
        ValueError: If transport_manager_class is not a subclass of TensorTransportManager.
    """
    global transport_manager_info
    global has_custom_transports

    transport_name = transport_name.upper()

    if transport_name in transport_manager_info:
        raise ValueError(f"Transport {transport_name} already registered.")

    if not issubclass(transport_manager_class, TensorTransportManager):
        raise ValueError(
            f"transport_manager_class {transport_manager_class.__name__} must be a subclass of TensorTransportManager."
        )

    transport_manager_info[transport_name] = TransportManagerInfo(
        transport_manager_class, devices
    )

    if transport_name not in DEFAULT_TRANSPORTS:
        has_custom_transports = True


DEFAULT_TRANSPORTS = ["NIXL", "GLOO", "NCCL"]

register_tensor_transport("NIXL", ["cuda", "cpu"], NixlTensorTransport)
register_tensor_transport("GLOO", ["cpu"], GLOOTensorTransport)
register_tensor_transport("NCCL", ["cuda"], NCCLTensorTransport)


def get_tensor_transport_manager(
    transport_name: str,
) -> "TensorTransportManager":
    """Get the tensor transport manager for the given tensor transport protocol.

    Args:
        transport_name: The tensor transport protocol to use for the GPU object.

    Returns:
        TensorTransportManager: The tensor transport manager for the given tensor transport protocol.
    """
    global transport_manager_info
    global transport_managers
    global transport_managers_lock

    with transport_managers_lock:
        if transport_name in transport_managers:
            return transport_managers[transport_name]

        if transport_name not in transport_manager_info:
            raise ValueError(f"Unsupported tensor transport protocol: {transport_name}")

        transport_managers[transport_name] = transport_manager_info[
            transport_name
        ].transport_manager_class()
        return transport_managers[transport_name]


def register_custom_tensor_transports_on_actor(
    actor: "ray.actor.ActorHandle",
) -> Optional[ObjectRef]:
    """
    If there's no custom transports to register, returns None.
    Otherwise returns an object ref for a task on the actor that will register the custom transports.
    """
    global transport_manager_info
    global has_custom_transports

    if not has_custom_transports:
        return None

    def register_transport_on_actor(
        self, owner_transport_manager_info: Dict[str, TransportManagerInfo]
    ):
        from ray.experimental.gpu_object_manager.util import (
            register_tensor_transport,
            transport_manager_info,
        )

        for transport_name, transport_info in owner_transport_manager_info.items():
            if transport_name not in transport_manager_info:
                register_tensor_transport(
                    transport_name,
                    transport_info.devices,
                    transport_info.transport_manager_class,
                )

    return actor.__ray_call__.options(concurrency_group="_ray_system").remote(
        register_transport_on_actor, transport_manager_info
    )


def device_match_transport(device: "torch.device", tensor_transport: str) -> bool:
    """Check if the device matches the transport."""

    if tensor_transport not in transport_manager_info:
        raise ValueError(f"Unsupported tensor transport protocol: {tensor_transport}")

    return device.type in transport_manager_info[tensor_transport].devices


def normalize_and_validate_tensor_transport(tensor_transport: str) -> str:
    tensor_transport = tensor_transport.upper()

    if tensor_transport not in transport_manager_info:
        raise ValueError(f"Invalid tensor transport: {tensor_transport}")

    return tensor_transport


def validate_one_sided(tensor_transport: str, ray_usage_func: str):
    if not transport_manager_info[
        tensor_transport
    ].transport_manager_class.is_one_sided():
        raise ValueError(
            f"Trying to use two-sided tensor transport: {tensor_transport} for {ray_usage_func}. "
            "This is only supported for one-sided transports such as NIXL or the OBJECT_STORE."
        )
