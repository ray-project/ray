from typing import Set

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.accelerators.nvidia_gpu import NvidiaGPUAcceleratorManager
from ray._private.accelerators.tpu import TPUAcceleratorManager
from ray._private.accelerators.neuron import NeuronAcceleratorManager


def get_all_accelerator_managers() -> Set[AcceleratorManager]:
    """Get all accelerator managers supported by Ray."""
    return {
        NvidiaGPUAcceleratorManager,
        TPUAcceleratorManager,
        NeuronAcceleratorManager,
    }


def get_all_accelerator_resource_names() -> Set[str]:
    """Get all resource names for accelerators."""
    return {
        accelerator_manager.get_resource_name()
        for accelerator_manager in get_all_accelerator_managers()
    }


_resource_name_to_accelerator_manager = {
    accelerator_manager.get_resource_name(): accelerator_manager
    for accelerator_manager in get_all_accelerator_managers()
}


def get_accelerator_manager_for_resource(resource_name: str) -> AcceleratorManager:
    """Get the corresponding accelerator manager for the given
    accelerator resource name

    E.g., TPUAcceleratorManager is returned if resource name is "TPU"
    """
    return _resource_name_to_accelerator_manager.get(resource_name, None)


__all__ = [
    "NvidiaGPUAcceleratorManager",
    "TPUAcceleratorManager",
    "NeuronAcceleratorManager",
    "get_all_accelerator_managers",
    "get_all_accelerator_resource_names",
    "get_accelerator_manager_for_resource",
]
