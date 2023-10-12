from typing import Set

from ray._private.accelerators.accelerator import Accelerator
from ray._private.accelerators.nvidia_gpu import NvidiaGPUAccelerator
from ray._private.accelerators.tpu import TPUAccelerator
from ray._private.accelerators.neuron import NeuronAccelerator


def get_all_accelerators() -> Set[Accelerator]:
    """Get all accelerators supported by Ray."""
    return {
        NvidiaGPUAccelerator,
        TPUAccelerator,
        NeuronAccelerator,
    }


def get_all_accelerator_resource_names() -> Set[str]:
    """Get all resource names for accelerators."""
    return {accelerator.get_resource_name() for accelerator in get_all_accelerators()}


_resource_name_to_accelerator = {
    accelerator.get_resource_name(): accelerator
    for accelerator in get_all_accelerators()
}


def get_accelerator_for_resource(resource_name: str) -> Accelerator:
    return _resource_name_to_accelerator.get(resource_name, None)


__all__ = [
    "NvidiaGPUAccelerator",
    "TPUAccelerator",
    "NeuronAccelerator",
    "get_all_accelerators",
    "get_all_accelerator_resource_names",
    "get_accelerator_for_resource",
]
