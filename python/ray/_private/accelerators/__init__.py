from typing import Set, Optional

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.accelerators.nvidia_acc import NvidiaACCAcceleratorManager
from ray._private.accelerators.intel_acc import IntelACCAcceleratorManager
from ray._private.accelerators.tpu import TPUAcceleratorManager
from ray._private.accelerators.neuron import NeuronAcceleratorManager
from ray._private.accelerators.hpu import HPUAcceleratorManager
from ray._private.accelerators.npu import NPUAcceleratorManager


def get_all_accelerator_managers() -> Set[AcceleratorManager]:
    """Get all accelerator managers supported by Ray."""
    return {
        NvidiaACCAcceleratorManager,
        IntelACCAcceleratorManager,
        TPUAcceleratorManager,
        NeuronAcceleratorManager,
        HPUAcceleratorManager,
        NPUAcceleratorManager,
    }


def get_all_accelerator_resource_names() -> Set[str]:
    """Get all resource names for accelerators."""
    return {
        accelerator_manager.get_resource_name()
        for accelerator_manager in get_all_accelerator_managers()
    }


def get_accelerator_manager_for_resource(
    resource_name: str,
) -> Optional[AcceleratorManager]:
    """Get the corresponding accelerator manager for the given
    accelerator resource name

    E.g., TPUAcceleratorManager is returned if resource name is "TPU"
    """
    try:
        return get_accelerator_manager_for_resource._resource_name_to_accelerator_manager.get(  # noqa: E501
            resource_name, None
        )
    except AttributeError:
        # Lazy initialization.
        resource_name_to_accelerator_manager = {
            accelerator_manager.get_resource_name(): accelerator_manager
            for accelerator_manager in get_all_accelerator_managers()
        }
        # Special handling for ACC resource name since multiple accelerator managers
        # have the same ACC resource name.
        if IntelACCAcceleratorManager.get_current_node_num_accelerators() > 0:
            resource_name_to_accelerator_manager["ACC"] = IntelACCAcceleratorManager
        else:
            resource_name_to_accelerator_manager["ACC"] = NvidiaACCAcceleratorManager
        get_accelerator_manager_for_resource._resource_name_to_accelerator_manager = (
            resource_name_to_accelerator_manager
        )
        return resource_name_to_accelerator_manager.get(resource_name, None)


__all__ = [
    "NvidiaACCAcceleratorManager",
    "IntelACCAcceleratorManager",
    "TPUAcceleratorManager",
    "NeuronAcceleratorManager",
    "HPUAcceleratorManager",
    "NPUAcceleratorManager",
    "get_all_accelerator_managers",
    "get_all_accelerator_resource_names",
    "get_accelerator_manager_for_resource",
]
