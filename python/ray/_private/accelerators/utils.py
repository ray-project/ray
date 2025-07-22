from typing import Dict, Optional, Tuple

from ray._private import accelerators
from ray._private.accelerators import AcceleratorManager


def get_current_node_accelerator(
    num_gpus, resources: Dict[str, float]
) -> Optional[Tuple[AcceleratorManager, int]]:
    """
    Returns the AcceleratorManager and accelerator count for the accelerator
    associated with this node.

    This assumes each node has at most one accelerator type. If no accelerators
    are present, returns None.
    """
    for resource_name in accelerators.get_all_accelerator_resource_names():
        accelerator_manager = accelerators.get_accelerator_manager_for_resource(
            resource_name
        )
        # Respect configured value for GPUs if set
        if resource_name == "GPU" and num_gpus:
            num_accelerators = num_gpus

        num_accelerators = resources.get(resource_name)
        if num_accelerators is None:
            num_accelerators = accelerator_manager.get_current_node_num_accelerators()
            visible_accelerator_ids = (
                accelerator_manager.get_current_process_visible_accelerator_ids()
            )
            if visible_accelerator_ids is not None:
                num_accelerators = min(num_accelerators, len(visible_accelerator_ids))

        if num_accelerators > 0:
            return accelerator_manager, num_accelerators

    return None, 0
