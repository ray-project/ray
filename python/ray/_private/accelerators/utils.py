from typing import Dict, Optional, Tuple

from ray._common.utils import RESOURCE_CONSTRAINT_PREFIX
from ray._private import accelerators
from ray._private.accelerators import AcceleratorManager


def get_current_node_accelerator(
    resources: Dict[str, float]
) -> Optional[Tuple[AcceleratorManager, str, int]]:
    """
    Returns the AcceleratorManager, resource name, and accelerator count for the
    accelerator associated with this node.

    This assumes each node has at most one accelerator type. If no accelerators
    are present, returns None.
    """
    for resource_name in accelerators.get_all_accelerator_resource_names():
        accelerator_manager = accelerators.get_accelerator_manager_for_resource(
            resource_name
        )
        accelerator_resource_name = accelerator_manager.get_resource_name()

        num_accelerators = resources.get(accelerator_resource_name)
        if num_accelerators is None:
            num_accelerators = accelerator_manager.get_current_node_num_accelerators()
            visible_accelerator_ids = (
                accelerator_manager.get_current_process_visible_accelerator_ids()
            )
            if visible_accelerator_ids is not None:
                num_accelerators = min(num_accelerators, len(visible_accelerator_ids))

        if num_accelerators > 0:
            return accelerator_manager, accelerator_resource_name, num_accelerators

    return None


def resolve_and_update_accelerator_resources(
    num_gpus: int, resources: Dict[str, float]
) -> int:
    """Detect and update accelerator resources on a node."""
    accelerator = get_current_node_accelerator(resources)
    if not accelerator:
        return num_gpus or 0

    accelerator_manager, accelerator_resource_name, num_accelerators = accelerator
    visible_accelerator_ids = (
        accelerator_manager.get_current_process_visible_accelerator_ids()
    )

    # Respect configured value for GPUs if set
    if accelerator_resource_name == "GPU" and num_gpus:
        num_accelerators = num_gpus

    # Check that the number of accelerators that the raylet wants doesn't
    # exceed the amount allowed by visible accelerator ids.
    if (
        num_accelerators is not None
        and visible_accelerator_ids is not None
        and num_accelerators > len(visible_accelerator_ids)
    ):
        raise ValueError(
            f"Attempting to start raylet with {num_accelerators} "
            f"{accelerator_resource_name}, "
            f"but {accelerator_manager.get_visible_accelerator_ids_env_var()} "
            f"contains {visible_accelerator_ids}."
        )

    if num_accelerators:
        if accelerator_resource_name == "GPU":
            num_gpus = num_accelerators
        else:
            resources[accelerator_resource_name] = num_accelerators

        accelerator_type = accelerator_manager.get_current_node_accelerator_type()
        if accelerator_type:
            resources[f"{RESOURCE_CONSTRAINT_PREFIX}{accelerator_type}"] = 1

            from ray._private.usage import usage_lib

            usage_lib.record_hardware_usage(accelerator_type)
        additional_resources = (
            accelerator_manager.get_current_node_additional_resources()
        )
        if additional_resources:
            resources.update(additional_resources)

    return num_gpus or 0


def get_current_node_accelerator_type(resources: dict) -> Optional[str]:
    """
    Returns the accelerator type (e.g. 'A100') if detectable on this node,
    or None if no accelerator is present.
    """
    accelerator = get_current_node_accelerator(resources)
    if accelerator:
        accelerator_manager, _, _ = accelerator
        accelerator_type = accelerator_manager.get_current_node_accelerator_type()
        return accelerator_type or None

    return None
