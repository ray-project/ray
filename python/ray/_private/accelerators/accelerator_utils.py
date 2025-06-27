from typing import Dict, Optional, Tuple

from ray._common.utils import RESOURCE_CONSTRAINT_PREFIX
from ray._private import accelerators


def resolve_and_update_accelerator_resources(
    num_gpus: int, resources: Dict[str, float]
) -> int:
    """Detect and update accelerator resources on a node."""
    for accelerator_resource_name in accelerators.get_all_accelerator_resource_names():
        accelerator_manager = accelerators.get_accelerator_manager_for_resource(
            accelerator_resource_name
        )
        # Respect configured value for GPUs if set
        num_accelerators = None
        if accelerator_resource_name == "GPU":
            num_accelerators = num_gpus
        else:
            num_accelerators = resources.get(accelerator_resource_name, None)
        visible_accelerator_ids = (
            accelerator_manager.get_current_process_visible_accelerator_ids()
        )

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

        if num_accelerators is None:
            # Try to automatically detect the number of accelerators.
            num_accelerators = accelerator_manager.get_current_node_num_accelerators()
            # Don't use more accelerators than allowed by visible accelerator ids.
            if visible_accelerator_ids is not None:
                num_accelerators = min(num_accelerators, len(visible_accelerator_ids))

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


def get_first_detectable_accelerator_type(
    resources: dict,
) -> Tuple[Optional[str], Optional[int]]:
    """
    Returns the first detectable accelerator type on this node and num_gpus.

    Args:
        resources: A dictionary of resolved resource quantities.

    Returns:
        Tuple of:
          - Accelerator type string (e.g., 'A100') or None if undetectable
    """
    for accelerator_resource_name in accelerators.get_all_accelerator_resource_names():
        accelerator_manager = accelerators.get_accelerator_manager_for_resource(
            accelerator_resource_name
        )

        num_accelerators = resources.get(accelerator_resource_name)
        if num_accelerators is None:
            num_accelerators = accelerator_manager.get_current_node_num_accelerators()

        accelerator_type = accelerator_manager.get_current_node_accelerator_type()

        if accelerator_type and num_accelerators:
            return accelerator_type

    return None
