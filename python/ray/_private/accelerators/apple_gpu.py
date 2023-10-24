import os
import logging
from typing import Optional, List, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

class AppleGPUAcceleratorManager(AcceleratorManager):
    """Apple GPU accelerators manager."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        # There isn't a direct equivalent to CUDA_VISIBLE_DEVICES for Apple Silicon GPUs.
        # However, the USE_MPS environment variable can be used for enabling MPS support in PyTorch.
        return "USE_MPS"

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        return 1

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        return "Apple GPU"  # Generic name, as there isn't a specific model like Nvidia's H100

    @staticmethod
    def validate_resource_request_quantity(quantity: float) -> Tuple[bool, Optional[str]]:
        if quantity >= 0:
            return True, None
        else:
            return False, "Quantity cannot be negative."

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        return None

    @staticmethod
    def set_current_process_visible_accelerator_ids(ids: List[str]) -> None:
        pass

    @staticmethod
    def get_ec2_instance_num_accelerators(instance_type: str, instances: dict) -> Optional[int]:
        return None

    @staticmethod
    def get_ec2_instance_accelerator_type(instance_type: str, instances: dict) -> Optional[str]:
        return None
