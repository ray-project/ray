import logging
import os
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

RBLN_RT_VISIBLE_DEVICES_ENV_VAR = "RBLN_DEVICES"
NOSET_RBLN_RT_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_RBLN_RT_VISIBLE_DEVICES"


class RBLNAcceleratorManager(AcceleratorManager):
    """Rebellions RBLN accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "RBLN"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return RBLN_RT_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        visible_devices = os.environ.get(
            RBLNAcceleratorManager.get_visible_accelerator_ids_env_var()
        )
        if visible_devices is None:
            return None
        if visible_devices == "":
            return []
        return visible_devices.split(",")

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """Detects the number of RBLN devices on the current machine."""
        try:
            from rebel import device_count

            return device_count()
        except Exception as e:
            logger.debug("Could not detect RBLN devices: %s", e)
            return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Gets the type of RBLN NPU on the current node."""
        try:
            from rebel import get_npu_name

            return get_npu_name()
        except Exception as e:
            logger.exception("Failed to detect RBLN NPU type: %s", e)
            return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        if isinstance(quantity, float) and not quantity.is_integer():
            return (
                False,
                f"{RBLNAcceleratorManager.get_resource_name()} resource quantity"
                " must be whole numbers. "
                f"The specified quantity {quantity} is invalid.",
            )
        else:
            return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_rbln_devices: List[str],
    ) -> None:
        if not os.getenv(NOSET_RBLN_RT_VISIBLE_DEVICES_ENV_VAR):
            os.environ[
                RBLNAcceleratorManager.get_visible_accelerator_ids_env_var()
            ] = ",".join(map(str, visible_rbln_devices))
