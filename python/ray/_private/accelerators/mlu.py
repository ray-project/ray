import glob
import logging
import os
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

MLU_VISIBLE_DEVICES_ENV_VAR = "MLU_VISIBLE_DEVICES"
NOSET_MLU_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_MLU_VISIBLE_DEVICES"


class MLUAcceleratorManager(AcceleratorManager):
    """Cambricon MLU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "MLU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return MLU_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        mlu_visible_devices = os.environ.get(
            MLUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )

        if mlu_visible_devices is None:
            return None

        if mlu_visible_devices == "":
            return []

        if mlu_visible_devices == "NoDevFiles":
            return []

        return list(mlu_visible_devices.split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """Attempt to detect the number of MLUs on this machine.

        Count the Cambricon device files exposed to this node.  Ray Core does
        not require a Python binding for the native CNDev library.

        Returns:
             The number of MLUs if any were detected, otherwise 0.
        """
        try:
            return len(glob.glob("/dev/cambricon_dev[0-9]*"))
        except Exception as e:
            logger.debug("Failed to detect number of MLUs: %s", e)
            return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """MLU type detection requires a device API that Ray does not depend on."""
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_mlu_devices: List[str],
    ) -> None:
        if env_bool(NOSET_MLU_VISIBLE_DEVICES_ENV_VAR, False):
            return

        os.environ[
            MLUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_mlu_devices])
