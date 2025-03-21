import os
import glob
import logging
from typing import Optional, List, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

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

        MLU chips are represented as devices within `/dev/`.

        Returns:
             The number of MLUs if any were detected, otherwise 0.
        """
        try:
            import torch_mlu

            return torch_mlu.mlu.device_count()
        except Exception as e:
            logger.debug("Could not import torch_mlu: %s", e)

        try:
            mlu_files = glob.glob("/dev/cambricon_dev[0-9]*")
            return len(mlu_files)
        except Exception as e:
            logger.debug("Failed to detect number of MLUs: %s", e)
        return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Get the type of the Cambricon MLU on the current node.

        Returns:
            A string of the type, such as "MLU290", "MLU370".
        """
        try:
            import torch_mlu

            return torch_mlu.mlu.get_device_name(0)
        except Exception:
            logger.exception("Failed to detect MLU type.")
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
        if os.environ.get(NOSET_MLU_VISIBLE_DEVICES_ENV_VAR):
            return

        os.environ[
            MLUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_mlu_devices])
