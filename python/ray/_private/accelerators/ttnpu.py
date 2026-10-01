import glob
import logging
import os
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

TENSTORRENT_VISIBLE_DEVICES_ENV_VAR = "TT_VISIBLE_DEVICES"
NOSET_TENSTORRENT_VISIBLE_DEVICES_ENV_VAR = (
    "RAY_EXPERIMENTAL_NOSET_TENSTORRENT_VISIBLE_DEVICES"
)


class TTNPUAcceleratorManager(AcceleratorManager):
    """Tenstorrent NPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "TTNPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return TENSTORRENT_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        tenstorrent_visible_devices = os.environ.get(
            TTNPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )

        if tenstorrent_visible_devices is None:
            return None
        if tenstorrent_visible_devices in ("", "NoDevFiles"):
            return []
        return tenstorrent_visible_devices.split(",")

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """Attempt to detect the number of TTNPUs on this machine."""

        try:
            npu_files = glob.glob("/dev/tenstorrent/*")
            return len(npu_files)
        except Exception as e:
            logger.debug("Failed to detect number of TTNPUs: %s", e)
        return 0

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        if quantity > 0 and quantity % 1 != 0:
            return (
                False,
                f"TTNPU resource quantity ({quantity}) must be an integer.",
            )
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_npu_devices: List[str],
    ) -> None:
        if env_bool(NOSET_TENSTORRENT_VISIBLE_DEVICES_ENV_VAR, False):
            return

        os.environ[
            TTNPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_npu_devices])

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        return None

    @staticmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        return None
