import glob
import logging
import os
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

SUPA_VISIBLE_DEVICES_ENV_VAR = "SUPA_VISIBLE_DEVICES"
NOSET_SUPA_VISIBLE_DEVICES_ENV_VAR = (
    "RAY_EXPERIMENTAL_NOSET_SUPA_VISIBLE_DEVICES"
)


class SupaGPUAcceleratorManager(AcceleratorManager):
    """Biren SUPA accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return SUPA_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        supa_visible_devices = os.environ.get(
            SupaGPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )

        if supa_visible_devices is None:
            return None

        if supa_visible_devices == "":
            return []

        if supa_visible_devices == "NoDevFiles":
            return []

        return list(supa_visible_devices.split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """Detect Biren cards via /dev/biren/card_N device nodes."""
        try:
            biren_files = glob.glob("/dev/biren/card_[0-9]*")
            return len(biren_files)
        except Exception as e:
            logger.debug("Failed to detect Biren cards via /dev/biren: %s", e)
        return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Get Biren card model via torch_supa."""
        try:
            import torch

            if hasattr(torch, "supa") and torch.supa.is_available():
                return torch.supa.get_device_name(0)
        except Exception:
            logger.exception("Failed to detect Biren accelerator type.")
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_supa_devices: List[str],
    ) -> None:
        if env_bool(NOSET_SUPA_VISIBLE_DEVICES_ENV_VAR, False):
            return

        os.environ[
            SupaGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_supa_devices])
