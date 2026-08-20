import glob
import logging
import os
from importlib.util import find_spec
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

MLU_VISIBLE_DEVICES_ENV_VAR = "MLU_VISIBLE_DEVICES"
NOSET_MLU_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_MLU_VISIBLE_DEVICES"


def is_package_present(package_name: str) -> bool:
    try:
        return find_spec(package_name) is not None
    except ModuleNotFoundError:
        return False


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

        MLU devices are represented as `/dev/cambricon_devN` entries.

        Returns:
             The number of MLUs if any were detected, otherwise 0.
        """
        try:
            mlu_files = glob.glob("/dev/cambricon_dev[0-9]*")
        except Exception as e:
            logger.debug("Failed to detect number of MLUs: %s", e)
            return 0

        if not mlu_files:
            return 0

        if is_package_present("torch_mlu"):
            try:
                import torch
                import torch_mlu  # noqa: F401

                return torch.mlu.device_count()
            except Exception as e:
                logger.debug("Could not detect MLUs with torch_mlu: %s", e)

        return len(mlu_files)

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
