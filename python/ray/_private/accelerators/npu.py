import os
import glob
import logging
from typing import Optional, List, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

ASCEND_RT_VISIBLE_DEVICES_ENV_VAR = "ASCEND_RT_VISIBLE_DEVICES"
NOSET_ASCEND_RT_VISIBLE_DEVICES_ENV_VAR = (
    "RAY_EXPERIMENTAL_NOSET_ASCEND_RT_VISIBLE_DEVICES"
)


class NPUAcceleratorManager(AcceleratorManager):
    """Ascend NPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "NPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return ASCEND_RT_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        ascend_visible_devices = os.environ.get(
            NPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )

        if ascend_visible_devices is None:
            return None

        if ascend_visible_devices == "":
            return []

        if ascend_visible_devices == "NoDevFiles":
            return []

        return list(ascend_visible_devices.split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """Attempt to detect the number of NPUs on this machine.

        NPU chips are represented as devices within `/dev/`, either as `/dev/davinci?`.

        Returns:
            The number of NPUs if any were detected, otherwise 0.
        """
        try:
            import acl

            device_count, ret = acl.rt.get_device_count()
            if ret == 0:
                return device_count
        except Exception as e:
            logger.debug("Could not import AscendCL: %s", e)

        try:
            npu_files = glob.glob("/dev/davinci?")
            return len(npu_files)
        except Exception as e:
            logger.debug("Failed to detect number of NPUs: %s", e)
        return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Get the type of the Ascend NPU on the current node.

        Returns:
            A string of the type, such as "Ascend910A", "Ascend910B", "Ascend310P1".
        """
        try:
            import acl

            return acl.get_soc_name()
        except Exception:
            logger.exception("Failed to detect NPU type.")
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_npu_devices: List[str],
    ) -> None:
        if os.environ.get(NOSET_ASCEND_RT_VISIBLE_DEVICES_ENV_VAR):
            return

        os.environ[
            NPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_npu_devices])
