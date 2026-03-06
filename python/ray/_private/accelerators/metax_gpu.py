import logging
import os
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

CUDA_VISIBLE_DEVICES_ENV_VAR = "CUDA_VISIBLE_DEVICES"
NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"


class MetaxGPUAcceleratorManager(AcceleratorManager):
    """Metax GPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return CUDA_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        cuda_visible_devices = os.environ.get(
            MetaxGPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )
        if cuda_visible_devices is None:
            return None

        if cuda_visible_devices == "":
            return []

        return list(cuda_visible_devices.split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        try:
            import pymxsml.mxsml_extension as pymxsml

            try:
                pymxsml.mxSmlExInit()
            except pymxsml.MXSMLEXError:
                return 0
            device_count = pymxsml.mxSmlExDeviceGetCount()
            pymxsml.mxSmlExShutdown()
            return device_count
        except Exception as e:
            logger.debug("Could not import pymxsml: %s", e)
        return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        try:
            import pymxsml.mxsml_extension as pymxsml

            try:
                pymxsml.mxSmlExInit()
            except pymxsml.MXSMLEXError:
                return None
            device_name = None
            device_count = pymxsml.mxSmlExDeviceGetCount()
            if device_count > 0:
                handle = pymxsml.mxSmlExDeviceGetHandleByIndex(0)
                device_name = pymxsml.mxSmlExDeviceGetName(handle)
                if isinstance(device_name, bytes):
                    device_name = device_name.decode("utf-8")
            pymxsml.mxSmlExShutdown()
            return device_name
        except Exception:
            logger.warning("Failed to detect GPU type.", exc_info=True)
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_cuda_devices: List[str],
    ) -> None:
        if os.environ.get(NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR):
            return

        os.environ[
            MetaxGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join(visible_cuda_devices)
