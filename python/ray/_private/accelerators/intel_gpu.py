import os
import logging
from typing import Optional, List, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

ONEAPI_DEVICE_SELECTOR_ENV_VAR = "ONEAPI_DEVICE_SELECTOR"
NOSET_ONEAPI_DEVICE_SELECTOR_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR"
ONEAPI_DEVICE_BACKEND_TYPE = "level_zero"
ONEAPI_DEVICE_TYPE = "gpu"


class IntelGPUAcceleratorManager(AcceleratorManager):
    """Intel GPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return ONEAPI_DEVICE_SELECTOR_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        oneapi_visible_devices = os.environ.get(
            IntelGPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )
        if oneapi_visible_devices is None:
            return None

        if oneapi_visible_devices == "":
            return []

        if oneapi_visible_devices == "NoDevFiles":
            return []

        prefix = ONEAPI_DEVICE_BACKEND_TYPE + ":"

        return list(oneapi_visible_devices.split(prefix)[1].split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        num_gpus = 0
        try:
            import dpctl
        except ImportError:
            dpctl = None
        if dpctl is not None:
            backend = ONEAPI_DEVICE_BACKEND_TYPE
            device_type = ONEAPI_DEVICE_TYPE
            num_gpus = len(dpctl.get_devices(backend=backend, device_type=device_type))
        return num_gpus

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Get the name of first Intel GPU. (supposed only one GPU type on a node)
        Example:
            name: 'Intel(R) Data Center GPU Max 1550'
            return name: 'Intel-GPU-Max-1550'
        Returns:
            A string representing the name of Intel GPU type.
        """
        try:
            import dpctl
        except ImportError:
            dpctl = None
        if dpctl is None:
            return None
        backend = ONEAPI_DEVICE_BACKEND_TYPE
        device_type = ONEAPI_DEVICE_TYPE
        devices = dpctl.get_devices(backend=backend, device_type=device_type)
        if len(devices) > 0:
            name = devices[0].name
            return "Intel-GPU" + "-".join(name.split(" ")[-2:])
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_xpu_devices: List[str],
    ) -> None:
        if os.environ.get(NOSET_ONEAPI_DEVICE_SELECTOR_ENV_VAR):
            return

        prefix = ONEAPI_DEVICE_BACKEND_TYPE + ":"
        os.environ[
            IntelGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = prefix + ",".join([str(i) for i in visible_xpu_devices])
