import os
import logging
from typing import Optional, List, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

ROCR_VISIBLE_DEVICES_ENV_VAR = "ROCR_VISIBLE_DEVICES"
NOSET_ROCR_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_ROCR_VISIBLE_DEVICES"

amd_product_dict = {
    "0x738c": "AMD-Instinct-MI100",
    "0x7408": "AMD-Instinct-MI250X",
    "0x740c": "AMD-Instinct-MI250X-MI250",
    "0x740f": "AMD-Instinct-MI210",
    "0x74a1": "AMD-Instinct-MI300X-OAM",
    "0x6798": "AMD-Radeon-R9-200-HD-7900",
    "0x6799": "AMD-Radeon-HD-7900",
    "0x679A": "AMD-Radeon-HD-7900",
    "0x679B": "AMD-Radeon-HD-7900",
}


class AMDGPUAcceleratorManager(AcceleratorManager):
    """AMD GPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return ROCR_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        amd_visible_devices = os.environ.get(
            AMDGPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )

        if amd_visible_devices is None:
            return None

        if amd_visible_devices == "":
            return []

        if amd_visible_devices == "NoDevFiles":
            return []

        return list(amd_visible_devices.split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        import ray._private.thirdparty.pyamdsmi as pyamdsmi

        num_gpus = 0

        try:
            pyamdsmi.smi_initialize()
            num_gpus = pyamdsmi.smi_get_device_count()
        except Exception:
            pass
        finally:
            try:
                pyamdsmi.smi_shutdown()
            except Exception:
                pass

        return num_gpus

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        try:
            device_ids = AMDGPUAcceleratorManager._get_amd_device_ids()
            if device_ids is None:
                return None
            return AMDGPUAcceleratorManager._gpu_name_to_accelerator_type(device_ids[0])
        except Exception:
            return None

    @staticmethod
    def _gpu_name_to_accelerator_type(name):
        if name is None:
            return None
        try:
            match = amd_product_dict[name]
            return match
        except Exception:
            return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_amd_devices: List[str],
    ) -> None:
        if os.environ.get(NOSET_ROCR_VISIBLE_DEVICES_ENV_VAR):
            return

        os.environ[
            AMDGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_amd_devices])

    @staticmethod
    def _get_amd_device_ids() -> List[str]:
        """Get the list of GPUs IDs
        Example:
            On a node with 2x MI210 GPUs
            pyamdsmi library python bindings
            return: ['0x740f', '0x740f']
        Returns:
            A list of strings containing GPU IDs
        """
        import ray._private.thirdparty.pyamdsmi as pyamdsmi

        device_ids = []
        try:
            pyamdsmi.smi_initialize()
            num_devices = pyamdsmi.smi_get_device_count()
            for i in range(num_devices):
                did = pyamdsmi.smi_get_device_id(i)
                if did >= 0:
                    device_ids.append(hex(did))
        except Exception:
            return None
        finally:
            try:
                pyamdsmi.pyamdsmi_shutdown()
            except Exception:
                pass

        return device_ids
