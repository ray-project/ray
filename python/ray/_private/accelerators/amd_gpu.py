import os
import sys
import logging
import subprocess
from typing import Optional, List, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

try:
    from amdsmi import (
        amdsmi_init,
        amdsmi_shut_down,
        amdsmi_get_processor_handles,
        amdsmi_get_gpu_asic_info,
        AmdSmiException,
    )
except Exception:
    pass


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
        num_gpus = 0
        major_version = 0

        try:
            major_version = AMDGPUAcceleratorManager._get_rocm_major_version()
        except Exception:
            return 0

        if major_version >= 6:
            try:
                amdsmi_init()
                try:
                    num_gpus = len(amdsmi_get_processor_handles())
                except AmdSmiException:
                    return 0
            except AmdSmiException:
                return 0
            finally:
                try:
                    amdsmi_shut_down()
                except AmdSmiException:
                    pass
        else:
            try:
                num_gpus = (
                    len(
                        subprocess.check_output(["rocm-smi", "-i", "--csv"])
                        .decode("utf-8")
                        .strip()
                        .split("\n")
                    )
                    - 1
                )
            except Exception:
                pass
        return num_gpus

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        try:
            if sys.platform.startswith("linux"):
                device_ids = AMDGPUAcceleratorManager._get_amd_device_ids()
                if device_ids is None:
                    return None
                return AMDGPUAcceleratorManager._gpu_name_to_accelerator_type(
                    device_ids[0]
                )
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
            For ROCm version < 6.0
                Uses rocm-smi subprocess call
            For ROCm version >= 6.0
                Uses amismi library python binding
            return: ['0x740f', '0x740f']
        Returns:
            A list of strings containing GPU IDs
        """
        try:
            major_version = AMDGPUAcceleratorManager._get_rocm_major_version()
        except Exception:
            return None

        device_ids = []
        if major_version >= 6:
            try:
                amdsmi_init()
                devices = amdsmi_get_processor_handles()
                if len(devices) > 0:
                    for device in devices:
                        asic_info = amdsmi_get_gpu_asic_info(device)
                        device_ids.append(hex(asic_info["device_id"]))
            except AmdSmiException:
                return None
            finally:
                try:
                    amdsmi_shut_down()
                except AmdSmiException:
                    pass
        else:
            try:
                amd_ids = eval(
                    subprocess.check_output(["rocm-smi", "--showid", "--json"]).decode(
                        "utf-8"
                    )
                )
                keys = amd_ids.keys()
                for k in keys:
                    device_ids.append(amd_ids[k]["GPU ID"])
            except Exception:
                return None

        return device_ids

    @staticmethod
    def _get_rocm_major_version() -> int:
        """Get ROCm version from hipconfig
        Example:
        ['HIPversion:6.0.32830-d62f6a171', '', '==hipconfig',
        'HIP_PATH:/opt/rocm-6.0.0', 'ROCM_PATH:/opt/rocm-6.0.0',
        'HIP_COMPILER:clang', 'HIP_PLATFORM:amd', 'HIP_RUNTIME:rocclr',
        'CPP_CONFIG:-D__HIP_PLATFORM_HCC__=-D__HIP_PLATFORM_AMD__=-I/opt/rocm-6.0.0/include',
        '', '', '==hip-clang', 'HIP_CLANG_PATH:/opt/rocm-6.0.0/llvm/bin']
        return: '6'

        Returns:
        ROCm major version string
        """
        major_version = 0

        if sys.platform.startswith("linux"):
            try:
                hipconfig = (
                    subprocess.check_output(["hipconfig"])
                    .decode("utf-8")
                    .replace(" ", "")
                    .split("\n")
                )
                subs = "HIPversion"
                res = [i for i in hipconfig if subs in i]

                if len(res) > 0:
                    version = res[0].split(":")[1]
                    major_version = int(version.split(".")[0])
            except Exception:
                raise Exception("Unable to find ROCm version.")

        return major_version
