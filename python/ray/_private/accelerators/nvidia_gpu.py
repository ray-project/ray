import re
import os
import logging
from typing import Optional, List, Tuple
import ray._private.thirdparty.pynvml as pynvml
from packaging.version import Version

from ray._private.accelerators.accelerator import AcceleratorManager
import ray._private.ray_constants as ray_constants

logger = logging.getLogger(__name__)

CUDA_VISIBLE_DEVICES_ENV_VAR = "CUDA_VISIBLE_DEVICES"
NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"

# TODO(Alex): This pattern may not work for non NVIDIA Tesla GPUs (which have
# the form "Tesla V100-SXM2-16GB" or "Tesla K80").
NVIDIA_GPU_NAME_PATTERN = re.compile(r"\w+\s+([A-Z0-9]+)")

# version with mig uuid
MIG_UUID_DRIVER_VERSION = "470.42.01"


class NvidiaGPUAcceleratorManager(AcceleratorManager):
    """Nvidia GPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return CUDA_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        cuda_visible_devices = os.environ.get(
            NvidiaGPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )
        if cuda_visible_devices is None:
            return None

        if cuda_visible_devices == "":
            return []

        if cuda_visible_devices == "NoDevFiles":
            return []

        return list(cuda_visible_devices.split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        try:
            pynvml.nvmlInit()
        except pynvml.NVMLError:
            return 0  # pynvml init failed
        driver_version = pynvml.nvmlSystemGetDriverVersion()
        device_count = pynvml.nvmlDeviceGetCount()
        cuda_devices = []
        for index in range(device_count):
            handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            mig_enabled = os.environ.get(
                ray_constants.RAY_ENABLE_MIG_DETECTION_ENV_VAR, False
            )
            if mig_enabled:
                try:
                    max_mig_count = pynvml.nvmlDeviceGetMaxMigDeviceCount(handle)
                except pynvml.NVMLError_NotSupported:
                    cuda_devices.append(str(index))
                    continue
                for mig_index in range(max_mig_count):
                    try:
                        mig_handle = pynvml.nvmlDeviceGetMigDeviceHandleByIndex(
                            handle, mig_index
                        )
                        mig_uuid = ""
                        if Version(driver_version) >= Version(MIG_UUID_DRIVER_VERSION):
                            mig_uuid = pynvml.nvmlDeviceGetUUID(mig_handle)
                        else:
                            mig_uuid = (
                                f"MIG-{pynvml.nvmlDeviceGetUUID(handle)}"
                                f"/{pynvml.nvmlDeviceGetComputeInstanceId(mig_handle)}"
                                f"/{pynvml.nvmlDeviceGetGpuInstanceId(mig_handle)}"
                            )
                        cuda_devices.append(mig_uuid)
                    except pynvml.NVMLError:
                        break
            else:
                cuda_devices.append(str(index))
        os.environ[
            NvidiaGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join(cuda_devices)
        pynvml.nvmlShutdown()
        return len(cuda_devices)

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        try:
            pynvml.nvmlInit()
            device_count = pynvml.nvmlDeviceGetCount()
            cuda_devices_names = []
            for index in range(device_count):
                handle = pynvml.nvmlDeviceGetHandleByIndex(index)
                mig_enabled = os.environ.get(
                    ray_constants.RAY_ENABLE_MIG_DETECTION_ENV_VAR, False
                )
                if mig_enabled:
                    try:
                        max_mig_count = pynvml.nvmlDeviceGetMaxMigDeviceCount(handle)
                    except pynvml.NVMLError_NotSupported:
                        cuda_devices_names.append(pynvml.nvmlDeviceGetName(handle))
                        continue
                    for mig_index in range(max_mig_count):
                        try:
                            mig_handle = pynvml.nvmlDeviceGetMigDeviceHandleByIndex(
                                handle, mig_index
                            )
                            cuda_devices_names.append(
                                pynvml.nvmlDeviceGetName(mig_handle)
                            )
                        except pynvml.NVMLError:
                            break
                else:
                    cuda_devices_names.append(pynvml.nvmlDeviceGetName(handle))
            pynvml.nvmlShutdown()
            return NvidiaGPUAcceleratorManager._gpu_name_to_accelerator_type(
                cuda_devices_names.pop()
            )
        except Exception:
            logger.exception("Could not parse gpu information.")
        return None

    @staticmethod
    def _gpu_name_to_accelerator_type(name):
        if name is None:
            return None
        match = NVIDIA_GPU_NAME_PATTERN.match(name)
        return match.group(1) if match else None

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
            NvidiaGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_cuda_devices])

    @staticmethod
    def get_ec2_instance_num_accelerators(
        instance_type: str, instances: dict
    ) -> Optional[int]:
        if instance_type not in instances:
            return None

        gpus = instances[instance_type].get("GpuInfo", {}).get("Gpus")
        if gpus is not None:
            # TODO(ameer): currently we support one gpu type per node.
            assert len(gpus) == 1
            return gpus[0]["Count"]
        return None

    @staticmethod
    def get_ec2_instance_accelerator_type(
        instance_type: str, instances: dict
    ) -> Optional[str]:
        if instance_type not in instances:
            return None

        gpus = instances[instance_type].get("GpuInfo", {}).get("Gpus")
        if gpus is not None:
            # TODO(ameer): currently we support one gpu type per node.
            assert len(gpus) == 1
            return gpus[0]["Name"]
        return None
