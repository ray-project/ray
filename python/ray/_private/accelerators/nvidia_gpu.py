import re
import os
import logging
from typing import Optional, List, Tuple
from packaging.version import Version

from ray._private.accelerators.accelerator import AcceleratorManager
import ray._private.ray_constants as ray_constants

logger = logging.getLogger(__name__)

CUDA_VISIBLE_DEVICES_ENV_VAR = "CUDA_VISIBLE_DEVICES"
NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"

# TODO(Alex): This pattern may not work for non NVIDIA Tesla GPUs (which have
# the form "Tesla V100-SXM2-16GB" or "Tesla K80").
NVIDIA_GPU_NAME_PATTERN = re.compile(r"\w+\s+([A-Z0-9]+)")

# only newer version support mig uuid
# for version < 470.42.01, mig uuid has format
# MIG-{gpu-uuid}/{gpu instance id}/{compute instance id}
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
        import ray._private.thirdparty.pynvml as pynvml

        try:
            pynvml.nvmlInit()
        except pynvml.NVMLError:
            return 0  # pynvml init failed
        driver_version = pynvml.nvmlSystemGetDriverVersion()
        device_count = pynvml.nvmlDeviceGetCount()
        cuda_devices = []
        mig_enabled = ray_constants.RAY_ENABLE_MIG_DETECTION
        mig_uuid = None
        for index in range(device_count):
            try:
                handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            except pynvml.NVMLError_GpuIsLost:
                continue
            if mig_enabled:
                try:
                    max_mig_count = pynvml.nvmlDeviceGetMaxMigDeviceCount(handle)
                except pynvml.NVMLError_NotSupported:
                    cuda_devices.append(str(index))
                    continue
                if max_mig_count == 0:
                    cuda_devices.append(str(index))
                    continue
                for mig_index in range(max_mig_count):
                    try:
                        mig_handle = pynvml.nvmlDeviceGetMigDeviceHandleByIndex(
                            handle, mig_index
                        )
                        if Version(driver_version) >= Version(MIG_UUID_DRIVER_VERSION):
                            mig_uuid = pynvml.nvmlDeviceGetUUID(mig_handle)
                        else:
                            mig_uuid = (
                                f"MIG-{pynvml.nvmlDeviceGetUUID(handle)}"
                                f"/{pynvml.nvmlDeviceGetGpuInstanceId(mig_handle)}"
                                f"/{pynvml.nvmlDeviceGetComputeInstanceId(mig_handle)}"
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
        import ray._private.thirdparty.pynvml as pynvml

        try:
            pynvml.nvmlInit()
        except pynvml.NVMLError:
            return None  # pynvml init failed
        device_count = pynvml.nvmlDeviceGetCount()
        cuda_device_type = None
        if device_count > 0:
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            device_name = pynvml.nvmlDeviceGetName(handle)
            if isinstance(device_name, bytes):
                device_name = device_name.decode("utf-8")
            cuda_device_type = (
                NvidiaGPUAcceleratorManager._gpu_name_to_accelerator_type(device_name)
            )
        pynvml.nvmlShutdown()
        return cuda_device_type

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
