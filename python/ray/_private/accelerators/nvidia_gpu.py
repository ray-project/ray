import glob
import logging
import os
import re
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

CUDA_VISIBLE_DEVICES_ENV_VAR = "CUDA_VISIBLE_DEVICES"
NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"

# TODO(Alex): This pattern may not work for non NVIDIA Tesla GPUs (which have
# the form "Tesla V100-SXM2-16GB" or "Tesla K80").
NVIDIA_GPU_NAME_PATTERN = re.compile(r"\w+\s+([A-Z0-9]+)")


def _detect_nvidia_device_files() -> int:
    """Detect NVIDIA GPUs by counting /dev/nvidia* device files.

    Returns the number of GPU device files found (excluding nvidiactl, nvidia-uvm, etc.)
    """
    try:
        # Match /dev/nvidia0, /dev/nvidia1, etc. but not nvidiactl, nvidia-uvm, etc.
        device_files = glob.glob("/dev/nvidia[0-9]*")
        return len(device_files)
    except Exception:
        return 0


def _count_cuda_visible_devices() -> int:
    """Count GPUs from CUDA_VISIBLE_DEVICES environment variable.

    Returns the number of devices specified, or 0 if not set or empty.
    """
    cuda_visible_devices = os.environ.get(CUDA_VISIBLE_DEVICES_ENV_VAR)
    if not cuda_visible_devices or cuda_visible_devices == "NoDevFiles":
        return 0
    # CUDA_VISIBLE_DEVICES can be comma-separated IDs like "0,1,2" or UUIDs
    return len(cuda_visible_devices.split(","))


class NvidiaGPUAcceleratorManager(AcceleratorManager):
    """NVIDIA GPU accelerators."""

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

        nvml_error = None
        nvml_initialized = False
        try:
            pynvml.nvmlInit()
            nvml_initialized = True
            device_count = pynvml.nvmlDeviceGetCount()
            return device_count
        except pynvml.NVMLError as e:
            nvml_error = e
            # NVML failed - try fallback detection methods
        finally:
            if nvml_initialized:
                try:
                    pynvml.nvmlShutdown()
                except pynvml.NVMLError:
                    pass  # Ignore shutdown errors

        # Fallback 1: Check /dev/nvidia* device files
        device_file_count = _detect_nvidia_device_files()

        # Fallback 2: Check CUDA_VISIBLE_DEVICES environment variable
        cuda_env_count = _count_cuda_visible_devices()

        # Determine if GPUs are likely present despite NVML failure
        evidence_of_gpus = device_file_count > 0 or cuda_env_count > 0

        if evidence_of_gpus:
            # Use the maximum of fallback counts as our best estimate
            fallback_count = max(device_file_count, cuda_env_count)
            logger.warning(
                "NVML failed to detect GPUs (error: %s), but found evidence of "
                "GPU devices: %d device file(s) in /dev/nvidia*, "
                "CUDA_VISIBLE_DEVICES indicates %d device(s). "
                "Ray will report 0 GPUs since NVML-based detection failed. "
                "To use GPUs, specify the GPU count manually with "
                "'ray start --num-gpus=%d' or set 'num_gpus' in ray.init(). "
                "This may happen if the NVIDIA driver is not fully loaded or "
                "NVML library is not accessible in this environment.",
                nvml_error,
                device_file_count,
                cuda_env_count,
                fallback_count,
            )
        else:
            logger.debug(
                "NVML initialization failed (error: %s) and no GPU device files "
                "or CUDA_VISIBLE_DEVICES found. Assuming no GPUs are available.",
                nvml_error,
            )

        return 0

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
