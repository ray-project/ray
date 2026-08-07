import logging
import os
import re
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

CUDA_VISIBLE_DEVICES_ENV_VAR = "CUDA_VISIBLE_DEVICES"
NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"

# Capture the accelerator model from the NVML device name: the run of leading
# all-caps tokens (e.g. "RTX", "PRO") up to and including the first token that
# contains a digit. This keeps datacenter cards stable ("Tesla V100-SXM2-16GB"
# -> "V100", "NVIDIA A100-SXM4-40GB" -> "A100") while disambiguating the RTX
# line, whose first token is only a brand prefix ("NVIDIA RTX PRO 6000 Blackwell
# Server Edition" -> "RTX PRO 6000"). A trailing SKU suffix after a hyphen is
# dropped. Mixed-case consumer names ("NVIDIA GeForce RTX 5090") don't match and
# fall back to a hyphen-joined product name in _gpu_name_to_accelerator_type.
NVIDIA_GPU_NAME_PATTERN = re.compile(r"\w+\s+((?:[A-Z]+\s+)*[A-Z0-9]*\d[A-Z0-9]*)")


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

        try:
            pynvml.nvmlInit()
        except pynvml.NVMLError:
            return 0  # pynvml init failed
        device_count = pynvml.nvmlDeviceGetCount()
        pynvml.nvmlShutdown()
        return device_count

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
        result = match.group(1).replace(" ", "-") if match else None
        if result and len(result) > 1:
            return result
        # The pattern above requires an all-uppercase/numeric model token, which
        # works for datacenter cards ("Tesla V100-SXM2-16GB" -> "V100",
        # "NVIDIA RTX PRO 6000 ..." -> "RTX-PRO-6000") but not for consumer
        # cards whose product line is mixed case ("NVIDIA GeForce RTX 5090").
        # Fall back to a hyphen-joined product name so callers get a useful
        # accelerator_type label like "GeForce-RTX-5090".
        cleaned = re.sub(r"^NVIDIA\s+", "", name).strip()
        return cleaned.replace(" ", "-") if cleaned else None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_cuda_devices: List[str],
    ) -> None:
        if env_bool(NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR, False):
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
