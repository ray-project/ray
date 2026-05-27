import os
from typing import List, Union

import torch

import ray
from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager

HIP_VISIBLE_DEVICES_ENV_VAR = "HIP_VISIBLE_DEVICES"
CUDA_VISIBLE_DEVICES_ENV_VAR = "CUDA_VISIBLE_DEVICES"


class AMDGPUTorchDeviceManager(TorchDeviceManager):
    """AMD GPU (ROCm/HIP) device manager.

    Handles device selection for AMD GPUs where HIP_VISIBLE_DEVICES
    supersedes CUDA_VISIBLE_DEVICES at the ROCm runtime level.
    """

    def is_available(self) -> bool:
        return torch.cuda.is_available()

    def get_devices(self) -> List[torch.device]:
        """Gets the correct torch device list configured for this process.

        Returns a list of AMD GPU devices allocated for the current worker.
        If no GPUs are assigned, returns a list with a single CPU device.

        Reads HIP_VISIBLE_DEVICES first (since it supersedes CUDA_VISIBLE_DEVICES
        on AMD/ROCm), falling back to CUDA_VISIBLE_DEVICES.

        Assumes that HIP_VISIBLE_DEVICES (or CUDA_VISIBLE_DEVICES) is set and
        is a superset of ray.get_gpu_ids().
        """
        gpu_ids = [str(id) for id in ray.get_gpu_ids()]

        device_ids = []

        if len(gpu_ids) > 0:
            # HIP_VISIBLE_DEVICES supersedes CUDA_VISIBLE_DEVICES on AMD/ROCm.
            # Use whichever is set, preferring HIP_VISIBLE_DEVICES.
            hip_visible_str = os.environ.get(HIP_VISIBLE_DEVICES_ENV_VAR, "")
            cuda_visible_str = os.environ.get(CUDA_VISIBLE_DEVICES_ENV_VAR, "")
            visible_str = hip_visible_str or cuda_visible_str

            if visible_str and visible_str != "NoDevFiles":
                visible_list = visible_str.split(",")
            else:
                visible_list = []

            for gpu_id in gpu_ids:
                try:
                    device_ids.append(visible_list.index(gpu_id))
                except ValueError:
                    raise RuntimeError(
                        "HIP_VISIBLE_DEVICES/CUDA_VISIBLE_DEVICES set incorrectly. "
                        f"Got {visible_str!r}, expected to include {gpu_id}. "
                        "Did you override the HIP_VISIBLE_DEVICES or "
                        "CUDA_VISIBLE_DEVICES environment variable? "
                        "If not, please file an issue on Github."
                    )
        else:
            device_ids.append(0)

        return [torch.device(f"cuda:{device_id}") for device_id in device_ids]

    def set_device(self, device: Union[torch.device, int, str, None]):
        torch.cuda.set_device(device)

    def supports_stream(self) -> bool:
        return True

    def create_stream(self, device: torch.device) -> torch.cuda.Stream:
        return torch.cuda.Stream(device)

    def get_stream_context(self, stream):
        return torch.cuda.stream(stream)

    def get_current_stream(self) -> torch.cuda.Stream:
        return torch.cuda.current_stream()
