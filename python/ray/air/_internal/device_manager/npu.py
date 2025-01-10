import os
from importlib.util import find_spec
from typing import List, Union

import torch

import ray
import ray._private.ray_constants as ray_constants
from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager


def is_package_present(package_name: str) -> bool:
    try:
        return find_spec(package_name) is not None
    except ModuleNotFoundError:
        return False


NPU_TORCH_PACKAGE_AVAILABLE = is_package_present("torch_npu")


if NPU_TORCH_PACKAGE_AVAILABLE:
    import torch_npu  # noqa: F401


class NPUTorchDeviceManager(TorchDeviceManager):
    """Ascend NPU device manager"""

    @staticmethod
    def register_custom_torch_dist_backend():
        if NPU_TORCH_PACKAGE_AVAILABLE:
            import torch_npu  # noqa: F401, F811

    def is_available(self) -> bool:
        if not NPU_TORCH_PACKAGE_AVAILABLE:
            return False

        return torch.npu.is_available()

    def get_devices(self) -> List[torch.device]:
        """Gets the correct torch device list configured for this process.

        Returns a list of torch NPU devices allocated for the current worker.
        If no NPUs are assigned, then it returns a list with a single CPU device.
        """
        if NPU_TORCH_PACKAGE_AVAILABLE and torch.npu.is_available():
            npu_ids = [
                str(id)
                for id in ray.get_runtime_context().get_accelerator_ids()[
                    ray_constants.NPU
                ]
            ]

            device_ids = []

            if len(npu_ids) > 0:
                npu_visible_str = os.environ.get(
                    ray_constants.NPU_RT_VISIBLE_DEVICES_ENV_VAR, ""
                )
                if npu_visible_str and npu_visible_str != "NoDevFiles":
                    npu_visible_list = npu_visible_str.split(",")
                else:
                    npu_visible_list = []

                for npu_id in npu_ids:
                    try:
                        device_ids.append(npu_visible_list.index(npu_id))
                    except IndexError:
                        raise RuntimeError(
                            "ASCEND_RT_VISIBLE_DEVICES set incorrectly. "
                            f"Got {npu_visible_str}, expected to include {npu_id}. "
                            "Did you override the `ASCEND_RT_VISIBLE_DEVICES` "
                            "environment variable?"
                        )
            else:
                # If called on the driver or outside of Ray Train, return the
                # 0th device.
                device_ids.append(0)

            devices = [torch.device(f"npu:{device_id}") for device_id in device_ids]
        else:
            raise RuntimeError(
                "Using NPUTorchDeviceManager but torch npu is not available."
            )

        return devices

    def set_device(self, device: Union[torch.device, int]):
        torch.npu.set_device(device)

    def supports_stream(self) -> bool:
        """Validate if the device type support to create a stream"""
        return True

    def create_stream(self, device):
        """Create a stream on NPU device"""
        return torch.npu.Stream(device)

    def get_stream_context(self, stream):
        """Get a torch.stream context on NPU device"""
        return torch.npu.stream(stream)

    def get_current_stream(self):
        """Get current stream for NPU device"""
        return torch.npu.current_stream()
