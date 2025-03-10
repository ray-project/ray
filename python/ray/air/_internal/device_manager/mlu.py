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


MLU_TORCH_PACKAGE_AVAILABLE = is_package_present("torch_mlu")


if MLU_TORCH_PACKAGE_AVAILABLE:
    import torch_mlu  # noqa: F401


class MLUTorchDeviceManager(TorchDeviceManager):
    """Cambricon MLU device manager"""

    @staticmethod
    def register_custom_torch_dist_backend():
        if MLU_TORCH_PACKAGE_AVAILABLE:
            import torch_mlu  # noqa: F401, F811

    def is_available(self) -> bool:
        if not MLU_TORCH_PACKAGE_AVAILABLE:
            return False

        return torch.mlu.is_available()

    def get_devices(self) -> List[torch.device]:
        """Gets the correct torch device list configured for this process.

        Returns a list of torch MLU devices allocated for the current worker.
        If no MLUs are assigned, then it returns a list with a single CPU device.
        """
        if MLU_TORCH_PACKAGE_AVAILABLE and torch.mlu.is_available():
            mlu_ids = [
                str(id)
                for id in ray.get_runtime_context().get_accelerator_ids()[
                    ray_constants.MLU
                ]
            ]

            device_ids = []

            if len(mlu_ids) > 0:
                mlu_visible_str = os.environ.get(
                    ray_constants.MLU_VISIBLE_DEVICES_ENV_VAR, ""
                )
                if mlu_visible_str and mlu_visible_str != "NoDevFiles":
                    mlu_visible_list = mlu_visible_str.split(",")
                else:
                    mlu_visible_list = []

                for mlu_id in mlu_ids:
                    try:
                        device_ids.append(mlu_visible_list.index(mlu_id))
                    except IndexError:
                        raise RuntimeError(
                            "MLU_VISIBLE_DEVICES set incorrectly. "
                            f"Got {mlu_visible_str}, expected to include {mlu_id}. "
                            "Did you override the `MLU_VISIBLE_DEVICES` "
                            "environment variable?"
                        )
            else:
                # If called on the driver or outside of Ray Train, return the
                # 0th device.
                device_ids.append(0)

            devices = [torch.device(f"mlu:{device_id}") for device_id in device_ids]
        else:
            raise RuntimeError(
                "Using MLUTorchDeviceManager but torch mlu is not available."
            )

        return devices

    def set_device(self, device: Union[torch.device, int]):
        torch.mlu.set_device(device)

    def supports_stream(self) -> bool:
        """Validate if the device type support to create a stream"""
        return True

    def create_stream(self, device):
        """Create a stream on MLU device"""
        return torch.mlu.Stream(device)

    def get_stream_context(self, stream):
        """Get a torch.stream context on MLU device"""
        return torch.mlu.stream(stream)

    def get_current_stream(self):
        """Get current stream for MLU device"""
        return torch.mlu.current_stream()
