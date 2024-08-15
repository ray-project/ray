from typing import List

import torch

from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager


class CPUTorchDeviceManager(TorchDeviceManager):
    """CPU device manager"""

    def is_available(self) -> bool():
        return True

    def get_devices(self) -> List[torch.device]:
        """Gets the correct torch device list configured for this process."""
        return [torch.device("cpu")]

    def supports_stream(self) -> bool:
        """Validate if the device type support create a stream"""
        return False
