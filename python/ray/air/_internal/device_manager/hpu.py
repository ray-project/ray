from typing import List, Union

import torch

from ray._private.accelerators.hpu import HPU_PACKAGE_AVAILABLE
from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager

if HPU_PACKAGE_AVAILABLE:
    import habana_frameworks.torch.hpu as torch_hpu


class HPUTorchDeviceManager(TorchDeviceManager):
    """HPU device manager"""

    @staticmethod
    def get_accelerator_name() -> str:
        return "HPU"

    @staticmethod
    def get_device_type() -> str:
        return "hpu"

    def is_device_available(self) -> bool():
        if not HPU_PACKAGE_AVAILABLE:
            return False

        return torch_hpu.is_available()

    def get_devices(self) -> List[torch.device]:
        if HPU_PACKAGE_AVAILABLE and torch_hpu.is_available():
            devices = [torch.device("hpu")]
        else:
            devices = [torch.device("cpu")]

        return devices

    def set_device(self,  device: Union[torch.device, int, str, None]):
        torch_hpu.set_device(device)

    def is_support_stream(self) -> bool:
        """Validate if the device type support create a stream"""
        return False
