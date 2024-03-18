import torch
from typing import List

from ray._private.accelerators.hpu import HPU_PACKAGE_AVAILABLE
from ray.air._internal.accelerator_utils.device_manager import TorchDeviceManager

if HPU_PACKAGE_AVAILABLE:
    import habana_frameworks.torch.hpu as torch_hpu


class HPUTorchDeviceManager(TorchDeviceManager):
    """HPU device manager
    """

    @staticmethod
    def get_accelerator_name() -> str:
        return "HPU"
    
    @staticmethod
    def get_device_type() -> str:
        return "hpu"

    @staticmethod
    def get_device_module():
        if not HPU_PACKAGE_AVAILABLE:
            raise ImportError(
                "habana_frameworks is not installed so that hpu is not available."
            )
        return torch_hpu
    
    @staticmethod
    def is_device_available() -> bool():
        if not HPU_PACKAGE_AVAILABLE:
            return False
        
        return torch_hpu.is_available()
    
    @staticmethod
    def get_devices() -> List[torch.device]:
        if HPU_PACKAGE_AVAILABLE and torch_hpu.is_available():
            devices = [torch.device("hpu")]
        else:
            devices = [torch.device("cpu")]
        
        return devices