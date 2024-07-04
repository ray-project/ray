from typing import List, Union

import torch

from ray.air._internal.device_manager.device_manager import DeviceManager


class TorchDeviceManager(DeviceManager):
    """This class contains the function needed for supporting
    an acclerator family in Ray AI Library.
    """

    def is_device_available(self) -> bool:
        """Validate if device is available."""
        ...

    def get_devices(self) -> List[torch.device]:
        """Gets the correct torch device configured for this process"""
        ...

    def set_device(self, device: Union[torch.device, int, str, None]):
        """Set the correct device for this process"""
        ...

    def is_support_stream(self) -> bool:
        """Validate if the device type support create a stream"""
        ...

    def create_stream(self, device: torch.device):
        """Create a device stream"""
        ...

    def get_stream_context(self, stream):
        """Get a stream context like torch.cuda.stream"""
        ...

    def get_current_stream(self):
        """Get a torch stream like torch.cuda.current_stream"""
        ...
