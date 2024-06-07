from typing import List

import torch

from ray.air._internal.device_manager.device_manager import DeviceManager


class TorchDeviceManager(DeviceManager):
    """This class contains the function needed for supporting
    an acclerator family in Ray AI Library.
    """

    def is_device_available(self) -> bool:
        """Validate if device is available."""

    def get_devices(self) -> List[torch.device]:
        """Gets the correct torch device confiured for this process"""

    def set_device(self):
        """Set the correct device for thist process"""

    def create_stream(self):
        """Create a device stream"""

    def get_stream_context(self):
        """Get a stream context like torch.cuda.stream"""

    def get_current_stream(self):
        """Get a torch stream like torch.cuda.current_stream"""

    def cleanup(self):
        """Clean up the device when shutdown the torch."""
