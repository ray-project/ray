from abc import ABC, abstractmethod
from typing import List, Union

import torch


class TorchDeviceManager(ABC):
    """This class contains the function needed for supporting
    an acclerator family in Ray AI Library.
    """

    @abstractmethod
    def is_available(self) -> bool:
        """Validate if device is available."""
        ...

    @abstractmethod
    def get_devices(self) -> List[torch.device]:
        """Gets the correct torch device configured for this process"""
        ...

    @abstractmethod
    def set_device(self, device: Union[torch.device, int, str, None]):
        """Set the correct device for this process"""
        ...

    @abstractmethod
    def supports_stream(self) -> bool:
        """Validate if the device type support create a stream"""
        ...

    @abstractmethod
    def create_stream(self, device: torch.device):
        """Create a device stream"""
        ...

    @abstractmethod
    def get_stream_context(self, stream):
        """Get a stream context of device. If device didn't support stream,
        this should return a empty context manager instead of  None.
        """
        ...

    @abstractmethod
    def get_current_stream(self):
        """Get current stream on accelerators like torch.cuda.current_stream"""
        ...
