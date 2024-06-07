from abc import ABC, abstractmethod
from types import ModuleType
from typing import List, Optional

import torch


class TorchDeviceManager(ABC):
    """This class contains the function needed for supporting
    an acclerator family in Ray AI Library.
    """

    @staticmethod
    @abstractmethod
    def get_accelerator_name() -> str:
        """Gets the corresponding accelerator type, e.g. GPU, NPU."""

    @staticmethod
    @abstractmethod
    def get_device_type() -> str:
        """Gets the device type in deeplearning framwork,
        e.g. cuda, hpu, npu in torch.
        """

    def is_device_available(self) -> bool():
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
