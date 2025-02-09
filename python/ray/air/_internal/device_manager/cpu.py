from contextlib import contextmanager
from typing import List, Union

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

    def get_stream_context(self, stream):
        """Return empty context mananger for CPU."""

        @contextmanager
        def default_context_manager():
            yield

        return default_context_manager()

    def set_device(self, device: Union[torch.device, int, str, None]):
        raise NotImplementedError

    def create_stream(self, device: torch.device):
        raise NotImplementedError

    def get_current_stream(self):
        raise NotImplementedError
