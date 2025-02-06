from contextlib import contextmanager
from typing import List, Union

import torch

from ray._private.accelerators.hpu import HPU_PACKAGE_AVAILABLE
from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager

if HPU_PACKAGE_AVAILABLE:
    import habana_frameworks.torch.hpu as torch_hpu


class HPUTorchDeviceManager(TorchDeviceManager):
    """HPU device manager"""

    @staticmethod
    def register_custom_torch_dist_backend():
        if HPU_PACKAGE_AVAILABLE:
            import habana_frameworks.torch.core  # noqa: F401
            import habana_frameworks.torch.distributed.hccl  # noqa: F401

    def is_available(self) -> bool():
        if not HPU_PACKAGE_AVAILABLE:
            return False

        return torch_hpu.is_available()

    def get_devices(self) -> List[torch.device]:
        if not self.is_available():
            raise RuntimeError(
                "Using HPUTorchDeviceManager but torch hpu is not available."
            )

        return [torch.device("hpu")]

    def set_device(self, device: Union[torch.device, int, str, None]):
        torch_hpu.set_device(device)

    def supports_stream(self) -> bool:
        """Validate if the device type support create a stream"""
        return False

    def get_stream_context(self, stream):
        """Get HPU stream context manager, empty so far."""

        @contextmanager
        def default_context_manager():
            yield

        return default_context_manager()
