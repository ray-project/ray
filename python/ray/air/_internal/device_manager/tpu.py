import logging
from importlib.util import find_spec
from typing import List, Union

import torch

from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager

logger = logging.getLogger(__name__)


def is_package_present(package_name: str) -> bool:
    try:
        return find_spec(package_name) is not None
    except ModuleNotFoundError:
        return False


TORCH_TPU_PACKAGE_AVAILABLE = is_package_present("torch_tpu")


class TPUTorchDeviceManager(TorchDeviceManager):
    """TPU device manager"""

    @staticmethod
    def register_custom_torch_dist_backend():
        if TORCH_TPU_PACKAGE_AVAILABLE:
            from torch_tpu import api

            # Trigger torch_tpu initialization for correct backend registration
            _ = api.tpu_device()  # noqa: F841

    def is_available(self) -> bool:
        return TORCH_TPU_PACKAGE_AVAILABLE

    def get_devices(self) -> List[torch.device]:
        """Gets the correct torch device list configured for this process.

        Returns a list of torch TPU devices allocated for the current worker.
        """
        if TORCH_TPU_PACKAGE_AVAILABLE:
            from torch_tpu import api

            return [api.tpu_device()]
        else:
            raise RuntimeError(
                "Using TPUTorchDeviceManager but torch_tpu is not available."
            )

    def set_device(self, device: Union[torch.device, int, str, None]):
        # TPU device setting is typically handled by torch_tpu.api.tpu_device()
        pass

    def supports_stream(self) -> bool:
        """Validate if the device type support create a stream"""
        return False

    def get_stream_context(self, stream):
        """Return empty context manager for TPU."""
        from contextlib import contextmanager

        @contextmanager
        def default_context_manager():
            yield

        return default_context_manager()
