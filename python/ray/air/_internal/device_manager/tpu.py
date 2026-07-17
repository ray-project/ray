import logging
from contextlib import nullcontext
from typing import List, Union

import torch
import torch.distributed  # noqa: F401

from ray.air._internal.device_manager.torch_device_manager import TorchDeviceManager

logger = logging.getLogger(__name__)


class TPUTorchDeviceManager(TorchDeviceManager):
    """TPU device manager using torch_tpu backend"""

    def is_available(self) -> bool:
        try:
            import torch_tpu  # noqa: F401

            return True
        except ImportError:
            return False

    def get_devices(self) -> List[torch.device]:
        if not self.is_available():
            raise RuntimeError(
                "Using TPUTorchDeviceManager but torch_tpu is not available."
            )

        # Under PyTorch TPU backend (torch_tpu), each process binds to a single TPU core/device
        # represented as torch.device("tpu"). The specific core is managed by the TPU runtime
        # environment variables (e.g. TPU_VISIBLE_CHIPS/LOCAL_RANK).
        return [torch.device("tpu")]

    def set_device(self, device: Union[torch.device, int, str, None]):
        # TPU backend (XLA/PJRT) binds a process to a single TPU device upon initialization.
        # Changing the active device dynamically within a process is not supported.
        pass

    def supports_stream(self) -> bool:
        """Validate if the device type support create a stream"""
        return False

    def get_stream_context(self, stream):
        """Return empty context manager for TPU."""
        return nullcontext()

    @classmethod
    def register_custom_torch_dist_backend(cls) -> None:
        try:
            import torch_tpu._loader

            torch_tpu._loader.load()
        except ImportError as e:
            raise ImportError(
                "The `torch_tpu` module is required to use PyTorch TPU distributed training. "
                "Please install it or ensure it is available in your environment."
            ) from e
