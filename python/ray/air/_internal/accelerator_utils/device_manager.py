from abc import ABC, abstractmethod
from typing import List
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

    @staticmethod
    @abstractmethod
    def get_device_module():
        """Gets the torch device extention module, e.g.
        torch.cuda, torch.hpu, torch.npu
        """

    @staticmethod
    @abstractmethod
    def is_device_available() -> bool():
        """Validate if device is available."""

    @staticmethod
    @abstractmethod
    def get_devices() -> List[torch.device]:
        """Gets the correct torch device confiured for this process"""
