from abc import ABC, abstractmethod


class DeviceManager(ABC):
    """This class contains the function needed for supporting
    an acclerator family in Ray AI Library.
    """

    @staticmethod
    @abstractmethod
    def get_accelerator_name() -> str:
        """Gets the corresponding accelerator type, e.g. GPU, NPU."""
        ...

    @staticmethod
    @abstractmethod
    def get_device_type() -> str:
        """Gets the device type in deeplearning framwork,
        e.g. cuda, hpu, npu in torch.
        """
        ...
