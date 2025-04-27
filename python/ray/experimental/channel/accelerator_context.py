import threading
import importlib
import ray
from typing import TYPE_CHECKING
from contextlib import nullcontext
from ray.experimental.channel.communicator import Communicator

if TYPE_CHECKING:
    import torch


class AcceleratorContext:
    """
    Base class for managing device-specific runtime operations. This class
    provides an interface for device-related operations such as stream
    management, event creation, and device communication.
    """

    _torch_module_name = None
    _instance = None
    _communicator_cls = None
    _torch_mod = None

    @classmethod
    def get(cls):
        """
        Singleton method to get the runtime instance based on the default torch
        device. It initializes the appropriate runtime class based on the
        detected device type.
        Returns:
            AcceleratorContext: An instance of the appropriate runtime class
            (CudaContext, or CpuContext).
        """
        if cls._instance is not None:
            return cls._instance
        _accelerator_context_lock = threading.Lock()
        with _accelerator_context_lock:
            # not registrey yet.
            if cls._torch_mod is None:
                if len(ray.get_gpu_ids()) > 0:
                    from ray.experimental.channel.nccl_group import _NcclGroup

                    cls._set_context("cuda", _NcclGroup)
                else:
                    cls._set_context("cpu")
            cls._instance = cls()

        return cls._instance

    @classmethod
    def _set_context(cls, name, communicator=None):
        cls._torch_module_name = name
        cls._communicator_cls = communicator
        cls._torch_mod = importlib.import_module(f"torch.{cls._torch_module_name}")

    def get_default_device(self) -> "torch.device":
        """Gets the correct torch device list configured for this process.

        Returns a list of torch devices allocated for the current worker.
        If no devices are assigned, then it returns a list with a single CPU device.
        """
        import torch

        if self._torch_module_name == "cpu":
            return torch.device("cpu")

        return torch.device(f"{self._torch_module_name}:0")

    def get_device_context(self, device):
        """
        Retrieves the context manager for the specified accelerator device.
        This function checks the type of the provided `device` and returns the
        appropriate context manager for that device.
        Args:
            device (torch.device): The target device for which the context manager
            is required.
        Returns:
            contextmanager: A context manager specific to the device type.
        """
        if device.type == "cpu":
            return nullcontext()

        return self._torch_mod.device(device)

    def current_stream(self):
        """
        Retrieves the current execution stream for the accelerator device.
        """
        return self._torch_mod.current_stream()

    def create_event(self):
        """
        Creates an event object for the accelerator device.
        """
        return self._torch_mod.Event()

    def generate_communicator_id(self) -> str:
        """
        Generates a unique identifier for communication purposes.
        """
        return self._communicator_cls.generate_communicator_id()

    def get_communicator(self, *args, **kwargs):
        """
        Retrieves the communication group for collective operations.
        """
        return self._communicator_cls(*args, **kwargs)


def register_accelerator_context(device_type: str, communicator: Communicator = None):
    AcceleratorContext._set_context(device_type, communicator)
