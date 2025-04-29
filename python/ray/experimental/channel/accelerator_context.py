import threading
import importlib
import ray
from typing import TYPE_CHECKING
from contextlib import nullcontext
from ray.experimental.channel.communicator import Communicator

if TYPE_CHECKING:
    import torch

_accelerator_context_lock = threading.Lock()


class AcceleratorContext:
    """
    Provides a general interface for the Accelerator, including stream, event,
    and device management.

    This is a singleton class. Before using it, you must register the accelerator's
    device module name and the Communicator class. If nothing is registered,
    it will automatically fall back to GPU or CPU.
    """

    _torch_module_name = None
    _instance = None
    _communicator_cls = None
    _torch_mod = None

    @classmethod
    def get(cls):
        """
        Returns the singleton instance of the accelerator context.

        If a custom accelerator has been registered, initializes the context
        based on the registration. Otherwise, selects an appropriate runtime
        based on the available device (CUDA or CPU) and registers the
        corresponding default communicator.

        Returns:
            AcceleratorContext: A singleton instance of the appropriate
            runtime context.
        """
        if cls._instance is not None:
            return cls._instance

        with _accelerator_context_lock:
            # Check _instance again to avoid creating a duplicate instance
            # if it was initialized while waiting for the lock.
            if cls._instance is None:
                # Not registrey yet.
                if cls._torch_mod is None:
                    if len(ray.get_gpu_ids()) > 0:
                        from ray.experimental.channel.nccl_group import _NcclGroup

                        cls._set_context("cuda", _NcclGroup)
                    else:
                        from ray.experimental.channel.cpu_communicator import (
                            CPUCommunicator,
                        )

                        cls._set_context("cpu", CPUCommunicator)
                cls._instance = cls()

        return cls._instance

    @classmethod
    def _set_context(cls, name: str, communicator: Communicator):
        """
        Registers the accelerator's name and communicator class.

        Args:
            name: The name of the device module under torch (e.g., 'cuda', 'cpu').
            communicator: The communicator class associated with the device.
        """
        cls._torch_module_name = name
        cls._communicator_cls = communicator
        # Save the torch module corresponding to the device module name.
        cls._torch_mod = importlib.import_module(f"torch.{cls._torch_module_name}")

    def get_default_device(self) -> "torch.device":
        """
        Returns the default device used by the compiled graph.

        Currently, the default device for the compiled graph is the first visible
        device. By default, it returns the device with logical ID 0.

        Returns:
            torch.device: The default device.
        """
        import torch

        if self._torch_module_name == "cpu":
            return torch.device("cpu")

        return torch.device(f"{self._torch_module_name}:0")

    def get_device_context(self, device):
        """
        Retrieves the context manager for the specified accelerator device.
        There is no device context for CPU, returning a nullcontext.

        Args:
            device (torch.device): The target device for which the context manager
            is required.
        Returns:
            device_context: A context manager specific to the device type.
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
        Generates a communication identifier for communication group.
        """
        return self._communicator_cls.generate_communicator_id()

    def get_communicator(self, *args, **kwargs):
        """
        Retrieves the communication group for collective operations.
        """
        return self._communicator_cls(*args, **kwargs)


def register_accelerator_context(name: str, communicator: Communicator):
    """
    Registers the accelerator context with the specified device type and communicator.

    Args:
        name: The name of the device module under torch (e.g., 'cuda', 'cpu').
        communicator: The communicator class associated with the device.
    """
    AcceleratorContext._set_context(name, communicator)
