import threading
import importlib
import ray
from typing import TYPE_CHECKING, Optional, Type
from contextlib import nullcontext
from ray.experimental.channel.communicator import Communicator

if TYPE_CHECKING:
    import torch

# The accelerator context singleton on this process.
_accelerator_context_lock = threading.Lock()
_default_accelerator_context: Optional["AcceleratorContext"] = None


class AcceleratorContext:
    """
    Provides a unified interface for managing different accelerator backends
    This eincludes stram management, event creation, device context control,
    and communicator support for distributed communication.
    """

    def __init__(self, torch_module_name: str, commumcator_cls: Type[Communicator]):
        """
        Initializes an accelerator context with the specified torch device module
        and communicator class.

        Args:
            torch_module_name: Name of the torch device module (e.g., "cuda", "cpu").
            commumcator_cls: Class used to handle communication.
        """

        # The name of the torch module (e.g., 'cuda', 'npu')
        self._torch_module_name: str = torch_module_name
        # The Communicator class used to manage communication
        self._communicator_cls: Type[Communicator] = commumcator_cls

        # Save the torch module corresponding to the device module name.
        self._torch_mod = importlib.import_module(f"torch.{torch_module_name}")

        # Whether the accelerator is registered.
        self._is_registered: bool = False

    @staticmethod
    def get() -> "AcceleratorContext":
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

        global _default_accelerator_context

        with _accelerator_context_lock:
            if _default_accelerator_context is None:
                if len(ray.get_gpu_ids()) > 0:
                    from ray.experimental.channel.nccl_group import _NcclGroup

                    _default_accelerator_context = AcceleratorContext(
                        "cuda", _NcclGroup
                    )
                else:
                    from ray.experimental.channel.cpu_communicator import (
                        CPUCommunicator,
                    )

                    _default_accelerator_context = AcceleratorContext(
                        "cpu", CPUCommunicator
                    )

            return _default_accelerator_context

    @staticmethod
    def set(accelerator_context: "AcceleratorContext") -> None:
        """
        Sets the accelerator context to the default context.

        Args:
            accelerator_context: The context to register.
        """
        global _default_accelerator_context

        # Accelerator context is registered.
        accelerator_context._is_registered = True
        _default_accelerator_context = accelerator_context

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

    def create_communicator(self, *args, **kwargs) -> Communicator:
        """
        Creates a communication group for collective operations.
        """
        return self._communicator_cls(*args, **kwargs)

    def get_module_name(self) -> str:
        """
        Gets the name of the torch module backing the accelerator.
        """
        return self._torch_module_name

    def get_registed_module_name(self) -> Optional[str]:
        """
        Returns the name of the module if registered. Otherwise, returns None.
        """
        if self._is_registered:
            return self._torch_module_name
        return None

    def get_registed_communicator_cls(self) -> Optional[Type[Communicator]]:
        """
        Returns the communicator class if registered. Otherwise, returns None.
        """
        if self._is_registered:
            return self._communicator_cls
        return None

    def get_accelerator_count(self) -> int:
        """
        Returns the number of accelerators assigned by ray.
        """
        if self._torch_module_name == "cuda":
            return len(ray.get_gpu_ids())
        else:
            accelerator_ids = ray.get_runtime_context().get_accelerator_ids()
            return len(accelerator_ids.get(self._torch_module_name.upper(), []))


def register_accelerator_context(
    torch_module_name: str, communicator_cls: Type[Communicator]
):
    """
    Registers the accelerator context with the specified device type and communicator.

    Args:
        torch_module_name: The name of the device module under torch.
        communicator: The communicator class associated with the device.
    """
    accleerator_context = AcceleratorContext(torch_module_name, communicator_cls)
    AcceleratorContext.set(accleerator_context)
