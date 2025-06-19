import threading
import importlib
import ray
from typing import TYPE_CHECKING, Optional, Type, ContextManager, List
from contextlib import nullcontext
from ray.experimental.channel.communicator import Communicator
from ray._private.accelerators import get_accelerator_manager_for_resource

if TYPE_CHECKING:
    import torch

# The accelerator context singleton on this process.
_accelerator_context_lock = threading.Lock()
_default_accelerator_context: Optional["AcceleratorContext"] = None
_global_custom_context: Optional["AcceleratorContext"] = None


class AcceleratorContext:
    """
    Provides a unified interface for managing different accelerator backends
    This includes stream management, event creation, device context control,
    and communicator support for distributed communication.
    """

    def __init__(self, torch_module_name: str, communicator_cls: Type[Communicator]):
        """
        Initializes an accelerator context with the specified torch device module
        and communicator class.

        Args:
            torch_module_name: Name of the torch device module (e.g., "cuda", "cpu").
            communicator_cls: Class used to handle communication.
        """

        # The name of the torch module (e.g., 'cuda', 'npu')
        self._torch_module_name: str = torch_module_name
        # The Communicator class used to manage communication
        self._communicator_cls: Type[Communicator] = communicator_cls

        # Import the torch backend module (e.g., torch.cuda) if the device is not 'cpu'.
        if torch_module_name != "cpu":
            self._torch_mod = importlib.import_module(f"torch.{torch_module_name}")

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

        global _default_accelerator_context, _global_custom_context

        with _accelerator_context_lock:
            if _global_custom_context is not None:
                return _global_custom_context

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
        Overwrites the default accelerator context.

        Args:
            accelerator_context: The context to register.
        """
        global _global_custom_context

        # Accelerator context is registered.
        _global_custom_context = accelerator_context

    def get_accelerator_devices(self) -> List["torch.device"]:
        """
        Gets the torch device list configured for this process.

        Returns:
            List[torch.device]: The torch device list.
        """
        import torch

        if self._torch_module_name == "cpu":
            return [torch.device("cpu")]

        if self._torch_module_name == "cuda":
            accelerator_ids = [str(id) for id in ray.get_gpu_ids()]
            accelerator_manager = get_accelerator_manager_for_resource("GPU")
        else:
            accelerator_ids = [
                str(id)
                for id in ray.get_runtime_context().get_accelerator_ids()[
                    self._torch_module_name.upper()
                ]
            ]
            accelerator_manager = get_accelerator_manager_for_resource(
                self._torch_module_name.upper()
            )

        device_ids = []

        if len(accelerator_ids) > 0:
            accelerator_visible_list = (
                accelerator_manager.get_current_process_visible_accelerator_ids()
            )
            if accelerator_visible_list is None:
                accelerator_visible_list = []

            # If there are multiple Accelerators, return a list of devices.
            # If using fractional Accelerators, these IDs are not guaranteed
            # to be unique across different processes.
            for accelerator_id in accelerator_ids:
                try:
                    device_ids.append(accelerator_visible_list.index(accelerator_id))
                except IndexError:
                    raise RuntimeError(
                        f"{accelerator_manager.get_visible_accelerator_ids_env_var()} set incorrectly. "
                        f"expected to include {accelerator_id}. "
                        "Did you override this environment"
                        " variable? If not, please help file an issue on Github."
                    )

        else:
            # If called on the driver or outside of Ray Train, return the
            # 0th device.
            device_ids.append(0)

        return [
            torch.device(f"{self._torch_module_name}:{device_id}")
            for device_id in device_ids
        ]

    def get_device_context(self, device: "torch.device") -> ContextManager:
        """
        Retrieves the context manager for the specified accelerator device.
        There is no device context for CPU, returning a nullcontext.

        Args:
            device: The target device for which the context manager is required.

        Returns:
            ContextManager: A context manager specific to the device type.
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

    @property
    def module_name(self) -> str:
        """
        Gets the name of the torch module backing the accelerator.
        """
        return self._torch_module_name

    @property
    def communicator_cls(self) -> Optional[Type[Communicator]]:
        """
        Returns the communicator class.
        """
        return self._communicator_cls

    @property
    def accelerator_count(self) -> int:
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
        communicator_cls: The communicator class associated with the device.
    """
    accelerator_context = AcceleratorContext(torch_module_name, communicator_cls)
    AcceleratorContext.set(accelerator_context)


def is_accelerator_context_registered():
    """
    Checks whether a custom accelerator context has been registered.

    Returns:
        bool: True if a custom accelerator context is registered
              (_global_custom_context is not None), False otherwise.
    """
    if _global_custom_context is not None:
        return True
    return False
