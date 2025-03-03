from typing import TYPE_CHECKING, List, Optional, Tuple

import ray
import os

if TYPE_CHECKING:
    import torch


def get_self_actor() -> Optional["ray.actor.ActorHandle"]:
    """
    Get the current actor handle in this worker.
    If this is called in a driver process, it will return None.
    """
    try:
        return ray.get_runtime_context().current_actor
    except RuntimeError:
        return None


def split_readers_by_locality(
    writer: "ray.actor.ActorHandle",
    reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
) -> Tuple[
    List[Tuple["ray.actor.ActorHandle", str]], List[Tuple["ray.actor.ActorHandle", str]]
]:
    """Split readers into remote and local readers based on writer.

    Args:
        writer: The actor handle of the writer
        reader_and_node_list: List of (reader, node) tuples

    Returns:
        Tuple containing:
            - List of (reader, node) tuples for remote readers
            - List of (reader, node) tuples for local readers
    """
    remote_readers = []
    local_readers = []

    for reader, node in reader_and_node_list:
        if reader != writer:
            remote_readers.append((reader, node))
        else:
            local_readers.append((reader, node))

    return remote_readers, local_readers


def split_actors_by_node_locality(
    node: str,
    actor_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
) -> Tuple[
    List[Tuple["ray.actor.ActorHandle", str]], List[Tuple["ray.actor.ActorHandle", str]]
]:
    """Split actors into remote and local actors based on node. The local actors will be
    on the same node as the given node. The remote actors will be on a different node.

    Args:
        writer_node: The node of the writer
        actor_and_node_list: List of (actor, node) tuples

    Returns:
        Tuple containing:
            - List of (actor, node) tuples for actors on the same node
            - List of (actor, node) tuples for actors on a different node
    """
    actors_on_same_node = []
    actors_on_different_node = []

    for actor, actor_node in actor_and_node_list:
        if node == actor_node:
            actors_on_same_node.append((actor, actor_node))
        else:
            actors_on_different_node.append((actor, actor_node))

    return actors_on_same_node, actors_on_different_node


def get_actor_node(actor: Optional["ray.actor.ActorHandle"]) -> str:
    """Get the node of the actor.

    Args:
        actor: The actor handle of the actor

    Returns:
        The node of the actor
    """
    if actor is None or actor == ray.get_runtime_context().current_actor:
        return ray.get_runtime_context().get_node_id()
    else:
        return ray.get(
            actor.__ray_call__.remote(
                lambda self: ray.get_runtime_context().get_node_id()
            )
        )


class AcceleratorRuntime:
    """
    Base class for managing device-specific runtime operations. This class
    provides an interface for device-related operations such as stream
    management, event creation, and device communication.
    """

    _instance = None

    @classmethod
    def get(cls):
        """
        Singleton method to get the runtime instance based on the default torch
        device. It initializes the appropriate runtime class based on the
        detected device type.
        Returns:
            AcceleratorRuntime: An instance of the appropriate runtime class
            (CudaRuntime, or CpuRuntime).
        """

        if cls._instance is None:
            if len([str(id) for id in ray.get_gpu_ids()]) > 0:
                cls._instance = CudaRuntime()
            else:
                cls._instance = CpuRuntime()

        return cls._instance

    def get_devices(self) -> List["torch.device"]:
        """Gets the correct torch device list configured for this process.

        Returns a list of torch devices allocated for the current worker.
        If no devices are assigned, then it returns a list with a single CPU device.
        """
        raise NotImplementedError

    def has_communicator(self) -> bool:
        """
        Check if an accelerator communicator is available for the specified device.
        If no device is provided, the function determines the default non-CPU device.
        Args:
            device (Optional[Union["torch.device", str]]):
                The target device or device type, which can be a `torch.device`
                object or a string. If not provided, the function uses the default
                non-CPU device.
        Returns:
            bool: True if the device support out of band communication, False otherwise.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def is_available(self) -> bool:
        """
        Check if the specific accelerator is available.
        Returns:
            bool: False by default, overridden by subclasses.
        """
        return False

    def current_stream(self):
        """
        Retrieves the current execution stream for the accelerator device.
        """
        raise NotImplementedError

    def create_event(self):
        """
        Creates an event object for the accelerator device.
        """
        raise NotImplementedError

    def get_unique_id(self) -> str:
        """
        Generates a unique identifier for communication purposes.
        """
        raise NotImplementedError

    def get_communicator(self, *args, **kwargs):
        """
        Retrieves the communication group for collective operations.
        """
        raise NotImplementedError

    def get_device_type(self) -> str:
        """
        Retrieves the device type for the accelerator.
        """
        raise NotImplementedError


class CudaRuntime(AcceleratorRuntime):
    """
    CUDA implementation of AcceleratorRuntime.
    """

    def get_devices(self) -> List["torch.device"]:
        """Gets the correct torch cuda device list configured for this process.

        Assumes that `CUDA_VISIBLE_DEVICES` is set and is a
        superset of the `ray.get_gpu_ids()`.
        """
        # Note: currently this method replicates the logic from
        # `CUDATorchDeviceManager.get_devices()`.
        # TODO(rui): tailor and clean up the logic for proper use in
        # Compiled Graphs.
        import torch

        # GPU IDs are assigned by Ray after you specify "use_gpu"
        # GPU `ray.get_gpu_ids()` may return ints or may return strings.
        # We should always convert to strings.
        gpu_ids = [str(id) for id in ray.get_gpu_ids()]

        device_ids = []

        if len(gpu_ids) > 0:
            cuda_visible_str = os.environ.get("CUDA_VISIBLE_DEVICES", "")
            if cuda_visible_str and cuda_visible_str != "NoDevFiles":
                cuda_visible_list = cuda_visible_str.split(",")
            else:
                cuda_visible_list = []

            # By default, there should only be one GPU ID if `use_gpu=True`.
            # If there are multiple GPUs, return a list of devices.
            # If using fractional GPUs, these IDs are not guaranteed
            # to be unique across different processes.
            for gpu_id in gpu_ids:
                try:
                    device_ids.append(cuda_visible_list.index(gpu_id))
                except IndexError:
                    raise RuntimeError(
                        "CUDA_VISIBLE_DEVICES set incorrectly. "
                        f"Got {cuda_visible_str}, expected to include {gpu_id}. "
                        "Did you override the `CUDA_VISIBLE_DEVICES` environment"
                        " variable? If not, please help file an issue on Github."
                    )

        else:
            # If called on the driver or outside of Ray Train, return the
            # 0th device.
            device_ids.append(0)

        return [torch.device(f"cuda:{device_id}") for device_id in device_ids]

    def has_communicator(self):
        return True

    def is_available(self) -> bool:
        import torch

        return torch.cuda.is_available()

    def current_stream(self):
        import torch

        return torch.cuda.current_stream()

    def create_event(self):
        import torch

        return torch.cuda.Event()

    def get_device_context(self, device):
        import torch

        return torch.cuda.device(device)

    def get_unique_id(self) -> str:
        from ray.experimental.channel.nccl_group import get_nccl_unique_id

        return get_nccl_unique_id()

    def get_communicator(self, *args, **kwargs):
        from ray.experimental.channel.nccl_group import _NcclGroup

        return _NcclGroup(*args, **kwargs)

    def get_device_type(self) -> str:
        return "cuda"


class CpuRuntime(AcceleratorRuntime):
    """
    CPU implementation of AcceleratorRuntime.
    """

    def get_devices(self) -> List["torch.device"]:
        import torch

        return [torch.device("cpu")]

    def has_communicator(self):
        return False

    def get_device_context(self, device):
        from contextlib import nullcontext

        return nullcontext()

    def get_unique_id(self) -> str:
        """
        Generate a unique identifier.
        This function returns a randomly generated UUID as cpu communicator's unique identifier.
        Returns:
            str: A unique identifier as a string.
        """
        import uuid

        return str(uuid.uuid4())

    def get_device_type(self):
        return "cpu"
