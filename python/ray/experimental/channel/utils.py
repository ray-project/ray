from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import ray

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


def get_default_torch_device(*, allow_cpu: bool) -> "torch.device":
    """Get the default torch device inside this actor or driver.

    If any acclerators are available, the default device will be index 0 and we
    will rely on torch to handle mapping the VISIBLE_DEVICES to a physical device.

    If no acclerators are available, a CPU device will be returned if allow_cpu
    is true, else the function will raise a RuntimeError.
    """
    import torch

    accelerator_ids = ray.get_runtime_context().get_accelerator_ids()
    if accelerator_ids.get("GPU", []):
        return torch.device("cuda:0")
    elif accelerator_ids.get("NPU", []):
        return torch.device("npu:0")
    else:
        if allow_cpu:
            return torch.device("cpu")
        else:
            raise RuntimeError("No acclerator available.")


class AccleratorRuntime:
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
            (CudaRuntime, NpuRuntime, or CpuRuntime).
        """
        if cls._instance is None:
            device = get_default_torch_device(allow_cpu=True)
            if device.type == "cuda":
                cls._instance = CudaRuntime()
            elif device.type == "npu":
                cls._instance = NpuRuntime()
            elif device.type == "cpu":
                cls._instance = CpuRuntime()
            else:
                raise RuntimeError(f"Device type {device.type} is not support.")

        return cls._instance

    @classmethod
    def is_acclerator_communicator_available(
        self, device: Optional[Union["torch.device", str]] = None
    ):
        """
        Check if an accelerator communicator is available for the specified device.
        If no device is provided, the function determines the default non-CPU device.
        Args:
            device (Optional[Union["torch.device", str]]):
                The target device or device type, which can be a `torch.device`
                object or a string. If not provided, the function uses the default
                non-CPU device.
        Returns:
            bool: True if the device supports CUDA or NPU accelerators, False otherwise.
        """
        if device is None:
            device = get_default_torch_device(allow_cpu=False)
        if device in ["cuda", "npu"]:
            return True
        if device.type in ["cuda", "npu"]:
            return True
        return False

    def get_device_context(self, device):
        """
        Retrieves the context manager for the specified accelerator device.
        This function checks the type of the provided `device` and returns the
        appropriate context manager for that device. Currently, it supports CUDA
        and NPU devices.
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


class CudaRuntime(AccleratorRuntime):
    """
    CUDA implementation of AcceleratorRuntime.
    """

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


class NpuRuntime(AccleratorRuntime):
    """
    Ascend NPU implementation of AcceleratorRuntime.
    """

    def is_available(self) -> bool:
        import torch
        import torch_npu  # noqa F401

        return torch.npu.is_available()

    def current_stream(self):
        import torch
        import torch_npu  # noqa F401

        return torch.npu.current_stream()

    def create_event(self):
        import torch
        import torch_npu  # noqa F401

        return torch.npu.Event()

    def get_device_context(self, device):
        import torch
        import torch_npu  # noqa F401

        return torch.npu.device(device)

    def get_unique_id(self) -> str:
        from ray.experimental.channel.hccl_group import get_hccl_unique_id

        return get_hccl_unique_id()

    def get_communicator(self, *args, **kwargs):
        from ray.experimental.channel.hccl_group import _HcclGroup

        return _HcclGroup(*args, **kwargs)


class CpuRuntime(AccleratorRuntime):
    """
    CPU implementation of AcceleratorRuntime.
    """

    def get_unique_id(self) -> str:
        from ray.experimental.channel.cpu_communicator import get_cpu_unique_id

        return get_cpu_unique_id()
