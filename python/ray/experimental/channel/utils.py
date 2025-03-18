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


DEVICE_RUNTIME_FUNCTIONS = {}


def fill_device_runtime_functions():
    """
    Populate the DEVICE_RUNTIME_FUNCTIONS dictionary with device-specific
    runtime functions.

    This function determines the default device type (CUDA, NPU, or CPU) and
    imports the necessary modules to populate the dictionary with relevant
    runtime functions.

    For CUDA and NPU devices, the following functions are added:
    - `is_available`: Checks if the device is available.
    - `current_stream`: Retrieves the current stream for the device.
    - `create_event`: Creates an event object for the device.
    - `get_unique_id`: Generates a unique ID for communication purposes.
    - `get_communicator`: Provides a communicator group for collective operations.

    For CPU devices, only the `get_unique_id` function is added.

    No parameters or return values are defined for this function.
    """
    if len(DEVICE_RUNTIME_FUNCTIONS) == 0:
        device = get_default_torch_device(allow_cpu=True)
        if device.type == "cuda":
            import torch
            from ray.experimental.channel.nccl_group import (
                _NcclGroup,
                get_nccl_unique_id,
            )

            DEVICE_RUNTIME_FUNCTIONS.update(
                {
                    "is_available": torch.cuda.is_available,
                    "current_stream": torch.cuda.current_stream,
                    "create_event": torch.cuda.Event,
                    "get_unique_id": get_nccl_unique_id,
                    "get_communicator": _NcclGroup,
                }
            )
        elif device.type == "npu":
            import torch
            import torch_npu  # noqa: F401
            from ray.experimental.channel.hccl_group import (
                _HcclGroup,
                get_hccl_unique_id,
            )

            DEVICE_RUNTIME_FUNCTIONS.update(
                {
                    "is_available": torch.npu.is_available,
                    "current_stream": torch.npu.current_stream,
                    "create_event": torch.npu.Event,
                    "get_unique_id": get_hccl_unique_id,
                    "get_communicator": _HcclGroup,
                }
            )
        elif device.type == "cpu":
            from ray.experimental.channel.cpu_communicator import get_cpu_unique_id

            DEVICE_RUNTIME_FUNCTIONS.update(
                {
                    "get_unique_id": get_cpu_unique_id,
                }
            )


def is_acclerator_communicator_available(
    device: Optional[Union["torch.device", str]] = None
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


def acclerator_is_available():
    """
    Checks if the accelerator is available.

    Returns:
        bool: True if the accelerator is available, False otherwise.
    """
    fill_device_runtime_functions()
    if "is_available" in DEVICE_RUNTIME_FUNCTIONS:
        return DEVICE_RUNTIME_FUNCTIONS["is_available"]()
    return False


def get_current_acclerator_stream():
    """
    Retrieves the current accelerator stream based on the device type.

    Returns:
        The current stream object for the accelerator. This could be a CUDA
        stream, or an NPU stream.
    """
    fill_device_runtime_functions()
    return DEVICE_RUNTIME_FUNCTIONS["current_stream"]()


def create_acclerator_event():
    """
    Creates an event object specific to the current accelerator device.

    Returns:
        An event object associated with the accelerator. This could be a CUDA
        event, or an NPU event.
    """
    fill_device_runtime_functions()
    return DEVICE_RUNTIME_FUNCTIONS["create_event"]()


def get_acclerator_context(device):
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
    if device.type == "cuda":
        import torch

        return torch.cuda.device(device)
    elif device.type == "npu":
        import torch
        import torch_npu  # noqa: F401

        return torch.npu.device(device)


def get_acclerator_unique_id():
    """
    Generates a unique ID for the current accelerator device.

    Returns:
        str: A unique identifier associated with the accelerator device.
    """
    fill_device_runtime_functions()
    return DEVICE_RUNTIME_FUNCTIONS["get_unique_id"]()


def get_acclerator_communicator(*args, **kwargs):
    """
    Retrieves the communicator group for the current accelerator device.

    Args:
        same as the `_NcclGroup` or `_HcclGroup` constructor.

    Returns:
        The communicator group object specific to the accelerator device.
    """
    fill_device_runtime_functions()
    return DEVICE_RUNTIME_FUNCTIONS["get_communicator"](*args, **kwargs)
