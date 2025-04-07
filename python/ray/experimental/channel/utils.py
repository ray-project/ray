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


def get_cuda_devices() -> List["torch.device"]:
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


def get_devices() -> List["torch.device"]:
    """Gets the correct torch device list configured for this process.

    Returns a list of torch devices allocated for the current worker.
    If no devices are assigned, then it returns a list with a single CPU device.
    """

    import torch

    gpu_ids = [str(id) for id in ray.get_gpu_ids()]
    if len(gpu_ids) > 0:
        return get_cuda_devices()
    else:
        return [torch.device("cpu")]
