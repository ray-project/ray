"""
This module contains the utility functions to get the tensor transport metadata,
and send/receive GPU objects.
"""
from ray.util.collective.types import (
    TensorTransportMetadata,
    NixlTransportMetadata,
    CollectiveTransportMetadata,
    Backend,
)
from ray._private.custom_types import TensorTransportEnum
import ray.util.collective as collective
from ray.util.collective.types import NIXL_GROUP_NAME
from typing import Optional
import ray


def __ray_get_tensor_transport_metadata__(
    self: "ray.actor.ActorHandle",
    obj_id: str,
    tensor_transport: TensorTransportEnum,
) -> TensorTransportMetadata:
    """
    Get the tensor transport metadata for the GPU object.
    This function retrieves metadata about tensors stored in the GPU object store,
    including their shapes and dtypes. When NIXL transport is enabled, it also gets
    NIXL-specific metadata needed for tensor transport.

    Args:
        self: The actor that runs this function.
        obj_id: The ID of the GPU object to get metadata for
        tensor_transport: The tensor transport protocol to use for the GPU object.

    Returns:
        TensorTransportMetadata: A named tuple containing the tensor metadata.
    """
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    assert gpu_object_store.has_gpu_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    tensors = gpu_object_store.get_gpu_object(obj_id)
    if tensor_transport == TensorTransportEnum.NIXL:
        from ray.util.collective.collective_group.nixl_backend import NixlBackend
        from ray.util.collective.types import NIXL_GROUP_NAME

        nixl_backend: NixlBackend = collective.get_group_handle(NIXL_GROUP_NAME)
        serialized_descs, agent_meta = nixl_backend.get_nixl_metadata(tensors)
        return NixlTransportMetadata(
            tensor_meta=[(t.shape, t.dtype) for t in tensors],
            nixl_serialized_descs=serialized_descs,
            nixl_agent_meta=agent_meta,
        )
    else:
        return CollectiveTransportMetadata(
            tensor_meta=[(t.shape, t.dtype) for t in tensors],
        )


def __ray_update_collective_metadata__(
    self: "ray.actor.ActorHandle",
    tensor_transport_metadata: TensorTransportMetadata,
    communicator_name: Optional[str],
    src_rank: Optional[int],
    dst_rank: Optional[int],
):

    if communicator_name is not None:
        tensor_transport_metadata.communicator_name = communicator_name
    if src_rank is not None:
        tensor_transport_metadata.src_rank = src_rank
    if dst_rank is not None:
        tensor_transport_metadata.dst_rank = dst_rank
    return tensor_transport_metadata


def get_tensor_transport_metadata(
    src_actor: "ray.actor.ActorHandle",
    obj_id: str,
    tensor_transport: TensorTransportEnum,
) -> TensorTransportMetadata:
    # Submit a Ray actor task to the source actor to get the tensor metadata.
    # The metadata is a list of tuples, where each tuple contains the shape and dtype
    # of a tensor in the GPU object store. This function returns an ObjectRef that
    # points to the tensor metadata.
    # NOTE(swang): We put this task on the background thread to avoid tasks
    # executing on the main thread blocking this task.

    return src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
        __ray_get_tensor_transport_metadata__, obj_id, tensor_transport
    )


def get_collective_metadata(
    src_actor: "ray.actor.ActorHandle",
    dst_actor: "ray.actor.ActorHandle",
    tensor_transport_metadata: TensorTransportMetadata,
    backend: Optional[str] = None,
) -> TensorTransportMetadata:
    """
    Update the collective metadata before sending the GPU object.
    """

    from ray.experimental.collective import get_collective_groups

    if backend == Backend.NIXL:
        return src_actor.__ray_call__.remote(
            __ray_update_collective_metadata__,
            tensor_transport_metadata,
            NIXL_GROUP_NAME,
            None,
            None,
        )
    else:
        communicators = get_collective_groups(
            [src_actor, dst_actor],
            backend=backend,
        )
        # TODO(kevin85421): Support multiple communicators.
        if len(communicators) == 0:
            raise ValueError(
                f"No communicators found for actors {src_actor} and {dst_actor}. "
                "Create a communicator with "
                "`ray.experimental.collective.create_collective_group` "
                "before calling actor tasks."
            )
        elif len(communicators) > 1:
            raise ValueError(
                f"There are {len(communicators)} possible communicators that contain actors {src_actor} and {dst_actor}. "
                "Currently, GPU objects only support one communicator. Please make sure only "
                "one communicator exists."
            )
        communicator = communicators[0]
        src_rank = communicator.get_rank(src_actor)
        if src_rank == -1:
            raise ValueError(
                f"Sender actor {src_actor} not found in communicator. "
                "Please make sure the sender and receiver are in the same communicator."
            )
        dst_rank = communicator.get_rank(dst_actor)
        if dst_rank == -1:
            raise ValueError(
                f"Receiver actor {dst_actor} not found in communicator. "
                "Please make sure the sender and receiver are in the same communicator."
            )
        return src_actor.__ray_call__.remote(
            __ray_update_collective_metadata__,
            tensor_transport_metadata,
            communicator.name,
            src_rank,
            dst_rank,
        )


def _is_one_sided(backend: Optional[str] = None) -> bool:
    """
    Check if the backend is one-sided.
    """
    return backend == Backend.NIXL


def send_gpu_object(
    src_actor: "ray.actor.ActorHandle",
    obj_id: str,
    tensor_transport_metadata: TensorTransportMetadata,
    backend: Optional[str] = None,
):
    if _is_one_sided(backend):
        pass
    else:
        from ray.experimental.gpu_object_manager.gpu_object_store import __ray_send__

        # Send tensors stored in the `src_actor`'s GPU object store to the
        # destination rank `dst_rank`.
        # NOTE(swang): We put this task on the background thread to avoid tasks
        # executing on the main thread blocking the data transfer.
        src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
            __ray_send__,
            obj_id,
            tensor_transport_metadata,
        )


def recv_gpu_object(
    dst_actor: "ray.actor.ActorHandle",
    obj_id: str,
    tensor_transport_metadata: TensorTransportMetadata,
    backend: Optional[str] = None,
):
    from ray.experimental.gpu_object_manager.gpu_object_store import __ray_recv__

    # Receive tensors from the source rank and store them in the
    # `dst_actor`'s GPU object store.
    # NOTE(swang): We put this task on the background thread to avoid tasks
    # executing on the main thread blocking the data transfer. Technically,
    # this is only needed for the sender task, but we put the receiver task
    # on the same background thread to ensure that all communication
    # operations are executed in a global order.
    dst_actor.__ray_call__.remote.options(concurrency_group="_ray_system").remote(
        __ray_recv__, obj_id, tensor_transport_metadata
    )
