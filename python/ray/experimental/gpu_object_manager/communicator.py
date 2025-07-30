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


def get_initial_tensor_transport_metadata(
    obj_id: str, tensor_transport: TensorTransportEnum
) -> TensorTransportMetadata:
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


def update_transport_metadata_before_send(
    src_actor: "ray.actor.ActorHandle",
    dst_actor: "ray.actor.ActorHandle",
    tensor_transport_metadata: TensorTransportMetadata,
    backend: Optional[str] = None,
) -> TensorTransportMetadata:
    def __ray_update_transport_metadata__(
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

    from ray.experimental.collective import get_collective_groups

    if backend == Backend.NIXL:
        return src_actor.__ray_call__.remote(
            __ray_update_transport_metadata__,
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
            __ray_update_transport_metadata__,
            tensor_transport_metadata,
            communicator.name,
            src_rank,
            dst_rank,
        )


def is_one_sided(backend: Optional[str] = None) -> bool:
    # FIXME(Qiaolin-Yu): This is a temporary solution to check if the transport is one-sided.
    return backend == Backend.NIXL


def send_gpu_object(
    src_actor: "ray.actor.ActorHandle",
    obj_id: str,
    tensor_transport_metadata: TensorTransportMetadata,
    backend: Optional[str] = None,
):
    if is_one_sided(backend):
        pass
    else:
        from ray.experimental.gpu_object_manager.gpu_object_store import __ray_send__

        # Send tensors stored in the `src_actor`'s GPU object store to the
        # destination rank `dst_rank`.
        src_actor.__ray_call__.remote(
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
    dst_actor.__ray_call__.remote(__ray_recv__, obj_id, tensor_transport_metadata)
