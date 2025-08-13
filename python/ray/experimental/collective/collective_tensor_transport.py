from typing import Optional

import ray
from ray.experimental.collective.tensor_transport_manager import (
    TensorTransportManager,
    TensorTransportMetadata,
    TensorTransportEnum,
)


class CollectiveTensorTransport(TensorTransportManager):
    @staticmethod
    def is_one_sided() -> bool:
        return False

    @staticmethod
    def get_tensor_transport_metadata(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport: TensorTransportEnum,
    ) -> TensorTransportMetadata:
        def __ray_get_tensor_transport_metadata__(
            self: "ray.actor.ActorHandle",
            obj_id: str,
            tensor_transport: TensorTransportEnum,
        ) -> TensorTransportMetadata:

            from ray._private.worker import global_worker
            from ray.util.collective.types import CollectiveTransportMetadata

            gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
            # NOTE: We do not specify a timeout here because the user task that returns
            # it could take arbitrarily long and we don't want to trigger a spurious
            # timeout.
            gpu_object = gpu_object_store.wait_and_get_object(obj_id)
            return CollectiveTransportMetadata(
                tensor_meta=[(t.shape, t.dtype) for t in gpu_object.data],
            )

        # Submit a Ray actor task to the source actor to get the tensor metadata.
        # The metadata is a list of tuples, where each tuple contains the shape and dtype
        # of a tensor in the GPU object store. This function returns an ObjectRef that
        # points to the tensor metadata.
        # NOTE(swang): We put this task on the background thread to avoid tasks
        # executing on the main thread blocking this task.

        return src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
            __ray_get_tensor_transport_metadata__, obj_id, tensor_transport
        )

    @staticmethod
    def get_collective_metadata(
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        tensor_transport_metadata: TensorTransportMetadata,
        backend: Optional[str] = None,
    ) -> TensorTransportMetadata:
        """
        Update the collective metadata (e.g. communicator name, src/dst rank)
        before sending the GPU object.
        """
        from ray.experimental.collective import get_collective_groups

        def __ray_update_collective_metadata__(
            self: "ray.actor.ActorHandle",
            tensor_transport_metadata: TensorTransportMetadata,
            communicator_name: str,
            src_rank: int,
            dst_rank: int,
        ):
            tensor_transport_metadata.communicator_name = communicator_name
            tensor_transport_metadata.src_rank = src_rank
            tensor_transport_metadata.dst_rank = dst_rank
            return tensor_transport_metadata

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

    @staticmethod
    def send_object(
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        backend: Optional[str] = None,
    ):
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

    @staticmethod
    def recv_object(
        dst_actor: "ray.actor.ActorHandle",
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
    ):
        from ray.experimental.gpu_object_manager.gpu_object_store import __ray_recv__

        # Receive tensors from the source rank and store them in the
        # `dst_actor`'s GPU object store.
        #
        # NOTE(swang): We put this task on the background thread to avoid tasks
        # executing on the main thread blocking the data transfer. Technically,
        # this is only needed for the sender task, but we put the receiver task
        # on the same background thread to ensure that all communication
        # operations are executed in a global order.
        dst_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
            __ray_recv__, obj_id, tensor_transport_metadata
        )
