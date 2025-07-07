from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Tuple

import ray
from ray._private.custom_types import TensorTransportEnum
from ray._raylet import ObjectRef


if TYPE_CHECKING:
    import torch
    from ray.experimental.gpu_object_manager.gpu_object_store import GPUObjectStore

# GPUObjectMeta is a named tuple containing the source actor, tensor transport
# backend, and tensor metadata.
# - The tensor transport backend is the backend used to transport the tensors.
#   Currently, the supported backends are "nccl" and "torch_gloo".
# - The tensor metadata is a list of tuples, each containing the shape and dtype
#   of a tensor in the GPU object store.
class GPUObjectMeta(NamedTuple):
    src_actor: "ray.actor.ActorHandle"
    # Must be a valid backend name as defined in
    # `ray.util.collective.types.Backend`.
    tensor_transport_backend: str
    tensor_meta: List[Tuple["torch.Size", "torch.dtype"]]


def __ray_get_tensor_meta__(self, obj_id: str):
    """Helper function that runs on the src actor to get the tensor metadata."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    assert gpu_object_store.has_gpu_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    tensors = gpu_object_store.get_gpu_object(obj_id)
    return [(t.shape, t.dtype) for t in tensors]


def __ray_fetch_gpu_object__(self, obj_id: str):
    """Helper function that runs on the src actor to fetch tensors from the GPU object store via the object store."""
    from ray._private.worker import global_worker

    gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
    assert gpu_object_store.has_gpu_object(
        obj_id
    ), f"obj_id={obj_id} not found in GPU object store"
    tensors = gpu_object_store.get_gpu_object(obj_id)
    # TODO(kevin85421): The current garbage collection implementation for the
    # in-actor object store is naive. We garbage collect each object after it
    # is consumed once.
    gpu_object_store.remove_gpu_object(obj_id)
    return tensors


class GPUObjectManager:
    def __init__(self):
        # A dictionary that maps from owned object's ID to GPUObjectMeta.
        # This dictionary is hosted on the "driver" process of the actors that
        # store and send/receive GPU objects.
        self.managed_gpu_object_metadata: Dict[str, GPUObjectMeta] = {}
        # Per-actor local storage for GPU objects. We create the GPU object
        # store lazily, if a user specifies a non-default tensor_transport, to
        # avoid circular import and because it imports third-party dependencies
        # like PyTorch.
        self._gpu_object_store: Optional["GPUObjectStore"] = None

    @property
    def gpu_object_store(self) -> "ray.experimental.GPUObjectStore":
        if self._gpu_object_store is None:
            from ray.experimental.gpu_object_manager.gpu_object_store import (
                GPUObjectStore,
            )

            self._gpu_object_store = GPUObjectStore()
        return self._gpu_object_store

    def _get_tensor_meta(
        self, src_actor: "ray.actor.ActorHandle", obj_id: str
    ) -> ObjectRef:
        # Submit a Ray actor task to the source actor to get the tensor metadata.
        # The metadata is a list of tuples, where each tuple contains the shape and dtype
        # of a tensor in the GPU object store. This function returns an ObjectRef that
        # points to the tensor metadata.
        return src_actor.__ray_call__.remote(__ray_get_tensor_meta__, obj_id)

    def is_managed_gpu_object(self, obj_id: str) -> bool:
        """
        Check if the GPU object is managed by this process.

        Args:
            obj_id: The object ID of the GPU object.

        Returns:
            True if the current process is the driver process coordinating the data transfer
            of this GPU object.
        """
        return obj_id in self.managed_gpu_object_metadata

    def add_gpu_object_ref(
        self,
        obj_ref: ObjectRef,
        src_actor: "ray.actor.ActorHandle",
        tensor_transport: TensorTransportEnum,
    ):
        """Add a GPU object reference to the GPU object manager. This should be
        called whenever the current process calls a task that is annotated with
        `@ray.method(tensor_transport=...)`.

        Args:
            obj_ref: The ObjectRef of the task output.
            src_actor: The actor that executes the task and that creates the GPU object.
            tensor_transport: The tensor transport protocol to use for the GPU object.
        """
        from ray.experimental.gpu_object_manager.gpu_object_store import (
            _tensor_transport_to_collective_backend,
        )

        tensor_transport_backend = _tensor_transport_to_collective_backend(
            tensor_transport
        )
        obj_id = obj_ref.hex()
        tensor_meta = self._get_tensor_meta(src_actor, obj_id)
        self.managed_gpu_object_metadata[obj_id] = GPUObjectMeta(
            src_actor=src_actor,
            tensor_transport_backend=tensor_transport_backend,
            tensor_meta=tensor_meta,
        )

    def _get_gpu_object_metadata(self, obj_ref: ObjectRef) -> GPUObjectMeta:
        obj_id = obj_ref.hex()
        return self.managed_gpu_object_metadata[obj_id]

    def _send_gpu_object(
        self,
        communicator_name: str,
        src_actor: "ray.actor.ActorHandle",
        obj_id: str,
        dst_rank: int,
    ):
        from ray.experimental.gpu_object_manager.gpu_object_store import __ray_send__

        # Send tensors stored in the `src_actor`'s GPU object store to the
        # destination rank `dst_rank`.
        src_actor.__ray_call__.remote(__ray_send__, communicator_name, obj_id, dst_rank)

    def _recv_gpu_object(
        self,
        communicator_name: str,
        dst_actor: "ray.actor.ActorHandle",
        obj_id: str,
        src_rank: int,
        tensor_meta: List[Tuple["torch.Size", "torch.dtype"]],
    ):
        from ray.experimental.gpu_object_manager.gpu_object_store import __ray_recv__

        # Receive tensors from the source rank and store them in the
        # `dst_actor`'s GPU object store.
        dst_actor.__ray_call__.remote(
            __ray_recv__, communicator_name, obj_id, src_rank, tensor_meta
        )

    def fetch_gpu_object(self, obj_id: str):
        """
        Fetches the GPU object from the source actor's GPU object store via the object store
        instead of out-of-band tensor transfer and stores the tensors in the local GPU object store.

        This is useful when the current process does not support the designated out-of-band tensor transport.
        For example, if the tensor transport is NCCL but the driver does not have a GPU, we use this call to
        fulfill a `ray.get` call.

        Args:
            obj_id: The object ID of the GPU object.

        Returns:
            None
        """

        if self.gpu_object_store.has_gpu_object(obj_id):
            return

        gpu_object_meta = self.managed_gpu_object_metadata[obj_id]
        src_actor = gpu_object_meta.src_actor
        tensors = ray.get(
            src_actor.__ray_call__.remote(__ray_fetch_gpu_object__, obj_id)
        )
        self.gpu_object_store.add_gpu_object(obj_id, tensors)

    def trigger_out_of_band_tensor_transfer(
        self, dst_actor: "ray.actor.ActorHandle", task_args: Tuple[Any, ...]
    ):
        """
        Triggers tensor communication operations between actors. When a managed ObjectRef is passed
        to another actor task, CPU data will still be passed through the object store, but the in-actor
        tensors will be passed out-of-band.

        This function triggers the out-of-band tensor transfer by submitting Ray actor
        tasks `__ray_send__` to the sender actor and `__ray_recv__` to the receiver actor to initiate
        tensor communication using protocols like NCCL or GLOO.

        Before the receiver actor executes the actor task, the deserializer combines the
        CPU data with the tensors from the sender actor to reconstruct the original task output
        generated by the sender actor.

        Args:
            dst_actor: The target actor to receive tensors
            task_args: List of arguments for the target actor task that may contain ObjectRefs.
        """
        for arg in task_args:
            # If an ObjectRef is managed, it means the actual value is a list of tensors stored
            # on a remote actor. Therefore, this function will trigger a tensor communication
            # operation between the sender and receiver actors.
            if not isinstance(arg, ObjectRef):
                continue

            if not self.is_managed_gpu_object(arg.hex()):
                continue

            # Import get_collective_groups here to avoid dependency on
            # collective libraries for default Ray installation.
            from ray.experimental.collective import get_collective_groups

            gpu_object_meta = self._get_gpu_object_metadata(arg)

            src_actor = gpu_object_meta.src_actor
            tensor_meta = gpu_object_meta.tensor_meta
            communicators = get_collective_groups(
                [src_actor, dst_actor], backend=gpu_object_meta.tensor_transport_backend
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
            if src_rank == dst_rank:
                # If the source and destination ranks are the same, the tensors can
                # be transferred intra-process, so we skip the out-of-band tensor
                # transfer.
                continue
            self._send_gpu_object(communicator.name, src_actor, arg.hex(), dst_rank)
            self._recv_gpu_object(
                communicator.name, dst_actor, arg.hex(), src_rank, tensor_meta
            )
