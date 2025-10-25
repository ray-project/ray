import logging
import threading
import time
import warnings
from queue import Queue
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Set, Tuple

import ray
from ray._private import ray_constants
from ray._private.custom_types import TensorTransportEnum
from ray._raylet import ObjectRef

if TYPE_CHECKING:
    import torch

    from ray.experimental.gpu_object_manager.gpu_object_store import (
        GPUObjectStore,
    )
    from ray.util.collective.types import CommunicatorMetadata, TensorTransportMetadata

logger = logging.getLogger(__name__)

# GPUObjectMeta is a named tuple containing the source actor, tensor transport
# backend, tensor metadata, and other information that needs to be recorded.
# - The tensor transport backend is the backend used to transport the tensors.
#   Currently, the supported backends are "nccl" and "torch_gloo".
# - The tensor metadata is a list of tuples, each containing the shape and dtype
#   of a tensor in the GPU object store.
class GPUObjectMeta(NamedTuple):
    src_actor: "ray.actor.ActorHandle"
    # Must be a valid backend name as defined in
    # `ray.util.collective.types.Backend`.
    tensor_transport_backend: str
    tensor_transport_meta: "TensorTransportMetadata"
    # sent_dest_actors tracks the set of actor IDs that this object has been sent to.
    sent_dest_actors: Set[str]
    # sent_to_src_actor_and_others_warned indicates whether the object has already triggered a warning about being sent back to the source actor and other actors simultaneously.
    sent_to_src_actor_and_others_warned: bool


# This is used to periodically check in on the RDT transfer through the refs from
# __ray_send__ and __ray_recv__ and abort operations in case of failures / timeouts.
class TransferMetadata(NamedTuple):
    src_actor: "ray.actor.ActorHandle"
    dst_actor: "ray.actor.ActorHandle"
    send_ref: Optional[ObjectRef]
    recv_ref: ObjectRef
    communicator_meta: "CommunicatorMetadata"
    backend: str
    timeout: float


# TODO(swang): Uncomment and add an API docs page and example usage.
# @PublicAPI(stability="alpha")
def wait_tensor_freed(tensor: "torch.Tensor", timeout: Optional[float] = None):
    """
    Wait for the tensor to be freed.

    This function is useful for cases where an actor keeps a reference to a
    tensor after returning the tensor from a task annotated with
    `@ray.method(tensor_transport=...)`. In this case, Ray will store a
    *reference* to the tensor, so any in-place modifications made by the actor
    that returned the tensor could be seen by other actors. See
    :ref:`Ray Direct Transport (RDT) <direct-transport>` for more details.

    Call this function for RDT objects to ensure that all corresponding
    `ray.ObjectRefs` have gone out of scope and therefore the tensor is safe to
    write to again.

    Args:
        tensor: The tensor to wait to be freed. This should be a tensor that was
            previously returned by a task annotated with
            `@ray.method(tensor_transport=...)` or stored via
            `ray.put(_tensor_transport="...")`.
        timeout: The timeout in seconds to wait for all references to the tensor
            to go out of scope. Set to None to wait indefinitely. Note that if
            None is used, this function could hang if the `ray.ObjectRefs` that
            refer to this tensor never go out of scope.
    """
    gpu_object_manager = ray.worker.global_worker.gpu_object_manager
    gpu_object_manager.gpu_object_store.wait_tensor_freed(tensor, timeout)


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
        # Lock to ensure we only create the GPU object store once.
        self.gpu_object_store_lock = threading.Lock()

        # Thread safe queue of transport refs that the monitor thread needs to start monitoring
        self._unmonitored_transfers: Queue[TransferMetadata] = Queue()
        # Background thread to poll on the transfer operation.
        self._monitor_failures_thread = None
        # Event to signal the monitor_failures thread to shutdown
        self._monitor_failures_shutdown_event = threading.Event()

    @property
    def gpu_object_store(self) -> "ray.experimental.GPUObjectStore":
        with self.gpu_object_store_lock:
            if self._gpu_object_store is None:
                from ray.experimental.gpu_object_manager.gpu_object_store import (
                    GPUObjectStore,
                )

                self._gpu_object_store = GPUObjectStore()
        return self._gpu_object_store

    def shutdown(self):
        """
        Interrupt and join the monitor_failures thread.
        """
        if self._monitor_failures_thread:
            self._monitor_failures_shutdown_event.set()
            self._monitor_failures_thread.join()
            self._monitor_failures_shutdown_event.clear()
            self._monitor_failures_thread = None

    def _monitor_failures(self):
        """
        Monitor the refs from send and recv tasks and abort the transfers
        if they error out or timeout to prevent hanging.
        """
        not_done = []
        done = []
        ref_info_map = {}
        while not self._monitor_failures_shutdown_event.is_set():
            while not self._unmonitored_transfers.empty():
                ref_info = self._unmonitored_transfers.get()
                if ref_info.send_ref:
                    not_done.append(ref_info.send_ref)
                    ref_info_map[ref_info.send_ref.hex()] = ref_info
                not_done.append(ref_info.recv_ref)
                ref_info_map[ref_info.recv_ref.hex()] = ref_info
            if len(not_done) > 0:
                done, not_done = ray.wait(not_done, num_returns=1, timeout=1)
            if len(done) > 0:
                try:
                    ray.get(done[0])
                    ref_info_map.pop(done[0].hex(), None)
                except Exception as e:
                    self._abort_transport(done[0], ref_info_map, e)

            while len(not_done) > 0:
                if not_done[0].hex() not in ref_info_map:
                    # The associated transfer was already aborted.
                    not_done.pop(0)
                elif ref_info_map[not_done[0].hex()].timeout < time.time():
                    self._abort_transport(
                        not_done[0],
                        ref_info_map,
                        TimeoutError(
                            f"RDT transfer failed after {ray_constants.FETCH_FAIL_TIMEOUT_SECONDS}s."
                        ),
                    )
                else:
                    # wait returns lists in the same order they were passed in, so if
                    # the timeout of first hasn't been reached, neither have the others.
                    break
            if len(not_done) == 0:
                # If we emptied out _unmonitored_transfers on this iteration, wait for a bit.
                self._monitor_failures_shutdown_event.wait(1)

    def _abort_transport(
        self,
        failed_ref: ObjectRef,
        ref_info_map: Dict[str, TransferMetadata],
        exception: Exception,
    ):
        """
        Cleans up the ref_info_map, kill the src and dst actors, and destroy the
        collective group if necessary.
        """
        from ray.experimental.collective import destroy_collective_group
        from ray.util.collective.types import CollectiveCommunicatorMetadata

        ref_info = ref_info_map.pop(failed_ref.hex(), None)
        if ref_info is None:
            return

        logger.error(
            "RDT transfer with src actor %s and dst actor %s failed. Killing the actors. "
            "Transfer failed with exception: %s",
            ref_info.src_actor,
            ref_info.dst_actor,
            exception,
        )

        if ref_info.send_ref:
            ref_info_map.pop(ref_info.send_ref.hex(), None)
        ref_info_map.pop(ref_info.recv_ref.hex(), None)

        # TODO(#51276): Kill all actors in the collective group when we support more collective operations
        ray.kill(ref_info.src_actor)
        ray.kill(ref_info.dst_actor)

        # isinstance does an implicit cast and makes communicator_name inaccessible
        # so we have to get communicator_name before the cast.
        collective_group_name = ref_info.communicator_meta.communicator_name
        if isinstance(ref_info.communicator_meta, CollectiveCommunicatorMetadata):
            try:
                destroy_collective_group(collective_group_name)
                logger.error(
                    "Destroyed collective group %s due to a hanging/failed RDT transfer",
                    collective_group_name,
                )
            except ValueError:
                # Collective group was already destroyed
                pass

    def is_managed_object(self, obj_id: str) -> bool:
        """
        Check if the GPU object is managed by this process.

        Args:
            obj_id: The object ID of the GPU object.

        Returns:
            True if the current process is the driver process coordinating the data transfer
            of this GPU object.
        """
        return obj_id in self.managed_gpu_object_metadata

    def add_gpu_object_metadata(
        self, obj_ref: ObjectRef, gpu_object_meta: GPUObjectMeta
    ):
        """
        Add the GPU object metadata to the GPU object manager.

        Args:
            obj_ref: The ObjectRef of the GPU object.
            gpu_object_meta: The GPU object metadata.
        """
        obj_id = obj_ref.hex()
        self.managed_gpu_object_metadata[obj_id] = gpu_object_meta

    def add_gpu_object_ref(
        self,
        obj_ref: ObjectRef,
        src_actor: "ray.actor.ActorHandle",
        tensor_transport: TensorTransportEnum,
        tensor_transport_meta: Optional["TensorTransportMetadata"] = None,
    ):
        """Add a GPU object reference to the GPU object manager. This should be
        called whenever the current process calls a task that is annotated with
        `@ray.method(tensor_transport=...)`.

        Args:
            obj_ref: The ObjectRef of the task output.
            src_actor: The actor that executes the task and that creates the GPU object.
            tensor_transport: The tensor transport protocol to use for the GPU object.
            tensor_transport_meta: The tensor transport metadata that is pre-computed.
        """
        from ray.experimental.collective import get_tensor_transport_manager
        from ray.experimental.gpu_object_manager.gpu_object_store import (
            _tensor_transport_to_collective_backend,
        )

        tensor_transport_backend = _tensor_transport_to_collective_backend(
            tensor_transport
        )
        obj_id = obj_ref.hex()
        tensor_transport_manager = get_tensor_transport_manager(
            tensor_transport_backend
        )
        if not tensor_transport_meta:
            tensor_meta = tensor_transport_manager.get_tensor_transport_metadata(
                src_actor, obj_id
            )
        else:
            tensor_meta = tensor_transport_meta
        self.managed_gpu_object_metadata[obj_id] = GPUObjectMeta(
            src_actor=src_actor,
            tensor_transport_backend=tensor_transport_backend,
            tensor_transport_meta=tensor_meta,
            sent_dest_actors=set(),
            sent_to_src_actor_and_others_warned=False,
        )

    def _get_gpu_object_metadata(self, obj_ref: ObjectRef) -> GPUObjectMeta:
        obj_id = obj_ref.hex()
        return self.managed_gpu_object_metadata[obj_id]

    def _fetch_object(
        self,
        obj_id: str,
        tensor_transport: TensorTransportEnum = TensorTransportEnum.OBJECT_STORE,
    ):
        """
        Fetches the GPU object from the source actor's GPU object store via the object store
        instead of out-of-band tensor transfer and stores the tensors in the local GPU object store.

        This is useful when the current process does not support the designated out-of-band tensor transport.
        For example, if the tensor transport is NCCL but the driver does not have a GPU, we use this call to
        fulfill a `ray.get` call.

        Args:
            obj_id: The object ID of the GPU object.
            tensor_transport: The tensor transport to use to fetch the GPU object.

        Returns:
            None
        """
        from ray.experimental.collective import get_tensor_transport_manager
        from ray.experimental.gpu_object_manager.gpu_object_store import (
            __ray_fetch_gpu_object__,
        )

        if tensor_transport not in [
            TensorTransportEnum.OBJECT_STORE,
            TensorTransportEnum.NIXL,
        ]:
            raise ValueError(
                f"Currently ray.get() only supports OBJECT_STORE and NIXL tensor transport, got {tensor_transport}, please specify the correct tensor transport in ray.get()."
            )

        if self.gpu_object_store.has_object(obj_id):
            return
        gpu_object_meta = self.managed_gpu_object_metadata[obj_id]
        src_actor = gpu_object_meta.src_actor
        tensor_transport_backend = gpu_object_meta.tensor_transport_backend
        tensor_transport_manager = get_tensor_transport_manager(
            tensor_transport_backend
        )
        if tensor_transport == TensorTransportEnum.OBJECT_STORE:
            tensors = ray.get(
                src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                    __ray_fetch_gpu_object__, obj_id
                )
            )
            self.gpu_object_store.add_object(obj_id, tensors)
        else:
            if isinstance(gpu_object_meta.tensor_transport_meta, ObjectRef):
                # If the tensor transport meta is an ObjectRef, gpu object manager
                # needs to fetch the tensor transport meta from the src actor first.
                fetched_meta = ray.get(gpu_object_meta.tensor_transport_meta)

                gpu_object_meta = gpu_object_meta._replace(
                    tensor_transport_meta=fetched_meta
                )
                # Update the managed GPU object metadata so that the next time
                # it doesn't need to fetch the tensor transport meta again.
                self.managed_gpu_object_metadata[obj_id] = gpu_object_meta

            from ray.experimental.gpu_object_manager.gpu_object_store import (
                __ray_recv__,
            )

            communicator_meta = tensor_transport_manager.get_communicator_metadata(
                None, None, tensor_transport_backend
            )
            __ray_recv__(
                None, obj_id, gpu_object_meta.tensor_transport_meta, communicator_meta
            )

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

        gpu_object_refs = set()
        for arg in task_args:
            # If an ObjectRef is managed, it means the actual value is a list of tensors stored
            # on a remote actor. Therefore, this function will trigger a tensor communication
            # operation between the sender and receiver actors.
            if not isinstance(arg, ObjectRef):
                continue
            if self.is_managed_object(arg.hex()):
                gpu_object_refs.add(arg)
        if gpu_object_refs:
            from ray.experimental.collective import get_tensor_transport_manager
            from ray.experimental.gpu_object_manager.gpu_object_store import (
                __ray_recv__,
                __ray_send__,
            )

        # Count the number of readers for each GPU object.
        for obj_ref in gpu_object_refs:
            # Import get_collective_groups here to avoid dependency on
            # collective libraries for default Ray installation.

            gpu_object_meta = self._get_gpu_object_metadata(obj_ref)

            src_actor = gpu_object_meta.src_actor
            tensor_transport_meta = gpu_object_meta.tensor_transport_meta

            obj_id = obj_ref.hex()

            # Update the set of destination actors for this object
            # The set inside NamedTuple is mutable, so we can modify it directly
            gpu_object_meta.sent_dest_actors.add(dst_actor._actor_id)
            # Check if a warning should be triggered for this object:
            # 1. object has not triggered a warning yet.
            # 2. object is sent back to its source actor.
            # 3. object is also sent to at least one other actor
            if (
                not gpu_object_meta.sent_to_src_actor_and_others_warned
                and src_actor._actor_id in gpu_object_meta.sent_dest_actors
                and len(gpu_object_meta.sent_dest_actors) > 1
            ):
                warnings.warn(
                    f"GPU ObjectRef({obj_id}) is being passed back to the actor that created it {src_actor}. "
                    "Note that GPU objects are mutable. If the tensor is modified, Ray's internal copy will also be updated, and subsequent passes to other actors "
                    "will receive the updated version instead of the original.",
                    UserWarning,
                )
                # Mark the object as warned by creating a new NamedTuple instance
                self.managed_gpu_object_metadata[obj_id] = gpu_object_meta._replace(
                    sent_to_src_actor_and_others_warned=True
                )

            if src_actor._actor_id == dst_actor._actor_id:
                # If the source and destination actors are the same, the tensors can
                # be transferred intra-process, so we skip the out-of-band tensor
                # transfer.
                continue

            tensor_transport_manager = get_tensor_transport_manager(
                gpu_object_meta.tensor_transport_backend
            )
            communicator_meta = tensor_transport_manager.get_communicator_metadata(
                src_actor,
                dst_actor,
                gpu_object_meta.tensor_transport_backend,
            )

            send_ref = None
            if not tensor_transport_manager.is_one_sided():
                # Send tensors stored in the `src_actor`'s GPU object store to the
                # destination rank `dst_rank`.
                # NOTE: We put this task on the background thread to avoid tasks
                # executing on the main thread blocking the data transfer.
                send_ref = src_actor.__ray_call__.options(
                    concurrency_group="_ray_system"
                ).remote(
                    __ray_send__,
                    obj_id,
                    tensor_transport_meta,
                    communicator_meta,
                )

            # Receive tensors from the source rank and store them in the
            # `dst_actor`'s GPU object store.
            # NOTE: Putting this task on the background thread is technically only
            # needed for the sender task, but we put the receiver task on the same
            # background thread to ensure that all communication operations are
            # executed in a global order.
            recv_ref = dst_actor.__ray_call__.options(
                concurrency_group="_ray_system"
            ).remote(
                __ray_recv__,
                obj_id,
                tensor_transport_meta,
                communicator_meta,
            )

            self._unmonitored_transfers.put(
                TransferMetadata(
                    src_actor=src_actor,
                    dst_actor=dst_actor,
                    send_ref=send_ref,
                    recv_ref=recv_ref,
                    communicator_meta=communicator_meta,
                    backend=gpu_object_meta.tensor_transport_backend,
                    timeout=time.time() + ray_constants.FETCH_FAIL_TIMEOUT_SECONDS,
                )
            )
            if self._monitor_failures_thread is None:
                self._monitor_failures_thread = threading.Thread(
                    target=self._monitor_failures, daemon=True
                )
                self._monitor_failures_thread.start()

    def get_gpu_object(
        self,
        object_id: str,
        tensor_transport: TensorTransportEnum = TensorTransportEnum.OBJECT_STORE,
    ) -> List["torch.Tensor"]:
        """
        Get the GPU object for a given object ID.

        Args:
            object_id: The object ID of the GPU object.
            tensor_transport: The tensor transport to use to fetch the GPU object.

        Returns:
            The GPU object.
        """
        gpu_object_store = self.gpu_object_store
        if self.is_managed_object(object_id):
            self._fetch_object(object_id, tensor_transport)

        # If the GPU object is the primary copy, it means the transfer is intra-actor.
        # In this case, we should not remove the GPU object after it is consumed once,
        # because the GPU object reference may be used again.
        # Instead, we should wait for the GC callback to clean it up.
        pop_object = not gpu_object_store.is_primary_copy(object_id)
        if pop_object:
            gpu_object = gpu_object_store.wait_and_pop_object(
                object_id, timeout=ray_constants.FETCH_FAIL_TIMEOUT_SECONDS
            )
        else:
            gpu_object = gpu_object_store.wait_and_get_object(
                object_id, timeout=ray_constants.FETCH_FAIL_TIMEOUT_SECONDS
            )
        return gpu_object

    def free_object_primary_copy(self, object_id: str):
        """
        Free the RDT object on the primary copy holder and free metadata
        on the owner.
        """
        from ray.experimental.gpu_object_manager.gpu_object_store import (
            __ray_free__,
        )

        try:
            gpu_object_meta = self.managed_gpu_object_metadata[object_id]
            src_actor = gpu_object_meta.src_actor
            tensor_transport_backend = gpu_object_meta.tensor_transport_backend
            tensor_transport_meta = gpu_object_meta.tensor_transport_meta
            src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                __ray_free__, object_id, tensor_transport_backend, tensor_transport_meta
            )
            # NOTE: This may have to change if we support lineage reconstruction for RDT
            # TODO(#57962): Metadata is currently not removed on borrowers that borrow through
            # the NIXL ray.put / ray.get
            self.managed_gpu_object_metadata.pop(object_id, None)
        except Exception as e:
            logger.error(
                "Something went wrong while freeing the RDT object!", exc_info=e
            )

    def actor_has_tensor_transport(
        self, actor: "ray.actor.ActorHandle", tensor_transport: TensorTransportEnum
    ):
        """
        Check if the actor has a communicator for the given tensor transport backend.

        Args:
            actor: The actor to check.
            tensor_transport: The tensor transport backend to check.

        Returns:
            True if the actor has a communicator for the given tensor transport backend, False otherwise.
        """
        # Import get_collective_groups here to avoid dependency on
        # collective libraries for default Ray installation.
        from ray.experimental.collective import get_tensor_transport_manager
        from ray.experimental.gpu_object_manager.gpu_object_store import (
            _tensor_transport_to_collective_backend,
        )

        tensor_transport_backend = _tensor_transport_to_collective_backend(
            tensor_transport
        )
        tensor_transport_manager = get_tensor_transport_manager(
            tensor_transport_backend
        )
        return tensor_transport_manager.actor_has_tensor_transport(actor)

    def put_object(
        self,
        obj_ref: ObjectRef,
        tensor_transport: TensorTransportEnum,
        tensors: List["torch.Tensor"],
    ):
        """
        Put the GPU object into the GPU object manager.

        Args:
            obj_ref: The object ref of the GPU object.
            tensor_transport: The tensor transport backend to use.
            tensors: The tensors to put into the GPU object manager.

        """
        from ray.experimental.collective import get_tensor_transport_manager
        from ray.experimental.gpu_object_manager.gpu_object_store import (
            _tensor_transport_to_collective_backend,
        )

        tensor_transport_backend = _tensor_transport_to_collective_backend(
            tensor_transport
        )
        transport_manager = get_tensor_transport_manager(tensor_transport_backend)
        tensor_transport_meta = transport_manager.extract_tensor_transport_metadata(
            tensors
        )

        src_actor = ray.get_runtime_context().current_actor
        self.gpu_object_store.add_object(obj_ref.hex(), tensors, is_primary=True)
        self.add_gpu_object_ref(
            obj_ref,
            src_actor,
            tensor_transport,
            tensor_transport_meta=tensor_transport_meta,
        )
