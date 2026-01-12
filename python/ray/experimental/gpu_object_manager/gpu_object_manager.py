import logging
import threading
import time
import warnings
from collections import defaultdict
from queue import Queue
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)

import ray
from ray._private import ray_constants
from ray._raylet import ObjectRef
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch

    from ray.experimental.gpu_object_manager.gpu_object_store import (
        GPUObjectStore,
    )
    from ray.experimental.gpu_object_manager.tensor_transport_manager import (
        CommunicatorMetadata,
        TensorTransportMetadata,
    )

logger = logging.getLogger(__name__)

# GPUObjectMeta is a named tuple containing the source actor, tensor transport
# backend, tensor metadata, and other information that needs to be recorded.
# - The tensor transport backend is the backend used to transport the tensors.
# - The tensor metadata is a list of tuples, each containing the shape and dtype
#   of a tensor in the GPU object store.
class GPUObjectMeta(NamedTuple):
    src_actor: "ray.actor.ActorHandle"
    tensor_transport_backend: str
    # This is set when the actual object is created and the metadata makes it back to the owner.
    # For ray.put the owner is the creator so it's immediately set.
    tensor_transport_meta: Optional["TensorTransportMetadata"]
    # sent_dest_actors tracks the set of actor IDs that this object has been sent to.
    # Note that since the set is mutable, it shouldn't be accessed without a lock.
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
    obj_id: str
    timeout: float


@PublicAPI(stability="alpha")
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
        # This lock protects _managed_gpu_object_metadata, _queued_transfers, and _queued_frees since
        # they can be accessed from the user's python thread or the CoreWorker's main io service thread.
        self._lock = threading.Lock()

        # A dictionary that maps from owned object's ID to GPUObjectMeta.
        # This dictionary is hosted on the "driver" process of the actors that
        # store and send/receive GPU objects.
        self._managed_gpu_object_metadata: Dict[str, GPUObjectMeta] = {}
        # Condition variable to wait for the tensor transport meta to be set.
        self._tensor_transport_meta_cv = threading.Condition(self._lock)

        # A dictionary that maps from an object id to a list of actors
        # that are queued to receive the object.
        self._queued_transfers: Dict[str, List["ray.actor.ActorHandle"]] = defaultdict(
            list
        )
        # A set of object ids that are queued to be freed. This is used when the object is freed
        # before the owner knows it's created (the tensor transport metadata is not available yet).
        self._queued_frees: Set[str] = set()

        # This lock makes sure the _gpu_object_store and _monitor_failures_thread are only created once.
        self._init_lock = threading.Lock()

        # Per-actor local storage for GPU objects. We create the GPU object
        # store lazily, if a user specifies a non-default tensor_transport, to
        # avoid circular import and because it imports third-party dependencies
        # like PyTorch.
        self._gpu_object_store: Optional["GPUObjectStore"] = None

        # Thread safe queue of transport refs that the monitor thread needs to start monitoring
        self._unmonitored_transfers: Queue[TransferMetadata] = Queue()
        # Background thread to poll on the transfer operation.
        self._monitor_failures_thread = None
        # Event to signal the monitor_failures thread to shutdown
        self._monitor_failures_shutdown_event = threading.Event()

        # If the actor isn't in the dict, the task to launch the custom transport registration task hasn't been submitted yet.
        # If the value is an object ref, we have to wait for the registration task to complete.
        # If the value is True, the actor has registered any custom transports.
        # The value should never be False.
        # TODO: This is a short-term solution. In the future, we'll do registration with actor initialization
        # to make actor restarts and submitting from another worker work.
        self.actor_id_to_transports_registered: Dict[str, Union[ObjectRef, bool]] = {}

    def register_custom_transports_on_actor(self, actor: "ray.actor.ActorHandle"):
        from ray.experimental.gpu_object_manager.util import (
            register_custom_tensor_transports_on_actor,
        )

        ref = register_custom_tensor_transports_on_actor(actor)
        # ref is None if there are no custom transports registered.
        self.actor_id_to_transports_registered[actor._actor_id] = (
            True if ref is None else ref
        )

    def wait_until_custom_transports_registered(self, actor: "ray.actor.ActorHandle"):
        actor_id = actor._actor_id
        if actor_id not in self.actor_id_to_transports_registered:
            self.register_custom_transports_on_actor(actor)

        if self.actor_id_to_transports_registered[actor_id] is not True:
            ray.get(self.actor_id_to_transports_registered[actor_id])
            self.actor_id_to_transports_registered[actor_id] = True

    @property
    def gpu_object_store(self) -> "ray.experimental.GPUObjectStore":
        with self._init_lock:
            if self._gpu_object_store is None:
                from ray.experimental.gpu_object_manager.gpu_object_store import (
                    GPUObjectStore,
                )

                self._gpu_object_store = GPUObjectStore()
            return self._gpu_object_store

    def shutdown(self):
        """
        Interrupt and join the _monitor_failures_thread
        """
        with self._init_lock:
            if self._monitor_failures_thread:
                self._monitor_failures_shutdown_event.set()
                self._monitor_failures_thread.join()
                self._monitor_failures_shutdown_event.clear()
                self._monitor_failures_thread = None

    def start_monitor_thread_if_needed(self):
        with self._init_lock:
            # To make sure _monitor_failures_thread is started only once
            if self._monitor_failures_thread is None:
                self._monitor_failures_thread = threading.Thread(
                    target=self._monitor_failures, daemon=True
                )
                self._monitor_failures_thread.start()

    def is_managed_object(self, obj_id: str) -> bool:
        """
        Check if the GPU object is owned or borrowed by this process.
        """
        with self._lock:
            return obj_id in self._managed_gpu_object_metadata

    def set_gpu_object_metadata(self, obj_id: str, gpu_object_meta: GPUObjectMeta):
        with self._lock:
            self._managed_gpu_object_metadata[obj_id] = gpu_object_meta

    def get_gpu_object_metadata(self, obj_id: str) -> Optional[GPUObjectMeta]:
        with self._lock:
            return self._managed_gpu_object_metadata.get(obj_id, None)

    def wait_for_tensor_transport_metadata(
        self, obj_id: str, timeout: float
    ) -> Optional["TensorTransportMetadata"]:
        with self._tensor_transport_meta_cv:
            if self._tensor_transport_meta_cv.wait_for(
                lambda: self._managed_gpu_object_metadata[obj_id].tensor_transport_meta
                is not None,
                timeout=timeout,
            ):
                return self._managed_gpu_object_metadata[obj_id].tensor_transport_meta
            else:
                return None

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
                            f"RDT transfer failed after {ray_constants.RDT_FETCH_FAIL_TIMEOUT_SECONDS}s. "
                            "You can increase the timeout by setting RAY_rdt_fetch_fail_timeout_milliseconds"
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
        from ray.experimental.gpu_object_manager.collective_tensor_transport import (
            CollectiveCommunicatorMetadata,
        )
        from ray.experimental.gpu_object_manager.gpu_object_store import (
            __ray_abort_transport__,
        )
        from ray.experimental.gpu_object_manager.util import (
            get_tensor_transport_manager,
        )

        ref_info = ref_info_map.pop(failed_ref.hex(), None)
        if ref_info is None:
            return

        if ref_info.send_ref:
            ref_info_map.pop(ref_info.send_ref.hex(), None)
        ref_info_map.pop(ref_info.recv_ref.hex(), None)

        tensor_transport_manager = get_tensor_transport_manager(ref_info.backend)
        if tensor_transport_manager.can_abort_transport():
            if not tensor_transport_manager.__class__.is_one_sided():
                # This is dead code until we implement a NCCL abort since NIXL
                # is the only abortable transport for now and is one-sided.
                ref_info.src_actor.__ray_call__.options(
                    concurrency_group="_ray_system_error"
                ).remote(
                    __ray_abort_transport__,
                    ref_info.obj_id,
                    ref_info.communicator_meta,
                    ref_info.backend,
                )
            ref_info.dst_actor.__ray_call__.options(
                concurrency_group="_ray_system_error"
            ).remote(
                __ray_abort_transport__,
                ref_info.obj_id,
                ref_info.communicator_meta,
                ref_info.backend,
            )
            logger.info(
                "RDT transfer with src actor %s and dst actor %s failed due to %s.",
                ref_info.src_actor,
                ref_info.dst_actor,
                exception,
            )
        else:
            # TODO(#51276): Kill all actors in the collective group when we support more collective operations
            ray.kill(ref_info.src_actor)
            ray.kill(ref_info.dst_actor)
            logger.error(
                "RDT transfer with src actor %s and dst actor %s failed. Killing the actors. "
                "Transfer failed with exception: %s",
                ref_info.src_actor,
                ref_info.dst_actor,
                exception,
            )

        # isinstance does an implicit cast and makes communicator_name inaccessible
        # so we have to get communicator_name before the cast.
        if isinstance(ref_info.communicator_meta, CollectiveCommunicatorMetadata):
            try:
                collective_group_name = ref_info.communicator_meta.communicator_name
                destroy_collective_group(collective_group_name)
                logger.error(
                    "Destroyed collective group %s due to a hanging/failed RDT transfer",
                    collective_group_name,
                )
            except ValueError:
                # Collective group was already destroyed
                pass

    def add_gpu_object_ref(
        self,
        obj_ref: ObjectRef,
        src_actor: "ray.actor.ActorHandle",
        tensor_transport: str,
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
                This is known at ref creation time if the object is created through ray.put.
        """
        self.set_gpu_object_metadata(
            obj_ref.hex(),
            GPUObjectMeta(
                src_actor=src_actor,
                tensor_transport_backend=tensor_transport,
                tensor_transport_meta=tensor_transport_meta,  # None if not from ray.put
                sent_dest_actors=set(),
                sent_to_src_actor_and_others_warned=False,
            ),
        )

    def set_tensor_transport_metadata_and_trigger_queued_operations(
        self, obj_id: str, tensor_transport_meta: "TensorTransportMetadata"
    ):
        """
        Sets the tensor transport metadata for an object and triggers any queued
        up transfers or frees for that object.
        """
        dst_actors = None
        free_object = False
        with self._tensor_transport_meta_cv:
            self._managed_gpu_object_metadata[
                obj_id
            ] = self._managed_gpu_object_metadata[obj_id]._replace(
                tensor_transport_meta=tensor_transport_meta
            )
            dst_actors = self._queued_transfers.pop(obj_id, None)
            free_object = obj_id in self._queued_frees
            if free_object:
                self._queued_frees.remove(obj_id)
                # There shouldn't be any transfers queued if the free was queued,
                # since we clear the queued transfers when queueing the free.
                assert dst_actors is None
            self._tensor_transport_meta_cv.notify_all()

        if free_object:
            self.free_object_primary_copy(obj_id)
        if dst_actors:
            for dst_actor in dst_actors:
                # Trigger the transfer now that the metadata is available.
                self.trigger_out_of_band_tensor_transfer(dst_actor, obj_id)

    def _fetch_object(
        self,
        obj_id: str,
        use_object_store: bool,
    ):
        """
        Fetches the GPU object from the source actor's GPU object store via the object store
        instead of out-of-band tensor transfer and stores the tensors in the local GPU object store.

        This is useful when the current process does not support the designated out-of-band tensor transport.
        For example, if the tensor transport is NCCL but the driver does not have a GPU, we use this call to
        fulfill a `ray.get` call.

        Args:
            obj_id: The object ID of the RDT object.
            use_object_store: Whether to fetch the RDT object through the
                object store or through its designated tensor transport.

        Returns:
            None
        """
        from ray.experimental.gpu_object_manager.gpu_object_store import (
            __ray_fetch_gpu_object__,
        )
        from ray.experimental.gpu_object_manager.util import (
            get_tensor_transport_manager,
            validate_one_sided,
        )

        if self.gpu_object_store.has_object(obj_id):
            return

        gpu_object_meta = self.get_gpu_object_metadata(obj_id)
        assert gpu_object_meta is not None

        if use_object_store:
            src_actor = gpu_object_meta.src_actor
            tensors = ray.get(
                src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                    __ray_fetch_gpu_object__, obj_id
                )
            )
            self.gpu_object_store.add_object(obj_id, tensors)
        else:
            from ray.experimental.gpu_object_manager.gpu_object_store import (
                __ray_recv__,
            )

            tensor_transport = gpu_object_meta.tensor_transport_backend
            validate_one_sided(tensor_transport, "ray.get")
            tensor_transport_manager = get_tensor_transport_manager(tensor_transport)
            communicator_meta = tensor_transport_manager.get_communicator_metadata(
                None, None, tensor_transport
            )
            tensor_transport_meta = gpu_object_meta.tensor_transport_meta
            if tensor_transport_meta is None:
                # We can't fetch the object until we know the creator has actually created the object.
                timeout = ray_constants.RDT_FETCH_FAIL_TIMEOUT_SECONDS
                tensor_transport_meta = self.wait_for_tensor_transport_metadata(
                    obj_id, timeout
                )
                if tensor_transport_meta is None:
                    raise TimeoutError(
                        f"Timed out after {timeout}s waiting for object {obj_id} to be created while trying to get the object. "
                        "You can increase the timeout by setting RAY_rdt_fetch_fail_timeout_milliseconds."
                    )
            __ray_recv__(
                None,
                obj_id,
                tensor_transport_meta,
                communicator_meta,
                tensor_transport,
            )

    def queue_or_trigger_out_of_band_tensor_transfer(
        self, dst_actor: "ray.actor.ActorHandle", task_args: Tuple[Any, ...]
    ):
        """
        Triggers the transfer if the tensor metadata is available for the object. If it's
        not available, the transfer is queued up until the metadata is available.
        """
        gpu_object_ids: Set[str] = set()
        for arg in task_args:
            # If an ObjectRef is managed, it means the actual value is a list of tensors stored
            # on a remote actor. Therefore, this function will trigger a tensor communication
            # operation between the sender and receiver actors.
            if not isinstance(arg, ObjectRef):
                continue
            obj_id = arg.hex()
            if self.is_managed_object(obj_id):
                gpu_object_ids.add(obj_id)
        if gpu_object_ids:
            self.wait_until_custom_transports_registered(dst_actor)
            for obj_id in gpu_object_ids:
                # Atomically gets the tensor transport metadata for an object and queues up a transfer
                # if the tensor transport metadata is not available.
                with self._lock:
                    tensor_transport_meta = self._managed_gpu_object_metadata[
                        obj_id
                    ].tensor_transport_meta
                    if tensor_transport_meta is None:
                        self._queued_transfers[obj_id].append(dst_actor)
                if tensor_transport_meta is not None:
                    self.trigger_out_of_band_tensor_transfer(dst_actor, obj_id)

    def trigger_out_of_band_tensor_transfer(
        self, dst_actor: "ray.actor.ActorHandle", obj_id: str
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
            obj_id: ID of the object to send to the dst_actor.

        Returns:
            None
        """
        from ray.experimental.gpu_object_manager.gpu_object_store import (
            __ray_recv__,
            __ray_send__,
        )
        from ray.experimental.gpu_object_manager.util import (
            get_tensor_transport_manager,
        )

        with self._lock:
            # Since sent_dest_actors is mutable, this whole block needs to be protected.
            gpu_object_meta = self._managed_gpu_object_metadata[obj_id]
            src_actor = gpu_object_meta.src_actor
            tensor_transport_meta = gpu_object_meta.tensor_transport_meta

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
                    "Note that GPU objects are mutable. If the tensor is modified, Ray's internal copy will "
                    "also be updated, and subsequent passes to other actors will receive the updated version "
                    "instead of the original.",
                    UserWarning,
                )
                # Mark the object as warned so that we don't warn again for this object.
                self._managed_gpu_object_metadata[obj_id] = gpu_object_meta._replace(
                    sent_to_src_actor_and_others_warned=True
                )

        if src_actor._actor_id == dst_actor._actor_id:
            # If the source and destination actors are the same, the tensors can
            # be transferred intra-process, so we skip the out-of-band tensor
            # transfer.
            return

        tensor_transport_manager = get_tensor_transport_manager(
            gpu_object_meta.tensor_transport_backend
        )
        communicator_meta = tensor_transport_manager.get_communicator_metadata(
            src_actor,
            dst_actor,
            gpu_object_meta.tensor_transport_backend,
        )

        send_ref = None
        if not tensor_transport_manager.__class__.is_one_sided():
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
                gpu_object_meta.tensor_transport_backend,
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
            gpu_object_meta.tensor_transport_backend,
        )

        self._unmonitored_transfers.put(
            TransferMetadata(
                src_actor=src_actor,
                dst_actor=dst_actor,
                send_ref=send_ref,
                recv_ref=recv_ref,
                communicator_meta=communicator_meta,
                backend=gpu_object_meta.tensor_transport_backend,
                obj_id=obj_id,
                timeout=time.time() + ray_constants.RDT_FETCH_FAIL_TIMEOUT_SECONDS,
            )
        )
        self.start_monitor_thread_if_needed()

    def get_gpu_object(
        self,
        object_id: str,
        use_object_store: bool = False,
    ) -> List["torch.Tensor"]:
        """
        Get the RDT object for a given object ID.

        Args:
            object_id: The object ID of the RDT object.
            use_object_store: Whether to fetch the RDT object
                through the object store or through its designated tensor transport.

        Returns:
            The RDT object.
        """
        gpu_object_store = self.gpu_object_store
        if self.is_managed_object(object_id):
            self._fetch_object(object_id, use_object_store)

        # If the GPU object is the primary copy, it means the transfer is intra-actor.
        # In this case, we should not remove the GPU object after it is consumed once,
        # because the GPU object reference may be used again.
        # Instead, we should wait for the GC callback to clean it up.
        pop_object = not gpu_object_store.is_primary_copy(object_id)
        if pop_object:
            gpu_object = gpu_object_store.wait_and_pop_object(
                object_id, timeout=ray_constants.RDT_FETCH_FAIL_TIMEOUT_SECONDS
            )
        else:
            gpu_object = gpu_object_store.wait_and_get_object(
                object_id, timeout=ray_constants.RDT_FETCH_FAIL_TIMEOUT_SECONDS
            )
        return gpu_object

    def queue_or_free_object_primary_copy(self, object_id: str):
        """
        Free the RDT object on the primary copy holder and free metadata
        if the tensor metadata is available (the object has been created).
        Otherwise, queue up the free operation until the tensor metadata is available.
        """
        # NOTE: This may have to change if we support lineage reconstruction for RDT
        # TODO(#57962): Metadata is currently not removed on borrowers that borrow through
        # the NIXL ray.put / ray.get
        with self._lock:
            self._queued_transfers.pop(object_id, None)
            gpu_object_meta = self._managed_gpu_object_metadata[object_id]
            tensor_transport_meta = gpu_object_meta.tensor_transport_meta
            if tensor_transport_meta is None:
                # The object hasn't been created at the time of the free.
                self._queued_frees.add(object_id)

        if tensor_transport_meta is not None:
            self.free_object_primary_copy(object_id)

    def free_object_primary_copy(self, object_id: str):
        from ray.experimental.gpu_object_manager.gpu_object_store import (
            __ray_free__,
        )

        with self._lock:
            gpu_object_meta = self._managed_gpu_object_metadata.pop(object_id)
        src_actor = gpu_object_meta.src_actor
        tensor_transport_backend = gpu_object_meta.tensor_transport_backend
        tensor_transport_meta = gpu_object_meta.tensor_transport_meta
        src_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
            __ray_free__,
            object_id,
            tensor_transport_backend,
            tensor_transport_meta,
        )

    @staticmethod
    def actor_has_tensor_transport(
        actor: "ray.actor.ActorHandle", tensor_transport: str
    ):
        """
        Check if the actor has a communicator for the given tensor transport backend.

        Args:
            actor: The actor to check.
            tensor_transport: The tensor transport backend to check.

        Returns:
            True if the actor has a communicator for the given tensor transport backend, False otherwise.
        """
        from ray.experimental.gpu_object_manager.util import (
            get_tensor_transport_manager,
        )

        tensor_transport_manager = get_tensor_transport_manager(tensor_transport)
        return tensor_transport_manager.actor_has_tensor_transport(actor)

    def put_object(
        self,
        obj_ref: ObjectRef,
        tensor_transport: str,
        tensors: List["torch.Tensor"],
    ):
        """
        Put the GPU object into the GPU object manager.

        Args:
            obj_ref: The object ref of the GPU object.
            tensor_transport: The tensor transport backend to use.
            tensors: The tensors to put into the GPU object manager.
        """
        src_actor = ray.get_runtime_context().current_actor
        tensor_transport_meta = self.gpu_object_store.add_object_primary(
            obj_ref.hex(), tensors, tensor_transport
        )
        self.add_gpu_object_ref(
            obj_ref,
            src_actor,
            tensor_transport,
            tensor_transport_meta=tensor_transport_meta,
        )
