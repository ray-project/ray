import threading
import time
import traceback
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray._private.ray_constants import NIXL_REMOTE_AGENT_CACHE_MAXSIZE
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    FetchRequest,
    TensorTransportManager,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import torch


@dataclass
class NixlCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the NIXL communicator."""


@dataclass
class NixlTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors stored in the GPU object store for NIXL transport.

    Args:
        nixl_serialized_descs: Serialized tensor descriptors for NIXL transport.
        nixl_agent_meta: The additional metadata of the remote NIXL agent.
        nixl_agent_name: The name of the NIXL agent.
        nixl_agent_meta_version: The version of the NIXL agent metadata.
    """

    nixl_serialized_descs: Optional[bytes] = None
    nixl_agent_meta: Optional[bytes] = None
    nixl_agent_name: Optional[str] = None
    nixl_agent_meta_version: Optional[int] = 0

    __eq__ = object.__eq__
    __hash__ = object.__hash__


@dataclass
class TensorDesc:
    reg_desc: Any  # nixlRegDList
    metadata_count: int  # tracks the number of NIXL metadata containing the tensor



@dataclass
class NixlFetchRequest(FetchRequest):
    """NIXL-specific FetchRequest carrying the async transfer state.

    Returned by fetch_multiple_tensors and consumed by wait_fetch_complete.

    Args:
        obj_id: The object ID for the transfer, used for abort checks and cleanup.
        tensors: Pre-allocated output tensors (populated before the transfer starts).
        xfer_handle: NIXL transfer request handle.
        nixl_agent: Reference to the NIXL agent.
        remote_name: Name of the remote NIXL agent.
        remove_tensor_descs: Whether to remove tensor descriptors from the cache during cleanup.
    """

    xfer_handle: Any = None
    nixl_agent: Any = None
    remote_name: Optional[str] = None
    remove_tensor_descs: bool = False
    transport: Any = None

    def __del__(self):
        if self.transport is not None:
            self.transport._cleanup_transfer(
                self.obj_id,
                self.tensors,
                self.xfer_handle,
                self.remote_name,
                self.remove_tensor_descs,
            )


class NixlTensorTransport(TensorTransportManager):
    def __init__(self):
        # This is lazily initialized because it requires NIXL to actually be installed and we want to allow an owner that is just coordinating to not need to have NIXL installed.
        self._nixl_agent = None
        self._aborted_transfer_obj_ids = set()
        self._aborted_transfer_obj_ids_lock = threading.Lock()
        # Mapping from tensor storage data pointer to the NIXL descriptor and reference count.
        # Unlike _managed_meta_nixl, we only deregister tensors when ALL metadata containing the tensor is freed.
        self._tensor_desc_cache: Dict[int, TensorDesc] = {}
        # Mapping from object ID to the NIXL managed meta.
        # The lifetime of _managed_meta_nixl is tied to the object ref and freed when the ref goes out of scope.
        self._managed_meta_nixl: Dict[str, Any] = {}
        # Lock protecting _tensor_desc_cache and _managed_meta_nixl since they can be
        # accessed from the main task execution thread or the _ray_system thread.
        self._cache_lock = threading.RLock()
        # LRU cache of remote agent names. When full, the least
        # recently used remote agent is evicted and remove_remote_agent is called.
        self._remote_agents: OrderedDict = OrderedDict()
        # Increment the version whenever memory is deregistered.
        self._nixl_agent_meta_version = 0

    def tensor_transport_backend(self) -> str:
        return "NIXL"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return True

    def register_nixl_memory(self, tensor: "torch.Tensor") -> None:
        """Registers the tensor's memory with NIXL and bumps the reference count so the memory region is never deregistered."""
        self._add_tensor_descs([tensor])

    def get_nixl_agent(self):
        """
        Creates a NIXL agent with UCX backend if not already created.
        """
        if self._nixl_agent is not None:
            return self._nixl_agent

        from nixl._api import nixl_agent, nixl_agent_config

        agent_config = nixl_agent_config(backends=["UCX"])
        ctx = ray.get_runtime_context()
        actor_id = ctx.get_actor_id()
        if actor_id is None:
            # If the actor id is None, it means the current process is a driver.
            import uuid

            actor_id = f"RAY-DRIVER-{uuid.uuid4()}"
        self._nixl_agent = nixl_agent(actor_id, agent_config)
        return self._nixl_agent

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        # TODO(dayshah): This is called on a .remote RDT call, so it's quite expensive.
        def __ray_actor_has_tensor_transport__(
            self: "ray.actor.ActorHandle",
        ) -> bool:
            # Check if nixl is installed
            try:
                from ray.experimental.rdt.util import (
                    get_tensor_transport_manager,
                )

                get_tensor_transport_manager("NIXL").get_nixl_agent()
                return True
            except Exception:
                return False

        return ray.get(
            actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                __ray_actor_has_tensor_transport__
            )
        )

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List["torch.Tensor"],
    ) -> NixlTransportMetadata:
        import torch

        with self._cache_lock:
            device = None
            tensor_meta = []

            if rdt_object:
                # We assume all tensors in one RDT object have the same device type,
                # but we don't assume they're all on the same device.
                devices = set()
                device = rdt_object[0].device
                for t in rdt_object:
                    if t.device.type != device.type:
                        raise ValueError(
                            "All tensors in an RDT object must have the same device type."
                        )
                    if not t.is_contiguous():
                        raise ValueError(
                            "All tensors in an RDT object must be contiguous."
                        )
                    tensor_meta.append((t.shape, t.dtype))
                    devices.add(t.device)
                if device.type == "cuda":
                    # We have to synchronize before memory registration to assure the
                    # object has been created because nixl doesn't guarantee it will.
                    for dev in devices:
                        torch.cuda.synchronize(dev)

                nixl_agent = self.get_nixl_agent()
                self._add_tensor_descs(rdt_object)
                xfer_descs = nixl_agent.get_xfer_descs(rdt_object)
                serialized_descs = nixl_agent.get_serialized_descs(xfer_descs)
                agent_meta = nixl_agent.get_agent_metadata()
                agent_name = nixl_agent.name
                agent_meta_version = self._nixl_agent_meta_version
            else:
                serialized_descs, agent_meta = None, None
                agent_name, agent_meta_version = None, None

            ret = NixlTransportMetadata(
                tensor_meta=tensor_meta,
                tensor_device=device.type if device else None,
                nixl_serialized_descs=serialized_descs,
                nixl_agent_meta=agent_meta,
                nixl_agent_name=agent_name,
                nixl_agent_meta_version=agent_meta_version,
            )
            self._put_meta(obj_id, ret)
            return ret

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> NixlCommunicatorMetadata:
        return NixlCommunicatorMetadata()

    def fetch_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> NixlFetchRequest:
        """Initiates an async transfer for multiple tensors.

        This triggers the transfer but does not wait for completion.
        Call wait_fetch_complete(fetch_request) to wait for the transfer to
        finish and retrieve the tensors.

        Args:
            obj_id: The object ID for the transfer.
            tensor_transport_metadata: Metadata for the tensor transport.
            communicator_metadata: Metadata for the communicator.
            target_buffers: Optional pre-allocated buffers to receive tensors into.

        Returns:
            A NixlFetchRequest carrying the async transfer state.
        """
        from ray.experimental.rdt.util import (
            create_empty_tensors_from_metadata,
        )

        tensors = target_buffers or create_empty_tensors_from_metadata(
            tensor_transport_metadata
        )

        assert isinstance(tensor_transport_metadata, NixlTransportMetadata)
        assert isinstance(communicator_metadata, NixlCommunicatorMetadata)

        nixl_serialized_descs = tensor_transport_metadata.nixl_serialized_descs
        remote_nixl_agent_meta = tensor_transport_metadata.nixl_agent_meta

        with self._aborted_transfer_obj_ids_lock:
            if obj_id in self._aborted_transfer_obj_ids:
                self._aborted_transfer_obj_ids.remove(obj_id)
                raise RuntimeError(f"NIXL transfer aborted for object id: {obj_id}")

        remote_name = None
        xfer_handle = None
        added_tensor_descs = False

        if not tensors:
            return NixlFetchRequest(tensors=tensors, obj_id=obj_id)

        try:
            nixl_agent = self.get_nixl_agent()
            remote_xfer_descs = nixl_agent.deserialize_descs(nixl_serialized_descs)
            # This creates a placeholder for the tensor in the tensor_desc_cache even though it doesn't have an object ref for caching purposes.
            self._add_tensor_descs(tensors)
            added_tensor_descs = True
            local_xfer_descs = nixl_agent.get_xfer_descs(tensors)

            remote_name = tensor_transport_metadata.nixl_agent_name
            remote_agent_meta_version = (
                tensor_transport_metadata.nixl_agent_meta_version
            )

            # Nixl agent reuse is enabled.
            if NIXL_REMOTE_AGENT_CACHE_MAXSIZE > 0:
                if remote_name in self._remote_agents:
                    # If the remote agent metadata version is different from the cached one,
                    # it means there was memory deregistered. We need to remove the remote agent
                    # before adding it, because `nixlRemoteSection` currently does not support
                    # updating descriptor list in such a case (there is potential memory overlap).
                    if remote_agent_meta_version != self._remote_agents[remote_name]:
                        nixl_agent.remove_remote_agent(remote_name)
                    self._remote_agents.move_to_end(remote_name)
                elif len(self._remote_agents) >= NIXL_REMOTE_AGENT_CACHE_MAXSIZE:
                    evicted_agent_name, _ = self._remote_agents.popitem(last=False)
                    nixl_agent.remove_remote_agent(evicted_agent_name)

                self._remote_agents[remote_name] = remote_agent_meta_version

            nixl_agent.add_remote_agent(remote_nixl_agent_meta)

            xfer_handle = nixl_agent.initialize_xfer(
                "READ",
                local_xfer_descs,
                remote_xfer_descs,
                remote_name,
                # TODO: Use str(num_transfers).encode() as the UUID.
                "UUID",
            )

            state = nixl_agent.transfer(xfer_handle)
            if state == "ERR":
                raise RuntimeError("NIXL transfer got to Error state.")

            return NixlFetchRequest(
                tensors=tensors,
                obj_id=obj_id,
                xfer_handle=xfer_handle,
                nixl_agent=nixl_agent,
                remote_name=remote_name,
                remove_tensor_descs=added_tensor_descs,
                transport=self,
            )
        except Exception:
            self._cleanup_transfer(
                obj_id, tensors, xfer_handle, remote_name, added_tensor_descs
            )
            from ray.exceptions import RayDirectTransportError

            raise RayDirectTransportError(
                f"The NIXL transfer failed for object id: {obj_id}. The source actor may have died during the transfer. "
                f"The exception thrown from nixl transfer was:\n {traceback.format_exc()}"
            ) from None

    def wait_fetch_complete(
        self, fetch_request: FetchRequest, timeout: float = -1
    ) -> List["torch.Tensor"]:
        """Waits for a previously initiated fetch to complete and returns the tensors.

        Args:
            fetch_request: The NixlFetchRequest returned by fetch_multiple_tensors.
            timeout: Maximum time in seconds to wait. -1 means wait indefinitely.
                0 means return immediately if not ready.

        Returns:
            List of tensors that were transferred.

        Raises:
            RayDirectTransportError: If the transfer failed.
            TimeoutError: If the timeout is exceeded.
        """
        assert isinstance(fetch_request, NixlFetchRequest)
        obj_id = fetch_request.obj_id

        if not fetch_request.tensors:
            return fetch_request.tensors

        try:
            # Check the state of the transfer continuously.
            deadline = None if timeout < 0 else time.monotonic() + timeout
            while True:
                state = fetch_request.nixl_agent.check_xfer_state(
                    fetch_request.xfer_handle
                )
                if state == "ERR":
                    raise RuntimeError("NIXL transfer got to Error state.")
                if state == "PROC":
                    if deadline is not None and time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"NIXL transfer timed out after {timeout}s for object id: {obj_id}"
                        )
                    with self._aborted_transfer_obj_ids_lock:
                        if obj_id in self._aborted_transfer_obj_ids:
                            self._aborted_transfer_obj_ids.remove(obj_id)
                            raise RuntimeError(
                                f"NIXL transfer aborted for object id: {obj_id}"
                            )
                    time.sleep(0.001)  # Avoid busy waiting
                elif state == "DONE":
                    break

            return fetch_request.tensors
        except TimeoutError:
            raise
        except Exception:
            from ray.exceptions import RayDirectTransportError

            raise RayDirectTransportError(
                f"The NIXL transfer failed for object id: {obj_id}. The source actor may have died during the transfer. "
                f"The exception thrown from nixl transfer was:\n {traceback.format_exc()}"
            ) from None

    def _cleanup_transfer(
        self,
        obj_id: str,
        tensors: List["torch.Tensor"],
        xfer_handle: Any,
        remote_name: Optional[str],
        remove_tensor_descs: bool,
    ) -> None:
        """Cleans up resources after a transfer completes or fails."""
        # We could raise errors or NIXL could raise errors like NIXL_ERR_REMOTE_DISCONNECT,
        # so doing best effort cleanup.
        nixl_agent = self._nixl_agent
        if nixl_agent is None:
            return

        with self._aborted_transfer_obj_ids_lock:
            self._aborted_transfer_obj_ids.discard(obj_id)
        if xfer_handle:
            nixl_agent.release_xfer_handle(xfer_handle)
        if NIXL_REMOTE_AGENT_CACHE_MAXSIZE == 0 and remote_name:
            nixl_agent.remove_remote_agent(remote_name)
        if remove_tensor_descs:
            with self._cache_lock:
                for tensor in tensors:
                    key = tensor.untyped_storage().data_ptr()
                    tensor_desc = self._tensor_desc_cache.get(key)
                    if tensor_desc is None:
                        continue
                    tensor_desc.metadata_count -= 1

                    if tensor_desc.metadata_count == 0:
                        nixl_agent.deregister_memory(tensor_desc.reg_desc)
                        self._tensor_desc_cache.pop(key)
                        self._nixl_agent_meta_version += 1

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> List["torch.Tensor"]:
        """Receives multiple tensors synchronously."""
        fetch_request = self.fetch_multiple_tensors(
            obj_id, tensor_transport_metadata, communicator_metadata, target_buffers
        )
        return self.wait_fetch_complete(fetch_request)

    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        raise NotImplementedError(
            "NIXL transport does not support send_multiple_tensors, since it is a one-sided transport."
        )

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List["torch.Tensor"],
    ):
        with self._cache_lock:
            assert isinstance(tensor_transport_meta, NixlTransportMetadata)
            if obj_id not in self._managed_meta_nixl:
                return
            self._managed_meta_nixl.pop(obj_id, None)
            for tensor in tensors:
                key = tensor.untyped_storage().data_ptr()
                if key in self._tensor_desc_cache:
                    tensor_desc = self._tensor_desc_cache[key]
                    tensor_desc.metadata_count -= 1
                    if tensor_desc.metadata_count == 0:
                        self._tensor_desc_cache.pop(key)
                        self.get_nixl_agent().deregister_memory(tensor_desc.reg_desc)
                        self._nixl_agent_meta_version += 1

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        with self._aborted_transfer_obj_ids_lock:
            self._aborted_transfer_obj_ids.add(obj_id)

    def _get_num_managed_meta_nixl(self) -> int:
        with self._cache_lock:
            return len(self._managed_meta_nixl)

    def _get_meta(self, object_id: str) -> Optional[NixlTransportMetadata]:
        """
        Get the NIXL transport metadata for the given object ID if it exists
        """
        with self._cache_lock:
            if object_id in self._managed_meta_nixl:
                return self._managed_meta_nixl[object_id]
            return None

    def _put_meta(self, object_id: str, meta: NixlTransportMetadata):
        """
        Store the NIXL transport metadata for the given object ID
        """
        with self._cache_lock:
            self._managed_meta_nixl[object_id] = meta

    def _add_tensor_descs(self, tensors: List["torch.Tensor"]):
        """
        If this is the first time the tensor is being registered, we register the
        full underlying pytorch storage object with NIXL. Otherwise, we increment the reference count.
        """
        with self._cache_lock:
            for tensor in tensors:
                key = tensor.untyped_storage().data_ptr()
                if key in self._tensor_desc_cache:
                    self._tensor_desc_cache[key].metadata_count += 1
                else:
                    mem_type = "cuda" if tensor.is_cuda else "cpu"
                    # the GPU ID of the device the tensor is on.
                    # NOTE: we clip this to 0 since the GPU ID is not used for CPU tensors, and get_device returns -1 for CPU tensors.
                    # This triggers an error in nixl since it expects an unsigned.
                    gpu_id = max(tensor.get_device(), 0)
                    # Registering the full underlying pytorch storage object by constructing a memory region
                    # with the data pointer, size, GPU ID, and meta info. Doing the equivalent of what nixl does for pytorch tensors
                    # internally: https://github.com/ai-dynamo/nixl/blob/dd23ef01bd366aef89fa552f2b042f89a0b45fcb/src/api/python/_api.py#L1034
                    reg_desc = self.get_nixl_agent().register_memory(
                        [
                            (
                                tensor.untyped_storage().data_ptr(),
                                tensor.untyped_storage().nbytes(),
                                gpu_id,
                                "",
                            )
                        ],
                        mem_type=mem_type,
                    )
                    self._tensor_desc_cache[key] = TensorDesc(reg_desc, 1)
