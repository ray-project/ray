import threading
import time
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray.experimental.gpu_object_manager.tensor_transport_manager import (
    CommunicatorMetadata,
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
    """

    nixl_serialized_descs: Optional[bytes] = None
    nixl_agent_meta: Optional[bytes] = None

    __eq__ = object.__eq__
    __hash__ = object.__hash__


@dataclass
class TensorDesc:
    reg_desc: Any  # nixlRegDList
    metadata_count: int  # tracks the number of NIXL metadata containing the tensor
    nbytes: int  # the number of bytes in the tensor


class NixlTensorTransport(TensorTransportManager):
    def __init__(self):
        # This is lazily initialized because it requires NIXL to actually be installed and we want to allow an owner that is just coordinating to not need to have NIXL installed.
        self._nixl_agent = None
        self._aborted_transfer_obj_ids = set()
        self._aborted_transfer_obj_ids_lock = threading.Lock()
        # Mapping from tensor data pointer to the NIXL descriptor, reference count, and nbytes.
        # Unlike _managed_meta_nixl, we only deregister tensors when ALL metadata containing the tensor is freed.
        self._tensor_desc_cache: Dict[int, TensorDesc] = {}
        # Mapping from object ID to the NIXL managed meta.
        # The lifetime of _managed_meta_nixl is tied to the object ref and freed when the ref goes out of scope.
        self._managed_meta_nixl: Dict[str, Any] = {}
        # Lock protecting _tensor_desc_cache and _managed_meta_nixl since they can be
        # accessed from the main task execution thread or the _ray_system thread.
        self._cache_lock = threading.Lock()

    def tensor_transport_backend(self) -> str:
        return "NIXL"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return True

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
                from ray.experimental.gpu_object_manager.util import (
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
        gpu_object: List["torch.Tensor"],
    ) -> NixlTransportMetadata:
        import torch

        with self._cache_lock:
            device = None
            tensor_meta = []
            duplicate_meta = self._record_and_get_meta_if_duplicate(obj_id, gpu_object)
            if duplicate_meta is not None:
                return duplicate_meta

            if gpu_object:
                # We assume all tensors in one GPU object have the same device type,
                # but we don't assume they're all on the same device.
                devices = set()
                device = gpu_object[0].device
                for t in gpu_object:
                    if t.device.type != device.type:
                        raise ValueError(
                            "All tensors in an RDT object must have the same device type."
                        )
                    if not t.is_contiguous():
                        raise ValueError(
                            "All tensors in an RDT object must be contiguous."
                        )
                    if (
                        t.data_ptr() in self._tensor_desc_cache
                        and self._tensor_desc_cache[t.data_ptr()].nbytes != t.nbytes
                    ):
                        raise ValueError(
                            "Tensors in an RDT object can not be partially overlapping with already registered tensors"
                        )
                    tensor_meta.append((t.shape, t.dtype))
                    devices.add(t.device)
                if device.type == "cuda":
                    # We have to synchronize before memory registration to assure the
                    # object has been created because nixl doesn't guarantee it will.
                    for dev in devices:
                        torch.cuda.synchronize(dev)

                nixl_agent = self.get_nixl_agent()
                self._add_tensor_descs(gpu_object)
                xfer_descs = nixl_agent.get_xfer_descs(gpu_object)
                serialized_descs = nixl_agent.get_serialized_descs(xfer_descs)
                agent_meta = nixl_agent.get_agent_metadata()
            else:
                serialized_descs, agent_meta = None, None

            ret = NixlTransportMetadata(
                tensor_meta=tensor_meta,
                tensor_device=device,
                nixl_serialized_descs=serialized_descs,
                nixl_agent_meta=agent_meta,
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

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ) -> List["torch.Tensor"]:
        from ray.experimental.gpu_object_manager.util import (
            create_empty_tensors_from_metadata,
        )

        tensors = create_empty_tensors_from_metadata(tensor_transport_metadata)

        assert isinstance(tensor_transport_metadata, NixlTransportMetadata)
        assert isinstance(communicator_metadata, NixlCommunicatorMetadata)

        nixl_serialized_descs = tensor_transport_metadata.nixl_serialized_descs
        remote_nixl_agent_meta = tensor_transport_metadata.nixl_agent_meta

        with self._aborted_transfer_obj_ids_lock:
            if obj_id in self._aborted_transfer_obj_ids:
                self._aborted_transfer_obj_ids.remove(obj_id)
                raise RuntimeError(f"NIXL transfer aborted for object id: {obj_id}")

        if not tensors:
            return []

        local_descs = None
        remote_name = None
        xfer_handle = None
        try:
            nixl_agent = self.get_nixl_agent()
            remote_descs = nixl_agent.deserialize_descs(nixl_serialized_descs)
            local_descs = nixl_agent.register_memory(tensors)
            remote_name = nixl_agent.add_remote_agent(remote_nixl_agent_meta)

            xfer_handle = nixl_agent.initialize_xfer(
                # "UUID" here is just a placeholder, can be any bytes, but without it,
                # nixl will fail to transfer multiple times.
                "READ",
                local_descs.trim(),
                remote_descs,
                remote_name,
                "UUID",
            )

            state = nixl_agent.transfer(xfer_handle)
            if state == "ERR":
                raise RuntimeError("NIXL transfer got to Error state.")
            # Since current nixl does not provide a better way, we need to check the state of
            # the transfer continuously.
            while True:
                state = nixl_agent.check_xfer_state(xfer_handle)
                if state == "ERR":
                    raise RuntimeError("NIXL transfer got to Error state.")
                if state == "PROC":
                    with self._aborted_transfer_obj_ids_lock:
                        if obj_id in self._aborted_transfer_obj_ids:
                            self._aborted_transfer_obj_ids.remove(obj_id)
                            raise RuntimeError(
                                f"NIXL transfer aborted for object id: {obj_id}"
                            )
                    time.sleep(0.001)  # Avoid busy waiting
                elif state == "DONE":
                    break
        except Exception:
            from ray.exceptions import RayDirectTransportError

            raise RayDirectTransportError(
                f"The NIXL recv failed for object id: {obj_id}. The source actor may have died during the transfer. "
                f"The exception thrown from the nixl recv was:\n {traceback.format_exc()}"
            ) from None
        finally:
            # We could raise errors or NIXL could raise errors like NIXL_ERR_REMOTE_DISCONNECT,
            # so doing best effort cleanup.
            with self._aborted_transfer_obj_ids_lock:
                self._aborted_transfer_obj_ids.discard(obj_id)
            if xfer_handle:
                nixl_agent.release_xfer_handle(xfer_handle)
            if remote_name:
                nixl_agent.remove_remote_agent(remote_name)
            if local_descs:
                nixl_agent.deregister_memory(local_descs)

        return tensors

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
        self, obj_id: str, tensor_transport_meta: TensorTransportMetadata
    ):
        from ray._private.worker import global_worker

        with self._cache_lock:
            assert isinstance(tensor_transport_meta, NixlTransportMetadata)
            gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
            if obj_id not in self._managed_meta_nixl:
                return
            self._managed_meta_nixl.pop(obj_id, None)
            tensors = gpu_object_store.get_object(obj_id)
            for tensor in tensors:
                key = tensor.data_ptr()
                if key in self._tensor_desc_cache:
                    tensor_desc = self._tensor_desc_cache[key]
                    tensor_desc.metadata_count -= 1
                    if tensor_desc.metadata_count == 0:
                        self._tensor_desc_cache.pop(key)
                        self.get_nixl_agent().deregister_memory(tensor_desc.reg_desc)

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        with self._aborted_transfer_obj_ids_lock:
            self._aborted_transfer_obj_ids.add(obj_id)

    # NOTE: The below methods are intended to be used internally hence they assume the caller is already holding the cache lock.
    def _record_and_get_meta_if_duplicate(
        self, src_obj_id: str, src_gpu_object: List["torch.Tensor"]
    ) -> Optional[NixlTransportMetadata]:
        """
        Record the NIXL managed meta for the given object ID if it is a duplicate of another object, and return the meta if it is.
        Assumes that the caller is already holding the cache lock.
        """
        from ray._private.worker import global_worker

        gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
        duplicate_obj_id = gpu_object_store.get_duplicate_objects(
            src_obj_id, src_gpu_object
        )
        if duplicate_obj_id is not None:
            meta = self._get_meta(duplicate_obj_id)
            if meta is None:
                raise ValueError(
                    f"NIXL transport metadata for object id {duplicate_obj_id} not found"
                )
            self._put_meta(src_obj_id, meta)
            self._add_tensor_descs(src_gpu_object)
            return meta
        return None

    def _get_num_managed_meta_nixl(self) -> int:
        with self._cache_lock:
            return len(self._managed_meta_nixl)

    def _get_meta(self, object_id: str) -> Optional[NixlTransportMetadata]:
        """
        Get the NIXL transport metadata for the given object ID if it exists
        """
        if object_id in self._managed_meta_nixl:
            return self._managed_meta_nixl[object_id]
        return None

    def _put_meta(self, object_id: str, meta: NixlTransportMetadata):
        """
        Store the NIXL transport metadata for the given object ID
        """
        self._managed_meta_nixl[object_id] = meta

    def _add_tensor_descs(self, tensors: List["torch.Tensor"]):
        """
        If this is the first time the tensor is being added, we register the memory with NIXL.
        Otherwise, we increment the reference count.
        """
        for tensor in tensors:
            key = tensor.data_ptr()
            if key in self._tensor_desc_cache:
                if tensor.nbytes != self._tensor_desc_cache[key].nbytes:
                    raise ValueError(
                        "Tensors in an RDT object cannot partially overlap with each other."
                    )
                self._tensor_desc_cache[key].metadata_count += 1
            else:
                reg_desc = self.get_nixl_agent().register_memory([tensor])
                self._tensor_desc_cache[key] = TensorDesc(reg_desc, 1, tensor.nbytes)
