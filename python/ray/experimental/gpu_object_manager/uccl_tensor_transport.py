import os
import threading
import time
import traceback
from collections import OrderedDict
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

UCCL_REMOTE_ENDPOINT_CACHE_MAXSIZE = int(
    os.environ.get("RAY_UCCL_REMOTE_ENDPOINT_CACHE_MAXSIZE", "1000")
)
UCCL_NUM_CPUS = int(os.environ.get("RAY_UCCL_NUM_CPUS", "4"))


@dataclass
class UCCLCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the UCCL communicator."""


@dataclass
class UCCLTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors stored in the GPU object store for UCCL transport.

    Args:
        uccl_serialized_descs: Serialized tensor descriptors for UCCL transport.
        uccl_endpoint_meta: The endpoint metadata of the remote UCCL endpoint,
            used by the receiver to establish a connection.
        uccl_endpoint_name: A unique name identifying the remote UCCL endpoint
            (derived from the Ray actor ID).
        uccl_endpoint_meta_version: Version counter incremented whenever memory
            is deregistered on the sender. The receiver uses this to detect
            stale cached connections and re-establish when needed.
    """

    uccl_serialized_descs: Optional[bytes] = None
    uccl_endpoint_meta: Optional[bytes] = None
    uccl_endpoint_name: Optional[str] = None
    uccl_endpoint_meta_version: Optional[int] = 0

    __eq__ = object.__eq__
    __hash__ = object.__hash__


@dataclass
class TensorDesc:
    desc: Any
    metadata_count: int


class UCCLTensorTransport(TensorTransportManager):
    def __init__(self):
        self._uccl_endpoint = None
        self._endpoint_name = None
        self._aborted_transfer_obj_ids = set()
        self._aborted_transfer_obj_ids_lock = threading.Lock()
        # Mapping from tensor data pointer to the UCCL descriptor and reference count.
        self._tensor_desc_cache: Dict[int, TensorDesc] = {}
        # Mapping from object ID to the UCCL managed metadata.
        self._managed_meta: Dict[str, UCCLTransportMetadata] = {}
        # Lock protecting _tensor_desc_cache and _managed_meta since they can be
        # accessed from the main task execution thread or the _ray_system thread.
        self._cache_lock = threading.Lock()
        # LRU cache of remote endpoint connections: endpoint_name -> (conn_id, version).
        # When full, the least recently used entry is evicted.
        self._remote_endpoints: OrderedDict = OrderedDict()
        # Increment whenever memory is deregistered on this endpoint so that
        # receivers can detect stale cached connections.
        self._uccl_endpoint_meta_version = 0

    def tensor_transport_backend(self) -> str:
        return "UCCL"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return True

    def _get_uccl_endpoint(self):
        """
        Creates a UCCL P2P endpoint with passive accept if not already created.
        """
        if self._uccl_endpoint is not None:
            return self._uccl_endpoint

        import torch
        from uccl import p2p

        gpu_idx = torch.cuda.current_device() if torch.cuda.is_available() else 0
        self._uccl_endpoint = p2p.Endpoint(gpu_idx, UCCL_NUM_CPUS)

        ctx = ray.get_runtime_context()
        actor_id = ctx.get_actor_id()
        if actor_id is None:
            import uuid

            actor_id = f"RAY-DRIVER-{uuid.uuid4()}"
        self._endpoint_name = actor_id

        return self._uccl_endpoint

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        def __ray_actor_has_tensor_transport__(
            self: "ray.actor.ActorHandle",
        ) -> bool:
            try:
                from ray.experimental.gpu_object_manager.util import (
                    get_tensor_transport_manager,
                )

                get_tensor_transport_manager("UCCL")._get_uccl_endpoint()
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
    ) -> UCCLTransportMetadata:
        import torch

        with self._cache_lock:
            device = None
            tensor_meta = []

            if gpu_object:
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
                    tensor_meta.append((t.shape, t.dtype))
                    devices.add(t.device)
                if device.type == "cuda":
                    for dev in devices:
                        torch.cuda.synchronize(dev)

                ep = self._get_uccl_endpoint()
                self._uccl_endpoint.start_passive_accept()
                self._add_tensor_descs(gpu_object)

                all_descs = []
                for t in gpu_object:
                    key = t.data_ptr()
                    all_descs.append(self._tensor_desc_cache[key].desc)

                serialized_descs = ep.get_serialized_descs(all_descs)
                endpoint_meta = ep.get_metadata()
                endpoint_name = self._endpoint_name
                endpoint_meta_version = self._uccl_endpoint_meta_version
            else:
                serialized_descs = None
                endpoint_meta = None
                endpoint_name = None
                endpoint_meta_version = None

            ret = UCCLTransportMetadata(
                tensor_meta=tensor_meta,
                tensor_device=device.type if device else None,
                uccl_serialized_descs=serialized_descs,
                uccl_endpoint_meta=endpoint_meta,
                uccl_endpoint_name=endpoint_name,
                uccl_endpoint_meta_version=endpoint_meta_version,
            )
            self._put_meta(obj_id, ret)
            return ret

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> UCCLCommunicatorMetadata:
        return UCCLCommunicatorMetadata()

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> List["torch.Tensor"]:
        from ray.experimental.gpu_object_manager.util import (
            create_empty_tensors_from_metadata,
        )

        tensors = target_buffers or create_empty_tensors_from_metadata(
            tensor_transport_metadata
        )

        assert isinstance(tensor_transport_metadata, UCCLTransportMetadata)
        assert isinstance(communicator_metadata, UCCLCommunicatorMetadata)

        with self._aborted_transfer_obj_ids_lock:
            if obj_id in self._aborted_transfer_obj_ids:
                self._aborted_transfer_obj_ids.remove(obj_id)
                raise RuntimeError(f"UCCL transfer aborted for object id: {obj_id}")

        if not tensors:
            return []

        local_descs = None
        remote_name = None
        try:
            ep = self._get_uccl_endpoint()
            local_descs = ep.register_memory(tensors)
            remote_name = tensor_transport_metadata.uccl_endpoint_name
            remote_meta = tensor_transport_metadata.uccl_endpoint_meta
            remote_meta_version = tensor_transport_metadata.uccl_endpoint_meta_version

            # Agent reuse is enabled.
            if UCCL_REMOTE_ENDPOINT_CACHE_MAXSIZE > 0:
                if remote_name in self._remote_endpoints:
                    cached_conn_id, cached_version = self._remote_endpoints[remote_name]
                    # If the remote agent metadata version is different from the cached one,
                    # it means there was memory deregistered. We need to remove the remote endpoint
                    # before adding it, since there is potential memory overlap.
                    if remote_meta_version != cached_version:
                        ep.remove_remote_endpoint(cached_conn_id)
                    self._remote_endpoints.move_to_end(remote_name)
                elif len(self._remote_endpoints) >= UCCL_REMOTE_ENDPOINT_CACHE_MAXSIZE:
                    evicted_name, (evicted_conn_id, _) = self._remote_endpoints.popitem(last=False)
                    ep.remove_remote_endpoint(evicted_conn_id)

            # Establish the connection and read the data.
            success, conn_id = ep.add_remote_endpoint(remote_meta)
            if not success:
                raise RuntimeError(
                    f"Failed to connect to remote UCCL endpoint: {remote_name}"
                )
            if UCCL_REMOTE_ENDPOINT_CACHE_MAXSIZE > 0:
                self._remote_endpoints[remote_name] = (conn_id, remote_meta_version)

            remote_descs = ep.deserialize_descs(
                tensor_transport_metadata.uccl_serialized_descs
            )

            success, transfer_id = ep.transfer(
                conn_id, "read", local_descs, remote_descs
            )
            if not success:
                raise RuntimeError("UCCL transfer initiation failed.")

            while True:
                success, is_done = ep.poll_async(transfer_id)
                if not success:
                    raise RuntimeError("UCCL transfer got to Error state.")
                if is_done:
                    break
                with self._aborted_transfer_obj_ids_lock:
                    if obj_id in self._aborted_transfer_obj_ids:
                        self._aborted_transfer_obj_ids.remove(obj_id)
                        raise RuntimeError(
                            f"UCCL transfer aborted for object id: {obj_id}"
                        )
                time.sleep(0.001) # Avoid busy waiting.
        except Exception:
            from ray.exceptions import RayDirectTransportError

            raise RayDirectTransportError(
                f"The UCCL recv failed for object id: {obj_id}. The source actor may "
                f"have died during the transfer. The exception thrown from the UCCL "
                f"recv was:\n {traceback.format_exc()}"
            ) from None
        finally:
            with self._aborted_transfer_obj_ids_lock:
                self._aborted_transfer_obj_ids.discard(obj_id)
            if UCCL_REMOTE_ENDPOINT_CACHE_MAXSIZE == 0 and remote_name:
                ep.remove_remote_endpoint(
                    self._remote_endpoints[remote_name][0]
                )
            if local_descs:
                with self._cache_lock:
                    ep.deregister_memory(local_descs)
                    self._uccl_endpoint_meta_version += 1


        return tensors

    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        raise NotImplementedError(
            "UCCL transport does not support send_multiple_tensors, "
            "since it is a one-sided transport."
        )

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List["torch.Tensor"],
    ):
        with self._cache_lock:
            assert isinstance(tensor_transport_meta, UCCLTransportMetadata)
            if obj_id not in self._managed_meta:
                return
            self._managed_meta.pop(obj_id, None)
            ep = self._get_uccl_endpoint()
            for tensor in tensors:
                key = tensor.data_ptr()
                if key in self._tensor_desc_cache:
                    tensor_desc = self._tensor_desc_cache[key]
                    tensor_desc.metadata_count -= 1
                    if tensor_desc.metadata_count == 0:
                        self._tensor_desc_cache.pop(key)
                        ep.deregister_memory([tensor_desc.desc])
                        self._uccl_endpoint_meta_version += 1

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        with self._aborted_transfer_obj_ids_lock:
            self._aborted_transfer_obj_ids.add(obj_id)

    def _get_num_managed_meta(self) -> int:
        with self._cache_lock:
            return len(self._managed_meta)

    def _get_meta(self, object_id: str) -> Optional[UCCLTransportMetadata]:
        if object_id in self._managed_meta:
            return self._managed_meta[object_id]
        return None

    def _put_meta(self, object_id: str, meta: UCCLTransportMetadata):
        self._managed_meta[object_id] = meta

    def _add_tensor_descs(self, tensors: List["torch.Tensor"]):
        """
        Register tensors with the UCCL endpoint and cache their descriptors.
        If a tensor is already registered (by data_ptr), its reference count is
        incremented. New tensors are registered one at a time so that duplicates
        within the same input list are detected immediately via the cache.

        Unlike NIXL which registers at the storage level, UCCL registers each
        tensor view individually — ep.register_memory([tensor]) returns a
        descriptor for that specific view's data region. Using data_ptr() as
        the key ensures each view gets its own descriptor, which is necessary
        for correct transfer of overlapping views.
        """
        ep = self._get_uccl_endpoint()
        for tensor in tensors:
            key = tensor.data_ptr()
            if key in self._tensor_desc_cache:
                self._tensor_desc_cache[key].metadata_count += 1
            else:
                descs = ep.register_memory([tensor])
                self._tensor_desc_cache[key] = TensorDesc(
                    desc=descs[0], metadata_count=1
                )
