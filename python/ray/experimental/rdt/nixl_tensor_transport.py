import collections
import functools
import glob
import logging
import os
import threading
import time
import traceback
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray._private.ray_constants import (
    NIXL_REMOTE_AGENT_CACHE_MAXSIZE,
)
from ray.experimental.rdt.nixl_memory_pool import MemoryPoolManager
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    FetchRequest,
    TensorTransportManager,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import torch

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def _is_efa_available() -> bool:
    """Detect whether AWS EFA (Elastic Fabric Adapter) devices are present."""
    if glob.glob("/sys/class/net/efa*"):
        return True
    for ib_dev in glob.glob("/sys/class/infiniband/*"):
        try:
            driver = os.path.realpath(os.path.join(ib_dev, "device", "driver"))
        except OSError:
            continue
        if os.path.basename(driver) == "efa":
            return True
    return False


def _nixl_transport_available_in_process() -> bool:
    """Returns whether the NIXL tensor transport can be initialized in this process."""
    try:
        from ray.experimental.rdt.util import get_tensor_transport_manager

        get_tensor_transport_manager("NIXL").get_nixl_agent()
        return True
    except Exception:
        logger.debug("NIXL tensor transport unavailable on actor.", exc_info=True)
        return False


@dataclass
class NixlCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the NIXL communicator."""


@dataclass
class NixlTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors stored in the GPU object store for NIXL transport."""

    nixl_serialized_descs: Optional[bytes] = None
    nixl_agent_meta: Optional[bytes] = None
    nixl_agent_name: Optional[str] = None
    nixl_agent_meta_version: Optional[int] = 0

    __eq__ = object.__eq__
    __hash__ = object.__hash__


@dataclass
class TensorDesc:
    reg_desc: Any
    metadata_count: int


@dataclass
class NixlFetchRequest(FetchRequest):
    """NIXL-specific FetchRequest carrying the async transfer state."""

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
        self._nixl_agent = None
        self._aborted_transfer_obj_ids = set()
        self._aborted_transfer_obj_ids_lock = threading.Lock()
        self._tensor_desc_cache: Dict[int, TensorDesc] = {}
        self._managed_meta_nixl: Dict[str, Any] = {}
        self._cache_lock = threading.RLock()
        self._remote_agents: OrderedDict = OrderedDict()
        
        # Tracks active in-flight transfers per remote agent (#65905)
        self._inflight_transfers: Dict[str, int] = collections.defaultdict(int)
        self._inflight_lock = threading.Lock()
        self._inflight_cond = threading.Condition(self._inflight_lock)
        self._pending_removal_agents = set()

        self._nixl_agent_meta_version = 0
        self._memory_pool: Optional[MemoryPoolManager] = None
        self._backend: Optional[str] = None
        self._cuda_stream: Optional["torch.cuda.Stream"] = None

    def tensor_transport_backend(self) -> str:
        return "NIXL"

    def set_cuda_stream(self, stream: Optional["torch.cuda.Stream"]) -> None:
        self._cuda_stream = stream

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return True

    def register_nixl_memory(self, tensor: "torch.Tensor") -> None:
        self._add_tensor_descs([tensor])

    def register_nixl_memory_pool(self, size: int, device: "torch.device") -> None:
        if self._memory_pool is not None:
            raise ValueError("A memory pool is already registered.")
        nixl_agent = self.get_nixl_agent()
        pool = MemoryPoolManager(pool_size=size, device=device)
        nixl_agent.register_memory(pool.get_pool_tensor())
        self._memory_pool = pool

    def deregister_nixl_memory(self, tensor: "torch.Tensor") -> None:
        self._remove_tensor_descs([tensor])

    def select_backend(self) -> str:
        return "LIBFABRIC" if _is_efa_available() else "UCX"

    def _make_nixl_agent(self, backend: str):
        from nixl._api import nixl_agent, nixl_agent_config

        agent_config = nixl_agent_config(backends=[backend])
        ctx = ray.get_runtime_context()
        actor_id = ctx.get_actor_id()
        if actor_id is None:
            import uuid
            actor_id = f"RAY-DRIVER-{uuid.uuid4()}"
        return nixl_agent(actor_id, agent_config)

    def get_nixl_agent(self):
        if self._nixl_agent is None:
            self._nixl_agent = self._init_nixl_agent()
        return self._nixl_agent

    def _init_nixl_agent(self):
        backend = self.select_backend()
        agent = self._make_nixl_agent(backend)
        self._backend = backend
        logger.info("Using NIXL backend: %s", backend)
        return agent

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        def __ray_actor_has_tensor_transport__(self: "ray.actor.ActorHandle") -> bool:
            return _nixl_transport_available_in_process()

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
                devices = set()
                device = rdt_object[0].device
                for t in rdt_object:
                    if t.device.type != device.type:
                        raise ValueError("All tensors in an RDT object must have the same device type.")
                    if not t.is_contiguous():
                        raise ValueError("All tensors in an RDT object must be contiguous.")
                    tensor_meta.append((t.shape, t.dtype))
                    devices.add(t.device)
                if device.type == "cuda":
                    stream = self._cuda_stream
                    if stream is None:
                        for dev in devices:
                            torch.cuda.synchronize(dev)
                    else:
                        for dev in devices:
                            if dev != stream.device:
                                raise ValueError("Device mismatch between CUDA stream and RDT object tensors.")
                        stream.synchronize()

                nixl_agent = self.get_nixl_agent()
                pool_device = (
                    self._memory_pool.get_pool_tensor().device
                    if self._memory_pool is not None
                    else None
                )
                pool_eligible = (
                    self._memory_pool is not None
                    and not any(self._tensor_memory_registered(t) for t in rdt_object)
                    and (
                        pool_device.type == "cpu"
                        or all(t.device == pool_device for t in rdt_object)
                    )
                )
                if pool_eligible:
                    xfer_descs = self._allocate_pool_xfer_descs(rdt_object)
                else:
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
        from ray.experimental.rdt.util import create_empty_tensors_from_metadata

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

        assert tensors

        try:
            nixl_agent = self.get_nixl_agent()
            remote_xfer_descs = nixl_agent.deserialize_descs(nixl_serialized_descs)
            self._add_tensor_descs(tensors)
            added_tensor_descs = True
            local_xfer_descs = nixl_agent.get_xfer_descs(tensors)

            remote_name = tensor_transport_metadata.nixl_agent_name
            remote_agent_meta_version = (
                tensor_transport_metadata.nixl_agent_meta_version
            )

            if NIXL_REMOTE_AGENT_CACHE_MAXSIZE > 0 and remote_name:
                with self._inflight_cond:
                    # Wait for in-flight transfers on the old version to finish
                    while (
                        remote_name in self._remote_agents
                        and remote_agent_meta_version != self._remote_agents[remote_name]
                        and self._inflight_transfers[remote_name] > 0
                    ):
                        self._inflight_cond.wait()

                    if remote_name in self._remote_agents:
                        if (
                            remote_agent_meta_version
                            != self._remote_agents[remote_name]
                        ):
                            nixl_agent.remove_remote_agent(remote_name)
                        self._remote_agents.move_to_end(remote_name)
                    elif len(self._remote_agents) >= NIXL_REMOTE_AGENT_CACHE_MAXSIZE:
                        evicted_agent_name, _ = self._remote_agents.popitem(last=False)
                        if self._inflight_transfers[evicted_agent_name] == 0:
                            nixl_agent.remove_remote_agent(evicted_agent_name)
                        else:
                            self._pending_removal_agents.add(evicted_agent_name)

                    self._remote_agents[remote_name] = remote_agent_meta_version
                    self._inflight_transfers[remote_name] += 1

            nixl_agent.add_remote_agent(remote_nixl_agent_meta)

            xfer_handle = nixl_agent.initialize_xfer(
                "READ",
                local_xfer_descs,
                remote_xfer_descs,
                remote_name,
                b"UUID",
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
                f"The NIXL transfer failed for object id: {obj_id}.\n {traceback.format_exc()}"
            ) from None

    def wait_fetch_complete(
        self, fetch_request: FetchRequest, timeout: float = -1
    ) -> List["torch.Tensor"]:
        assert isinstance(fetch_request, NixlFetchRequest)
        obj_id = fetch_request.obj_id

        if not fetch_request.tensors:
            return fetch_request.tensors

        try:
            deadline = None if timeout < 0 else time.monotonic() + timeout
            while True:
                state = self.get_nixl_agent().check_xfer_state(
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
                            raise RuntimeError(f"NIXL transfer aborted for object id: {obj_id}")
                    time.sleep(0.001)
                elif state == "DONE":
                    break

            return fetch_request.tensors
        except TimeoutError:
            raise
        except Exception:
            from ray.exceptions import RayDirectTransportError

            raise RayDirectTransportError(
                f"The NIXL transfer failed for object id: {obj_id}.\n {traceback.format_exc()}"
            ) from None

    def _cleanup_transfer(
        self,
        obj_id: str,
        tensors: List["torch.Tensor"],
        xfer_handle: Any,
        remote_name: Optional[str],
        remove_tensor_descs: bool,
    ) -> None:
        nixl_agent = self._nixl_agent
        if nixl_agent is None:
            return

        with self._aborted_transfer_obj_ids_lock:
            self._aborted_transfer_obj_ids.discard(obj_id)
        if xfer_handle:
            nixl_agent.release_xfer_handle(xfer_handle)

        if remote_name:
            with self._inflight_cond:
                if self._inflight_transfers[remote_name] > 0:
                    self._inflight_transfers[remote_name] -= 1
                if self._inflight_transfers[remote_name] == 0:
                    if remote_name in self._pending_removal_agents:
                        self._pending_removal_agents.remove(remote_name)
                        nixl_agent.remove_remote_agent(remote_name)
                    self._inflight_cond.notify_all()

        if NIXL_REMOTE_AGENT_CACHE_MAXSIZE == 0 and remote_name:
            nixl_agent.remove_remote_agent(remote_name)
        if remove_tensor_descs:
            self._remove_tensor_descs(tensors)

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> List["torch.Tensor"]:
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
        raise NotImplementedError("NIXL transport does not support send_multiple_tensors.")

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
            self._remove_tensor_descs(tensors)

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
        with self._cache_lock:
            return self._managed_meta_nixl.get(object_id)

    def _put_meta(self, object_id: str, meta: NixlTransportMetadata):
        with self._cache_lock:
            self._managed_meta_nixl[object_id] = meta

    def _remove_tensor_descs(self, tensors: List["torch.Tensor"]):
        with self._cache_lock:
            pool_return_tensors: List["torch.Tensor"] = []
            for tensor in tensors:
                key = tensor.untyped_storage().data_ptr()
                if key not in self._tensor_desc_cache:
                    continue
                tensor_desc = self._tensor_desc_cache[key]
                tensor_desc.metadata_count -= 1
                if tensor_desc.metadata_count == 0:
                    self._tensor_desc_cache.pop(key)
                    if tensor_desc.reg_desc is not None:
                        self.get_nixl_agent().deregister_memory(tensor_desc.reg_desc)
                        self._nixl_agent_meta_version += 1
                    else:
                        pool_return_tensors.append(tensor)
            if pool_return_tensors and self._memory_pool is not None:
                self._memory_pool.free_tensors(pool_return_tensors)

    def _add_tensor_descs(self, tensors: List["torch.Tensor"]):
        with self._cache_lock:
            for tensor in tensors:
                key = tensor.untyped_storage().data_ptr()
                if key in self._tensor_desc_cache:
                    self._tensor_desc_cache[key].metadata_count += 1
                    continue
                mem_type = "cuda" if tensor.is_cuda else "cpu"
                gpu_id = max(tensor.get_device(), 0)
                try:
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
                except Exception as e:
                    troubleshooting = (
                        "See https://github.com/ai-dynamo/nixl for details."
                    )
                    raise RuntimeError(
                        f"Failed to register {mem_type} memory with NIXL: {e}"
                    ) from e
                self._tensor_desc_cache[key] = TensorDesc(reg_desc, 1)

    def _tensor_memory_registered(self, t: "torch.Tensor") -> bool:
        entry = self._tensor_desc_cache.get(t.untyped_storage().data_ptr())
        return entry is not None and entry.reg_desc is not None

    def _add_pool_tensor_descs(self, tensors: List["torch.Tensor"]):
        with self._cache_lock:
            for tensor in tensors:
                key = tensor.untyped_storage().data_ptr()
                if key in self._tensor_desc_cache:
                    self._tensor_desc_cache[key].metadata_count += 1
                else:
                    self._tensor_desc_cache[key] = TensorDesc(
                        reg_desc=None, metadata_count=1
                    )

    def _allocate_pool_xfer_descs(self, tensors: List["torch.Tensor"]) -> Any:
        pool = self._memory_pool
        pre_existing = {
            t.untyped_storage().data_ptr() for t in tensors if pool.has_block(t)
        }
        pool_tensor_views = pool.allocate_for_tensors(tensors)
        try:
            xfer_descs = self._nixl_agent.get_xfer_descs(pool_tensor_views)
        except Exception:
            new_tensors = [
                t for t in tensors if t.untyped_storage().data_ptr() not in pre_existing
            ]
            if new_tensors:
                pool.free_tensors(new_tensors)
            raise
        self._add_pool_tensor_descs(tensors)
        return xfer_descs
