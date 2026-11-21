"""TPU Sync implementation of Ray Direct Transport (RDT).

Transfers torch tensors directly between TPU devices across nodes using TPU Sync.

Known limitation:
    Coordinator failure is fail-fast (no restart recovery; coordinator actor uses
    max_restarts=0). Any coordinator death or WSM communication failure raises an
    exception and fails the transfer immediately, requiring restarting the job/actors
    using TPU_SYNC. This matches other Ray Direct Transport implementations (e.g. NIXL,
    collective, CUDA_IPC), none of which implement reconnection or coordinator recovery.
"""

import importlib.util
import logging
import os
import secrets
import sys
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

import ray
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)

logger = logging.getLogger(__name__)

_WSM_COORDINATOR_ACTOR_NAME = "_ray_internal_tpu_sync_wsm_coordinator"
_WSM_COORDINATOR_NAMESPACE = "ray_internal_rdt"


def _get_pod_control_ip() -> str:
    """Returns the primary pod/host IP address used for control-plane RPCs."""
    env_ip = os.environ.get("RAY_TPU_SYNC_CONTROL_IP", "").strip()
    if env_ip:
        return env_ip
    return ray.util.get_node_ip_address()


class _WsmCoordinatorActor:
    """Cluster-wide singleton Ray actor hosting TPU Sync's WeightSynchronizerManager."""

    def __init__(self) -> None:
        from tpu_sync.api.weight_synchronizer_manager import WeightSynchronizerManager
        from tpu_sync.rpc.raiden_controller import WeightSyncWorkerRpcClient

        self._rpc_client = WeightSyncWorkerRpcClient(name_resolver=None)
        self._wsm = WeightSynchronizerManager(
            port=0,
            worker_rpc_client=self._rpc_client,
            enable_plan_cache=True,
            auto_start_server=True,
        )
        self._ip = _get_pod_control_ip()
        self._address = f"{self._ip}:{self._wsm.port}"
        logger.info("[_WsmCoordinatorActor] Started WSM at %s", self._address)

    def get_address(self) -> str:
        return self._address


def _is_tpu_sync_importable() -> bool:
    """Returns True if tpu_sync is loaded or importable in the current process."""
    if "tpu_sync" in sys.modules:
        return sys.modules["tpu_sync"] is not None
    try:
        return importlib.util.find_spec("tpu_sync") is not None
    except Exception:
        return False


def _select_coordinator_node_id() -> Optional[str]:
    """Selects a preferred node ID for scheduling _WsmCoordinatorActor.

    Prefers a live node in `ray.nodes()` that advertises TPU resources > 0 (preferring
    the current node if it is one of them). Falls back to the current node ID only when
    `tpu_sync` is importable in the calling process so a CPU-only driver without
    `tpu_sync` installed never biases placement onto itself.
    """
    current_node_id: Optional[str] = None
    try:
        current_node_id = ray.get_runtime_context().get_node_id()
    except Exception:
        current_node_id = None

    try:
        if ray.is_initialized():
            tpu_node_ids: List[str] = []
            for node in ray.nodes():
                if not isinstance(node, dict) or not node.get("Alive", False):
                    continue
                resources = node.get("Resources") or {}
                if resources.get("TPU", 0) > 0 and node.get("NodeID"):
                    tpu_node_ids.append(node["NodeID"])
            if tpu_node_ids:
                if current_node_id and current_node_id in tpu_node_ids:
                    return current_node_id
                return tpu_node_ids[0]
    except Exception:
        pass

    if current_node_id and _is_tpu_sync_importable():
        return current_node_id
    return None


def _get_or_create_cluster_wsm_address() -> str:
    configured = os.environ.get("RAY_TPU_SYNC_MANAGER_ADDRESS")
    if configured:
        return configured.strip()
    if not ray.is_initialized():
        raise RuntimeError("Ray must be initialized to discover WSM coordinator.")
    try:
        actor = ray.get_actor(
            _WSM_COORDINATOR_ACTOR_NAME, namespace=_WSM_COORDINATOR_NAMESPACE
        )
        return ray.get(actor.get_address.remote())
    except ValueError:
        pass
    actor_options: Dict[str, Any] = {
        "name": _WSM_COORDINATOR_ACTOR_NAME,
        "namespace": _WSM_COORDINATOR_NAMESPACE,
        "lifetime": "detached",
    }
    target_node_id = _select_coordinator_node_id()
    if target_node_id:
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        actor_options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
            node_id=target_node_id,
            soft=True,
        )
    try:
        remote_cls = ray.remote(num_cpus=0, max_restarts=0)(_WsmCoordinatorActor)
        actor = remote_cls.options(**actor_options).remote()
        return ray.get(actor.get_address.remote())
    except ValueError:
        actor = ray.get_actor(
            _WSM_COORDINATOR_ACTOR_NAME, namespace=_WSM_COORDINATOR_NAMESPACE
        )
        return ray.get(actor.get_address.remote())


def _compute_tensor_signature(
    tensor_meta: Sequence[Tuple[Sequence[int], Any]],
) -> str:
    parts = []
    for shape, dtype in tensor_meta:
        shape_str = "x".join(str(d) for d in shape) if shape else "scalar"
        dtype_str = str(dtype).replace("torch.", "")
        parts.append(f"{shape_str}_{dtype_str}")
    return "-".join(parts)


def _build_variable_protos_from_tensor_meta(
    tensor_meta: Sequence[Tuple[Sequence[int], Any]],
) -> List[Any]:
    import torch
    from tpu_sync.rpc import raiden_service_pb2

    protos = []
    for idx, (shape, dtype) in enumerate(tensor_meta):
        shape_list = [int(d) for d in shape] or [1]
        ndim = len(shape_list)
        item_size = getattr(dtype, "itemsize", 0)
        if not isinstance(item_size, int) or item_size <= 0:
            item_size = torch.empty(1, dtype=dtype).element_size()
        protos.append(
            raiden_service_pb2.VariableMetadataProto(
                name=f"tensor_{idx}",
                shape=shape_list,
                mesh_shape=[1] * ndim,
                layout=list(range(ndim - 1, -1, -1)),
                item_size=item_size,
                layer_idx=idx,
                sharding_spec=[""] * ndim,
            )
        )
    return protos


def _synchronize_tpu_tensors(tensors: Sequence[Any]) -> None:
    if not tensors:
        return
    first = tensors[0]
    if getattr(first, "device", None) is None or first.device.type != "tpu":
        return
    from torch_tpu._internal.sync import synchronize

    synchronize(tensors, wait=True)


def _prepare_tensors_for_ws(
    tensors: Sequence[Any],
) -> List[List[Any]]:
    grouped: List[List[Any]] = []
    cloned_scalars: List[Any] = []
    for t in tensors:
        if t.ndim == 0:
            ws_t = t.reshape(1).clone()
            cloned_scalars.append(ws_t)
        else:
            ws_t = t
        grouped.append([ws_t])
    if cloned_scalars:
        _synchronize_tpu_tensors(cloned_scalars)
    return grouped


def _resolve_ws_endpoints(ws: Any) -> Tuple[str, List[str]]:
    control_ip = _get_pod_control_ip()
    control_endpoint = f"{control_ip}:{ws.listener_port}"
    eps = ws.get_local_endpoints()
    for ep in eps:
        if isinstance(ep, dict) and ep.get("endpoint"):
            ep_shards = ep.get("shards")
            if ep_shards is None or 0 in ep_shards:
                return control_endpoint, [ep["endpoint"]]
    if eps and isinstance(eps[0], dict) and eps[0].get("endpoint"):
        return control_endpoint, [eps[0]["endpoint"]]
    return control_endpoint, [f"{control_ip}:{ws.local_port}"]


@dataclass
class _SynchronizerSlot:
    slot_key: Tuple[str, str]  # (role, sig)
    slot_idx: int
    ws: Any
    unit: Any
    tensor_meta: List[Tuple[Any, Any]] = field(default_factory=list)
    _closed: bool = False

    @staticmethod
    def _teardown_ws(ws: Any) -> None:
        if ws is None:
            return
        ws.unbind_weights()
        ws.shutdown()

    def unbind(self) -> None:
        if not self._closed and self.ws is not None:
            self.ws.unbind_weights()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            ws = self.ws
            self.ws = None
            if ws is not None:
                self._teardown_ws(ws)


@dataclass
class TpuSyncCommunicatorMetadata(CommunicatorMetadata):
    pass


@dataclass
class TpuSyncTransportMetadata(TensorTransportMetadata):
    src_job_name: str = ""
    src_data_name: str = ""
    manager_address: Optional[str] = None


class TpuSyncTensorTransport(TensorTransportManager):
    """Minimal Ray Direct Transport implementation for TPU backed by TPU Sync.

    Known limitation:
        Coordinator failure is fail-fast (max_restarts=0). Losing the coordinator actor
        or encountering WSM RPC errors causes the transfer to fail immediately without
        reconnection or recovery, requiring restarting the job/actors using TPU_SYNC.
        This matches other RDT transports (NIXL, collective, CUDA_IPC).
    """

    _MAX_CACHED_SLOTS: int = 16

    def __init__(self, manager_address: Optional[str] = None):
        self._manager_address: Optional[str] = manager_address
        self._wsm_clients: Dict[str, Any] = {}
        self._free_slots: Dict[Tuple[str, str], List[_SynchronizerSlot]] = defaultdict(
            list
        )
        self._active_senders: Dict[str, _SynchronizerSlot] = {}
        self._available_slot_indices: List[int] = list(range(64, 0, -1))
        self._lock = threading.Lock()
        self._instance_id = secrets.token_hex(4)
        self._parallelism = int(os.environ.get("RAY_TPU_SYNC_PARALLELISM", "1"))
        self._bind_ip = os.environ.get("RAY_TPU_SYNC_BIND_IP") or None

    def tensor_transport_backend(self) -> str:
        return "TPU_SYNC"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        return True

    def _get_manager_address(self) -> str:
        with self._lock:
            if self._manager_address:
                return self._manager_address
        addr = _get_or_create_cluster_wsm_address()
        with self._lock:
            self._manager_address = addr
            return addr

    def _get_wsm_client(self, manager_address: Optional[str] = None) -> Any:
        from tpu_sync.rpc.raiden_controller import RaidenControllerClientFacade

        addr = manager_address or self._get_manager_address()
        with self._lock:
            client = self._wsm_clients.get(addr)
            if client is None:
                client = RaidenControllerClientFacade(addr)
                self._wsm_clients[addr] = client
            return client

    def _retire_slot(self, slot: _SynchronizerSlot) -> None:
        if slot._closed:
            return
        try:
            slot.close()
        finally:
            with self._lock:
                self._available_slot_indices.append(slot.slot_idx)

    def _release_slot(self, slot: _SynchronizerSlot) -> None:
        evict_slot = None
        with self._lock:
            total_cached = sum(len(slots) for slots in self._free_slots.values())
            if total_cached >= self._MAX_CACHED_SLOTS:
                for key, slots in list(self._free_slots.items()):
                    if slots:
                        evict_slot = slots.pop(0)
                        if not slots:
                            self._free_slots.pop(key, None)
                        break
            free_list = self._free_slots.pop(slot.slot_key, [])
            free_list.append(slot)
            self._free_slots[slot.slot_key] = free_list
        if evict_slot is not None:
            self._retire_slot(evict_slot)

    def _register_unit(
        self,
        manager_addr: str,
        unit: Any,
        shards: List[str],
        control_ep: str,
        tensor_meta: List[Tuple[Any, Any]],
    ) -> None:
        var_protos = _build_variable_protos_from_tensor_meta(tensor_meta)
        client = self._get_wsm_client(manager_addr)
        client.register_work_unit(
            unit=unit,
            shards=shards,
            control_plane_rpc_address=control_ep,
            mesh_shape=[1],
            mesh_axes=["fsdp"],
            variables=var_protos,
            host_subgrid=[1],
        )

    def _acquire_slot(
        self,
        role: str,
        sig: str,
        tensors: List[List[Any]],
        tensor_meta: List[Tuple[Any, Any]],
        manager_addr: str,
    ) -> _SynchronizerSlot:
        slot_key = (role, sig)
        while True:
            with self._lock:
                free_list = self._free_slots.get(slot_key)
                slot = free_list.pop() if free_list else None
                if free_list is not None and not free_list:
                    self._free_slots.pop(slot_key, None)
            if slot is None:
                break
            if slot.ws is None or slot._closed:
                continue
            try:
                slot.ws.bind_weights(tensors)
                slot.tensor_meta = list(tensor_meta)
                return slot
            except Exception:
                self._retire_slot(slot)

        with self._lock:
            if not self._available_slot_indices:
                raise RuntimeError(
                    "TPU_SYNC slot index pool exhausted (maximum 64 concurrent slots reached)."
                )
            slot_idx = self._available_slot_indices.pop()

        from tpu_sync.api.torch.weight_synchronizer import WeightSynchronizer
        from tpu_sync.rpc.raiden_controller import RaidenId

        ws = WeightSynchronizer(
            device_tensors=tensors,
            local_port=0,
            listener_port=0,
            parallelism=self._parallelism,
            bind_ip=self._bind_ip,
            auto_h2d=(role == "dst"),
        )
        control_ep, shard_eps = _resolve_ws_endpoints(ws)
        ws_name = f"rdt_{role}_{self._instance_id}_{slot_idx}"
        unit = RaidenId(
            job_name=ws_name,
            job_replica_id="0",
            data_name=ws_name,
        )
        try:
            self._register_unit(manager_addr, unit, shard_eps, control_ep, tensor_meta)
        except Exception as e:
            _SynchronizerSlot._teardown_ws(ws)
            with self._lock:
                self._available_slot_indices.append(slot_idx)
            raise RuntimeError(
                f"TPU_SYNC failed to register {role} unit {unit} with WSM at {manager_addr}: {e}"
            ) from e

        return _SynchronizerSlot(
            slot_key=slot_key,
            slot_idx=slot_idx,
            ws=ws,
            unit=unit,
            tensor_meta=list(tensor_meta),
        )

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List[Any],
    ) -> TensorTransportMetadata:
        import torch

        if not isinstance(rdt_object, (list, tuple)) or len(rdt_object) == 0:
            raise ValueError("TPU_SYNC requires a non-empty list of tensors.")

        device = None
        tensor_meta: List[Tuple[Any, Any]] = []
        for t in rdt_object:
            if not isinstance(t, torch.Tensor):
                raise ValueError(
                    f"TPU_SYNC requires torch.Tensor instances, got {type(t)}"
                )
            if getattr(t, "device", None) is None or t.device.type != "tpu":
                raise ValueError(
                    f"TPU_SYNC only supports 'tpu' tensors, got tensor on device '{getattr(t, 'device', None)}'."
                )
            if device is None:
                device = t.device
            elif t.device != device:
                raise ValueError(
                    f"All tensors in an RDT object must reside on the same device, "
                    f"got {device} and {t.device}."
                )
            if not t.is_contiguous():
                raise ValueError("All tensors in an RDT object must be contiguous.")
            if t.nelement() == 0:
                raise ValueError(
                    "TPU_SYNC does not support zero-element (empty) tensors."
                )
            if t.storage_offset() != 0:
                raise ValueError(
                    "TPU_SYNC requires tensors with storage_offset == 0 "
                    "(tensor slices/views are not supported; call .clone() first)."
                )
            tensor_nbytes = t.nelement() * t.element_size()
            try:
                storage_nbytes = t.untyped_storage().nbytes()
            except Exception:
                storage_nbytes = 0
            if storage_nbytes > 0 and storage_nbytes != tensor_nbytes:
                raise ValueError(
                    "TPU_SYNC requires tensors that span their full underlying storage "
                    "(partial tensor views are not supported; call .clone() first)."
                )
            tensor_meta.append((t.shape, t.dtype))

        _synchronize_tpu_tensors(rdt_object)
        grouped_tensors = _prepare_tensors_for_ws(rdt_object)
        sig_key = _compute_tensor_signature(tensor_meta)
        manager_addr = self._get_manager_address()

        with self._lock:
            old_slot = self._active_senders.pop(obj_id, None)
        if old_slot is not None:
            try:
                old_slot.unbind()
            except Exception:
                self._retire_slot(old_slot)
                raise
            self._release_slot(old_slot)

        slot = self._acquire_slot(
            role="src",
            sig=sig_key,
            tensors=grouped_tensors,
            tensor_meta=tensor_meta,
            manager_addr=manager_addr,
        )
        with self._lock:
            self._active_senders[obj_id] = slot

        return TpuSyncTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device.type if device else None,
            src_job_name=slot.unit.job_name,
            src_data_name=slot.unit.data_name,
            manager_address=manager_addr,
        )

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> CommunicatorMetadata:
        return TpuSyncCommunicatorMetadata()

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List[Any]] = None,
    ) -> List[Any]:
        import torch
        from tpu_sync.rpc.raiden_controller import RaidenId, RaidenMemoryType

        dev_str = tensor_transport_metadata.tensor_device or "tpu"
        if target_buffers is not None:
            if len(target_buffers) != len(tensor_transport_metadata.tensor_meta):
                raise ValueError(
                    f"Expected {len(tensor_transport_metadata.tensor_meta)} target_buffers, "
                    f"got {len(target_buffers)}."
                )
            target_dev = None
            for idx, (b, (expected_shape, expected_dtype)) in enumerate(
                zip(target_buffers, tensor_transport_metadata.tensor_meta)
            ):
                if b.device.type != "tpu":
                    raise ValueError(
                        f"TPU_SYNC target_buffers must reside on 'tpu', got '{b.device}'."
                    )
                if target_dev is None:
                    target_dev = b.device
                elif b.device != target_dev:
                    raise ValueError(
                        "All TPU_SYNC target_buffers must reside on the same device."
                    )
                if tuple(b.shape) != tuple(expected_shape):
                    raise ValueError(
                        f"target_buffers[{idx}] shape {tuple(b.shape)} != expected {tuple(expected_shape)}."
                    )
                if b.dtype != expected_dtype:
                    raise ValueError(
                        f"target_buffers[{idx}] dtype {b.dtype} != expected {expected_dtype}."
                    )
            dev = target_dev if target_dev is not None else torch.device(dev_str)
        else:
            dev = torch.device(dev_str)

        ws_buffers: List[Any] = []
        result_buffers: List[Any] = []
        for idx, (shape, dtype) in enumerate(tensor_transport_metadata.tensor_meta):
            if target_buffers is not None:
                buf = target_buffers[idx]
                if len(shape) == 0:
                    ws_buf = torch.empty((1,), dtype=dtype, device=dev)
                else:
                    ws_buf = buf
            else:
                ws_buf = torch.empty(
                    (1,) if len(shape) == 0 else tuple(shape), dtype=dtype, device=dev
                )
                buf = ws_buf.reshape(()) if len(shape) == 0 else ws_buf

            ws_buffers.append(ws_buf)
            result_buffers.append(buf)

        _synchronize_tpu_tensors(ws_buffers)
        grouped_buffers = [[b] for b in ws_buffers]
        sig_key = _compute_tensor_signature(tensor_transport_metadata.tensor_meta)

        manager_addr = (
            tensor_transport_metadata.manager_address or self._get_manager_address()
        )
        dst_slot = self._acquire_slot(
            role="dst",
            sig=sig_key,
            tensors=grouped_buffers,
            tensor_meta=tensor_transport_metadata.tensor_meta,
            manager_addr=manager_addr,
        )

        transfer_uuid = secrets.randbits(63) or 1
        src_unit = RaidenId(
            job_name=tensor_transport_metadata.src_job_name,
            job_replica_id="0",
            data_name=tensor_transport_metadata.src_data_name,
        )

        transfer_success = False
        try:
            wsm_client = self._get_wsm_client(manager_addr)
            wsm_client.coordinate_transfer(
                src_units=[src_unit],
                dst_units=[dst_slot.unit],
                use_block_chunks=True,
                # Although this is called from the Ray receiver, is_sender is
                # True to select the WSM controller's role as coordinator of
                # the transfer, which is intended for a one-sided transfer.
                is_sender=True,
                uuid=transfer_uuid,
                dst_mem_type=RaidenMemoryType.DRAM,
            )

            dst_slot.ws.wait_for_transfer_completion(transfer_uuid)
            _synchronize_tpu_tensors(ws_buffers)

            if target_buffers is not None:
                for idx, (shape, _) in enumerate(tensor_transport_metadata.tensor_meta):
                    if len(shape) == 0:
                        target_buffers[idx].copy_(ws_buffers[idx].reshape(()))
                _synchronize_tpu_tensors(target_buffers)

            transfer_success = True
            return result_buffers
        finally:
            if transfer_success:
                try:
                    dst_slot.unbind()
                except Exception:
                    self._retire_slot(dst_slot)
                    raise
                self._release_slot(dst_slot)
            else:
                self._retire_slot(dst_slot)

    def send_multiple_tensors(
        self,
        tensors: List[Any],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        raise NotImplementedError(
            "TPU_SYNC transport is one-sided and does not support send_multiple_tensors."
        )

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List[Any],
    ):
        with self._lock:
            slot = self._active_senders.pop(obj_id, None)
        if slot is not None:
            try:
                slot.unbind()
            except Exception:
                self._retire_slot(slot)
                raise
            self._release_slot(slot)

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        pass
