import atexit
import contextlib
import hashlib
import importlib.util
import logging
import os
import secrets
import socket
import sys
import threading
import uuid
import weakref
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Deque, Dict, List, Optional, Tuple

import ray
from ray._private import ray_constants
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    FetchRequest,
    TensorTransportManager,
    TensorTransportMetadata,
)

# NOTE: Do not import from `ray.exceptions` or `ray.util` at module top-level.
# `ray.experimental.rdt.util` imports this module while `ray._raylet` is
# initializing `ray.exceptions` (which imports `ray.util.annotations` ->
# `ray.util.rpdb` -> `ray.experimental.internal_kv` -> `ray.experimental.rdt`).

if TYPE_CHECKING:
    import torch

logger = logging.getLogger(__name__)

# Name of the singleton Ray actor that hosts WeightSynchronizerManager when
# no external RAY_TPU_SYNC_MANAGER_ADDRESS is configured.
_WSM_COORDINATOR_ACTOR_NAME = "_ray_rdt_tpu_sync_wsm_coordinator"
_WSM_COORDINATOR_NAMESPACE = "ray_internal_rdt"
_WSM_ACTOR_TIMEOUT_S = 30.0

# Maximum number of cached WeightSynchronizer slots retained per (role, manager_address,
# signature) key on a single worker process, and maximum number of distinct keys cached
# before LRU eviction.
_MAX_CACHED_SLOTS_PER_SIGNATURE = 4
_MAX_CACHED_SIGNATURES = 16
_MAX_ABORTED_OBJ_IDS = 1024
_MAX_TRANSFER_THREADS = 64
_IDLE_WORKER_TIMEOUT_S = 60.0

_CONTROLLER_UNIT_REGISTRIES = (
    "_registered_shards",
    "_registered_control_plane_endpoints",
    "_registered_mesh_shapes",
    "_registered_mesh_axes",
    "_registered_layouts",
    "_registered_global_shapes",
    "_registered_itemsizes",
    "_registered_pool_manifests",
    "_registered_layout_fingerprints",
    "_registered_page_tokens",
    "_registered_transfer_parallelism",
    "_registered_transfer_ranks",
    "_registered_variables",
    "_registered_host_subgrids",
    "_computed_phys_meshes",
    "_registered_tensor_metadata",
    "_registered_sharding_specs",
    "_registered_host_endpoints",
    "_registered_control_endpoints",
)


def _get_configured_manager_address() -> Optional[str]:
    """Returns the externally configured WSM address from RAY_TPU_SYNC_MANAGER_ADDRESS, if any."""
    addr = os.environ.get("RAY_TPU_SYNC_MANAGER_ADDRESS", "").strip()
    return addr or None


def _get_configured_parallelism(default: int = 4) -> int:
    """Returns the configured transfer parallelism from RAY_TPU_SYNC_PARALLELISM or default."""
    raw = os.environ.get("RAY_TPU_SYNC_PARALLELISM", "").strip()
    if not raw:
        return default
    try:
        val = int(raw)
    except ValueError as e:
        raise ValueError(
            f"RAY_TPU_SYNC_PARALLELISM must be a positive integer, got {raw!r}"
        ) from e
    if val <= 0:
        raise ValueError(
            f"RAY_TPU_SYNC_PARALLELISM must be a positive integer, got {val}"
        )
    return val


def _get_configured_bind_ip() -> Optional[str]:
    """Returns the configured bind IP from RAY_TPU_SYNC_BIND_IP, if any."""
    ip = os.environ.get("RAY_TPU_SYNC_BIND_IP", "").strip()
    return ip or None


def _get_pod_control_ip() -> str:
    """Returns the primary pod/host IP address used for control-plane RPCs."""
    env_ip = os.environ.get("RAY_TPU_SYNC_CONTROL_IP", "").strip()
    if env_ip:
        return env_ip
    try:
        if ray.is_initialized():
            node_ip = ray.util.get_node_ip_address()
            if node_ip and not node_ip.startswith("127.") and node_ip != "0.0.0.0":
                return node_ip
    except Exception:
        pass
    try:
        ip = socket.gethostbyname(socket.gethostname())
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    return "127.0.0.1"


class _TransferWorkerPool:
    """Dynamically-scaling daemon thread pool that reuses up to `max_workers` idle threads.

    Idle threads are reused across sequential transfers so iterative workloads do not
    spawn a new OS thread per fetch, while new daemon worker threads are spawned on demand
    whenever `len(self._tasks) > self._idle_workers` so stuck transfers can never starve
    subsequent fetches even if more than `max_workers` transfers are hung in C++.
    """

    def __init__(
        self,
        max_workers: int = _MAX_TRANSFER_THREADS,
        idle_timeout_s: float = _IDLE_WORKER_TIMEOUT_S,
    ) -> None:
        self._max_workers = max(1, max_workers)
        self._idle_timeout_s = idle_timeout_s
        self._cond = threading.Condition(threading.Lock())
        self._tasks: Deque[Callable[[], None]] = deque()
        self._total_workers = 0
        self._idle_workers = 0
        self._worker_seq = 0

    def submit(self, fn: Callable[[], None]) -> None:
        spawn_thread = False
        worker_id = 0
        busy_workers = 0
        with self._cond:
            self._tasks.append(fn)
            if len(self._tasks) <= self._idle_workers:
                self._cond.notify()
            else:
                self._total_workers += 1
                self._worker_seq += 1
                worker_id = self._worker_seq
                busy_workers = self._total_workers - self._idle_workers
                spawn_thread = True

        if spawn_thread:
            if busy_workers > self._max_workers:
                logger.warning(
                    "[TpuSyncTensorTransport] Active transfer worker threads (%d) "
                    "exceeded soft limit (%d); C++ or WSM transfers may be hung.",
                    busy_workers,
                    self._max_workers,
                )
            t = threading.Thread(
                target=self._worker_loop,
                name=f"ray_rdt_tpu_sync_worker_{worker_id}",
                daemon=True,
            )
            t.start()

    def _worker_loop(self) -> None:
        try:
            while True:
                with self._cond:
                    while not self._tasks:
                        if self._idle_workers >= self._max_workers:
                            return
                        self._idle_workers += 1
                        try:
                            notified = self._cond.wait(timeout=self._idle_timeout_s)
                        finally:
                            self._idle_workers -= 1
                        if not notified and not self._tasks:
                            return
                    fn = self._tasks.popleft()

                try:
                    fn()
                finally:
                    fn = None  # type: ignore[assignment]
        finally:
            with self._cond:
                self._total_workers -= 1


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
        logger.info(
            "[_WsmCoordinatorActor] Started WeightSynchronizerManager server at %s",
            self._address,
        )

    def get_address(self) -> str:
        return self._address

    def get_stats(self) -> Dict[str, int]:
        return {
            "plan_cache_size": int(self._wsm.get_plan_cache_size()),
            "registered_entities": len(self._wsm.get_all_metadata()),
        }

    def unregister_work_units(self, units: List[Any]) -> int:
        """Removes a batch of retired per-slot work units and cached plans from the controller."""
        if not units:
            return 0
        try:
            from tpu_sync.rpc.raiden_controller import RaidenId
        except Exception:
            return 0

        controller = getattr(self._wsm, "_controller", None)
        if controller is None:
            return 0

        lock = getattr(controller, "_lock", None)
        ctx = lock if lock is not None else contextlib.nullcontext()
        removed = 0
        with ctx:
            for item in units:
                try:
                    if hasattr(item, "job_name"):
                        unit = item
                    else:
                        job_name, job_replica_id, data_name = item
                        unit = RaidenId(
                            job_name=job_name,
                            job_replica_id=job_replica_id,
                            data_name=data_name,
                        )
                except Exception:
                    continue

                for attr in _CONTROLLER_UNIT_REGISTRIES:
                    reg = getattr(controller, attr, None)
                    if isinstance(reg, dict):
                        reg.pop(unit, None)

                # Each slot has a unique job_name, so its JobEntity is exclusively
                # owned by `unit` and keyed by `RaidenId(job_name=unit.job_name)`.
                ent_key = RaidenId(job_name=unit.job_name)
                for ent_attr in ("_entities", "_entities_by_key"):
                    ent_map = getattr(controller, ent_attr, None)
                    if isinstance(ent_map, dict):
                        ent_map.pop(unit.job_name, None)
                        ent_map.pop(ent_key, None)
                        ent_map.pop(unit, None)

                plan_cache = getattr(controller, "_plan_cache", None)
                if isinstance(plan_cache, dict):
                    stale_keys = [
                        k
                        for k in plan_cache
                        if isinstance(k, tuple)
                        and len(k) >= 2
                        and (
                            k[0] == unit
                            or k[1] == unit
                            or (isinstance(k[0], (tuple, list, set)) and unit in k[0])
                            or (isinstance(k[1], (tuple, list, set)) and unit in k[1])
                        )
                    ]
                    for k in stale_keys:
                        plan_cache.pop(k, None)
                removed += 1
        return removed


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
    """Returns the address of the cluster's WeightSynchronizerManager server.

    Checks RAY_TPU_SYNC_MANAGER_ADDRESS first; otherwise lazily creates or looks up
    the singleton _WsmCoordinatorActor in the Ray cluster with soft node affinity to
    a TPU node (or the calling node when `tpu_sync` is available locally).
    """
    env_addr = _get_configured_manager_address()
    if env_addr:
        return env_addr

    try:
        actor = ray.get_actor(
            _WSM_COORDINATOR_ACTOR_NAME, namespace=_WSM_COORDINATOR_NAMESPACE
        )
    except ValueError:
        actor_options: Dict[str, Any] = {
            "name": _WSM_COORDINATOR_ACTOR_NAME,
            "namespace": _WSM_COORDINATOR_NAMESPACE,
            "lifetime": "detached",
            "get_if_exists": True,
            "max_restarts": 0,
            "max_task_retries": 0,
        }
        target_node_id = _select_coordinator_node_id()
        if target_node_id:
            from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

            actor_options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                node_id=target_node_id,
                soft=True,
            )
        try:
            remote_cls = ray.remote(num_cpus=0)(_WsmCoordinatorActor)
            actor = remote_cls.options(**actor_options).remote()
        except Exception:
            actor = ray.get_actor(
                _WSM_COORDINATOR_ACTOR_NAME, namespace=_WSM_COORDINATOR_NAMESPACE
            )
    return ray.get(actor.get_address.remote(), timeout=_WSM_ACTOR_TIMEOUT_S)


def _compute_tensor_signature(tensor_meta: List[Tuple[Any, Any]]) -> str:
    """Computes a deterministic 12-character hex digest for a list of (shape, dtype) pairs."""
    sig_parts = [
        f"{tuple(int(d) for d in shape)}:{dtype}" for shape, dtype in tensor_meta
    ]
    digest = hashlib.sha256("|".join(sig_parts).encode("utf-8")).hexdigest()
    return digest[:12]


def _build_variable_protos_from_tensor_meta(
    tensor_meta: List[Tuple[Any, Any]],
) -> List[Any]:
    """Builds VariableMetadataProto descriptors for a list of (shape, dtype) pairs."""
    from tpu_sync.rpc import raiden_service_pb2

    protos = []
    for idx, (shape, dtype) in enumerate(tensor_meta):
        shape_list = [int(d) for d in shape] or [1]
        ndim = len(shape_list)
        item_size = getattr(dtype, "itemsize", 0)
        if not isinstance(item_size, int) or item_size <= 0:
            raise ValueError(
                f"TPU_SYNC requires dtype with a positive integer itemsize, got {dtype!r}"
            )

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


def _synchronize_tpu_tensors(tensors: List["torch.Tensor"]) -> None:
    """Synchronizes pending operations on TPU tensors before D2H or after H2D.

    Only `(ImportError, AttributeError)` when resolving the TPU synchronization entrypoints
    are ignored (e.g. in CPU-only environments without `torch_tpu`); actual runtime errors
    during device synchronization raise `RayDirectTransportError`.
    """
    if not tensors or tensors[0].device.type != "tpu":
        return
    try:
        import torch
    except ImportError:
        return
    if not isinstance(tensors[0], torch.Tensor):
        return

    sync_fn = None
    try:
        from torch_tpu._internal import sync

        sync_fn = getattr(sync, "synchronize", None)
    except (ImportError, AttributeError):
        sync_fn = None

    if callable(sync_fn):
        try:
            sync_fn(tensors, wait=True)
            return
        except Exception as e:
            from ray.exceptions import RayDirectTransportError

            if isinstance(e, RayDirectTransportError):
                raise
            raise RayDirectTransportError(
                f"TPU_SYNC failed to synchronize TPU tensors: {e}"
            ) from e

    tpu_mod = getattr(torch, "tpu", None)
    fallback_sync = (
        getattr(tpu_mod, "synchronize", None) if tpu_mod is not None else None
    )
    if callable(fallback_sync):
        try:
            fallback_sync()
        except Exception as e:
            from ray.exceptions import RayDirectTransportError

            if isinstance(e, RayDirectTransportError):
                raise
            raise RayDirectTransportError(
                f"TPU_SYNC failed to synchronize TPU device: {e}"
            ) from e


def _prepare_tensors_for_ws(
    tensors: List["torch.Tensor"],
) -> List[List["torch.Tensor"]]:
    """Prepares single-shard grouped tensor lists for WeightSynchronizer.

    0-D scalar tensors on TPU have a 0-dimensional PJRT base storage buffer
    even after a view reshape, which C++ UnpackTorchTensor rejects with
    'Tensor base storage buffer has no dimensions'. Materializing a 1-D clone
    (t.reshape(1).clone()) allocates a 1-D PJRT storage buffer. Note that because
    0-D scalars bind a cloned 1-D buffer at extraction time, subsequent in-place
    modifications to the caller's original 0-D scalar tensor will not be reflected
    in transfers, unlike 1-D+ tensors.
    """
    grouped: List[List["torch.Tensor"]] = []
    cloned_scalars: List["torch.Tensor"] = []
    for t in tensors:
        if t.ndim == 0:
            ws_tensor = t.reshape(1).clone()
            cloned_scalars.append(ws_tensor)
        else:
            ws_tensor = t
        grouped.append([ws_tensor])
    if cloned_scalars:
        _synchronize_tpu_tensors(cloned_scalars)
    return grouped


def _resolve_ws_endpoints(ws: Any) -> Tuple[str, List[str]]:
    """Resolves the control-plane RPC endpoint and shard 0 data endpoint for a WeightSynchronizer."""
    control_ip = _get_pod_control_ip()
    control_endpoint = f"{control_ip}:{ws.listener_port}"

    eps = ws.get_local_endpoints()
    fallback_endpoint: Optional[str] = None
    for ep in eps:
        if not isinstance(ep, dict):
            continue
        ep_addr = ep.get("endpoint")
        if not ep_addr:
            continue
        if fallback_endpoint is None:
            fallback_endpoint = ep_addr
        ep_shards = ep.get("shards")
        if ep_shards is None or 0 in ep_shards:
            return control_endpoint, [ep_addr]
    if fallback_endpoint is not None:
        return control_endpoint, [fallback_endpoint]

    return control_endpoint, [f"{control_ip}:{ws.local_port}"]


@dataclass
class _SynchronizerSlot:
    """Cached WeightSynchronizer instance and registered WSM work unit for a tensor signature."""

    slot_key: Tuple[str, str, str]
    slot_idx: int
    ws: Any
    unit: Any
    data_endpoints: List[str]
    control_endpoint: str
    tensor_meta: List[Tuple[Any, Any]] = field(default_factory=list)
    bound_tensors: List[List[Any]] = field(default_factory=list)
    _closed: bool = field(default=False, init=False, repr=False)
    _unregistered: bool = field(default=False, init=False, repr=False)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )
    _in_flight: int = field(default=0, init=False, repr=False)
    _on_close_callbacks: List[Any] = field(default_factory=list, init=False, repr=False)

    @property
    def is_closed(self) -> bool:
        with self._lock:
            return self._closed

    def mark_in_flight(self) -> bool:
        """Marks an in-flight transfer on this slot so close/unbind defer C++ teardown."""
        with self._lock:
            if self._closed or self.ws is None:
                return False
            self._in_flight += 1
            return True

    def finish_in_flight(self) -> None:
        """Completes an in-flight transfer and runs deferred C++ teardown and unregistration if closed."""
        ws_to_close = None
        callbacks: List[Any] = []
        with self._lock:
            if self._in_flight > 0:
                self._in_flight -= 1
            if self._in_flight == 0 and self._closed:
                ws_to_close = self.ws
                self.ws = None
                self.bound_tensors = []
                callbacks = list(self._on_close_callbacks)
                self._on_close_callbacks.clear()
        if ws_to_close is not None:
            self._teardown_ws(ws_to_close)
        for cb in callbacks:
            try:
                cb()
            except Exception:
                pass

    @staticmethod
    def _teardown_ws(ws: Any) -> None:
        try:
            ws.unbind_weights()
        except Exception:
            pass
        try:
            ws.shutdown()
        except Exception:
            pass

    def unbind(self) -> bool:
        """Releases bound TPU HBM tensors while keeping the WeightSynchronizer alive for reuse."""
        with self._lock:
            if self.ws is None or self._closed or self._in_flight > 0:
                return False
            ws = self.ws
            self.bound_tensors = []
        try:
            ws.unbind_weights()
        except Exception:
            with self._lock:
                self._closed = True
            return False
        return True

    def close(self, on_deferred_close: Optional[Any] = None) -> bool:
        """Idempotently unbinds device tensors and shuts down the C++ WeightSynchronizer.

        If a C++ transfer (`_in_flight > 0`) is currently active on this slot, marks the
        slot closed and defers `ws.unbind_weights()`, `ws.shutdown()`, releasing
        `bound_tensors`, and `on_deferred_close` (e.g. WSM unregistration) until
        `finish_in_flight()` runs after `wait_for_transfer_completion` returns, so
        `UnbindWeights()` and unit unregistration never race with an in-flight transfer.

        Returns True if the slot is already quiescent (`_in_flight == 0`) so immediate
        unregistration can proceed, or False if teardown was deferred to `finish_in_flight()`.
        """
        ws_to_close = None
        with self._lock:
            if self._closed and (self.ws is None or self._in_flight > 0):
                if self._in_flight > 0 and on_deferred_close is not None:
                    self._on_close_callbacks.append(on_deferred_close)
                return self._in_flight == 0
            self._closed = True
            if self._in_flight == 0:
                ws_to_close = self.ws
                self.ws = None
                self.bound_tensors = []
                quiescent = True
            else:
                if on_deferred_close is not None:
                    self._on_close_callbacks.append(on_deferred_close)
                quiescent = False
        if ws_to_close is not None:
            self._teardown_ws(ws_to_close)
        return quiescent


def _get_local_sender_registry() -> Tuple[
    threading.Lock,
    "weakref.WeakValueDictionary[Tuple[str, str, str], _SynchronizerSlot]",
]:
    """Returns the process-local (lock, WeakValueDictionary) of active sender slots keyed by unit."""
    reg = getattr(threading, "_ray_rdt_tpu_sync_sender_slots", None)
    if reg is None:
        reg = (threading.Lock(), weakref.WeakValueDictionary())
        threading._ray_rdt_tpu_sync_sender_slots = reg  # type: ignore[attr-defined]
    return reg


@dataclass
class TpuSyncCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the TPU Sync communicator (one-sided)."""


@dataclass
class TpuSyncTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors registered with TPU Sync and WeightSynchronizerManager.

    Args:
        src_job_name: Source job name in RaidenId.
        src_data_name: Source data name in RaidenId.
        manager_address: Address ('ip:port') of the WeightSynchronizerManager server.
    """

    src_job_name: str = "rdt_src"
    src_data_name: str = ""
    manager_address: Optional[str] = None


@dataclass(eq=False)
class TpuSyncFetchRequest(FetchRequest):
    """TPU Sync FetchRequest carrying the transfer state for wait_fetch_complete."""

    transfer_uuid: int = 0
    receiver_slot: Optional[_SynchronizerSlot] = None
    staging_tensors: List["torch.Tensor"] = field(default_factory=list)
    target_buffers: Optional[List["torch.Tensor"]] = None
    aborted: bool = False
    error: Optional[BaseException] = None
    transport_manager: Optional["TpuSyncTensorTransport"] = None
    done_or_aborted_event: threading.Event = field(
        default_factory=threading.Event, init=False, repr=False
    )
    _ws_buffers: List["torch.Tensor"] = field(
        default_factory=list, init=False, repr=False
    )
    _cleaned_up: bool = field(default=False, init=False, repr=False)

    def cleanup(self, reusable: bool = False, from_finalizer: bool = False) -> None:
        if self._cleaned_up:
            return
        self._cleaned_up = True
        mgr = self.transport_manager
        slot = self.receiver_slot
        self.receiver_slot = None
        self.tensors = []
        self.staging_tensors = []
        self._ws_buffers = []
        self.target_buffers = None
        self.transport_manager = None
        if mgr is not None:
            can_reuse = reusable and not self.aborted
            with mgr._lock:
                req_refs = mgr._active_fetch_requests.get(self.obj_id)
                if req_refs is not None:
                    remaining = [
                        r for r in req_refs if r() is not None and r() is not self
                    ]
                    if remaining:
                        mgr._active_fetch_requests[self.obj_id] = remaining
                    else:
                        mgr._active_fetch_requests.pop(self.obj_id, None)
                if self.obj_id in mgr._aborted_obj_ids:
                    can_reuse = False
            mgr._retire_slot(
                slot,
                reusable=can_reuse,
                from_finalizer=from_finalizer,
            )

    def __del__(self):
        try:
            self.cleanup(reusable=False, from_finalizer=True)
        except Exception:
            pass


class TpuSyncTensorTransport(TensorTransportManager):
    """One-sided RDT TensorTransportManager backed by TPU Sync's WeightSynchronizerManager.

    Notes on cancellation, coordinator lifecycle, and sender unbinding:
    - `abort_transport()` immediately unblocks the waiting Python caller in
      `wait_fetch_complete()` and prevents copying into caller-supplied `target_buffers`.
      Because `tpu-sync` does not expose a C++ transfer cancellation API, an already-started
      `coordinate_transfer` keeps its internal staging slot and worker thread in flight
      until `wait_for_transfer_completion` returns (and leaks that slot/thread if C++
      hangs indefinitely).
    - When `RAY_TPU_SYNC_MANAGER_ADDRESS` is not set, the auto-launched
      `_WsmCoordinatorActor` runs with `max_restarts=0` and is fate-shared with active
      transfers; if the coordinator dies, in-flight and subsequent transfers fail fast
      with `RayDirectTransportError`.
    - Process-local sender slots are marked in-flight around `coordinate_transfer`, and
      any slot whose `unbind_weights()` fails is closed and evicted rather than reused.
      For cross-process transfers, C++ listener pushes on the sender do not notify Python.
    """

    def __init__(
        self,
        manager_address: Optional[str] = None,
        default_parallelism: Optional[int] = None,
        bind_ip: Optional[str] = None,
    ):
        if default_parallelism is None:
            resolved_parallelism = _get_configured_parallelism(default=4)
        else:
            if int(default_parallelism) <= 0:
                raise ValueError(
                    f"default_parallelism must be positive, got {default_parallelism}"
                )
            resolved_parallelism = int(default_parallelism)

        self._explicit_manager_address = manager_address
        self._manager_address = manager_address or _get_configured_manager_address()
        self._parallelism = resolved_parallelism
        self._bind_ip = bind_ip if bind_ip is not None else _get_configured_bind_ip()
        self._instance_id = uuid.uuid4().hex[:8]
        self._lock = threading.RLock()
        self._coordinator_actor: Optional[Any] = None
        self._transfer_pool = _TransferWorkerPool()
        # Active sender _SynchronizerSlot instances keyed by obj_id
        self._active_senders: Dict[str, _SynchronizerSlot] = {}
        # Active in-flight TpuSyncFetchRequest instances keyed by obj_id (weak refs so
        # dropping a FetchRequest without calling wait_fetch_complete triggers __del__)
        self._active_fetch_requests: Dict[
            str, List["weakref.ReferenceType[TpuSyncFetchRequest]"]
        ] = defaultdict(list)
        # Unified LRU cache of free _SynchronizerSlot deques keyed by (role, manager_address, sig)
        self._free_slots: "OrderedDict[Tuple[str, str, str], Deque[_SynchronizerSlot]]" = (
            OrderedDict()
        )
        self._slot_counts: Dict[Tuple[str, str, str], int] = defaultdict(int)
        # Cached RaidenControllerClientFacade instances keyed by manager_address
        self._wsm_clients: Dict[str, Any] = {}
        self._aborted_obj_ids: "OrderedDict[str, None]" = OrderedDict()
        # Lock-free queue of retired slots from __del__ finalizers awaiting WSM unregistration
        self._pending_unregister_slots: Deque[_SynchronizerSlot] = deque()
        self._logged_external_unregister_unsupported: bool = False

        self_ref = weakref.ref(self)

        def _cleanup_on_exit() -> None:
            inst = self_ref()
            if inst is not None:
                try:
                    inst.clear_cached_synchronizers(include_active=True)
                except Exception:
                    pass

        atexit.register(_cleanup_on_exit)

    def tensor_transport_backend(self) -> str:
        return "TPU_SYNC"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        """Returns True because `abort_transport()` wakes blocked Python waiters and
        prevents writing into caller target buffers. Note that an already-started C++
        transfer holds its internal staging slot and worker thread until
        `wait_for_transfer_completion` returns (and leaks that slot/thread if C++ hangs).
        """
        return True

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        def __ray_actor_has_tpu_sync__(self_actor: "ray.actor.ActorHandle") -> bool:
            try:
                import torch
                import tpu_sync  # noqa: F401

                tpu_mod = getattr(torch, "tpu", None)
                if tpu_mod is None:
                    try:
                        import torch_tpu  # noqa: F401

                        tpu_mod = getattr(torch, "tpu", None)
                    except ImportError:
                        return False
                if tpu_mod is None:
                    return False
                is_avail = getattr(tpu_mod, "is_available", None)
                if callable(is_avail):
                    return bool(is_avail())
                return True
            except Exception:
                return False

        return ray.get(
            actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                __ray_actor_has_tpu_sync__
            )
        )

    def _has_pinned_manager_address(self) -> bool:
        return bool(self._explicit_manager_address or _get_configured_manager_address())

    def _get_coordinator_actor(self) -> Optional[Any]:
        if self._has_pinned_manager_address() or not ray.is_initialized():
            return None
        with self._lock:
            if self._coordinator_actor is not None:
                return self._coordinator_actor
        try:
            actor = ray.get_actor(
                _WSM_COORDINATOR_ACTOR_NAME, namespace=_WSM_COORDINATOR_NAMESPACE
            )
            with self._lock:
                self._coordinator_actor = actor
            return actor
        except Exception:
            with self._lock:
                self._coordinator_actor = None
            return None

    def _get_manager_address(self) -> str:
        with self._lock:
            if self._manager_address:
                return self._manager_address
        resolved_addr = _get_or_create_cluster_wsm_address()
        with self._lock:
            if not self._manager_address:
                self._manager_address = resolved_addr
            return self._manager_address

    def _get_wsm_client(self, manager_address: Optional[str] = None):
        from tpu_sync.rpc.raiden_controller import RaidenControllerClientFacade

        addr = manager_address or self._get_manager_address()
        with self._lock:
            client = self._wsm_clients.get(addr)
            if client is None:
                client = RaidenControllerClientFacade(addr)
                self._wsm_clients[addr] = client
            return client

    def _register_unit(
        self,
        manager_addr: str,
        unit: Any,
        shards: List[str],
        control_ep: str,
        tensor_meta: List[Tuple[Any, Any]],
    ) -> None:
        """Registers a single-shard work unit and its variable metadata with WSM."""
        var_protos = _build_variable_protos_from_tensor_meta(tensor_meta)
        wsm_client = self._get_wsm_client(manager_addr)
        wsm_client.register_work_unit(
            unit=unit,
            shards=shards,
            control_plane_rpc_address=control_ep,
            mesh_shape=[1],
            mesh_axes=["fsdp"],
            variables=var_protos,
            host_subgrid=[1],
        )

    def _unregister_slots(
        self, slots: List[_SynchronizerSlot], from_finalizer: bool = False
    ) -> None:
        """Removes retired slots' work unit registrations from WSM, batching actor RPCs.

        When called from a `__del__` finalizer (`from_finalizer=True`), pushes eligible
        slots onto the lock-free `self._pending_unregister_slots` deque instead of
        performing synchronous Ray calls inside GC finalization.
        """
        if from_finalizer:
            for slot in slots:
                if (
                    slot is not None
                    and slot.unit is not None
                    and not slot._unregistered
                ):
                    self._pending_unregister_slots.append(slot)
            return

        if not slots and not self._pending_unregister_slots:
            return

        combined_slots: List[_SynchronizerSlot] = []
        while self._pending_unregister_slots:
            try:
                combined_slots.append(self._pending_unregister_slots.popleft())
            except IndexError:
                break
        if slots:
            combined_slots.extend(slots)
        if not combined_slots:
            return

        coordinator_units: List[Tuple[str, str, str]] = []
        for slot in combined_slots:
            unit = slot.unit
            if unit is None or slot._unregistered:
                continue
            slot._unregistered = True
            if not self._has_pinned_manager_address():
                coordinator_units.append(
                    (unit.job_name, unit.job_replica_id, unit.data_name)
                )
            elif not self._logged_external_unregister_unsupported:
                self._logged_external_unregister_unsupported = True
                logger.warning(
                    "[TpuSyncTensorTransport] External WeightSynchronizerManager at %s "
                    "does not support work-unit unregistration; retired unit %s remains "
                    "registered on the external manager.",
                    slot.slot_key[1],
                    unit,
                )

        if coordinator_units and ray.is_initialized():
            actor = self._get_coordinator_actor()
            if actor is not None:
                try:
                    actor.unregister_work_units.remote(coordinator_units)
                except Exception:
                    with self._lock:
                        self._coordinator_actor = None

    def _close_and_unregister_slots(
        self, slots: List[_SynchronizerSlot], from_finalizer: bool = False
    ) -> None:
        """Closes slots and unregisters quiescent ones immediately, deferring in-flight slots."""
        if not slots:
            self._unregister_slots([], from_finalizer=from_finalizer)
            return
        immediate_unregister: List[_SynchronizerSlot] = []
        self_weak = weakref.ref(self)
        for s in slots:

            def _deferred_unreg(slot_to_unreg: _SynchronizerSlot = s) -> None:
                mgr = self_weak()
                if mgr is not None:
                    mgr._unregister_slots(
                        [slot_to_unreg], from_finalizer=from_finalizer
                    )

            if s.close(on_deferred_close=_deferred_unreg):
                immediate_unregister.append(s)
        self._unregister_slots(immediate_unregister, from_finalizer=from_finalizer)

    def clear_cached_synchronizers(
        self, include_active: bool = False, from_finalizer: bool = False
    ) -> None:
        """Destroys cached WeightSynchronizer instances and unregisters their work units."""
        active_sender_slots: List[_SynchronizerSlot] = []
        with self._lock:
            slots_to_close: List[_SynchronizerSlot] = []
            for slot_q in self._free_slots.values():
                slots_to_close.extend(slot_q)
            self._free_slots.clear()
            if include_active:
                active_sender_slots = list(self._active_senders.values())
                slots_to_close.extend(active_sender_slots)
                self._active_senders.clear()
                for req_refs in self._active_fetch_requests.values():
                    for req_ref in req_refs:
                        req = req_ref()
                        if (
                            req is not None
                            and req.receiver_slot is not None
                            and req.receiver_slot not in slots_to_close
                        ):
                            slots_to_close.append(req.receiver_slot)
                self._active_fetch_requests.clear()
        if active_sender_slots:
            reg_lock, reg_map = _get_local_sender_registry()
            with reg_lock:
                for s in active_sender_slots:
                    if s.unit is not None:
                        reg_map.pop(
                            (s.unit.job_name, s.unit.job_replica_id, s.unit.data_name),
                            None,
                        )
        self._close_and_unregister_slots(slots_to_close, from_finalizer=from_finalizer)

    def _acquire_slot(
        self,
        role: str,
        sig: str,
        tensors: List[List["torch.Tensor"]],
        tensor_meta: List[Tuple[Any, Any]],
        manager_addr: str,
    ) -> _SynchronizerSlot:
        """Acquires a cached free WeightSynchronizer slot or creates and registers a new one."""
        from tpu_sync.api.torch.weight_synchronizer import WeightSynchronizer
        from tpu_sync.rpc.raiden_controller import RaidenId

        self._unregister_slots([])
        slot_key = (role, manager_addr, sig)
        reusable_slot: Optional[_SynchronizerSlot] = None
        slot_idx = 0
        with self._lock:
            free_q = self._free_slots.get(slot_key)
            if free_q is not None:
                while free_q:
                    candidate = free_q.popleft()
                    if candidate.ws is not None and not candidate.is_closed:
                        reusable_slot = candidate
                        break
                if free_q:
                    self._free_slots.move_to_end(slot_key)
                else:
                    self._free_slots.pop(slot_key, None)
            if reusable_slot is None:
                slot_idx = self._slot_counts[slot_key]
                self._slot_counts[slot_key] += 1

        if reusable_slot is not None:
            try:
                reusable_slot.ws.bind_weights(tensors)
                reusable_slot.bound_tensors = tensors
                reusable_slot.tensor_meta = list(tensor_meta)
                return reusable_slot
            except Exception:
                reusable_slot.close()
                self._unregister_slots([reusable_slot])
                raise

        ws = WeightSynchronizer(
            device_tensors=tensors,
            local_port=0,
            listener_port=0,
            parallelism=self._parallelism,
            bind_ip=self._bind_ip,
            auto_h2d=(role == "dst"),
        )
        control_endpoint, shard_endpoints = _resolve_ws_endpoints(ws)
        ws_name = f"rdt_{role}_{self._instance_id}_{sig}_{slot_idx}"
        unit = RaidenId(
            job_name=ws_name,
            job_replica_id="0",
            data_name=ws_name,
        )
        try:
            self._register_unit(
                manager_addr, unit, shard_endpoints, control_endpoint, tensor_meta
            )
        except Exception as e:
            from ray.exceptions import RayDirectTransportError

            _SynchronizerSlot._teardown_ws(ws)
            if isinstance(e, RayDirectTransportError):
                raise
            raise RayDirectTransportError(
                f"TPU_SYNC failed to register {role} work unit {unit} with WSM at "
                f"{manager_addr}: {e}"
            ) from e

        return _SynchronizerSlot(
            slot_key=slot_key,
            slot_idx=slot_idx,
            ws=ws,
            unit=unit,
            data_endpoints=shard_endpoints,
            control_endpoint=control_endpoint,
            tensor_meta=list(tensor_meta),
            bound_tensors=tensors,
        )

    def _retire_slot(
        self,
        slot: Optional[_SynchronizerSlot],
        reusable: bool = True,
        from_finalizer: bool = False,
    ) -> None:
        """Unbinds and returns a slot to `_free_slots`, or closes and unregisters it."""
        if slot is None:
            self._unregister_slots([], from_finalizer=from_finalizer)
            return

        can_reuse = reusable and not from_finalizer and slot.unbind()
        evicted_slots: List[_SynchronizerSlot] = []
        with self._lock:
            if can_reuse and not slot.is_closed and slot.ws is not None:
                free_q = self._free_slots.get(slot.slot_key)
                if free_q is not None and slot in free_q:
                    self._free_slots.move_to_end(slot.slot_key)
                elif (
                    free_q is not None
                    and len(free_q) >= _MAX_CACHED_SLOTS_PER_SIGNATURE
                ):
                    self._free_slots.move_to_end(slot.slot_key)
                    evicted_slots.append(slot)
                else:
                    if free_q is None:
                        free_q = deque()
                        self._free_slots[slot.slot_key] = free_q
                    else:
                        self._free_slots.move_to_end(slot.slot_key)
                    free_q.append(slot)
                    while len(self._free_slots) > _MAX_CACHED_SIGNATURES:
                        _, oldest_q = self._free_slots.popitem(last=False)
                        evicted_slots.extend(oldest_q)
            else:
                evicted_slots.append(slot)

        self._close_and_unregister_slots(evicted_slots, from_finalizer=from_finalizer)

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List["torch.Tensor"],
    ) -> TpuSyncTransportMetadata:
        """Called on the source actor/driver upon tensor creation or ray.put."""
        tensor_meta: List[Tuple[Any, Any]] = []
        device: Optional["torch.device"] = None

        if not rdt_object:
            return TpuSyncTransportMetadata(
                tensor_meta=[],
                tensor_device=None,
                src_data_name=obj_id,
            )

        for t in rdt_object:
            if t.device.type != "tpu":
                raise ValueError(
                    f"TPU_SYNC only supports 'tpu' tensors, got tensor on device '{t.device}'."
                )
            if device is None:
                device = t.device
            elif t.device != device:
                raise ValueError(
                    f"All tensors in an RDT object must reside on the same TPU device, "
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
            if old_slot.unit is not None:
                reg_lock, reg_map = _get_local_sender_registry()
                with reg_lock:
                    reg_map.pop(
                        (
                            old_slot.unit.job_name,
                            old_slot.unit.job_replica_id,
                            old_slot.unit.data_name,
                        ),
                        None,
                    )
            self._retire_slot(old_slot, reusable=True)

        slot = self._acquire_slot(
            role="src",
            sig=sig_key,
            tensors=grouped_tensors,
            tensor_meta=tensor_meta,
            manager_addr=manager_addr,
        )
        with self._lock:
            self._active_senders[obj_id] = slot
        if slot.unit is not None:
            reg_lock, reg_map = _get_local_sender_registry()
            with reg_lock:
                reg_map[
                    (slot.unit.job_name, slot.unit.job_replica_id, slot.unit.data_name)
                ] = slot

        return TpuSyncTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device.type,
            src_job_name=slot.unit.job_name,
            src_data_name=slot.unit.data_name,
            manager_address=slot.slot_key[1],
        )

    def get_communicator_metadata(
        self,
        src_actor: Optional["ray.actor.ActorHandle"],
        dst_actor: Optional["ray.actor.ActorHandle"],
        backend: Optional[str] = None,
    ) -> TpuSyncCommunicatorMetadata:
        return TpuSyncCommunicatorMetadata()

    def fetch_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> TpuSyncFetchRequest:
        """Initiates an asynchronous one-sided transfer orchestrated by WeightSynchronizerManager."""
        import torch
        from tpu_sync.rpc.raiden_controller import RaidenId, RaidenMemoryType

        from ray.exceptions import RayDirectTransportError

        assert isinstance(tensor_transport_metadata, TpuSyncTransportMetadata)
        assert isinstance(communicator_metadata, TpuSyncCommunicatorMetadata)

        with self._lock:
            if obj_id in self._aborted_obj_ids:
                self._aborted_obj_ids.pop(obj_id, None)
                raise RayDirectTransportError(
                    f"TPU_SYNC transfer aborted for obj_id={obj_id}"
                )

        if not tensor_transport_metadata.tensor_meta:
            req = TpuSyncFetchRequest(obj_id=obj_id, tensors=[])
            req.done_or_aborted_event.set()
            return req

        dev_str = tensor_transport_metadata.tensor_device or "tpu"
        if target_buffers is not None:
            if len(target_buffers) != len(tensor_transport_metadata.tensor_meta):
                raise ValueError(
                    f"Expected {len(tensor_transport_metadata.tensor_meta)} target_buffers, "
                    f"got {len(target_buffers)}."
                )
            target_dev: Optional["torch.device"] = None
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
                        f"All TPU_SYNC target_buffers must reside on the same TPU device, "
                        f"got {target_dev} and {b.device}."
                    )
                if tuple(b.shape) != tuple(expected_shape):
                    raise ValueError(
                        f"target_buffers[{idx}] shape {tuple(b.shape)} does not match "
                        f"expected shape {tuple(expected_shape)}."
                    )
                if b.dtype != expected_dtype:
                    raise ValueError(
                        f"target_buffers[{idx}] dtype {b.dtype} does not match "
                        f"expected dtype {expected_dtype}."
                    )
            dev = target_dev if target_dev is not None else torch.device(dev_str)
        else:
            dev = torch.device(dev_str)

        ws_buffers: List["torch.Tensor"] = []
        staging_buffers: List["torch.Tensor"] = []
        for shape, dtype in tensor_transport_metadata.tensor_meta:
            ws_shape = (1,) if len(shape) == 0 else tuple(shape)
            ws_buf = torch.empty(ws_shape, dtype=dtype, device=dev)
            ws_buffers.append(ws_buf)
            staging_buffers.append(ws_buf.reshape(()) if len(shape) == 0 else ws_buf)

        _synchronize_tpu_tensors(ws_buffers)
        grouped_buffers = [[buf] for buf in ws_buffers]
        sig_key = _compute_tensor_signature(tensor_transport_metadata.tensor_meta)

        try:
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
        except RayDirectTransportError:
            raise
        except Exception as e:
            raise RayDirectTransportError(
                f"TPU_SYNC failed to initialize receiver for obj_id={obj_id}: {e}"
            ) from e

        transfer_uuid = secrets.randbits(63) or 1
        src_unit = RaidenId(
            job_name=tensor_transport_metadata.src_job_name,
            job_replica_id="0",
            data_name=tensor_transport_metadata.src_data_name,
        )
        dst_unit = dst_slot.unit

        fetch_request = TpuSyncFetchRequest(
            obj_id=obj_id,
            tensors=target_buffers if target_buffers is not None else staging_buffers,
            transfer_uuid=transfer_uuid,
            receiver_slot=dst_slot,
            staging_tensors=staging_buffers,
            target_buffers=target_buffers,
            transport_manager=self,
        )
        fetch_request._ws_buffers = ws_buffers
        del ws_buffers

        req_weak = weakref.ref(fetch_request)
        with self._lock:
            if obj_id in self._aborted_obj_ids:
                self._aborted_obj_ids.pop(obj_id, None)
                fetch_request.aborted = True
                fetch_request.cleanup(reusable=False)
                raise RayDirectTransportError(
                    f"TPU_SYNC transfer aborted for obj_id={obj_id}"
                )
            self._active_fetch_requests[obj_id].append(req_weak)

        self_weak = weakref.ref(self)
        done_event = fetch_request.done_or_aborted_event
        dst_slot.mark_in_flight()
        reg_lock, reg_map = _get_local_sender_registry()
        with reg_lock:
            local_src_slot = reg_map.get(
                (
                    src_unit.job_name,
                    src_unit.job_replica_id,
                    src_unit.data_name,
                )
            )
        src_in_flight = (
            local_src_slot.mark_in_flight() if local_src_slot is not None else False
        )

        def _run_transfer() -> None:
            try:
                if done_event.is_set() or dst_slot.is_closed:
                    return
                transport = self_weak()
                if transport is None:
                    return
                dst_ws = dst_slot.ws
                if dst_ws is None:
                    return
                wsm_client = transport._get_wsm_client(manager_addr)
                del transport
                wsm_client.coordinate_transfer(
                    src_units=[src_unit],
                    dst_units=[dst_unit],
                    use_block_chunks=True,
                    is_sender=True,
                    uuid=transfer_uuid,
                    dst_mem_type=RaidenMemoryType.DRAM,
                )
                # Once coordinate_transfer is called, always wait for C++ transfer
                # completion before finish_in_flight() can run unbind_weights()/shutdown().
                dst_ws.wait_for_transfer_completion(transfer_uuid)
                del dst_ws

                if done_event.is_set() or dst_slot.is_closed:
                    return
                req = req_weak()
                if (
                    req is not None
                    and not req.aborted
                    and not req._cleaned_up
                    and req._ws_buffers
                ):
                    _synchronize_tpu_tensors(list(req._ws_buffers))
            except BaseException as exc:
                req = req_weak()
                if req is not None:
                    req.error = exc
            finally:
                if src_in_flight and local_src_slot is not None:
                    local_src_slot.finish_in_flight()
                dst_slot.finish_in_flight()
                done_event.set()

        self._transfer_pool.submit(_run_transfer)
        return fetch_request

    def wait_fetch_complete(
        self,
        fetch_request: FetchRequest,
        timeout: float = -1,
    ) -> List["torch.Tensor"]:
        """Waits for the WeightSynchronizerManager transfer and H2D DMA to complete."""
        from ray.exceptions import RayDirectTransportError

        assert isinstance(fetch_request, TpuSyncFetchRequest)
        obj_id = fetch_request.obj_id
        if fetch_request._cleaned_up:
            raise RayDirectTransportError(
                f"TPU_SYNC fetch request for obj_id={obj_id} has already been cleaned up"
            )
        if not fetch_request.tensors:
            fetch_request.cleanup(reusable=True)
            return []

        with self._lock:
            if fetch_request.aborted or obj_id in self._aborted_obj_ids:
                self._aborted_obj_ids.pop(obj_id, None)
                fetch_request.aborted = True
                fetch_request.cleanup(reusable=False)
                raise RayDirectTransportError(
                    f"TPU_SYNC transfer aborted for obj_id={obj_id}"
                )

        wait_timeout = timeout if (timeout is not None and timeout >= 0) else None
        signaled = fetch_request.done_or_aborted_event.wait(timeout=wait_timeout)
        if not signaled:
            raise TimeoutError(
                f"TPU_SYNC transfer timed out after {timeout}s for obj_id={obj_id}"
            )

        with self._lock:
            if fetch_request.aborted or obj_id in self._aborted_obj_ids:
                self._aborted_obj_ids.pop(obj_id, None)
                fetch_request.aborted = True
                fetch_request.cleanup(reusable=False)
                raise RayDirectTransportError(
                    f"TPU_SYNC transfer aborted for obj_id={obj_id}"
                )

        if fetch_request.error is not None:
            err = fetch_request.error
            fetch_request.cleanup(reusable=False)
            if isinstance(err, (TimeoutError, RayDirectTransportError)):
                raise err
            raise RayDirectTransportError(
                f"TPU_SYNC transfer failed for obj_id={obj_id}: {err}"
            ) from err

        if fetch_request.target_buffers is not None:
            for dst_buf, src_buf in zip(
                fetch_request.target_buffers, fetch_request.staging_tensors
            ):
                dst_buf.copy_(src_buf.reshape(dst_buf.shape))
            _synchronize_tpu_tensors(fetch_request.target_buffers)

        result_tensors = fetch_request.tensors
        fetch_request.cleanup(reusable=True)
        return result_tensors

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> List["torch.Tensor"]:
        """Receives multiple tensors synchronously via fetch + wait."""
        fetch_request = self.fetch_multiple_tensors(
            obj_id,
            tensor_transport_metadata,
            communicator_metadata,
            target_buffers,
        )
        try:
            return self.wait_fetch_complete(
                fetch_request,
                timeout=ray_constants.RDT_FETCH_FAIL_TIMEOUT_SECONDS,
            )
        except BaseException:
            fetch_request.cleanup(reusable=False)
            raise

    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
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
        """Releases active sender bindings upon ObjectRef GC."""
        with self._lock:
            slot = self._active_senders.pop(obj_id, None)
            reusable = obj_id not in self._aborted_obj_ids
        if slot is not None and slot.unit is not None:
            reg_lock, reg_map = _get_local_sender_registry()
            with reg_lock:
                reg_map.pop(
                    (slot.unit.job_name, slot.unit.job_replica_id, slot.unit.data_name),
                    None,
                )
        self._retire_slot(slot, reusable=reusable)

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        """Aborts in-progress receive transfers, wakes blocked waiters, and cleans up state.

        Because TPU_SYNC is a one-sided transport, `abort_transport` only aborts in-flight
        receives on the destination and never closes active sender slots (`_active_senders`),
        which remain live for other readers until `garbage_collect` runs.
        """
        slots_to_close: List[_SynchronizerSlot] = []
        with self._lock:
            self._aborted_obj_ids.pop(obj_id, None)
            self._aborted_obj_ids[obj_id] = None
            while len(self._aborted_obj_ids) > _MAX_ABORTED_OBJ_IDS:
                self._aborted_obj_ids.popitem(last=False)

            active_req_refs = self._active_fetch_requests.pop(obj_id, ())
            for req_ref in active_req_refs:
                req = req_ref()
                if req is None:
                    continue
                req.aborted = True
                if (
                    req.receiver_slot is not None
                    and req.receiver_slot not in slots_to_close
                ):
                    slots_to_close.append(req.receiver_slot)
                req.done_or_aborted_event.set()

        self._close_and_unregister_slots(slots_to_close)
