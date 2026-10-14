import asyncio
import gc
import logging
import os
import random
import subprocess
import sys
import threading
import time
import weakref
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple

import pytest

import ray
import ray.experimental.rdt.tpu_sync_tensor_transport as tpu_sync_mod
from ray.exceptions import RayDirectTransportError
from ray.experimental.rdt.tpu_sync_tensor_transport import (
    _MAX_CACHED_SIGNATURES,
    _MAX_CACHED_SLOTS_PER_SIGNATURE,
    _MAX_TRANSFER_THREADS,
    TpuSyncCommunicatorMetadata,
    TpuSyncTensorTransport,
    TpuSyncTransportMetadata,
    _build_variable_protos_from_tensor_meta,
    _compute_tensor_signature,
    _get_configured_manager_address,
    _get_or_create_cluster_wsm_address,
    _get_pod_control_ip,
    _resolve_ws_endpoints,
    _select_coordinator_node_id,
    _synchronize_tpu_tensors,
    _TransferWorkerPool,
    _WsmCoordinatorActor,
)
from ray.experimental.rdt.util import (
    device_match_transport,
    is_one_sided_transport,
)


class _FakeStorage:
    def __init__(self, nbytes: int):
        self._nbytes = nbytes

    def nbytes(self) -> int:
        return self._nbytes


class _FakeDtype:
    def __init__(self, name: str = "torch.bfloat16", itemsize: int = 2):
        self.name = name
        self.itemsize = itemsize

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.name

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _FakeDtype) and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)


class _FakeDevice:
    def __init__(self, spec: Any = "tpu:0"):
        if isinstance(spec, _FakeDevice):
            self.type = spec.type
            self.index = spec.index
        else:
            spec_str = str(spec)
            if ":" in spec_str:
                dev_type, idx_str = spec_str.split(":", 1)
                self.type = dev_type
                self.index = int(idx_str)
            else:
                self.type = spec_str
                self.index = 0 if spec_str == "tpu" else None

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, _FakeDevice)
            and self.type == other.type
            and self.index == other.index
        )

    def __repr__(self) -> str:
        if self.index is not None:
            return f"{self.type}:{self.index}"
        return self.type


class _FakeTensor:
    """Lightweight fake tensor supporting TPU device/shape/storage metadata and payload values."""

    def __init__(
        self,
        shape: Tuple[int, ...] = (16, 16),
        dtype: Any = None,
        device: Any = "tpu:0",
        contiguous: bool = True,
        storage_offset: int = 0,
        storage_nbytes: int = -1,
        value: float = 0.0,
        shared_value_box: Optional[List[float]] = None,
    ):
        self.shape = tuple(shape)
        self.ndim = len(self.shape)
        self.dtype = dtype or _FakeDtype("torch.bfloat16", 2)
        self.device = _FakeDevice(device)
        self._contiguous = contiguous
        self._storage_offset = storage_offset
        numel = 1
        for d in self.shape:
            numel *= d
        self._numel = numel
        actual_nbytes = numel * getattr(self.dtype, "itemsize", 2)
        self._storage = _FakeStorage(
            actual_nbytes if storage_nbytes < 0 else storage_nbytes
        )
        self.reshaped_to: Any = None
        self._value_box = (
            shared_value_box if shared_value_box is not None else [float(value)]
        )

    @property
    def value(self) -> float:
        return self._value_box[0]

    @value.setter
    def value(self, val: float) -> None:
        self._value_box[0] = float(val)

    def copy_(self, src: "_FakeTensor") -> "_FakeTensor":
        self.value = src.value
        return self

    def is_contiguous(self) -> bool:
        return self._contiguous

    def nelement(self) -> int:
        return self._numel

    def element_size(self) -> int:
        return self.dtype.itemsize

    def storage_offset(self) -> int:
        return self._storage_offset

    def untyped_storage(self) -> _FakeStorage:
        return self._storage

    def reshape(self, *new_shape: Any) -> "_FakeTensor":
        if len(new_shape) == 1 and isinstance(new_shape[0], (tuple, list)):
            shape_tuple = tuple(new_shape[0])
        else:
            shape_tuple = tuple(new_shape)
        reshaped = _FakeTensor(
            shape=shape_tuple,
            dtype=self.dtype,
            device=self.device,
            contiguous=self._contiguous,
            storage_offset=self._storage_offset,
            storage_nbytes=self._storage.nbytes(),
            shared_value_box=self._value_box,
        )
        self.reshaped_to = shape_tuple
        return reshaped

    def clone(self) -> "_FakeTensor":
        return _FakeTensor(
            shape=self.shape,
            dtype=self.dtype,
            device=self.device,
            contiguous=True,
            storage_offset=0,
            storage_nbytes=-1,
            shared_value_box=list(self._value_box),
        )


class _FakeJobEntity:
    """Models tpu-sync JobEntity keyed by job_name only (controller_types._entity_key_from_unit)."""

    def __init__(self, job_name: str):
        self.job_name = job_name
        self._endpoints_by_replica: Dict[str, List[str]] = {}
        self._shards_by_replica: Dict[str, List[str]] = {}
        self._endpoints_by_unit: Dict[Any, List[str]] = {}
        self._shards_by_unit: Dict[Any, List[str]] = {}

    def update_registration(
        self,
        unit: Any,
        shards: List[str],
        control_endpoints: List[str],
    ) -> None:
        rep_id = getattr(unit, "job_replica_id", "") or "0"
        self.unregister_host_endpoints(rep_id)
        self._shards_by_replica[rep_id] = list(shards)
        self._endpoints_by_replica[rep_id] = list(control_endpoints)
        self._shards_by_unit[unit] = list(shards)
        self._endpoints_by_unit[unit] = list(control_endpoints)

    def unregister_host_endpoints(self, job_replica_id: str = "") -> None:
        if job_replica_id:
            self._endpoints_by_replica.pop(job_replica_id, None)
            self._shards_by_replica.pop(job_replica_id, None)
            for u in list(self._endpoints_by_unit):
                if getattr(u, "job_replica_id", "") == job_replica_id:
                    self._endpoints_by_unit.pop(u, None)
                    self._shards_by_unit.pop(u, None)
        else:
            self._endpoints_by_replica.clear()
            self._shards_by_replica.clear()
            self._endpoints_by_unit.clear()
            self._shards_by_unit.clear()

    def get_registered_endpoints(self, job_replica_id: str = "") -> List[str]:
        if job_replica_id and job_replica_id in self._endpoints_by_replica:
            return list(self._endpoints_by_replica[job_replica_id])
        res: List[str] = []
        for eps in self._endpoints_by_replica.values():
            res.extend(eps)
        return res

    async def resolve_endpoints(self, job_replica_id: str = "") -> List[str]:
        eps = self.get_registered_endpoints(job_replica_id)
        if not eps:
            raise RuntimeError(
                f"No registered endpoints on JobEntity({self.job_name!r})"
            )
        return eps


class _FakeTpuSyncEnv:
    """In-memory fake of the tpu_sync and torch modules for CPU/CI unit testing."""

    def __init__(self):
        self._lock = threading.RLock()
        self.ws_instances: List[Any] = []
        self.registered_units: List[Dict[str, Any]] = []
        self.unregistered_units: List[Any] = []
        self.coordinated_transfers: List[Dict[str, Any]] = []
        self.plan_cache_keys: List[Tuple[Any, Any]] = []
        self.unit_to_ws: Dict[Any, Any] = {}
        self.address_to_units: Dict[str, Dict[Any, Any]] = {}
        self.invalid_addresses: Set[str] = set()
        self.coordinate_delay_s: float = 0.0
        self.coordinate_error: Optional[Exception] = None
        self.register_error: Optional[Exception] = None
        self.unbind_error: Optional[Exception] = None
        self.block_obj_ids: Dict[str, threading.Event] = {}
        self.coordinate_entered_events: Dict[str, threading.Event] = {}
        self.wait_completion_entered_event: Optional[threading.Event] = None
        self.wait_completion_release_event: Optional[threading.Event] = None
        self.empty_calls: int = 0
        self.zeros_calls: int = 0
        self._next_port = 40000
        self.default_manager_address = "127.0.0.1:50051"

        # Controller backing the fake coordinator actor (models RaidenController with
        # JobEntity keyed by RaidenId(job_name=unit.job_name)).
        self.controller = SimpleNamespace(
            _lock=self._lock,
            _registered_shards={},
            _registered_control_plane_endpoints={},
            _registered_mesh_shapes={},
            _registered_mesh_axes={},
            _registered_variables={},
            _registered_host_subgrids={},
            _entities={},
            _plan_cache={},
        )
        self.coordinator = _WsmCoordinatorActor.__new__(_WsmCoordinatorActor)
        self.coordinator._address = self.default_manager_address
        self.coordinator._wsm = SimpleNamespace(
            _controller=self.controller,
            get_plan_cache_size=lambda: len(self.controller._plan_cache),
            get_all_metadata=lambda: list(self.controller._registered_shards.keys()),
        )

    def install_modules(self) -> Dict[str, ModuleType]:
        env = self

        class FakeRaidenId:
            def __init__(
                self,
                job_name: str = "",
                job_replica_id: str = "",
                data_name: str = "",
                data_replica_idx: int = 0,
            ):
                self.job_name = job_name
                self.job_replica_id = job_replica_id
                self.data_name = data_name
                self.data_replica_idx = data_replica_idx

            def __eq__(self, other: object) -> bool:
                return (
                    isinstance(other, FakeRaidenId)
                    and self.job_name == other.job_name
                    and self.job_replica_id == other.job_replica_id
                    and self.data_name == other.data_name
                    and self.data_replica_idx == other.data_replica_idx
                )

            def __hash__(self) -> int:
                return hash(
                    (
                        self.job_name,
                        self.job_replica_id,
                        self.data_name,
                        self.data_replica_idx,
                    )
                )

            def __repr__(self) -> str:
                return f"RaidenId({self.job_name}, {self.job_replica_id}, {self.data_name})"

        env.RaidenId = FakeRaidenId
        env.controller.get_entity = lambda u: env.controller._entities.get(
            FakeRaidenId(job_name=u.job_name)
        )

        class FakeRaidenMemoryType:
            DRAM = "DRAM"

        class FakeWeightSynchronizer:
            def __init__(
                self,
                device_tensors=None,
                local_port=0,
                listener_port=0,
                parallelism=4,
                bind_ip=None,
                auto_h2d=False,
            ):
                self.device_tensors = device_tensors
                with env._lock:
                    self.local_port = env._next_port
                    self.listener_port = env._next_port + 1
                    env._next_port += 2
                    env.ws_instances.append(self)
                self.parallelism = parallelism
                self.bind_ip = bind_ip
                self.auto_h2d = auto_h2d
                self.d2h_calls = 0
                self.bind_weights_calls = 0
                self.unbind_weights_calls = 0
                self.shutdown_calls = 0
                self.is_shutdown = False
                self.wait_calls: List[int] = []
                self.push_count = 0
                self.completed_d2h_uuids: Set[int] = set()
                self.staged_host_values: List[float] = []

            def d2h(self):
                self.d2h_calls += 1

            def bind_weights(self, grouped_tensors):
                self.bind_weights_calls += 1
                self.device_tensors = grouped_tensors

            def unbind_weights(self):
                self.unbind_weights_calls += 1
                if env.unbind_error is not None:
                    raise env.unbind_error
                self.device_tensors = None

            def shutdown(self):
                self.shutdown_calls += 1
                self.is_shutdown = True
                self.device_tensors = None

            def get_local_endpoints(self):
                return [{"endpoint": f"10.0.0.1:{self.local_port}", "shards": [0]}]

            def get_metrics(self):
                return {"push_resharded_call_count": self.push_count}

            def wait_for_transfer_completion(self, uuid: int):
                self.wait_calls.append(uuid)
                entered_ev = env.wait_completion_entered_event
                if entered_ev is not None:
                    entered_ev.set()
                release_ev = env.wait_completion_release_event
                if release_ev is not None:
                    release_ev.wait(timeout=10.0)

        class FakeWsmClient:
            def __init__(self, address: str):
                self.address = address

            def register_work_unit(self, **kwargs):
                if self.address in env.invalid_addresses:
                    raise ConnectionError(
                        f"WSM coordinator at {self.address} is unreachable"
                    )
                if env.register_error is not None:
                    raise env.register_error
                unit = kwargs["unit"]
                shards = list(kwargs.get("shards") or [])
                ctrl_ep = kwargs.get("control_plane_rpc_address", "")
                with env._lock:
                    env.registered_units.append({"address": self.address, **kwargs})
                    addr_map = env.address_to_units.setdefault(self.address, {})
                    for ws in env.ws_instances:
                        if ctrl_ep.endswith(f":{ws.listener_port}"):
                            env.unit_to_ws[unit] = ws
                            addr_map[unit] = ws
                    ent_key = FakeRaidenId(job_name=unit.job_name)
                    ent = env.controller._entities.get(ent_key)
                    if ent is None:
                        ent = _FakeJobEntity(unit.job_name)
                        env.controller._entities[ent_key] = ent
                    ent.update_registration(
                        unit=unit,
                        shards=shards,
                        control_endpoints=[ctrl_ep] if ctrl_ep else [],
                    )
                    env.controller._registered_shards[unit] = shards
                    env.controller._registered_control_plane_endpoints[unit] = (
                        [ctrl_ep] if ctrl_ep else []
                    )
                    env.controller._registered_mesh_shapes[unit] = list(
                        kwargs.get("mesh_shape") or [1]
                    )
                    env.controller._registered_mesh_axes[unit] = list(
                        kwargs.get("mesh_axes") or ["fsdp"]
                    )
                    env.controller._registered_variables[unit] = list(
                        kwargs.get("variables") or []
                    )
                    env.controller._registered_host_subgrids[unit] = list(
                        kwargs.get("host_subgrid") or [1]
                    )

            def coordinate_transfer(self, **kwargs):
                src_unit = kwargs["src_units"][0]
                dst_unit = kwargs["dst_units"][0]
                with env._lock:
                    addr_map = env.address_to_units.get(self.address, {})
                    src_ent = env.controller.get_entity(src_unit)
                    dst_ent = env.controller.get_entity(dst_unit)
                    src_ws = env.unit_to_ws.get(src_unit)
                    dst_ws = env.unit_to_ws.get(dst_unit)
                    units_registered = not (
                        (
                            addr_map
                            and (src_unit not in addr_map or dst_unit not in addr_map)
                        )
                        or src_ent is None
                        or not src_ent.get_registered_endpoints("0")
                        or dst_ent is None
                        or not dst_ent.get_registered_endpoints("0")
                    )
                entered_ev = env.coordinate_entered_events.get(src_unit.data_name)
                if entered_ev is not None:
                    entered_ev.set()
                block_ev = env.block_obj_ids.get(src_unit.data_name)
                if block_ev is not None:
                    block_ev.wait(timeout=10.0)
                if env.coordinate_delay_s > 0:
                    time.sleep(env.coordinate_delay_s)
                if self.address in env.invalid_addresses:
                    raise ConnectionError(
                        f"WSM coordinator at {self.address} is unreachable"
                    )
                if env.coordinate_error is not None:
                    raise env.coordinate_error
                if not units_registered:
                    raise KeyError(
                        f"Units not registered on coordinator {self.address}: "
                        f"{src_unit}, {dst_unit}"
                    )
                with env._lock:
                    env.coordinated_transfers.append(
                        {"address": self.address, **kwargs}
                    )
                    cache_key = (
                        tuple(kwargs["src_units"]),
                        tuple(kwargs["dst_units"]),
                    )
                    env.controller._plan_cache[cache_key] = "cached_plan"
                    if cache_key not in env.plan_cache_keys:
                        env.plan_cache_keys.append(cache_key)
                    transfer_uuid = kwargs.get("uuid", 0)
                    if src_ws is not None:
                        src_ws.push_count += 1
                        if (
                            transfer_uuid not in src_ws.completed_d2h_uuids
                            and src_ws.device_tensors
                        ):
                            src_ws.completed_d2h_uuids.add(transfer_uuid)
                            src_ws.staged_host_values = [
                                float(g[0].value) for g in src_ws.device_tensors
                            ]
                    if (
                        src_ws is not None
                        and dst_ws is not None
                        and src_ws.staged_host_values
                        and dst_ws.device_tensors
                    ):
                        for val, dst_group in zip(
                            src_ws.staged_host_values, dst_ws.device_tensors
                        ):
                            dst_group[0].value = val

        def _fake_empty(shape, dtype=None, device="tpu:0"):
            env.empty_calls += 1
            return _FakeTensor(shape=shape, dtype=dtype, device=device, value=0.0)

        def _fake_zeros(shape, dtype=None, device="tpu:0"):
            env.zeros_calls += 1
            return _FakeTensor(shape=shape, dtype=dtype, device=device, value=0.0)

        fake_torch = ModuleType("torch")
        fake_torch.Tensor = _FakeTensor
        fake_torch.device = _FakeDevice
        fake_torch.empty = _fake_empty
        fake_torch.zeros = _fake_zeros
        fake_torch.tpu = SimpleNamespace(
            synchronize=lambda: None,
            is_available=lambda: True,
        )

        fake_torch_tpu = ModuleType("torch_tpu")
        fake_torch_tpu_internal = ModuleType("torch_tpu._internal")
        fake_torch_tpu_internal.sync = SimpleNamespace(
            synchronize=lambda tensors, wait=True: None
        )
        fake_torch_tpu._internal = fake_torch_tpu_internal

        fake_tpu_sync = ModuleType("tpu_sync")
        fake_api = ModuleType("tpu_sync.api")
        fake_api_torch = ModuleType("tpu_sync.api.torch")
        fake_ws_mod = ModuleType("tpu_sync.api.torch.weight_synchronizer")
        fake_ws_mod.WeightSynchronizer = FakeWeightSynchronizer

        fake_rpc = ModuleType("tpu_sync.rpc")
        fake_rc_mod = ModuleType("tpu_sync.rpc.raiden_controller")
        fake_rc_mod.RaidenId = FakeRaidenId
        fake_rc_mod.RaidenMemoryType = FakeRaidenMemoryType
        fake_rc_mod.RaidenControllerClientFacade = FakeWsmClient

        fake_pb2_mod = ModuleType("tpu_sync.rpc.raiden_service_pb2")
        fake_pb2_mod.VariableMetadataProto = lambda **kwargs: SimpleNamespace(**kwargs)

        return {
            "torch": fake_torch,
            "torch_tpu": fake_torch_tpu,
            "torch_tpu._internal": fake_torch_tpu_internal,
            "tpu_sync": fake_tpu_sync,
            "tpu_sync.api": fake_api,
            "tpu_sync.api.torch": fake_api_torch,
            "tpu_sync.api.torch.weight_synchronizer": fake_ws_mod,
            "tpu_sync.rpc": fake_rpc,
            "tpu_sync.rpc.raiden_controller": fake_rc_mod,
            "tpu_sync.rpc.raiden_service_pb2": fake_pb2_mod,
        }

    def unregister_units_via_coordinator(self, units: List[Any]) -> int:
        removed = self.coordinator.unregister_work_units(units)
        with self._lock:
            for item in units:
                unit = (
                    item
                    if hasattr(item, "job_name")
                    else self.RaidenId(
                        job_name=item[0],
                        job_replica_id=item[1],
                        data_name=item[2],
                    )
                )
                self.unregistered_units.append(unit)
                self.unit_to_ws.pop(unit, None)
                for addr_map in self.address_to_units.values():
                    addr_map.pop(unit, None)
        return removed


@pytest.fixture
def fake_tpu_env(monkeypatch):
    env = _FakeTpuSyncEnv()
    for name, mod in env.install_modules().items():
        monkeypatch.setitem(sys.modules, name, mod)

    fake_actor_handle = SimpleNamespace(
        get_address=SimpleNamespace(remote=lambda: env.default_manager_address),
        unregister_work_units=SimpleNamespace(
            remote=lambda units: env.unregister_units_via_coordinator(units)
        ),
    )
    monkeypatch.setattr(
        tpu_sync_mod,
        "_get_or_create_cluster_wsm_address",
        lambda: env.default_manager_address,
    )
    monkeypatch.setattr(ray, "is_initialized", lambda: True)
    monkeypatch.setattr(ray, "get_actor", lambda *a, **kw: fake_actor_handle)
    return env


def test_tpu_sync_transport_contract_and_device_registration(fake_tpu_env, monkeypatch):
    """Verifies TPU_SYNC contract methods, platform-safe import, and 'tpu' registration."""
    import ray.experimental.rdt as rdt_pkg
    import ray.experimental.rdt.util as rdt_util

    # Verify TpuSyncTensorTransport is not eagerly exported on ray.experimental.rdt
    assert "TpuSyncTensorTransport" not in rdt_pkg.__all__

    transport = TpuSyncTensorTransport()
    assert transport.tensor_transport_backend() == "TPU_SYNC"
    assert transport.is_one_sided() is True
    assert transport.can_abort_transport() is True

    with pytest.raises(NotImplementedError, match="one-sided"):
        transport.send_multiple_tensors(
            [],
            TpuSyncTransportMetadata(tensor_meta=[], tensor_device="tpu"),
            TpuSyncCommunicatorMetadata(),
        )

    monkeypatch.setattr(rdt_util, "_default_transports_registered", False)
    monkeypatch.setattr(rdt_util, "transport_manager_info", {})
    monkeypatch.setattr(rdt_util, "transport_managers", {})

    assert is_one_sided_transport("TPU_SYNC") is True
    assert device_match_transport("tpu", "TPU_SYNC") is True
    assert device_match_transport("cpu", "TPU_SYNC") is False
    assert device_match_transport("cuda", "TPU_SYNC") is False
    assert device_match_transport("xla", "TPU_SYNC") is False


def test_extract_metadata_validation_scalar_and_zero_copy_creation(fake_tpu_env):
    """Tests tensor validation, 0-D scalar handling, and no eager D2H copy at creation time."""
    transport = TpuSyncTensorTransport()

    empty_meta = transport.extract_tensor_transport_metadata("obj_empty", [])
    assert empty_meta.tensor_meta == []
    assert empty_meta.tensor_device is None

    cpu_t = _FakeTensor(shape=(4, 4), device="cpu")
    with pytest.raises(ValueError, match="only supports 'tpu' tensors"):
        transport.extract_tensor_transport_metadata("obj_cpu", [cpu_t])

    t0 = _FakeTensor(shape=(4, 4), device="tpu:0")
    t1 = _FakeTensor(shape=(4, 4), device="tpu:1")
    with pytest.raises(ValueError, match="same TPU device"):
        transport.extract_tensor_transport_metadata("obj_mixed", [t0, t1])

    non_contig = _FakeTensor(shape=(4, 4), device="tpu:0", contiguous=False)
    with pytest.raises(ValueError, match="contiguous"):
        transport.extract_tensor_transport_metadata("obj_non_contig", [non_contig])

    zero_elem = _FakeTensor(shape=(0, 4), device="tpu:0")
    with pytest.raises(ValueError, match="zero-element"):
        transport.extract_tensor_transport_metadata("obj_zero", [zero_elem])

    offset_view = _FakeTensor(shape=(4, 4), device="tpu:0", storage_offset=4)
    with pytest.raises(ValueError, match="storage_offset == 0"):
        transport.extract_tensor_transport_metadata("obj_offset", [offset_view])

    partial_view = _FakeTensor(shape=(4, 4), device="tpu:0", storage_nbytes=1024)
    with pytest.raises(ValueError, match="full underlying storage"):
        transport.extract_tensor_transport_metadata("obj_partial", [partial_view])

    scalar_t = _FakeTensor(shape=(), device="tpu:0", value=7.0)
    meta = transport.extract_tensor_transport_metadata("obj_scalar", [scalar_t])
    assert meta.tensor_meta == [((), scalar_t.dtype)]
    assert meta.tensor_device == "tpu"
    assert scalar_t.reshaped_to == (1,)
    assert fake_tpu_env.ws_instances[0].d2h_calls == 0

    receiver = TpuSyncTensorTransport()
    comm_meta = transport.get_communicator_metadata(None, None)
    fetched_scalar = receiver.recv_multiple_tensors("obj_scalar", meta, comm_meta)
    assert len(fetched_scalar) == 1
    assert fetched_scalar[0].shape == ()
    assert fetched_scalar[0].value == 7.0

    target_scalar = _FakeTensor(shape=(), device="tpu:0", value=0.0)
    res_scalar = receiver.recv_multiple_tensors(
        "obj_scalar_target", meta, comm_meta, target_buffers=[target_scalar]
    )
    assert res_scalar[0] is target_scalar
    assert target_scalar.shape == ()
    assert target_scalar.value == 7.0
    transport.garbage_collect("obj_scalar", meta, [scalar_t])


def test_wsm_plan_cache_and_synchronizer_slot_reuse(fake_tpu_env):
    """Verifies that iterative transfers reuse WeightSynchronizer slots after GC and hit WSM plan cache."""
    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    for step in range(3):
        obj_id = f"iter_obj_{step}"
        val = float((step + 1) * 10)
        t_a = _FakeTensor(shape=(128, 256), device="tpu:0", value=val)
        t_b = _FakeTensor(shape=(256,), device="tpu:0", value=val + 1.0)

        meta = sender.extract_tensor_transport_metadata(obj_id, [t_a, t_b])
        fetched = receiver.recv_multiple_tensors(obj_id, meta, comm_meta)
        assert len(fetched) == 2
        assert fetched[0].value == val
        assert fetched[1].value == val + 1.0
        sender.garbage_collect(obj_id, meta, [t_a, t_b])

    assert len(fake_tpu_env.ws_instances) == 2
    src_ws, dst_ws = fake_tpu_env.ws_instances[0], fake_tpu_env.ws_instances[1]
    assert src_ws.bind_weights_calls == 2
    assert dst_ws.bind_weights_calls == 2
    assert src_ws.unbind_weights_calls == 3
    assert dst_ws.unbind_weights_calls == 3

    assert fake_tpu_env.empty_calls == 6
    assert fake_tpu_env.zeros_calls == 0

    assert receiver._transfer_pool._total_workers == 1

    assert len(fake_tpu_env.registered_units) == 2
    assert len(fake_tpu_env.coordinated_transfers) == 3
    assert len(fake_tpu_env.plan_cache_keys) == 1
    for tr in fake_tpu_env.coordinated_transfers:
        assert 1 <= tr["uuid"] < (1 << 63)


def test_transfer_uuid_uniqueness_under_random_seed(fake_tpu_env):
    """Verifies that seeding random.seed(0) across multiple transports produces unique UUIDs and never skips D2H."""
    sender = TpuSyncTensorTransport()
    receiver_a = TpuSyncTensorTransport()
    receiver_b = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    seen_uuids: Set[int] = set()
    for step in range(4):
        val = float((step + 1) * 100)
        t = _FakeTensor(shape=(64, 64), device="tpu:0", value=val)
        meta = sender.extract_tensor_transport_metadata(f"seed_obj_{step}", [t])

        random.seed(0)
        res_a = receiver_a.recv_multiple_tensors(f"seed_obj_{step}", meta, comm_meta)
        random.seed(0)
        res_b = receiver_b.recv_multiple_tensors(f"seed_obj_{step}", meta, comm_meta)

        assert res_a[0].value == val
        assert res_b[0].value == val
        sender.garbage_collect(f"seed_obj_{step}", meta, [t])

    for tr in fake_tpu_env.coordinated_transfers:
        seen_uuids.add(tr["uuid"])
    assert len(seen_uuids) == len(fake_tpu_env.coordinated_transfers) == 8


def test_per_slot_job_name_and_real_raiden_controller_unregister(
    fake_tpu_env, monkeypatch
):
    """Verifies each slot gets its own unique job_name and unregistering one slot never wipes endpoints of another slot on the same worker (R4-H1, R4-M7)."""
    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    # Create 5 concurrent objects with the same shape on the same sender worker.
    # Slots 0..4 each get their own unique job_name.
    num_objs = _MAX_CACHED_SLOTS_PER_SIGNATURE + 1
    tensors = [
        _FakeTensor(shape=(32, 32), device="tpu:0", value=float(10 + i))
        for i in range(num_objs)
    ]
    metas = [
        sender.extract_tensor_transport_metadata(f"obj_h1_{i}", [tensors[i]])
        for i in range(num_objs)
    ]
    job_names = [m.src_job_name for m in metas]
    assert len(set(job_names)) == num_objs
    for i, m in enumerate(metas):
        assert m.src_job_name == m.src_data_name
        assert m.src_job_name.endswith(f"_{i}")

    # GC objects 0..3 (filling _free_slots up to _MAX_CACHED_SLOTS_PER_SIGNATURE = 4),
    # then create a 6th object (reusing slot 0) and a 7th object, or GC object 0..4 while
    # keeping another object on the same worker live.
    live_t = _FakeTensor(shape=(64, 64), device="tpu:0", value=777.0)
    live_meta = sender.extract_tensor_transport_metadata("obj_h1_live", [live_t])

    for i in range(num_objs):
        sender.garbage_collect(f"obj_h1_{i}", metas[i], [tensors[i]])

    # The 5th slot overflowed _free_slots and was unregistered via _WsmCoordinatorActor.unregister_work_units.
    assert len(fake_tpu_env.unregistered_units) == 1
    # Verify that `obj_h1_live` (and the 4 cached free slots on the same worker) still have
    # intact JobEntity endpoints on the coordinator and can be fetched!
    fetched_live = receiver.recv_multiple_tensors("obj_h1_live", live_meta, comm_meta)
    assert fetched_live[0].value == 777.0

    t_reuse = _FakeTensor(shape=(32, 32), device="tpu:0", value=888.0)
    meta_reuse = sender.extract_tensor_transport_metadata("obj_h1_reuse", [t_reuse])
    fetched_reuse = receiver.recv_multiple_tensors(
        "obj_h1_reuse", meta_reuse, comm_meta
    )
    assert fetched_reuse[0].value == 888.0

    # Also test against the real tpu_sync.rpc.raiden_controller.RaidenController if available
    # (either installed or in sibling ../tpu-sync checkout).
    tpu_sync_repo = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "..", "..", "..", "..", "..", "tpu-sync"
        )
    )
    real_rc_cls = None
    real_id_cls = None
    with monkeypatch.context() as mp:
        for k in list(sys.modules):
            if k == "tpu_sync" or k.startswith("tpu_sync."):
                mp.delitem(sys.modules, k, raising=False)
        if os.path.isdir(os.path.join(tpu_sync_repo, "tpu_sync")):
            mp.syspath_prepend(tpu_sync_repo)
        if "absl" not in sys.modules:
            absl_mod = ModuleType("absl")
            absl_log = SimpleNamespace(
                info=logging.info,
                warning=logging.warning,
                error=logging.error,
                debug=logging.debug,
                vlog=lambda level, msg, *args: None,
            )
            absl_mod.logging = absl_log
            mp.setitem(sys.modules, "absl", absl_mod)
            mp.setitem(sys.modules, "absl.logging", absl_log)
        try:
            from tpu_sync.rpc.raiden_controller import (
                RaidenController as _RC,
                RaidenId as _RId,
            )

            real_rc_cls, real_id_cls = _RC, _RId
        except ImportError:
            # Provide minimal proto/C-extension stubs if running against pure-Python source checkout
            for mod_name in (
                "tpu_sync.proto",
                "tpu_sync.proto.control_pipe_pb2",
                "tpu_sync.common.control_pipe",
                "tpu_sync.common.control_pipe.control_pipe_client",
                "tpu_sync.kv_cache",
                "tpu_sync.kv_cache.nd_slice_math",
                "tpu_sync.rpc.controller_service_pb2",
                "tpu_sync.rpc.raiden_service_pb2",
            ):
                mp.setitem(sys.modules, mod_name, ModuleType(mod_name))
            sys.modules[
                "tpu_sync.common.control_pipe.control_pipe_client"
            ].ControlPipeClient = lambda *a, **kw: SimpleNamespace()
            sys.modules[
                "tpu_sync.rpc.raiden_service_pb2"
            ].HOST_TILING_MODE_UNSPECIFIED = 0
            sys.modules[
                "tpu_sync.rpc.raiden_service_pb2"
            ].VariableMetadataProto = SimpleNamespace
            try:
                from tpu_sync.rpc.raiden_controller import (
                    RaidenController as _RC,
                    RaidenId as _RId,
                )

                real_rc_cls, real_id_cls = _RC, _RId
            except Exception:
                real_rc_cls = None

        if real_rc_cls is not None and real_id_cls is not None:
            real_controller = real_rc_cls(port=0)
            u1 = real_id_cls(
                job_name="rdt_src_worker1_sig_0",
                job_replica_id="0",
                data_name="rdt_src_worker1_sig_0",
            )
            u2 = real_id_cls(
                job_name="rdt_src_worker1_sig_1",
                job_replica_id="0",
                data_name="rdt_src_worker1_sig_1",
            )
            var_proto = SimpleNamespace(
                name="tensor_0",
                shape=[16],
                mesh_shape=[1],
                layout=[0],
                item_size=2,
                layer_idx=0,
                sharding_spec=[""],
            )
            real_controller.register_work_unit(
                unit=u1,
                shards=["10.0.0.1:41000"],
                control_plane_rpc_address="10.0.0.1:41001",
                mesh_shape=[1],
                mesh_axes=["fsdp"],
                variables=[var_proto],
                host_subgrid=[1],
            )
            real_controller.register_work_unit(
                unit=u2,
                shards=["10.0.0.1:41002"],
                control_plane_rpc_address="10.0.0.1:41003",
                mesh_shape=[1],
                mesh_axes=["fsdp"],
                variables=[var_proto],
                host_subgrid=[1],
            )
            real_coord = _WsmCoordinatorActor.__new__(_WsmCoordinatorActor)
            real_coord._wsm = SimpleNamespace(_controller=real_controller)

            assert real_coord.unregister_work_units([u1]) == 1
            assert real_controller.get_entity(u1) is None
            assert u1 not in real_controller._registered_shards
            ent2 = real_controller.get_entity(u2)
            assert ent2 is not None
            assert asyncio.run(ent2.resolve_endpoints("0")) == ["10.0.0.1:41003"]


def test_no_ttl_or_cap_eviction_of_idle_units_and_no_activity_hooks(fake_tpu_env):
    """Verifies idle live units and cached free slots are never evicted by age or cap and no activity hooks exist (R4-H2, R4-M6)."""
    assert not hasattr(_WsmCoordinatorActor, "_install_activity_hooks")
    assert not hasattr(_WsmCoordinatorActor, "evict_stale_units")
    assert not hasattr(tpu_sync_mod, "_COORDINATOR_UNIT_TTL_S")
    assert not hasattr(tpu_sync_mod, "_MAX_COORDINATOR_REGISTERED_UNITS")

    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    # Create a long-lived RDT object once, then run >16 slot creations/reuses within the
    # signature limit, and verify the long-lived idle object is still registered and fetchable.
    long_lived_t = _FakeTensor(shape=(128, 128), device="tpu:0", value=314.0)
    long_lived_meta = sender.extract_tensor_transport_metadata(
        "long_lived_obj", [long_lived_t]
    )

    for i in range(20):
        sig_idx = i % (_MAX_CACHED_SIGNATURES - 2)
        t_step = _FakeTensor(shape=(8 + sig_idx, 8), device="tpu:0", value=float(i))
        m_step = sender.extract_tensor_transport_metadata(f"step_obj_{i}", [t_step])
        res = receiver.recv_multiple_tensors(f"step_obj_{i}", m_step, comm_meta)
        assert res[0].value == float(i)
        sender.garbage_collect(f"step_obj_{i}", m_step, [t_step])

    assert len(fake_tpu_env.unregistered_units) == 0
    fetched_long_lived = receiver.recv_multiple_tensors(
        "long_lived_obj", long_lived_meta, comm_meta
    )
    assert fetched_long_lived[0].value == 314.0


def test_abort_and_close_defer_teardown_until_wait_for_transfer_completion_returns(
    fake_tpu_env,
):
    """Verifies aborting after coordinate_transfer starts does not call unbind_weights() or shutdown() until wait_for_transfer_completion returns (R4-H3)."""
    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    t = _FakeTensor(shape=(48, 48), device="tpu:0", value=55.0)
    meta = sender.extract_tensor_transport_metadata("obj_abort_wait", [t])

    entered_wait_ev = threading.Event()
    release_wait_ev = threading.Event()
    fake_tpu_env.wait_completion_entered_event = entered_wait_ev
    fake_tpu_env.wait_completion_release_event = release_wait_ev

    try:
        req = receiver.fetch_multiple_tensors("obj_abort_wait", meta, comm_meta)
        recv_slot = req.receiver_slot
        assert recv_slot is not None
        recv_ws = recv_slot.ws

        # Wait until coordinate_transfer has returned and _run_transfer is blocked inside
        # dst_ws.wait_for_transfer_completion(transfer_uuid).
        assert entered_wait_ev.wait(timeout=5.0)
        assert recv_slot._in_flight == 1

        unreg_before = len(fake_tpu_env.unregistered_units)
        receiver.abort_transport("obj_abort_wait", comm_meta)
        with pytest.raises(RayDirectTransportError, match="aborted"):
            receiver.wait_fetch_complete(req)

        # Even though abort_transport and wait_fetch_complete have both completed and
        # marked recv_slot closed, wait_for_transfer_completion is still in flight, so
        # unbind_weights(), shutdown(), and WSM unregistration MUST NOT have been called yet!
        assert recv_slot.is_closed is True
        assert recv_slot._in_flight == 1
        assert recv_ws.unbind_weights_calls == 0
        assert recv_ws.shutdown_calls == 0
        assert recv_ws.is_shutdown is False
        assert recv_ws.device_tensors is not None
        assert len(fake_tpu_env.unregistered_units) == unreg_before

        # Now let wait_for_transfer_completion return and verify deferred teardown and unregistration run once.
        release_wait_ev.set()
        deadline = time.monotonic() + 5.0
        while recv_slot._in_flight > 0 and time.monotonic() < deadline:
            time.sleep(0.005)
        assert recv_slot._in_flight == 0
        assert recv_ws.unbind_weights_calls == 1
        assert recv_ws.shutdown_calls == 1
        assert recv_ws.is_shutdown is True
        assert recv_ws.device_tensors is None
        assert len(fake_tpu_env.unregistered_units) == unreg_before + 1
    finally:
        release_wait_ev.set()
        fake_tpu_env.wait_completion_entered_event = None
        fake_tpu_env.wait_completion_release_event = None


def test_sender_in_flight_guard_and_unbind_failure_on_garbage_collect(fake_tpu_env):
    """Verifies garbage_collect on an in-flight or failing-unbind sender slot never calls unbind_weights() or unregisters WSM unit while in flight and never reuses/rebinds the slot (R4-H4)."""
    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    # Case 1: Real in-flight transfer via receiver.fetch_multiple_tensors:
    # sender slot is marked in-flight synchronously in fetch_multiple_tensors (before worker
    # starts) and stays in-flight across both coordinate_transfer and wait_for_transfer_completion,
    # deferring both C++ teardown and WSM unregistration when garbage_collect runs early.
    t1 = _FakeTensor(shape=(32, 32), device="tpu:0", value=12.0)
    meta1 = sender.extract_tensor_transport_metadata("obj_src_inflight", [t1])
    src_slot = sender._active_senders["obj_src_inflight"]
    src_ws = src_slot.ws

    entered_coord = threading.Event()
    release_coord = threading.Event()
    entered_wait = threading.Event()
    release_wait = threading.Event()
    fake_tpu_env.coordinate_entered_events[meta1.src_data_name] = entered_coord
    fake_tpu_env.block_obj_ids[meta1.src_data_name] = release_coord
    fake_tpu_env.wait_completion_entered_event = entered_wait
    fake_tpu_env.wait_completion_release_event = release_wait

    try:
        unreg_before = len(fake_tpu_env.unregistered_units)
        req = receiver.fetch_multiple_tensors("obj_src_inflight", meta1, comm_meta)
        # Marked in-flight synchronously before fetch_multiple_tensors returns!
        assert src_slot._in_flight == 1
        assert entered_coord.wait(timeout=5.0)
        assert src_slot._in_flight == 1

        # Run garbage_collect on sender while coordinate_transfer is still in flight!
        sender.garbage_collect("obj_src_inflight", meta1, [t1])
        # unbind_weights(), shutdown(), and WSM unregistration must NOT run while _in_flight > 0,
        # and src_slot must be closed and NOT placed into _free_slots.
        assert src_ws.unbind_weights_calls == 0
        assert src_ws.shutdown_calls == 0
        assert src_slot.is_closed is True
        assert src_slot._unregistered is False
        assert len(fake_tpu_env.unregistered_units) == unreg_before
        assert len(sender._free_slots) == 0

        # Creating another object with the same shape while the push is in flight must
        # allocate a brand-new WeightSynchronizer (slot_idx=1) instead of calling
        # bind_weights() on the busy src_ws!
        t2 = _FakeTensor(shape=(32, 32), device="tpu:0", value=24.0)
        meta2 = sender.extract_tensor_transport_metadata("obj_src_concurrent", [t2])
        assert meta2.src_job_name != meta1.src_job_name
        assert src_ws.bind_weights_calls == 0

        # Release coordinate_transfer and verify src_slot STILL stays _in_flight == 1 while
        # blocked in dst_ws.wait_for_transfer_completion(transfer_uuid)!
        release_coord.set()
        assert entered_wait.wait(timeout=5.0)
        assert src_slot._in_flight == 1
        assert src_ws.unbind_weights_calls == 0
        assert src_ws.shutdown_calls == 0
        assert src_slot._unregistered is False

        # Finish wait_for_transfer_completion: deferred unbind_weights(), shutdown(), and WSM
        # unregistration now run on src_slot.
        release_wait.set()
        res = receiver.wait_fetch_complete(req, timeout=5.0)
        assert res[0].value == 12.0
        assert src_slot._in_flight == 0
        assert src_ws.unbind_weights_calls == 1
        assert src_ws.shutdown_calls == 1
        assert src_ws.is_shutdown is True
        assert src_slot._unregistered is True
        assert len(fake_tpu_env.unregistered_units) == unreg_before + 1
        sender.garbage_collect("obj_src_concurrent", meta2, [t2])
    finally:
        release_coord.set()
        release_wait.set()
        fake_tpu_env.coordinate_entered_events.clear()
        fake_tpu_env.block_obj_ids.clear()
        fake_tpu_env.wait_completion_entered_event = None
        fake_tpu_env.wait_completion_release_event = None

    # Case 2: Duplicate obj_id in extract_tensor_transport_metadata retires the old slot
    # via _retire_slot rather than calling bind_weights in-place under lock.
    sender.clear_cached_synchronizers()
    t_dup1 = _FakeTensor(shape=(32, 32), device="tpu:0", value=50.0)
    meta_dup1 = sender.extract_tensor_transport_metadata("obj_dup", [t_dup1])
    dup_slot1 = sender._active_senders["obj_dup"]
    assert dup_slot1.mark_in_flight() is True
    t_dup2 = _FakeTensor(shape=(32, 32), device="tpu:0", value=60.0)
    meta_dup2 = sender.extract_tensor_transport_metadata("obj_dup", [t_dup2])
    assert meta_dup2.src_job_name != meta_dup1.src_job_name
    assert dup_slot1.is_closed is True
    assert dup_slot1.ws.bind_weights_calls == 0
    dup_slot1.finish_in_flight()
    sender.garbage_collect("obj_dup", meta_dup2, [t_dup2])

    # Case 3: If ws.unbind_weights() raises an exception on GC, the slot is marked closed,
    # shut down, unregistered, and never placed in _free_slots for bind_weights() reuse.
    sender.clear_cached_synchronizers()
    t3 = _FakeTensor(shape=(32, 32), device="tpu:0", value=36.0)
    meta3 = sender.extract_tensor_transport_metadata("obj_unbind_err", [t3])
    slot3 = sender._active_senders["obj_unbind_err"]
    ws3 = slot3.ws
    unreg_before = len(fake_tpu_env.unregistered_units)
    fake_tpu_env.unbind_error = RuntimeError("C++ push still active")
    try:
        sender.garbage_collect("obj_unbind_err", meta3, [t3])
    finally:
        fake_tpu_env.unbind_error = None
    assert slot3.is_closed is True
    assert ws3.is_shutdown is True
    assert len(sender._free_slots) == 0
    assert len(fake_tpu_env.unregistered_units) == unreg_before + 1

    t4 = _FakeTensor(shape=(32, 32), device="tpu:0", value=48.0)
    meta4 = sender.extract_tensor_transport_metadata("obj_after_unbind_err", [t4])
    assert meta4.src_job_name != meta3.src_job_name
    assert ws3.bind_weights_calls == 0
    sender.garbage_collect("obj_after_unbind_err", meta4, [t4])


def test_live_object_not_overwritten_after_single_push(fake_tpu_env):
    """Verifies that a live ObjectRef is never rebound to a new object even after being fetched."""
    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    t1 = _FakeTensor(shape=(64, 64), device="tpu:0", value=111.0)
    meta1 = sender.extract_tensor_transport_metadata("live_obj_1", [t1])

    fetched_1a = receiver.recv_multiple_tensors("live_obj_1", meta1, comm_meta)
    assert fetched_1a[0].value == 111.0

    t2 = _FakeTensor(shape=(64, 64), device="tpu:0", value=222.0)
    meta2 = sender.extract_tensor_transport_metadata("live_obj_2", [t2])
    assert meta2.src_data_name != meta1.src_data_name

    fetched_2 = receiver.recv_multiple_tensors("live_obj_2", meta2, comm_meta)
    fetched_1b = receiver.recv_multiple_tensors("live_obj_1", meta1, comm_meta)
    assert fetched_2[0].value == 222.0
    assert fetched_1b[0].value == 111.0


def test_unique_slot_ids_under_concurrency_and_overflow(fake_tpu_env):
    """Verifies no two objects share a RaidenId under concurrency or when exceeding 4 cached slots."""
    sender = TpuSyncTensorTransport()
    num_objects = _MAX_CACHED_SLOTS_PER_SIGNATURE + 4  # 8 concurrent live objects
    metas: List[TpuSyncTransportMetadata] = [None] * num_objects  # type: ignore
    tensors = [
        _FakeTensor(shape=(64, 64), device="tpu:0", value=float(i))
        for i in range(num_objects)
    ]

    barrier = threading.Barrier(num_objects)

    def _extract(idx: int) -> None:
        barrier.wait()
        metas[idx] = sender.extract_tensor_transport_metadata(
            f"obj_conc_{idx}", [tensors[idx]]
        )

    threads = [threading.Thread(target=_extract, args=(i,)) for i in range(num_objects)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    data_names = [m.src_data_name for m in metas]
    assert len(set(data_names)) == num_objects

    for i in range(num_objects):
        sender.garbage_collect(f"obj_conc_{i}", metas[i], [tensors[i]])

    sig_key = _compute_tensor_signature(metas[0].tensor_meta)
    free_slots = sender._free_slots[("src", "127.0.0.1:50051", sig_key)]
    assert len(free_slots) == _MAX_CACHED_SLOTS_PER_SIGNATURE
    assert len(fake_tpu_env.unregistered_units) == (
        num_objects - _MAX_CACHED_SLOTS_PER_SIGNATURE
    )


def test_abort_interrupts_blocking_wait_and_stuck_transfers_do_not_starve_pool(
    fake_tpu_env,
):
    """Verifies abort_transport wakes a blocked wait_fetch_complete and >4 stuck transfers don't starve new ones."""
    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    stuck_events = []
    stuck_reqs = []
    for i in range(5):
        t_stuck = _FakeTensor(shape=(16 + i, 16), device="tpu:0", value=float(i))
        meta_stuck = sender.extract_tensor_transport_metadata(
            f"obj_stuck_{i}", [t_stuck]
        )
        ev = threading.Event()
        fake_tpu_env.block_obj_ids[meta_stuck.src_data_name] = ev
        stuck_events.append(ev)
        stuck_reqs.append(
            receiver.fetch_multiple_tensors(f"obj_stuck_{i}", meta_stuck, comm_meta)
        )

    try:

        def _trigger_abort():
            time.sleep(0.03)
            receiver.abort_transport("obj_stuck_0", comm_meta)

        abort_thread = threading.Thread(target=_trigger_abort)
        t_start = time.monotonic()
        abort_thread.start()
        with pytest.raises(RayDirectTransportError, match="aborted"):
            receiver.wait_fetch_complete(stuck_reqs[0], timeout=-1)
        elapsed = time.monotonic() - t_start
        abort_thread.join()
        assert elapsed < 1.0

        t_healthy = _FakeTensor(shape=(32, 32), device="tpu:0", value=42.0)
        meta_healthy = sender.extract_tensor_transport_metadata(
            "obj_healthy", [t_healthy]
        )
        fetched = receiver.recv_multiple_tensors("obj_healthy", meta_healthy, comm_meta)
        assert fetched[0].value == 42.0
    finally:
        for ev in stuck_events:
            ev.set()
        fake_tpu_env.block_obj_ids.clear()
        for req in stuck_reqs:
            req.done_or_aborted_event.wait(timeout=5.0)
            req.cleanup(reusable=False)


def test_timeout_does_not_write_into_caller_target_buffers(fake_tpu_env):
    """Verifies that a timed-out transfer never overwrites caller target_buffers when the background copy finishes."""
    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    src_tensor = _FakeTensor(shape=(32, 32), device="tpu:0", value=88.0)
    meta = sender.extract_tensor_transport_metadata("obj_target_timeout", [src_tensor])

    caller_target = _FakeTensor(shape=(32, 32), device="tpu:0", value=999.0)
    fake_tpu_env.coordinate_delay_s = 0.08
    req = receiver.fetch_multiple_tensors(
        "obj_target_timeout", meta, comm_meta, target_buffers=[caller_target]
    )
    with pytest.raises(TimeoutError, match="timed out"):
        receiver.wait_fetch_complete(req, timeout=0.01)

    req.cleanup(reusable=False)
    req.done_or_aborted_event.wait(timeout=5.0)
    assert caller_target.value == 999.0
    fake_tpu_env.coordinate_delay_s = 0.0

    caller_target_ok = _FakeTensor(shape=(32, 32), device="tpu:0", value=999.0)
    res = receiver.recv_multiple_tensors(
        "obj_target_ok", meta, comm_meta, target_buffers=[caller_target_ok]
    )
    assert res[0] is caller_target_ok
    assert caller_target_ok.value == 88.0


def test_wait_fetch_complete_non_destructive_polling_timeout_zero(fake_tpu_env):
    """Verifies polling wait_fetch_complete(req, timeout=0) does not destroy in-flight fetch_request."""
    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    src_tensor = _FakeTensor(shape=(32, 32), device="tpu:0", value=73.0)
    meta = sender.extract_tensor_transport_metadata("obj_poll", [src_tensor])
    gate_ev = threading.Event()
    fake_tpu_env.block_obj_ids[meta.src_data_name] = gate_ev

    target_buf = _FakeTensor(shape=(32, 32), device="tpu:0", value=0.0)
    req = receiver.fetch_multiple_tensors(
        "obj_poll", meta, comm_meta, target_buffers=[target_buf]
    )
    try:
        with pytest.raises(TimeoutError, match="timed out"):
            receiver.wait_fetch_complete(req, timeout=0)
        assert req._cleaned_up is False
        assert req.target_buffers is not None
        assert req.receiver_slot is not None
        assert not req.receiver_slot.is_closed

        gate_ev.set()
        res = receiver.wait_fetch_complete(req, timeout=5.0)
        assert res[0] is target_buf
        assert target_buf.value == 73.0
        assert req._cleaned_up is True

        with pytest.raises(RayDirectTransportError, match="already been cleaned up"):
            receiver.wait_fetch_complete(req, timeout=0)
    finally:
        gate_ev.set()
        fake_tpu_env.block_obj_ids.clear()


def test_coordinator_fail_fast_max_restarts_zero_and_node_affinity(
    fake_tpu_env, monkeypatch
):
    """Verifies coordinator actor uses max_restarts=0, fails fast on coordinator errors without receiver-side source re-registration, and selects TPU node affinity (R4-M9)."""
    captured_options: Dict[str, Any] = {}

    class _FakeRemoteActor:
        def options(self, **kwargs):
            captured_options.update(kwargs)
            return SimpleNamespace(
                remote=lambda: SimpleNamespace(
                    get_address=SimpleNamespace(remote=lambda: "10.0.0.9:50001")
                )
            )

    monkeypatch.delenv("RAY_TPU_SYNC_MANAGER_ADDRESS", raising=False)
    monkeypatch.setattr(
        ray,
        "get_actor",
        lambda *a, **kw: (_ for _ in ()).throw(ValueError("not found")),
    )
    monkeypatch.setattr(ray, "remote", lambda **kw: lambda cls: _FakeRemoteActor())
    monkeypatch.setattr(ray, "get", lambda val, **kw: val)
    assert _get_or_create_cluster_wsm_address() == "10.0.0.9:50001"
    assert captured_options["max_restarts"] == 0
    assert captured_options["max_task_retries"] == 0

    sender = TpuSyncTensorTransport(manager_address="127.0.0.1:50051")
    receiver = TpuSyncTensorTransport(manager_address="127.0.0.1:50051")
    comm_meta = sender.get_communicator_metadata(None, None)

    t_warm = _FakeTensor(shape=(16, 16), device="tpu:0", value=10.0)
    meta_warm = sender.extract_tensor_transport_metadata("obj_warm", [t_warm])
    assert (
        receiver.recv_multiple_tensors("obj_warm", meta_warm, comm_meta)[0].value
        == 10.0
    )
    sig_key = _compute_tensor_signature(meta_warm.tensor_meta)
    cached_slot = receiver._free_slots[("dst", "127.0.0.1:50051", sig_key)][0]

    # When coordinator is unreachable or fails, transfer fails fast with RayDirectTransportError
    # without receiver-side source re-registration and without evicting cached free slots.
    t_other = _FakeTensor(shape=(32, 32), device="tpu:0", value=20.0)
    meta_other = sender.extract_tensor_transport_metadata("obj_other", [t_other])
    reg_count_before = len(fake_tpu_env.registered_units)
    fake_tpu_env.coordinate_error = RuntimeError("coordinator unreachable")
    try:
        with pytest.raises(RayDirectTransportError, match="coordinator unreachable"):
            receiver.recv_multiple_tensors("obj_other", meta_other, comm_meta)
    finally:
        fake_tpu_env.coordinate_error = None

    # Only 1 receiver unit was registered (no receiver-side source unit re-registration)
    assert len(fake_tpu_env.registered_units) == reg_count_before + 1
    assert receiver._free_slots[("dst", "127.0.0.1:50051", sig_key)][0] is cached_slot
    assert cached_slot.is_closed is False

    # Verify _select_coordinator_node_id prefers a live node with TPU > 0 in ray.nodes()
    monkeypatch.setattr(ray, "is_initialized", lambda: True)
    monkeypatch.setattr(
        ray,
        "nodes",
        lambda: [
            {"NodeID": "cpu_head_node", "Alive": True, "Resources": {"CPU": 8}},
            {
                "NodeID": "tpu_worker_node",
                "Alive": True,
                "Resources": {"CPU": 4, "TPU": 4},
            },
        ],
    )
    monkeypatch.setattr(
        ray,
        "get_runtime_context",
        lambda: SimpleNamespace(get_node_id=lambda: "cpu_head_node"),
    )
    assert _select_coordinator_node_id() == "tpu_worker_node"

    monkeypatch.setattr(
        ray,
        "nodes",
        lambda: [{"NodeID": "cpu_head_node", "Alive": True, "Resources": {"CPU": 8}}],
    )
    monkeypatch.setattr(tpu_sync_mod, "_is_tpu_sync_importable", lambda: False)
    assert _select_coordinator_node_id() is None


def test_finalizer_deferred_unregistration_no_rpc_in_del(fake_tpu_env, monkeypatch):
    """Verifies FetchRequest.__del__ queues unregistrations without invoking ray.get_actor and drains on the next slot operation (R3-3)."""
    get_actor_calls = []
    orig_get_actor = ray.get_actor
    monkeypatch.setattr(
        ray,
        "get_actor",
        lambda *a, **kw: (get_actor_calls.append((a, kw)), orig_get_actor(*a, **kw))[1],
    )
    transport = TpuSyncTensorTransport()
    t = _FakeTensor(shape=(16, 16), device="tpu:0")
    meta_fin = transport.extract_tensor_transport_metadata("obj_fin", [t])
    comm_meta = transport.get_communicator_metadata(None, None)
    req_fin = transport.fetch_multiple_tensors("obj_fin", meta_fin, comm_meta)
    req_fin.done_or_aborted_event.wait(timeout=5.0)
    get_actor_calls.clear()

    req_fin.__del__()
    assert get_actor_calls == []
    assert len(transport._pending_unregister_slots) == 1
    unreg_before = len(fake_tpu_env.unregistered_units)

    # Next slot retirement drains _pending_unregister_slots
    transport.garbage_collect("obj_fin", meta_fin, [t])
    assert len(transport._pending_unregister_slots) == 0
    assert len(fake_tpu_env.unregistered_units) == unreg_before + 1


def test_worker_pool_warning_above_max_workers_and_back_to_back_submit(
    fake_tpu_env, caplog
):
    """Verifies back-to-back submit(), no starvation above _MAX_TRANSFER_THREADS, and warning log when busy workers exceed max_workers (R4-M5)."""
    pool = _TransferWorkerPool(max_workers=2)
    warmup_done = threading.Event()
    pool.submit(warmup_done.set)
    assert warmup_done.wait(timeout=2.0)
    deadline = time.monotonic() + 2.0
    while pool._idle_workers < 1 and time.monotonic() < deadline:
        time.sleep(0.005)
    assert pool._idle_workers == 1
    assert pool._total_workers == 1

    t1_entered = threading.Event()
    t2_entered = threading.Event()
    t3_entered = threading.Event()
    release_all = threading.Event()

    def _block(ev: threading.Event) -> None:
        ev.set()
        release_all.wait(timeout=5.0)

    with caplog.at_level(logging.WARNING, logger=tpu_sync_mod.logger.name):
        pool.submit(lambda: _block(t1_entered))
        pool.submit(lambda: _block(t2_entered))
        pool.submit(lambda: _block(t3_entered))
        try:
            assert t1_entered.wait(timeout=2.0)
            assert t2_entered.wait(timeout=2.0)
            assert t3_entered.wait(timeout=2.0)
            assert pool._total_workers == 3
            assert any("exceeded soft limit" in rec.message for rec in caplog.records)
        finally:
            release_all.set()

    # > _MAX_TRANSFER_THREADS (70 > 64) stuck transfers must not starve the 71st transfer
    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    num_stuck = _MAX_TRANSFER_THREADS + 6
    stuck_events = []
    stuck_reqs = []
    for i in range(num_stuck):
        t_stuck = _FakeTensor(shape=(16, 16), device="tpu:0", value=float(i))
        meta_stuck = sender.extract_tensor_transport_metadata(
            f"obj_stuck64_{i}", [t_stuck]
        )
        ev = threading.Event()
        fake_tpu_env.block_obj_ids[meta_stuck.src_data_name] = ev
        stuck_events.append(ev)
        stuck_reqs.append(
            receiver.fetch_multiple_tensors(f"obj_stuck64_{i}", meta_stuck, comm_meta)
        )

    try:
        t_ok = _FakeTensor(shape=(32, 32), device="tpu:0", value=314.0)
        meta_ok = sender.extract_tensor_transport_metadata("obj_after_70", [t_ok])
        fetched = receiver.recv_multiple_tensors("obj_after_70", meta_ok, comm_meta)
        assert fetched[0].value == 314.0
    finally:
        for ev in stuck_events:
            ev.set()
        fake_tpu_env.block_obj_ids.clear()
        for req in stuck_reqs:
            req.done_or_aborted_event.wait(timeout=5.0)
            req.cleanup(reusable=False)


def test_clean_subprocess_import_no_circular_import():
    """Verifies importing ray and ray.experimental.rdt.util in a fresh Python interpreter succeeds without circular imports."""
    rdt_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "experimental", "rdt")
    )
    code = (
        "import importlib.abc, importlib.machinery, os, sys\n"
        f"rdt_dir = {rdt_dir!r}\n"
        "class _RdtOverlayFinder(importlib.abc.MetaPathFinder):\n"
        "    def find_spec(self, fullname, path, target=None):\n"
        "        if fullname == 'ray.experimental.rdt':\n"
        "            return importlib.machinery.PathFinder.find_spec(fullname, [os.path.dirname(rdt_dir)])\n"
        "        if fullname.startswith('ray.experimental.rdt.'):\n"
        "            return importlib.machinery.PathFinder.find_spec(fullname, [rdt_dir])\n"
        "        return None\n"
        "sys.meta_path.insert(0, _RdtOverlayFinder())\n"
        "import ray\n"
        "import ray.experimental.rdt.util as u\n"
        "import ray.experimental.rdt.tpu_sync_tensor_transport as t\n"
        "import ray.experimental.rdt.rdt_manager as rm\n"
        "assert 'TPU_SYNC' in u.DEFAULT_TRANSPORTS\n"
        "assert t.TpuSyncTensorTransport.tensor_transport_backend(None) == 'TPU_SYNC'\n"
        "assert hasattr(rm, 'set_target_device_for_ref')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0, f"Subprocess import failed:\n{proc.stderr}"


def test_external_wsm_unregistration_warning_and_lru_signature_eviction(
    fake_tpu_env, monkeypatch, caplog
):
    """Verifies LRU signature eviction, clear_cached_synchronizers, and one-time warning when RAY_TPU_SYNC_MANAGER_ADDRESS is set (R4-M8)."""
    sender = TpuSyncTensorTransport()

    # Create and GC (_MAX_CACHED_SIGNATURES + 3) distinct tensor shapes against cluster coordinator
    total_sigs = _MAX_CACHED_SIGNATURES + 3
    for i in range(total_sigs):
        t = _FakeTensor(shape=(8 + i, 8), device="tpu:0", value=float(i))
        meta = sender.extract_tensor_transport_metadata(f"obj_sig_{i}", [t])
        sender.garbage_collect(f"obj_sig_{i}", meta, [t])

    assert len(sender._free_slots) == _MAX_CACHED_SIGNATURES
    assert len(fake_tpu_env.unregistered_units) == 3

    sender.clear_cached_synchronizers()
    assert len(sender._free_slots) == 0
    assert len(fake_tpu_env.unregistered_units) == total_sigs

    # When RAY_TPU_SYNC_MANAGER_ADDRESS is set, retiring a slot logs a one-time WARNING
    monkeypatch.setenv("RAY_TPU_SYNC_MANAGER_ADDRESS", "127.0.0.1:50055")
    ext_sender = TpuSyncTensorTransport()
    assert ext_sender._get_manager_address() == "127.0.0.1:50055"
    with caplog.at_level(logging.WARNING, logger=tpu_sync_mod.logger.name):
        t_ext = _FakeTensor(shape=(16, 16), device="tpu:0", value=1.0)
        m_ext = ext_sender.extract_tensor_transport_metadata("obj_ext", [t_ext])
        ext_sender.garbage_collect("obj_ext", m_ext, [t_ext])
        ext_sender.clear_cached_synchronizers()
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING
        and "External WeightSynchronizerManager" in r.message
    ]
    assert len(warnings) == 1


def test_fetch_request_del_and_error_wrapping(fake_tpu_env):
    """Verifies FetchRequest.__del__ cleanup, closure reference release, and RPC error wrapping."""
    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)

    t = _FakeTensor(shape=(32, 32), device="tpu:0")
    meta = sender.extract_tensor_transport_metadata("obj_dropped", [t])

    req = receiver.fetch_multiple_tensors("obj_dropped", meta, comm_meta)
    assert "obj_dropped" in receiver._active_fetch_requests
    done_ev = req.done_or_aborted_event
    buf_weak = weakref.ref(req.staging_tensors[0])

    del req
    gc.collect()
    done_ev.wait(timeout=5.0)
    gc.collect()

    assert "obj_dropped" not in receiver._active_fetch_requests
    assert buf_weak() is None
    assert len(receiver._free_slots) == 0

    fake_tpu_env.coordinate_error = RuntimeError("gRPC socket closed")
    try:
        req_err = receiver.fetch_multiple_tensors("obj_rpc_err", meta, comm_meta)
        with pytest.raises(RayDirectTransportError, match="gRPC socket closed"):
            receiver.wait_fetch_complete(req_err)
        req_err.done_or_aborted_event.wait(timeout=5.0)
        assert "obj_rpc_err" not in receiver._active_fetch_requests
    finally:
        fake_tpu_env.coordinate_error = None


def test_config_env_vars_sync_errors_and_target_buffer_validation(
    fake_tpu_env, monkeypatch
):
    """Verifies environment configuration, endpoint resolution, dtype validation, target_buffers validation, and TPU sync error surfacing."""
    monkeypatch.delenv("ENABLE_MULTI_NUMA", raising=False)
    monkeypatch.setenv("RAIDEN_MANAGER_ADDRESS", "127.0.0.1:59999")
    assert _get_configured_manager_address() is None

    monkeypatch.setenv("RAY_TPU_SYNC_PARALLELISM", "8")
    monkeypatch.setenv("RAY_TPU_SYNC_BIND_IP", "10.10.0.5")
    t_cfg = TpuSyncTensorTransport(manager_address="127.0.0.1:50051")
    assert "ENABLE_MULTI_NUMA" not in os.environ
    assert t_cfg._parallelism == 8
    assert t_cfg._bind_ip == "10.10.0.5"

    monkeypatch.setenv("RAY_TPU_SYNC_PARALLELISM", "0")
    with pytest.raises(ValueError, match="RAY_TPU_SYNC_PARALLELISM"):
        TpuSyncTensorTransport(manager_address="127.0.0.1:50051")
    monkeypatch.delenv("RAY_TPU_SYNC_PARALLELISM", raising=False)
    monkeypatch.delenv("RAY_TPU_SYNC_BIND_IP", raising=False)

    monkeypatch.setenv("RAY_TPU_SYNC_CONTROL_IP", "192.168.10.20")
    assert _get_pod_control_ip() == "192.168.10.20"
    fake_multi_nic_ws = SimpleNamespace(
        listener_port=41001,
        local_port=41000,
        get_local_endpoints=lambda: [
            {"endpoint": "10.0.1.1:41000", "shards": [0]},
            {"endpoint": "10.0.2.1:41000", "shards": [0]},
        ],
    )
    ctrl_ep, data_eps = _resolve_ws_endpoints(fake_multi_nic_ws)
    assert ctrl_ep == "192.168.10.20:41001"
    assert data_eps == ["10.0.1.1:41000"]
    monkeypatch.delenv("RAY_TPU_SYNC_CONTROL_IP", raising=False)

    bad_dtype = SimpleNamespace(name="bad_dtype")
    with pytest.raises(ValueError, match="positive integer itemsize"):
        _build_variable_protos_from_tensor_meta([((4, 4), bad_dtype)])

    sender = TpuSyncTensorTransport()
    receiver = TpuSyncTensorTransport()
    comm_meta = sender.get_communicator_metadata(None, None)
    t_src1 = _FakeTensor(shape=(8, 8), device="tpu:0", value=1.0)
    t_src2 = _FakeTensor(shape=(8, 8), device="tpu:0", value=2.0)
    meta = sender.extract_tensor_transport_metadata("obj_val", [t_src1, t_src2])

    with pytest.raises(ValueError, match="Expected 2 target_buffers"):
        receiver.fetch_multiple_tensors(
            "obj_val",
            meta,
            comm_meta,
            target_buffers=[_FakeTensor(shape=(8, 8), device="tpu:0")],
        )
    with pytest.raises(ValueError, match="must reside on 'tpu'"):
        receiver.fetch_multiple_tensors(
            "obj_val",
            meta,
            comm_meta,
            target_buffers=[
                _FakeTensor(shape=(8, 8), device="cpu"),
                _FakeTensor(shape=(8, 8), device="cpu"),
            ],
        )
    with pytest.raises(ValueError, match="same TPU device"):
        receiver.fetch_multiple_tensors(
            "obj_val",
            meta,
            comm_meta,
            target_buffers=[
                _FakeTensor(shape=(8, 8), device="tpu:0"),
                _FakeTensor(shape=(8, 8), device="tpu:1"),
            ],
        )
    with pytest.raises(ValueError, match="shape"):
        receiver.fetch_multiple_tensors(
            "obj_val",
            meta,
            comm_meta,
            target_buffers=[
                _FakeTensor(shape=(8, 8), device="tpu:0"),
                _FakeTensor(shape=(4, 4), device="tpu:0"),
            ],
        )
    with pytest.raises(ValueError, match="dtype"):
        receiver.fetch_multiple_tensors(
            "obj_val",
            meta,
            comm_meta,
            target_buffers=[
                _FakeTensor(shape=(8, 8), device="tpu:0"),
                _FakeTensor(
                    shape=(8, 8),
                    dtype=_FakeDtype("torch.float32", 4),
                    device="tpu:0",
                ),
            ],
        )

    class _FakeActorHandle:
        def __init__(self):
            self.__ray_call__ = self

        def options(self, **kwargs):
            return self

        def remote(self, fn):
            return fn(self)

    monkeypatch.setattr(ray, "get", lambda val: val)
    fake_actor = _FakeActorHandle()
    assert sender.actor_has_tensor_transport(fake_actor) is True
    sys.modules["torch"].tpu.is_available = lambda: False
    assert sender.actor_has_tensor_transport(fake_actor) is False
    sys.modules["torch"].tpu.is_available = lambda: True

    def _failing_sync(tensors, wait=True):
        raise RuntimeError("PJRT HBM sync error")

    monkeypatch.setattr(
        sys.modules["torch_tpu._internal"].sync, "synchronize", _failing_sync
    )
    with pytest.raises(RayDirectTransportError, match="PJRT HBM sync error"):
        _synchronize_tpu_tensors([t_src1])


def _has_tpu_sync_hardware() -> bool:
    if (
        os.environ.get("RAY_TPU_SYNC_TEST_CLUSTER") == "1"
        or os.environ.get("RAY_TPU_SYNC_RUN_HARDWARE_TESTS") == "1"
    ):
        return True
    try:
        import torch
        import torch_tpu  # noqa: F401
        import tpu_sync  # noqa: F401

        return bool(
            hasattr(torch, "tpu")
            and callable(getattr(torch.tpu, "is_available", None))
            and torch.tpu.is_available()
        )
    except Exception:
        return False


@pytest.fixture
def tpu_sync_ray_cluster():
    if ray.is_initialized():
        yield None
        return
    if (
        os.environ.get("RAY_TPU_SYNC_TEST_CLUSTER") == "1"
        or os.environ.get("RAY_TPU_SYNC_RUN_HARDWARE_TESTS") == "1"
    ):
        info = ray.init(address="auto", ignore_reinit_error=True)
    else:
        info = ray.init("local", num_cpus=2, ignore_reinit_error=True)
    try:
        yield info
    finally:
        ray.shutdown()


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_tpu_sync_ray_actors_e2e_ci(ray_start_regular):
    """CI-runnable multi-actor Ray RDT end-to-end test using an emulated TPU_SYNC fabric."""
    import ray.experimental.rdt.util as rdt_util
    from ray import cloudpickle
    from ray.experimental import (
        set_target_for_ref,
        wait_tensor_freed,
    )

    rdt_util._ensure_default_transports_registered()
    orig_info = dict(rdt_util.transport_manager_info)
    orig_managers = dict(rdt_util.transport_managers)
    orig_has_custom = rdt_util.has_custom_transports

    sender = None
    receiver = None
    broker = None

    try:
        rdt_util.transport_manager_info.pop("TPU_SYNC", None)
        rdt_util.transport_managers.pop("TPU_SYNC", None)
        rdt_util.register_tensor_transport(
            "TPU_SYNC", ["tpu"], TpuSyncTensorTransport, _FakeTensor
        )
        rdt_util.has_custom_transports = True
        cloudpickle.register_pickle_by_value(tpu_sync_mod)
        cloudpickle.register_pickle_by_value(sys.modules[__name__])

        @ray.remote
        class FabricBroker:
            def __init__(self):
                self.endpoints: Dict[str, List[float]] = {}
                self.unit_shards: Dict[Tuple[str, str, str], str] = {}
                self.plan_cache: List[Tuple[Any, Any]] = []

            def register_unit(
                self, job_name: str, replica_id: str, data_name: str, shard_ep: str
            ) -> None:
                self.unit_shards[(job_name, replica_id, data_name)] = shard_ep

            def push_stage(self, shard_ep: str, values: List[float]) -> None:
                self.endpoints[shard_ep] = list(values)

            def coordinate(
                self,
                src_key: Tuple[str, str, str],
                dst_key: Tuple[str, str, str],
            ) -> List[float]:
                cache_key = (src_key, dst_key)
                if cache_key not in self.plan_cache:
                    self.plan_cache.append(cache_key)
                src_ep = self.unit_shards[src_key]
                return list(self.endpoints[src_ep])

            def get_plan_cache_size(self) -> int:
                return len(self.plan_cache)

        @ray.remote(enable_tensor_transport=True)
        class EmulatedTpuWorker:
            def __init__(self, broker):
                self.broker = broker
                self._next_port = 50000
                self._ws_by_unit: Dict[Tuple[str, str, str], Any] = {}
                self._install_emulated_tpu_sync()

            def ready(self) -> bool:
                return True

            def _install_emulated_tpu_sync(self):
                worker = self

                class EmuRaidenId:
                    def __init__(self, job_name="", job_replica_id="0", data_name=""):
                        self.job_name = job_name
                        self.job_replica_id = job_replica_id
                        self.data_name = data_name

                    def key(self):
                        return (self.job_name, self.job_replica_id, self.data_name)

                class EmuRaidenMemoryType:
                    DRAM = "DRAM"

                class EmuWeightSynchronizer:
                    def __init__(
                        self,
                        device_tensors=None,
                        local_port=0,
                        listener_port=0,
                        parallelism=4,
                        bind_ip=None,
                        auto_h2d=False,
                    ):
                        self.device_tensors = device_tensors
                        self.local_port = worker._next_port
                        self.listener_port = worker._next_port + 1
                        worker._next_port += 2
                        self.auto_h2d = auto_h2d
                        self.shard_ep = f"127.0.0.1:{self.local_port}"
                        worker._ws_by_unit[("__pending__", "", self.shard_ep)] = self
                        if not self.auto_h2d and self.device_tensors:
                            vals = [float(g[0].value) for g in self.device_tensors]
                            ray.get(
                                worker.broker.push_stage.remote(self.shard_ep, vals)
                            )

                    def bind_weights(self, grouped_tensors):
                        self.device_tensors = grouped_tensors
                        if not self.auto_h2d and self.device_tensors:
                            vals = [float(g[0].value) for g in self.device_tensors]
                            ray.get(
                                worker.broker.push_stage.remote(self.shard_ep, vals)
                            )

                    def unbind_weights(self):
                        self.device_tensors = None

                    def shutdown(self):
                        self.device_tensors = None

                    def get_local_endpoints(self):
                        return [{"endpoint": self.shard_ep, "shards": [0]}]

                    def wait_for_transfer_completion(self, uuid: int):
                        pass

                class EmuWsmClient:
                    def __init__(self, address: str):
                        self.address = address

                    def register_work_unit(self, **kwargs):
                        unit = kwargs["unit"]
                        shard_ep = kwargs["shards"][0]
                        for ws in list(worker._ws_by_unit.values()):
                            if ws.shard_ep == shard_ep:
                                worker._ws_by_unit[unit.key()] = ws
                        ray.get(
                            worker.broker.register_unit.remote(
                                unit.job_name,
                                unit.job_replica_id,
                                unit.data_name,
                                shard_ep,
                            )
                        )

                    def coordinate_transfer(self, **kwargs):
                        src_u = kwargs["src_units"][0]
                        dst_u = kwargs["dst_units"][0]
                        vals = ray.get(
                            worker.broker.coordinate.remote(src_u.key(), dst_u.key())
                        )
                        dst_ws = worker._ws_by_unit[dst_u.key()]
                        for idx, group in enumerate(dst_ws.device_tensors):
                            group[0].value = vals[idx]

                fake_torch = ModuleType("torch")
                fake_torch.Tensor = _FakeTensor
                fake_torch.device = _FakeDevice
                fake_torch.empty = (
                    lambda shape, dtype=None, device="tpu:0": _FakeTensor(
                        shape=shape, dtype=dtype, device=device, value=0.0
                    )
                )
                fake_torch.zeros = (
                    lambda shape, dtype=None, device="tpu:0": _FakeTensor(
                        shape=shape, dtype=dtype, device=device, value=0.0
                    )
                )
                fake_torch.tpu = SimpleNamespace(
                    synchronize=lambda: None,
                    is_available=lambda: True,
                )

                fake_torch_tpu = ModuleType("torch_tpu")
                fake_torch_tpu_internal = ModuleType("torch_tpu._internal")
                fake_torch_tpu_internal.sync = SimpleNamespace(
                    synchronize=lambda tensors, wait=True: None
                )
                fake_torch_tpu._internal = fake_torch_tpu_internal

                fake_tpu_sync = ModuleType("tpu_sync")
                fake_api = ModuleType("tpu_sync.api")
                fake_api_torch = ModuleType("tpu_sync.api.torch")
                fake_ws_mod = ModuleType("tpu_sync.api.torch.weight_synchronizer")
                fake_ws_mod.WeightSynchronizer = EmuWeightSynchronizer
                fake_rpc = ModuleType("tpu_sync.rpc")
                fake_rc_mod = ModuleType("tpu_sync.rpc.raiden_controller")
                fake_rc_mod.RaidenId = EmuRaidenId
                fake_rc_mod.RaidenMemoryType = EmuRaidenMemoryType
                fake_rc_mod.RaidenControllerClientFacade = EmuWsmClient
                fake_pb2_mod = ModuleType("tpu_sync.rpc.raiden_service_pb2")
                fake_pb2_mod.VariableMetadataProto = lambda **kw: SimpleNamespace(**kw)

                for mod_name, mod_obj in {
                    "torch": fake_torch,
                    "torch_tpu": fake_torch_tpu,
                    "torch_tpu._internal": fake_torch_tpu_internal,
                    "tpu_sync": fake_tpu_sync,
                    "tpu_sync.api": fake_api,
                    "tpu_sync.api.torch": fake_api_torch,
                    "tpu_sync.api.torch.weight_synchronizer": fake_ws_mod,
                    "tpu_sync.rpc": fake_rpc,
                    "tpu_sync.rpc.raiden_controller": fake_rc_mod,
                    "tpu_sync.rpc.raiden_service_pb2": fake_pb2_mod,
                }.items():
                    sys.modules[mod_name] = mod_obj

                os.environ["RAY_TPU_SYNC_MANAGER_ADDRESS"] = "127.0.0.1:50099"
                import ray.experimental.rdt.util as worker_rdt_util

                worker_rdt_util._ensure_default_transports_registered()
                worker_rdt_util.transport_manager_info.pop("TPU_SYNC", None)
                worker_rdt_util.transport_managers.pop("TPU_SYNC", None)
                worker_rdt_util.register_tensor_transport(
                    "TPU_SYNC", ["tpu"], TpuSyncTensorTransport, _FakeTensor
                )

            @ray.method(tensor_transport="TPU_SYNC")
            def produce_weights(self, val: float):
                t1 = _FakeTensor(shape=(16, 16), device="tpu:0", value=val)
                t2 = _FakeTensor(shape=(16,), device="tpu:0", value=val + 5.0)
                return [t1, t2]

            def put_weight(self, val: float):
                t = _FakeTensor(shape=(16, 16), device="tpu:0", value=val)
                return ray.put(t, _tensor_transport="TPU_SYNC")

            def consume_weights(self, weights):
                return [float(w.value) for w in weights]

            def pull_with_target(self, ref_list):
                ref = ref_list[0]
                target = _FakeTensor(shape=(16, 16), device="tpu:0", value=0.0)
                set_target_for_ref(ref, [target])
                res = ray.get(ref)
                return float(target.value), float(res.value)

            def check_wait_tensor_freed(self):
                t = _FakeTensor(shape=(8, 8), device="tpu:0", value=1.0)
                ref = ray.put(t, _tensor_transport="TPU_SYNC")
                del ref
                wait_tensor_freed(t, timeout=5.0)
                return True

        broker = FabricBroker.remote()
        sender = EmulatedTpuWorker.remote(broker)
        receiver = EmulatedTpuWorker.remote(broker)
        ray.get([sender.ready.remote(), receiver.ready.remote()])

        for step in range(3):
            base_val = float((step + 1) * 10)
            w_ref = sender.produce_weights.remote(base_val)
            means = ray.get(receiver.consume_weights.remote(w_ref))
            assert means == pytest.approx([base_val, base_val + 5.0])
            del w_ref

        assert ray.get(broker.get_plan_cache_size.remote()) == 1

        put_ref = ray.get(sender.put_weight.remote(42.0))
        target_mean, fetched_mean = ray.get(receiver.pull_with_target.remote([put_ref]))
        assert target_mean == pytest.approx(42.0)
        assert fetched_mean == pytest.approx(42.0)
        del put_ref

        assert ray.get(sender.check_wait_tensor_freed.remote()) is True
    finally:
        for actor in (sender, receiver, broker):
            if actor is not None:
                try:
                    ray.kill(actor)
                except Exception:
                    pass
        cloudpickle.unregister_pickle_by_value(tpu_sync_mod)
        cloudpickle.unregister_pickle_by_value(sys.modules[__name__])
        rdt_util.transport_manager_info.clear()
        rdt_util.transport_manager_info.update(orig_info)
        rdt_util.transport_managers.clear()
        rdt_util.transport_managers.update(orig_managers)
        rdt_util.has_custom_transports = orig_has_custom


@pytest.mark.skipif(
    not _has_tpu_sync_hardware(),
    reason="Requires torch_tpu, tpu_sync, and TPU hardware",
)
def test_tpu_sync_hardware_e2e(tpu_sync_ray_cluster):
    """End-to-end hardware test for one-sided TPU_SYNC transfers and WSM plan cache hits."""
    if ray.cluster_resources().get("TPU", 0) < 2:
        pytest.skip("Ray cluster requires at least 2 TPU resources for hardware e2e")

    from ray.experimental import set_target_for_ref, wait_tensor_freed

    @ray.remote(resources={"TPU": 1}, enable_tensor_transport=True)
    class TpuWorker:
        def __init__(self):
            if os.environ.get("TPU_HOST_BOUNDS") == "1,1,1":
                os.environ.pop("TORCH_TPU_SLICEBUILDER_ADDRESSES", None)
                os.environ.update(
                    {
                        "TPU_WORKER_HOSTNAMES": "localhost",
                        "TPU_WORKER_ID": "0",
                        "TPU_CHIPS_PER_HOST_BOUNDS": "1,1,1",
                        "CHIPS_PER_HOST_BOUNDS": "1,1,1",
                        "TPU_HOST_BOUNDS": "1,1,1",
                        "HOST_BOUNDS": "1,1,1",
                        "TPU_TOPOLOGY": "1x1",
                        "TORCH_TPU_TOPOLOGY": "1,1,1",
                    }
                )
            import torch
            from torch_tpu._internal import sync

            self._torch = torch
            self._sync = sync
            self.device = torch.device("tpu:0")
            t = torch.zeros(1, device=self.device)
            sync.synchronize([t], wait=True)

        @ray.method(tensor_transport="TPU_SYNC")
        def create_weights(self, fill_val: float, shape=(512, 512)):
            t1 = self._torch.full(
                shape, fill_val, dtype=self._torch.bfloat16, device=self.device
            )
            t2 = self._torch.full(
                (shape[0],),
                fill_val + 1.0,
                dtype=self._torch.float32,
                device=self.device,
            )
            self._sync.synchronize([t1, t2], wait=True)
            return [t1, t2]

        @ray.method(tensor_transport="TPU_SYNC")
        def create_scalar(self, fill_val: float):
            t = self._torch.tensor(
                fill_val, dtype=self._torch.float32, device=self.device
            )
            self._sync.synchronize([t], wait=True)
            return t

        def put_weight(self, fill_val: float, shape=(512, 512)):
            t = self._torch.full(
                shape, fill_val, dtype=self._torch.bfloat16, device=self.device
            )
            self._sync.synchronize([t], wait=True)
            return ray.put(t, _tensor_transport="TPU_SYNC")

        def consume_weights(self, weights):
            self._sync.synchronize(weights, wait=True)
            return [float(w.float().cpu().mean().item()) for w in weights]

        def consume_scalar(self, scalar_tensor):
            self._sync.synchronize([scalar_tensor], wait=True)
            return list(scalar_tensor.shape), float(scalar_tensor.float().cpu().item())

        def pull_into_target_buffer(self, ref_holder, shape=(512, 512)):
            ref = ref_holder[0]
            buf = self._torch.zeros(
                shape, dtype=self._torch.bfloat16, device=self.device
            )
            self._sync.synchronize([buf], wait=True)
            set_target_for_ref(ref, [buf])
            fetched = ray.get(ref)
            self._sync.synchronize([fetched], wait=True)
            return float(buf.float().cpu().mean().item())

        def verify_gc(self):
            t = self._torch.ones(
                (128, 128), dtype=self._torch.bfloat16, device=self.device
            )
            self._sync.synchronize([t], wait=True)
            ref = ray.put(t, _tensor_transport="TPU_SYNC")
            del ref
            wait_tensor_freed(t, timeout=10.0)
            return True

    src = TpuWorker.remote()
    dst = TpuWorker.remote()
    try:
        for step in range(3):
            val = float(10 * (step + 1))
            w_ref = src.create_weights.remote(val)
            means = ray.get(dst.consume_weights.remote(w_ref))
            assert means == pytest.approx([val, val + 1.0], abs=1e-2)
            del w_ref

        scalar_ref = src.create_scalar.remote(28.0)
        scalar_shape, scalar_val = ray.get(dst.consume_scalar.remote(scalar_ref))
        assert scalar_shape == []
        assert scalar_val == pytest.approx(28.0, abs=1e-2)
        del scalar_ref

        put_ref = ray.get(src.put_weight.remote(77.0))
        mean_val = ray.get(dst.pull_into_target_buffer.remote([put_ref]))
        assert mean_val == pytest.approx(77.0, abs=1e-2)
        del put_ref

        assert ray.get(src.verify_gc.remote()) is True
    finally:
        for actor in (src, dst):
            try:
                ray.kill(actor)
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
