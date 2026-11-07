import logging
import os
import subprocess
import sys
import threading
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest

import ray
from ray.experimental.rdt.tpu_sync_tensor_transport import (
    TpuSyncCommunicatorMetadata,
    TpuSyncTensorTransport,
    TpuSyncTransportMetadata,
    _select_coordinator_node_id,
)
from ray.experimental.rdt.util import (
    DEFAULT_TRANSPORTS,
    get_tensor_transport_manager,
)

logger = logging.getLogger(__name__)


def _has_tpu_sync_hardware() -> bool:
    return os.environ.get("RAY_TPU_SYNC_TEST_CLUSTER") == "1"


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
    """Lightweight fake tensor for unit tests."""

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
        elem_size = getattr(self.dtype, "itemsize", 2)
        self._element_size = elem_size
        default_nbytes = numel * elem_size
        actual_storage = storage_nbytes if storage_nbytes >= 0 else default_nbytes
        self._storage = _FakeStorage(actual_storage)
        self._value_box = shared_value_box if shared_value_box is not None else [value]

    def is_contiguous(self) -> bool:
        return self._contiguous

    def nelement(self) -> int:
        return self._numel

    def element_size(self) -> int:
        return self._element_size

    def storage_offset(self) -> int:
        return self._storage_offset

    def untyped_storage(self) -> _FakeStorage:
        return self._storage

    def reshape(self, *new_shape: Any) -> "_FakeTensor":
        if len(new_shape) == 1 and isinstance(new_shape[0], (tuple, list)):
            target = tuple(new_shape[0])
        else:
            target = tuple(new_shape)
        return _FakeTensor(
            shape=target,
            dtype=self.dtype,
            device=self.device,
            contiguous=self._contiguous,
            storage_offset=self._storage_offset,
            storage_nbytes=self._storage.nbytes(),
            shared_value_box=self._value_box,
        )

    def clone(self) -> "_FakeTensor":
        return _FakeTensor(
            shape=self.shape,
            dtype=self.dtype,
            device=self.device,
            contiguous=True,
            storage_offset=0,
            value=self._value_box[0],
        )

    def copy_(self, src: "_FakeTensor") -> "_FakeTensor":
        self._value_box[0] = src._value_box[0]
        return self

    def float(self) -> "_FakeTensor":
        return self

    def cpu(self) -> "_FakeTensor":
        return self

    def mean(self) -> "_FakeTensor":
        return _FakeTensor(shape=(), value=self._value_box[0])

    def item(self) -> float:
        return float(self._value_box[0])


class _FakeTpuSyncEnv:
    """In-memory fake of tpu_sync and torch modules for CPU unit testing."""

    def __init__(self):
        self._lock = threading.RLock()
        self.ws_instances: List[Any] = []
        self.registered_units: Dict[str, Any] = {}
        self.coordinated_transfers: List[Dict[str, Any]] = []
        self.plan_cache: Dict[Tuple[str, str], Any] = {}
        self._next_port = 40000

    def install_modules(self) -> Dict[str, ModuleType]:
        env = self

        class FakeRaidenId:
            def __init__(
                self,
                job_name: str = "",
                job_replica_id: str = "0",
                data_name: str = "",
                data_replica_idx: int = 0,
            ):
                self.job_name = job_name
                self.job_replica_id = str(job_replica_id)
                self.data_name = data_name
                self.data_replica_idx = data_replica_idx

            def __eq__(self, other: object) -> bool:
                return (
                    isinstance(other, FakeRaidenId)
                    and self.job_name == other.job_name
                    and self.job_replica_id == other.job_replica_id
                    and self.data_name == other.data_name
                )

            def __hash__(self) -> int:
                return hash((self.job_name, self.job_replica_id, self.data_name))

            def __repr__(self) -> str:
                return f"RaidenId({self.job_name}, {self.job_replica_id}, {self.data_name})"

        class FakeRaidenMemoryType:
            DRAM = 1

        class FakeWeightSynchronizer:
            def __init__(
                self,
                device_tensors: List[List[Any]],
                local_port: Optional[int] = None,
                listener_port: Optional[int] = None,
                parallelism: int = 1,
                bind_ip: Optional[str] = None,
                auto_h2d: bool = False,
                **kwargs,
            ):
                self.device_tensors = device_tensors
                self.auto_h2d = auto_h2d
                with env._lock:
                    self.local_port = local_port or env._next_port
                    env._next_port += 1
                    self.listener_port = listener_port or env._next_port
                    env._next_port += 1
                    env.ws_instances.append(self)
                self.bound_tensors = device_tensors
                self.closed = False
                self.unbound = False
                self.completed_uuids: List[int] = []

            def bind_weights(self, device_tensors: List[List[Any]]) -> None:
                self.bound_tensors = device_tensors
                self.unbound = False

            def unbind_weights(self) -> None:
                self.unbound = True
                self.bound_tensors = []

            def shutdown(self) -> None:
                self.closed = True

            def get_local_endpoints(self) -> List[Dict[str, Any]]:
                return [{"endpoint": f"127.0.0.1:{self.local_port}", "shards": [0]}]

            def wait_for_transfer_completion(self, uuid: Optional[int] = None) -> None:
                if uuid is not None:
                    self.completed_uuids.append(uuid)

        class FakeControllerClientFacade:
            def __init__(self, address: str):
                self.address = address

            def register_work_unit(
                self,
                unit: Any,
                shards: List[str],
                control_plane_rpc_address: str = "",
                **kwargs,
            ) -> None:
                with env._lock:
                    env.registered_units[unit.job_name] = {
                        "unit": unit,
                        "shards": shards,
                        "control_plane": control_plane_rpc_address,
                    }

            def coordinate_transfer(
                self,
                src_units: List[Any],
                dst_units: List[Any],
                uuid: int = 0,
                **kwargs,
            ) -> bool:
                with env._lock:
                    transfer_info = {
                        "src": src_units,
                        "dst": dst_units,
                        "uuid": uuid,
                    }
                    env.coordinated_transfers.append(transfer_info)
                    key = (src_units[0].job_name, dst_units[0].job_name)
                    env.plan_cache[key] = transfer_info
                return True

        class FakeWeightSynchronizerManager:
            def __init__(self, port: int = 0, **kwargs):
                self.port = port or 50051

            def get_plan_cache_size(self) -> int:
                return len(env.plan_cache)

            def get_all_metadata(self) -> List[Any]:
                return list(env.registered_units.keys())

        # Build mock module structure
        tpu_sync_mod = ModuleType("tpu_sync")
        api_mod = ModuleType("tpu_sync.api")
        torch_api_mod = ModuleType("tpu_sync.api.torch")
        ws_mod = ModuleType("tpu_sync.api.torch.weight_synchronizer")
        ws_mod.WeightSynchronizer = FakeWeightSynchronizer
        wsm_mod = ModuleType("tpu_sync.api.weight_synchronizer_manager")
        wsm_mod.WeightSynchronizerManager = FakeWeightSynchronizerManager

        rpc_mod = ModuleType("tpu_sync.rpc")
        ctrl_mod = ModuleType("tpu_sync.rpc.raiden_controller")
        ctrl_mod.RaidenId = FakeRaidenId
        ctrl_mod.RaidenMemoryType = FakeRaidenMemoryType
        ctrl_mod.RaidenControllerClientFacade = FakeControllerClientFacade
        ctrl_mod.WeightSyncWorkerRpcClient = lambda name_resolver=None: None

        pb_mod = ModuleType("tpu_sync.rpc.raiden_service_pb2")
        pb_mod.VariableMetadataProto = lambda **kw: SimpleNamespace(**kw)

        sync_mod = ModuleType("torch_tpu._internal.sync")
        sync_mod.synchronize = lambda tensors=None, wait=True: None

        fake_torch = ModuleType("torch")
        fake_torch.Tensor = _FakeTensor
        fake_torch.device = _FakeDevice
        fake_torch.bfloat16 = _FakeDtype("torch.bfloat16", 2)
        fake_torch.float32 = _FakeDtype("torch.float32", 4)
        fake_torch.empty = lambda shape, dtype=None, device="tpu:0": _FakeTensor(
            shape=shape, dtype=dtype, device=device, value=0.0
        )
        fake_torch.zeros = lambda shape, dtype=None, device="tpu:0": _FakeTensor(
            shape=shape, dtype=dtype, device=device, value=0.0
        )

        return {
            "tpu_sync": tpu_sync_mod,
            "tpu_sync.api": api_mod,
            "tpu_sync.api.torch": torch_api_mod,
            "tpu_sync.api.torch.weight_synchronizer": ws_mod,
            "tpu_sync.api.weight_synchronizer_manager": wsm_mod,
            "tpu_sync.rpc": rpc_mod,
            "tpu_sync.rpc.raiden_controller": ctrl_mod,
            "tpu_sync.rpc.raiden_service_pb2": pb_mod,
            "torch_tpu": ModuleType("torch_tpu"),
            "torch_tpu._internal": ModuleType("torch_tpu._internal"),
            "torch_tpu._internal.sync": sync_mod,
            "torch": fake_torch,
        }


@pytest.fixture
def fake_tpu_env(monkeypatch):
    env = _FakeTpuSyncEnv()
    modules = env.install_modules()
    for mod_name, mod in modules.items():
        monkeypatch.setitem(sys.modules, mod_name, mod)
    return env


@pytest.fixture
def tpu_sync_ray_cluster():
    if not _has_tpu_sync_hardware():
        pytest.skip(
            "TPU hardware cluster not configured (RAY_TPU_SYNC_TEST_CLUSTER!=1)"
        )
    import ray._private.worker

    ray._private.worker.global_worker._rdt_manager = None
    ray.init(address="auto")
    try:
        yield
    finally:
        ray.shutdown()
        ray._private.worker.global_worker._rdt_manager = None


def test_tpu_sync_transport_contract():
    """Verify that TpuSyncTensorTransport implements the required interface contracts."""
    assert TpuSyncTensorTransport.is_one_sided() is True
    assert TpuSyncTensorTransport.can_abort_transport() is False
    transport = TpuSyncTensorTransport()
    assert transport.tensor_transport_backend() == "TPU_SYNC"
    assert transport.actor_has_tensor_transport(None) is True
    with pytest.raises(NotImplementedError):
        transport.send_multiple_tensors([], None, None)


def test_tpu_sync_registration():
    """Verify TPU_SYNC is registered in DEFAULT_TRANSPORTS and transport_managers."""
    assert "TPU_SYNC" in DEFAULT_TRANSPORTS
    manager = get_tensor_transport_manager("TPU_SYNC")
    assert isinstance(manager, TpuSyncTensorTransport)


def test_extract_metadata_validation(fake_tpu_env):
    """Test tensor validation rules in extract_tensor_transport_metadata."""
    transport = TpuSyncTensorTransport(manager_address="127.0.0.1:50051")

    # Empty list
    with pytest.raises(ValueError, match="non-empty"):
        transport.extract_tensor_transport_metadata("obj_1", [])

    # Non-TPU tensor
    cpu_tensor = _FakeTensor(device="cpu")
    with pytest.raises(ValueError, match="only supports 'tpu' tensors"):
        transport.extract_tensor_transport_metadata("obj_cpu", [cpu_tensor])

    # Contiguous requirement
    non_contig = _FakeTensor(contiguous=False)
    with pytest.raises(ValueError, match="contiguous"):
        transport.extract_tensor_transport_metadata("obj_2", [non_contig])

    # Zero elements
    zero_elem = _FakeTensor(shape=(0, 4))
    with pytest.raises(ValueError, match="zero-element"):
        transport.extract_tensor_transport_metadata("obj_3", [zero_elem])

    # Storage offset != 0
    offset_view = _FakeTensor(storage_offset=4)
    with pytest.raises(ValueError, match="storage_offset"):
        transport.extract_tensor_transport_metadata("obj_4", [offset_view])

    # Partial storage view
    partial_view = _FakeTensor(shape=(4, 4), storage_nbytes=1024)
    with pytest.raises(ValueError, match="underlying storage"):
        transport.extract_tensor_transport_metadata("obj_5", [partial_view])

    # Valid tensors
    t1 = _FakeTensor(shape=(16, 16), device="tpu:0", value=1.0)
    meta = transport.extract_tensor_transport_metadata("obj_valid", [t1])
    assert isinstance(meta, TpuSyncTransportMetadata)
    assert meta.tensor_device == "tpu"
    assert len(meta.tensor_meta) == 1
    assert meta.tensor_meta[0][0] == (16, 16)


def test_0d_scalar_tensor_handling(fake_tpu_env):
    """Verify that 0-D scalar tensors are cloned to 1-D for WeightSynchronizer."""
    transport = TpuSyncTensorTransport(manager_address="127.0.0.1:50051")
    scalar_t = _FakeTensor(shape=(), device="tpu:0", value=42.0)
    meta = transport.extract_tensor_transport_metadata("obj_scalar", [scalar_t])
    assert isinstance(meta, TpuSyncTransportMetadata)
    assert meta.tensor_meta[0][0] == ()
    slot = transport._active_senders["obj_scalar"]
    assert slot.ws.bound_tensors[0][0].shape == (1,)


def test_transfer_uuid_uniqueness():
    """Verify secrets.randbits(63) produces non-zero unique transfer UUIDs."""
    import secrets

    uuids = {secrets.randbits(63) or 1 for _ in range(1000)}
    assert len(uuids) == 1000
    assert 0 not in uuids


def test_slot_caching_and_plan_cache(fake_tpu_env):
    """Verify that slots are cached by signature and reused on sender and receiver."""
    transport = TpuSyncTensorTransport(manager_address="127.0.0.1:50051")
    t1 = _FakeTensor(shape=(32, 32), device="tpu:0", value=10.0)

    # First extract creates a slot and registers unit with WSM
    meta1 = transport.extract_tensor_transport_metadata("obj_1", [t1])
    assert len(fake_tpu_env.registered_units) == 1
    slot1 = transport._active_senders["obj_1"]

    # Garbage collect returns slot to free list
    transport.garbage_collect("obj_1", meta1, [t1])
    assert "obj_1" not in transport._active_senders
    assert len(transport._free_slots[slot1.slot_key]) == 1

    # Second extract reuses the cached slot
    t2 = _FakeTensor(shape=(32, 32), device="tpu:0", value=20.0)
    meta2 = transport.extract_tensor_transport_metadata("obj_2", [t2])
    slot2 = transport._active_senders["obj_2"]
    assert slot2 is slot1
    # No new WSM unit registration needed
    assert len(fake_tpu_env.registered_units) == 1

    # Receive
    comm_meta = TpuSyncCommunicatorMetadata()
    recv_tensors = transport.recv_multiple_tensors("obj_2", meta2, comm_meta)
    assert len(recv_tensors) == 1
    assert len(fake_tpu_env.coordinated_transfers) == 1
    assert len(fake_tpu_env.plan_cache) == 1


def test_target_buffers_copy(fake_tpu_env):
    """Verify recv_multiple_tensors receives into target_buffers (in-place for >0-D, copied for 0-D)."""
    transport = TpuSyncTensorTransport(manager_address="127.0.0.1:50051")
    t_src = _FakeTensor(shape=(16, 16), device="tpu:0", value=99.0)
    scalar_src = _FakeTensor(shape=(), device="tpu:0", value=42.0)
    meta = transport.extract_tensor_transport_metadata("obj_src", [t_src, scalar_src])

    target = _FakeTensor(shape=(16, 16), device="tpu:0", value=0.0)
    target_scalar = _FakeTensor(shape=(), device="tpu:0", value=0.0)
    comm_meta = TpuSyncCommunicatorMetadata()
    res = transport.recv_multiple_tensors(
        "obj_src", meta, comm_meta, target_buffers=[target, target_scalar]
    )
    assert res[0] is target
    assert res[1] is target_scalar


def test_tpu_sync_timeout(fake_tpu_env, monkeypatch):
    """Simulate a timeout by mocking wait_for_transfer_completion to raise TimeoutError.

    Verify recv_multiple_tensors propagates the timeout and the slot is correctly retired
    (not leaked to _free_slots).
    """
    transport = TpuSyncTensorTransport(manager_address="127.0.0.1:50051")
    t1 = _FakeTensor(shape=(16, 16), device="tpu:0", value=10.0)
    meta = transport.extract_tensor_transport_metadata("obj_1", [t1])

    ws_mod = sys.modules["tpu_sync.api.torch.weight_synchronizer"]

    def mock_wait(self, uuid=None):
        raise TimeoutError("Transfer timed out waiting for completion")

    monkeypatch.setattr(
        ws_mod.WeightSynchronizer, "wait_for_transfer_completion", mock_wait
    )

    comm_meta = TpuSyncCommunicatorMetadata()
    with pytest.raises(TimeoutError, match="Transfer timed out"):
        transport.recv_multiple_tensors("obj_1", meta, comm_meta)

    # Receiver slot must not be in _free_slots
    assert sum(len(slots) for slots in transport._free_slots.values()) == 0
    # Available slot indices should have returned the dst slot (only 1 active sender slot remains)
    assert len(transport._available_slot_indices) == 63


def test_tpu_sync_bounded_cache(fake_tpu_env):
    """Verify that exceeding _MAX_CACHED_SLOTS signatures correctly evicts old slots and returns their indices to _available_slot_indices."""
    transport = TpuSyncTensorTransport(manager_address="127.0.0.1:50051")
    for i in range(20):
        t = _FakeTensor(shape=(16, i + 1), device="tpu:0", value=float(i))
        meta = transport.extract_tensor_transport_metadata(f"obj_{i}", [t])
        transport.garbage_collect(f"obj_{i}", meta, [t])

    total_cached = sum(len(slots) for slots in transport._free_slots.values())
    assert total_cached == transport._MAX_CACHED_SLOTS == 16
    assert len(transport._available_slot_indices) == 64 - 16
    assert len(fake_tpu_env.registered_units) <= 64


def test_acquire_slot_bind_weights_failure_retires_slot(fake_tpu_env, monkeypatch):
    """Verify bind_weights failure in _acquire_slot retires the broken slot without deadlocking and allocates a fresh slot."""
    transport = TpuSyncTensorTransport(manager_address="127.0.0.1:50051")
    t1 = _FakeTensor(shape=(16, 16), device="tpu:0", value=1.0)
    meta1 = transport.extract_tensor_transport_metadata("obj_1", [t1])
    slot1 = transport._active_senders["obj_1"]
    slot1_ws = slot1.ws
    transport.garbage_collect("obj_1", meta1, [t1])

    def broken_bind(tensors):
        raise RuntimeError("bind_weights failed")

    monkeypatch.setattr(slot1_ws, "bind_weights", broken_bind)

    t2 = _FakeTensor(shape=(16, 16), device="tpu:0", value=2.0)
    meta2 = transport.extract_tensor_transport_metadata("obj_2", [t2])
    slot2 = transport._active_senders["obj_2"]
    assert slot2 is not slot1
    assert slot1._closed is True
    assert slot1_ws.closed is True
    assert len(transport._available_slot_indices) == 63
    transport.garbage_collect("obj_2", meta2, [t2])


def test_select_coordinator_node_id(monkeypatch):
    """Verify _select_coordinator_node_id prefers live TPU nodes and avoids CPU-only nodes without tpu_sync."""
    monkeypatch.setattr(ray, "is_initialized", lambda: True)
    monkeypatch.setattr(
        ray,
        "get_runtime_context",
        lambda: SimpleNamespace(get_node_id=lambda: "cpu_node"),
    )
    monkeypatch.setattr(
        ray,
        "nodes",
        lambda: [
            {"NodeID": "cpu_node", "Alive": True, "Resources": {"CPU": 8}},
            {"NodeID": "dead_tpu", "Alive": False, "Resources": {"TPU": 4}},
            {"NodeID": "tpu_node_1", "Alive": True, "Resources": {"TPU": 4}},
            {"NodeID": "tpu_node_2", "Alive": True, "Resources": {"TPU": 4}},
        ],
    )
    assert _select_coordinator_node_id() == "tpu_node_1"

    # Prefers current node if current node is a live TPU node
    monkeypatch.setattr(
        ray,
        "get_runtime_context",
        lambda: SimpleNamespace(get_node_id=lambda: "tpu_node_2"),
    )
    assert _select_coordinator_node_id() == "tpu_node_2"

    # On CPU-only cluster without tpu_sync importable, returns None
    monkeypatch.setattr(
        ray,
        "nodes",
        lambda: [{"NodeID": "cpu_node", "Alive": True, "Resources": {"CPU": 8}}],
    )
    monkeypatch.setattr(
        "ray.experimental.rdt.tpu_sync_tensor_transport._is_tpu_sync_importable",
        lambda: False,
    )
    assert _select_coordinator_node_id() is None


def test_clean_subprocess_import():
    """Verify importing ray.experimental.rdt.util in a fresh process does not circular import."""
    cmd = [
        sys.executable,
        "-c",
        "import ray.experimental.rdt.util; print('IMPORT_OK')",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    assert "IMPORT_OK" in res.stdout


@pytest.mark.skipif(
    not _has_tpu_sync_hardware(),
    reason="Requires TPU hardware cluster (RAY_TPU_SYNC_TEST_CLUSTER=1)",
)
def test_tpu_sync_hardware_e2e(tpu_sync_ray_cluster):
    """End-to-end hardware test for one-sided TPU_SYNC transfers on real TPUs."""
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

    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    nodes = [
        n
        for n in ray.nodes()
        if n.get("Alive") and n.get("Resources", {}).get("TPU", 0) > 0
    ]
    if len(nodes) >= 2:
        src = TpuWorker.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=nodes[0]["NodeID"], soft=False
            )
        ).remote()
        dst = TpuWorker.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=nodes[1]["NodeID"], soft=False
            )
        ).remote()
    else:
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
        assert scalar_val == pytest.approx(28.0, abs=1e-3)

        put_ref = ray.get(src.put_weight.remote(99.0))
        target_mean = ray.get(dst.pull_into_target_buffer.remote([put_ref]))
        assert target_mean == pytest.approx(99.0, abs=1e-2)

        assert ray.get(src.verify_gc.remote()) is True
    finally:
        ray.kill(src)
        ray.kill(dst)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
