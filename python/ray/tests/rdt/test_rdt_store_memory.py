"""Unit tests for RDTStore's core-worker memory-info publishing.

These tests exercise the scalar-only lifecycle metadata that :class:`RDTStore`
pushes into the core worker (``CoreWorker.set_rdt_object_memory_info``) without
requiring a running Ray cluster. A recording callback stands in for the real
core-worker setter, so no native build is needed to run these tests.
"""
import gc
import sys
import threading
import time
import types
import weakref

import pytest

import ray
from ray.experimental.rdt.rdt_store import RDTStore
from ray.experimental.rdt.tensor_transport_manager import TensorTransportMetadata

OBJ_ID = "00" * ray.ObjectRef.size()
OBJ_ID_2 = "01" * ray.ObjectRef.size()


class _RecordingCallback:
    """Records every ``set_rdt_object_memory_info`` invocation."""

    def __init__(self):
        self.calls = []

    def __call__(self, object_id_bytes, device_bytes, num_copies, num_errors):
        self.calls.append((object_id_bytes, list(device_bytes), num_copies, num_errors))


class _Weakrefable:
    """A minimal object that supports weak references and is not a tensor."""


class _FakeTorchTensor:
    """A stand-in torch.Tensor whose numel/element_size are fully controllable."""

    def __init__(self, numel, element_size=4, device="cpu", fail=False):
        self._numel = numel
        self._element_size = element_size
        self.device = device
        self._fail = fail

    def numel(self):
        if self._fail:
            raise RuntimeError("metadata read failed")
        return self._numel

    def element_size(self):
        return self._element_size


class _FakeTransportManager:
    """Minimal transport manager for ``add_object_primary``."""

    def extract_tensor_transport_metadata(self, obj_id, tensors):
        return TensorTransportMetadata(tensor_meta=[], tensor_device="cpu")


def _inject_fake_torch(monkeypatch):
    fake_torch = types.ModuleType("torch")
    fake_torch.Tensor = _FakeTorchTensor
    monkeypatch.setitem(sys.modules, "torch", fake_torch)


def test_add_pop_sequence_and_final_erase():
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    # An unrecognized object is reported as unknown/bytes -1, never 0, and the
    # add still succeeds.
    store.add_object(OBJ_ID, [object()])
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("unknown", -1)], 1, 0)

    # Popping the last copy erases the native entry (both counts zero).
    store.pop_object(OBJ_ID)
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 0, 0)


def test_primary_retry_does_not_duplicate_counts(monkeypatch):
    import ray.experimental.rdt.rdt_store as rdt_store_module

    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)
    store.add_object(OBJ_ID, [object()], is_primary=True)
    assert callback.calls[-1][2] == 1

    monkeypatch.setattr(
        rdt_store_module,
        "get_tensor_transport_manager",
        lambda backend: _FakeTransportManager(),
    )

    # A retried primary (already present) must not re-store / double-count.
    store.add_object_primary(OBJ_ID, [object()], "test_backend")
    assert callback.calls[-1][2] == 1
    assert store.get_num_objects() == 1


def test_repeated_queued_copies_accumulate():
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    t1 = object()
    t2 = object()
    store.add_object(OBJ_ID, [t1])
    store.add_object(OBJ_ID, [t2])
    # Two queued copies: two unknown tensors, no dedup.
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("unknown", -1)], 2, 0)

    store.pop_object(OBJ_ID)
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("unknown", -1)], 1, 0)

    store.pop_object(OBJ_ID)
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 0, 0)


def test_errors_only():
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    store.add_object(OBJ_ID, RuntimeError("boom"))
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 0, 1)

    with pytest.raises(RuntimeError, match="boom"):
        store.pop_object(OBJ_ID)
    # Removal is published before the queued exception is raised.
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 0, 0)


def test_errors_mixed_with_data():
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    store.add_object(OBJ_ID, RuntimeError("boom"))
    store.add_object(OBJ_ID, [object()])
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("unknown", -1)], 1, 1)

    with pytest.raises(RuntimeError, match="boom"):
        store.pop_object(OBJ_ID)
    # The data copy remains resident after the error is popped.
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("unknown", -1)], 1, 0)

    store.pop_object(OBJ_ID)
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 0, 0)


def test_empty_tensors():
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    # An empty tensor list is a successful copy with zero resident bytes.
    store.add_object(OBJ_ID, [])
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 1, 0)

    store.pop_object(OBJ_ID)
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 0, 0)


def test_zero_sized_tensors_keep_device(monkeypatch):
    _inject_fake_torch(monkeypatch)
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    t1 = _FakeTorchTensor(numel=0, element_size=4)  # 0 bytes on cpu
    t2 = _FakeTorchTensor(numel=0, element_size=4)  # 0 bytes on cpu
    store.add_object(OBJ_ID, [t1])
    store.add_object(OBJ_ID, [t2])
    # Two zero-byte copies: device "cpu" stays present with known 0 bytes.
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("cpu", 0)], 2, 0)

    store.pop_object(OBJ_ID)
    # Popping one still leaves a zero-byte tensor on cpu, so cpu must remain.
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("cpu", 0)], 1, 0)

    store.pop_object(OBJ_ID)
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 0, 0)


def test_per_device_sum_overflow_clamps_and_recovers(monkeypatch):
    _inject_fake_torch(monkeypatch)
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    big = 2**62  # fits int64 (< 2**63 - 1)
    t1 = _FakeTorchTensor(numel=big, element_size=1)  # 2**62 bytes
    t2 = _FakeTorchTensor(numel=big, element_size=1)  # 2**62 bytes

    store.add_object(OBJ_ID, [t1])
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("cpu", 2**62)], 1, 0)

    store.add_object(OBJ_ID, [t2])
    # Sum 2**63 overflows int64 -> published as -1 (unknown), not a stale value.
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("cpu", -1)], 2, 0)

    store.pop_object(OBJ_ID)
    # Removing one large copy recovers the known remaining 2**62 bytes.
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("cpu", 2**62)], 1, 0)

    store.pop_object(OBJ_ID)
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 0, 0)


def test_numpy_device_bytes():
    np = pytest.importorskip("numpy")
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    arr = np.zeros(10, dtype=np.float64)  # 10 * 8 = 80 bytes
    store.add_object(OBJ_ID, [arr])
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("cpu", 80)], 1, 0)

    store.pop_object(OBJ_ID)
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 0, 0)


def test_torch_alias_view_logical_not_dedup():
    torch = pytest.importorskip("torch")
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    base = torch.zeros(4, 4, dtype=torch.float32)  # 16 elements * 4 bytes = 64
    view = base.view(2, 8)  # same storage, still 64 logical bytes
    store.add_object(OBJ_ID, [base, view])
    # Aliases/views are counted per copy (logical), not deduplicated.
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [("cpu", 128)], 1, 0)


def test_metadata_exception_yields_unknown(monkeypatch):
    _inject_fake_torch(monkeypatch)
    from ray.experimental.rdt.rdt_store import _tensor_device_and_bytes

    device, num_bytes = _tensor_device_and_bytes(_FakeTorchTensor(numel=4))
    assert (device, num_bytes) == ("cpu", 16)

    # A metadata read failure is reported as unknown/bytes -1, never raised.
    device, num_bytes = _tensor_device_and_bytes(_FakeTorchTensor(numel=4, fail=True))
    assert (device, num_bytes) == ("unknown", -1)


def test_interleaved_objects_are_isolated():
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    store.add_object(OBJ_ID, [object()])
    store.add_object(OBJ_ID_2, [object()])

    store.pop_object(OBJ_ID)
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID), [], 0, 0)

    # The other object is untouched by the first object's pop.
    store.pop_object(OBJ_ID_2)
    assert callback.calls[-1] == (bytes.fromhex(OBJ_ID_2), [], 0, 0)


def test_no_retained_tensor_refs_after_gc():
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    tensor = _Weakrefable()
    ref = weakref.ref(tensor)
    store.add_object(OBJ_ID, [tensor])
    store.pop_object(OBJ_ID)
    assert not store.has_tensor(tensor)

    del tensor
    gc.collect()
    # The store's scalar aggregates must not retain the tensor.
    assert ref() is None


def test_no_callback_skips_introspection(monkeypatch):
    import ray.experimental.rdt.rdt_store as rdt_store_module

    calls = []

    def spy(tensors):
        calls.append(tensors)
        return {}, {}, 0

    monkeypatch.setattr(rdt_store_module, "_compute_copy_memory_info", spy)
    store = RDTStore()  # callback None
    store.add_object(OBJ_ID, [object()])
    assert calls == []  # no tensor introspection without a callback
    store.pop_object(OBJ_ID)


def test_callback_exception_propagates():
    def raising_callback(object_id_bytes, device_bytes, num_copies, num_errors):
        raise RuntimeError("native setter failed")

    store = RDTStore(memory_info_callback=raising_callback)
    with pytest.raises(RuntimeError, match="native setter failed"):
        store.add_object(OBJ_ID, [object()])


def test_cuda_tensor_device_and_logical_bytes():
    torch = pytest.importorskip("torch")
    if not torch.cuda.is_available():
        pytest.skip("CUDA not available")
    callback = _RecordingCallback()
    store = RDTStore(memory_info_callback=callback)

    tensor = torch.ones(8, device="cuda", dtype=torch.float32)  # 8 * 4 = 32 bytes
    store.add_object(OBJ_ID, [tensor])
    assert callback.calls[-1] == (
        bytes.fromhex(OBJ_ID),
        [(str(tensor.device), 32)],
        1,
        0,
    )
    store.pop_object(OBJ_ID)


def test_concurrent_add_pop_publishes_final_erase_last():
    # Concurrent add/pop with a delayed (native-like) callback must publish the
    # final erase last, never a reordered stale snapshot.
    recorded = []

    def slow_callback(object_id_bytes, device_bytes, num_copies, num_errors):
        time.sleep(0.0005)  # model native setter latency
        recorded.append((object_id_bytes, list(device_bytes), num_copies, num_errors))

    store = RDTStore(memory_info_callback=slow_callback)
    errors = []
    start = threading.Event()

    def worker():
        try:
            start.wait()
            for _ in range(50):
                store.add_object(OBJ_ID, [object()])
                store.pop_object(OBJ_ID)
        except Exception as exc:  # pragma: no cover - only on failure
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    start.set()
    for t in threads:
        t.join(timeout=30)

    assert not errors
    assert all(not t.is_alive() for t in threads)
    assert recorded[-1] == (bytes.fromhex(OBJ_ID), [], 0, 0)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
