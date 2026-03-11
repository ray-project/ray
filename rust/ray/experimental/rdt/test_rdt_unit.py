"""
Comprehensive unit tests for python/ray/experimental/rdt/

These tests mock hardware dependencies (NIXL, CUDA, torch, NCCL) to achieve
100% code coverage of the RDT Python module without requiring GPUs.

Prerequisites:
    - Ray must be installed (pip install ray) or built from source
    - PyTorch must be installed (for create_empty_tensors_from_metadata tests)
    - The test file must NOT be inside the source ray/ directory to avoid
      import conflicts with the development ray package

Run with:
    # Copy to a directory outside the ray source tree:
    cp rust/ray/experimental/rdt/test_rdt_unit.py /tmp/
    cd /tmp
    python -m pytest test_rdt_unit.py -sv

    # Or on the AWS GPU instance:
    cd ~/nixl-test
    PYTHONPATH=~/ray/python python -m pytest test_rdt_unit.py -sv
"""

import threading
import time
import weakref
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass
from queue import Queue
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers / Mocks
# ═══════════════════════════════════════════════════════════════════════════════


class FakeTensor:
    """Mimics a torch.Tensor for testing without requiring torch."""

    def __init__(self, shape=(4, 4), dtype="float32", device_type="cuda", device_index=0,
                 data_ptr=None, contiguous=True, nbytes=64):
        self.shape = shape
        self.dtype = dtype
        self._device_type = device_type
        self._device_index = device_index
        self._data_ptr = data_ptr or id(self)
        self._contiguous = contiguous
        self._nbytes = nbytes
        self._storage = MagicMock()
        self._storage.data_ptr.return_value = self._data_ptr
        self._storage.nbytes.return_value = self._nbytes
        self.device = MagicMock()
        self.device.type = device_type
        self.device.index = device_index

    @property
    def is_cuda(self):
        return self._device_type == "cuda"

    def get_device(self):
        return self._device_index if self.is_cuda else -1

    def is_contiguous(self):
        return self._contiguous

    def untyped_storage(self):
        return self._storage


class FakeActorHandle:
    """Mimics ray.actor.ActorHandle."""
    _next_id = 0

    def __init__(self, actor_id=None):
        if actor_id is None:
            FakeActorHandle._next_id += 1
            self._actor_id = f"actor-{FakeActorHandle._next_id}"
        else:
            self._actor_id = actor_id
        self.__ray_call__ = MagicMock()

    def __repr__(self):
        return f"FakeActorHandle({self._actor_id})"


class FakeObjectRef:
    """Mimics ray.ObjectRef."""

    def __init__(self, hex_id=None):
        self._hex_id = hex_id or f"objref-{id(self):x}"

    def hex(self):
        return self._hex_id


# ═══════════════════════════════════════════════════════════════════════════════
# 1. tensor_transport_manager.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestTensorTransportMetadata:
    def test_basic_creation(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        meta = TensorTransportMetadata(
            tensor_meta=[((4, 4), "float32"), ((8,), "int64")],
            tensor_device="cuda",
        )
        assert len(meta.tensor_meta) == 2
        assert meta.tensor_device == "cuda"

    def test_default_device(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        meta = TensorTransportMetadata(tensor_meta=[])
        assert meta.tensor_device is None


class TestCommunicatorMetadata:
    def test_creation(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            CommunicatorMetadata,
        )
        meta = CommunicatorMetadata()
        assert meta is not None


class TestTensorTransportManagerABC:
    def test_cannot_instantiate(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportManager,
        )
        with pytest.raises(TypeError):
            TensorTransportManager()

    def test_subclass_must_implement_all(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportManager,
        )

        class IncompleteTransport(TensorTransportManager):
            pass

        with pytest.raises(TypeError):
            IncompleteTransport()


# ═══════════════════════════════════════════════════════════════════════════════
# 2. util.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestTransportRegistry:
    """Tests for util.py transport registry functions."""

    def setup_method(self):
        """Reset module-level state before each test."""
        import ray.experimental.rdt.util as util_mod
        util_mod.transport_manager_info = {}
        util_mod.transport_managers = {}
        util_mod._default_transports_registered = False
        util_mod.has_custom_transports = False

    def test_register_tensor_transport(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportManager,
        )
        from ray.experimental.rdt.util import (
            register_tensor_transport,
            transport_manager_info,
        )

        class MyTransport(TensorTransportManager):
            def tensor_transport_backend(self): return "MY"
            @staticmethod
            def is_one_sided(): return True
            @staticmethod
            def can_abort_transport(): return False
            def actor_has_tensor_transport(self, actor): return True
            def extract_tensor_transport_metadata(self, obj_id, rdt_object): pass
            def get_communicator_metadata(self, src, dst, backend=None): pass
            def recv_multiple_tensors(self, obj_id, meta, comm, bufs=None): return []
            def send_multiple_tensors(self, tensors, meta, comm): pass
            def garbage_collect(self, obj_id, meta, tensors): pass
            def abort_transport(self, obj_id, comm): pass

        register_tensor_transport("MY_TRANSPORT", ["cuda"], MyTransport, object)
        assert "MY_TRANSPORT" in transport_manager_info

    def test_register_duplicate_raises(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportManager,
        )
        from ray.experimental.rdt.util import register_tensor_transport

        class T(TensorTransportManager):
            def tensor_transport_backend(self): return "DUP"
            @staticmethod
            def is_one_sided(): return True
            @staticmethod
            def can_abort_transport(): return False
            def actor_has_tensor_transport(self, actor): return True
            def extract_tensor_transport_metadata(self, obj_id, rdt_object): pass
            def get_communicator_metadata(self, src, dst, backend=None): pass
            def recv_multiple_tensors(self, obj_id, meta, comm, bufs=None): return []
            def send_multiple_tensors(self, tensors, meta, comm): pass
            def garbage_collect(self, obj_id, meta, tensors): pass
            def abort_transport(self, obj_id, comm): pass

        register_tensor_transport("DUP", ["cuda"], T, object)
        with pytest.raises(ValueError, match="already registered"):
            register_tensor_transport("DUP", ["cuda"], T, object)

    def test_register_non_subclass_raises(self):
        from ray.experimental.rdt.util import register_tensor_transport

        with pytest.raises(ValueError, match="must be a subclass"):
            register_tensor_transport("BAD", ["cuda"], dict, object)

    def test_register_custom_sets_flag(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportManager,
        )
        import ray.experimental.rdt.util as util_mod
        from ray.experimental.rdt.util import register_tensor_transport

        class T(TensorTransportManager):
            def tensor_transport_backend(self): return "CUSTOM"
            @staticmethod
            def is_one_sided(): return True
            @staticmethod
            def can_abort_transport(): return False
            def actor_has_tensor_transport(self, actor): return True
            def extract_tensor_transport_metadata(self, obj_id, rdt_object): pass
            def get_communicator_metadata(self, src, dst, backend=None): pass
            def recv_multiple_tensors(self, obj_id, meta, comm, bufs=None): return []
            def send_multiple_tensors(self, tensors, meta, comm): pass
            def garbage_collect(self, obj_id, meta, tensors): pass
            def abort_transport(self, obj_id, comm): pass

        assert not util_mod.has_custom_transports
        register_tensor_transport("CUSTOM_TRANSPORT", ["cpu"], T, object)
        assert util_mod.has_custom_transports

    def test_get_transport_manager_unknown(self):
        from ray.experimental.rdt.util import get_tensor_transport_manager
        # Ensure defaults are registered first
        from ray.experimental.rdt.util import _ensure_default_transports_registered
        _ensure_default_transports_registered()

        with pytest.raises(ValueError, match="Unsupported"):
            get_tensor_transport_manager("UNKNOWN")

    def test_get_transport_data_type_unknown(self):
        from ray.experimental.rdt.util import get_transport_data_type
        from ray.experimental.rdt.util import _ensure_default_transports_registered
        _ensure_default_transports_registered()

        with pytest.raises(ValueError, match="Unsupported"):
            get_transport_data_type("NONEXISTENT")

    def test_device_match_transport_unknown(self):
        from ray.experimental.rdt.util import device_match_transport
        from ray.experimental.rdt.util import _ensure_default_transports_registered
        _ensure_default_transports_registered()

        with pytest.raises(ValueError, match="Unsupported"):
            device_match_transport("cuda", "NONEXISTENT")

    def test_normalize_and_validate(self):
        from ray.experimental.rdt.util import normalize_and_validate_tensor_transport
        from ray.experimental.rdt.util import _ensure_default_transports_registered
        _ensure_default_transports_registered()

        with pytest.raises(ValueError, match="Invalid"):
            normalize_and_validate_tensor_transport("NONEXISTENT")

    def test_validate_one_sided_raises_for_two_sided(self):
        pytest.importorskip("torch")
        from ray.experimental.rdt.util import validate_one_sided
        from ray.experimental.rdt.util import _ensure_default_transports_registered
        _ensure_default_transports_registered()

        with pytest.raises(ValueError, match="two-sided"):
            validate_one_sided("NCCL", "ray.get")

    def test_validate_one_sided_ok_for_nixl(self):
        pytest.importorskip("torch")
        from ray.experimental.rdt.util import validate_one_sided
        from ray.experimental.rdt.util import _ensure_default_transports_registered
        _ensure_default_transports_registered()
        # Should not raise
        validate_one_sided("NIXL", "ray.get")

    def test_create_empty_tensors_from_metadata(self):
        """Test that create_empty_tensors_from_metadata creates correct tensors."""
        torch = pytest.importorskip("torch")
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        from ray.experimental.rdt.util import create_empty_tensors_from_metadata

        meta = TensorTransportMetadata(
            tensor_meta=[
                (torch.Size([4, 4]), torch.float32),
                (torch.Size([8]), torch.int64),
            ],
            tensor_device="cpu",
        )
        tensors = create_empty_tensors_from_metadata(meta)
        assert len(tensors) == 2
        assert tensors[0].shape == torch.Size([4, 4])
        assert tensors[0].dtype == torch.float32
        assert tensors[1].shape == torch.Size([8])
        assert tensors[1].dtype == torch.int64


# ═══════════════════════════════════════════════════════════════════════════════
# 3. rdt_store.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestRDTStore:
    def _make_store(self):
        from ray.experimental.rdt.rdt_store import RDTStore
        return RDTStore()

    def test_empty_store(self):
        store = self._make_store()
        assert not store.has_object("nonexistent")
        assert store.get_num_objects() == 0

    def test_add_and_get_object(self):
        store = self._make_store()
        tensors = [FakeTensor(), FakeTensor()]
        store.add_object("obj1", tensors)
        assert store.has_object("obj1")
        assert store.get_object("obj1") is tensors
        assert store.get_num_objects() == 1

    def test_add_error_object(self):
        store = self._make_store()
        err = RuntimeError("test error")
        store.add_object("obj1", err)
        assert store.has_object("obj1")
        with pytest.raises(RuntimeError, match="test error"):
            store.get_object("obj1")

    def test_pop_object(self):
        store = self._make_store()
        tensors = [FakeTensor()]
        store.add_object("obj1", tensors)
        result = store.pop_object("obj1")
        assert result is tensors
        assert not store.has_object("obj1")
        assert store.get_num_objects() == 0

    def test_pop_error_raises(self):
        store = self._make_store()
        err = ValueError("pop error")
        store.add_object("obj1", err)
        with pytest.raises(ValueError, match="pop error"):
            store.pop_object("obj1")

    def test_pop_nonexistent_raises(self):
        store = self._make_store()
        with pytest.raises(AssertionError):
            store.pop_object("nonexistent")

    def test_deque_fifo_ordering(self):
        store = self._make_store()
        t1 = [FakeTensor(data_ptr=100)]
        t2 = [FakeTensor(data_ptr=200)]
        store.add_object("obj1", t1)
        store.add_object("obj1", t2)
        assert store.get_num_objects() == 2
        result1 = store.pop_object("obj1")
        assert result1 is t1
        result2 = store.pop_object("obj1")
        assert result2 is t2

    def test_is_primary_copy(self):
        store = self._make_store()
        store.add_object("obj1", [FakeTensor()], is_primary=True)
        assert store.is_primary_copy("obj1")

        store.add_object("obj2", [FakeTensor()], is_primary=False)
        assert not store.is_primary_copy("obj2")

    def test_has_tensor(self):
        store = self._make_store()
        t = FakeTensor()
        assert not store.has_tensor(t)
        store.add_object("obj1", [t])
        assert store.has_tensor(t)
        store.pop_object("obj1")
        assert not store.has_tensor(t)

    def test_tensor_to_object_ids_tracking(self):
        store = self._make_store()
        t = FakeTensor()
        store.add_object("obj1", [t])
        store.add_object("obj2", [t])
        assert store.has_tensor(t)
        store.pop_object("obj1")
        assert store.has_tensor(t)  # still in obj2
        store.pop_object("obj2")
        assert not store.has_tensor(t)

    def test_wait_and_get_object(self):
        store = self._make_store()
        tensors = [FakeTensor()]

        def add_later():
            time.sleep(0.1)
            store.add_object("obj1", tensors)

        t = threading.Thread(target=add_later)
        t.start()
        result = store.wait_and_get_object("obj1", timeout=5)
        assert result is tensors
        t.join()

    def test_wait_and_get_object_timeout(self):
        store = self._make_store()
        with pytest.raises(TimeoutError):
            store.wait_and_get_object("obj1", timeout=0.1)

    def test_wait_and_pop_object(self):
        store = self._make_store()
        tensors = [FakeTensor()]

        def add_later():
            time.sleep(0.1)
            store.add_object("obj1", tensors)

        t = threading.Thread(target=add_later)
        t.start()
        result = store.wait_and_pop_object("obj1", timeout=5)
        assert result is tensors
        assert not store.has_object("obj1")
        t.join()

    def test_wait_and_pop_object_timeout(self):
        store = self._make_store()
        with pytest.raises(TimeoutError):
            store.wait_and_pop_object("obj1", timeout=0.1)

    def test_wait_tensor_freed(self):
        store = self._make_store()
        t = FakeTensor()
        store.add_object("obj1", [t])

        def pop_later():
            time.sleep(0.1)
            store.pop_object("obj1")

        thread = threading.Thread(target=pop_later)
        thread.start()
        store.wait_tensor_freed(t, timeout=5)
        assert not store.has_tensor(t)
        thread.join()

    def test_wait_tensor_freed_timeout(self):
        store = self._make_store()
        t = FakeTensor()
        store.add_object("obj1", [t])
        with pytest.raises(TimeoutError):
            store.wait_tensor_freed(t, timeout=0.1)

    def test_add_object_primary(self):
        """Test add_object_primary extracts metadata."""
        from ray.experimental.rdt.rdt_store import RDTStore
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )

        store = RDTStore()
        mock_meta = TensorTransportMetadata(tensor_meta=[], tensor_device=None)

        with patch("ray.experimental.rdt.rdt_store.get_tensor_transport_manager") as mock_get:
            mock_transport = MagicMock()
            mock_transport.extract_tensor_transport_metadata.return_value = mock_meta
            mock_get.return_value = mock_transport

            tensors = [FakeTensor()]
            result = store.add_object_primary("obj1", tensors, "NIXL")
            assert result is mock_meta
            assert store.is_primary_copy("obj1")


class TestRDTStoreHelperFunctions:
    """Test module-level helper functions in rdt_store.py."""

    def test_validate_tensor_buffers_shape_mismatch(self):
        from ray.experimental.rdt.rdt_store import validate_tensor_buffers

        t = FakeTensor(shape=(4, 4), dtype="float32", device_type="cuda")
        with pytest.raises(ValueError, match="Shape"):
            validate_tensor_buffers([t], [((8, 8), "float32")], "cuda")

    def test_validate_tensor_buffers_dtype_mismatch(self):
        from ray.experimental.rdt.rdt_store import validate_tensor_buffers

        t = FakeTensor(shape=(4, 4), dtype="float32", device_type="cuda")
        with pytest.raises(ValueError, match="Dtype"):
            validate_tensor_buffers([t], [((4, 4), "int64")], "cuda")

    def test_validate_tensor_buffers_device_mismatch(self):
        from ray.experimental.rdt.rdt_store import validate_tensor_buffers

        t = FakeTensor(shape=(4, 4), dtype="float32", device_type="cpu")
        with pytest.raises(ValueError, match="Device"):
            validate_tensor_buffers([t], [((4, 4), "float32")], "cuda")

    def test_validate_tensor_buffers_not_contiguous(self):
        from ray.experimental.rdt.rdt_store import validate_tensor_buffers

        t = FakeTensor(shape=(4, 4), dtype="float32", device_type="cuda", contiguous=False)
        with pytest.raises(ValueError, match="not contiguous"):
            validate_tensor_buffers([t], [((4, 4), "float32")], "cuda")

    def test_validate_tensor_buffers_length_mismatch(self):
        from ray.experimental.rdt.rdt_store import validate_tensor_buffers

        t = FakeTensor(shape=(4, 4), dtype="float32", device_type="cuda")
        with pytest.raises(ValueError, match="Length"):
            validate_tensor_buffers([t], [((4, 4), "float32"), ((8,), "int64")], "cuda")

    def test_validate_tensor_buffers_valid(self):
        from ray.experimental.rdt.rdt_store import validate_tensor_buffers

        t = FakeTensor(shape=(4, 4), dtype="float32", device_type="cuda")
        # Should not raise
        validate_tensor_buffers([t], [((4, 4), "float32")], "cuda")

    def test_ray_send(self):
        from ray.experimental.rdt.rdt_store import __ray_send__
        from ray.experimental.rdt.tensor_transport_manager import (
            CommunicatorMetadata,
            TensorTransportMetadata,
        )

        tensors = [FakeTensor()]
        meta = TensorTransportMetadata(
            tensor_meta=[((4, 4), "float32")],
            tensor_device="cuda",
        )
        comm = CommunicatorMetadata()

        mock_store = MagicMock()
        mock_store.has_object.return_value = True
        mock_store.get_object.return_value = tensors

        mock_transport = MagicMock()

        with patch("ray._private.worker.global_worker") as mock_worker, \
             patch("ray.experimental.rdt.rdt_store.device_match_transport", return_value=True), \
             patch("ray.experimental.rdt.rdt_store.get_tensor_transport_manager", return_value=mock_transport):
            mock_worker.rdt_manager._rdt_store = mock_store
            __ray_send__(None, "obj1", meta, comm, "NCCL")
            mock_transport.send_multiple_tensors.assert_called_once()

    def test_ray_send_device_mismatch(self):
        from ray.experimental.rdt.rdt_store import __ray_send__
        from ray.experimental.rdt.tensor_transport_manager import (
            CommunicatorMetadata,
            TensorTransportMetadata,
        )

        tensors = [FakeTensor()]
        meta = TensorTransportMetadata(
            tensor_meta=[((4, 4), "float32")],
            tensor_device="cpu",
        )
        comm = CommunicatorMetadata()

        mock_store = MagicMock()
        mock_store.has_object.return_value = True
        mock_store.get_object.return_value = tensors

        with patch("ray._private.worker.global_worker") as mock_worker, \
             patch("ray.experimental.rdt.rdt_store.device_match_transport", return_value=False):
            mock_worker.rdt_manager._rdt_store = mock_store
            with pytest.raises(ValueError, match="does not support"):
                __ray_send__(None, "obj1", meta, comm, "NCCL")

    def test_ray_recv(self):
        from ray.experimental.rdt.rdt_store import __ray_recv__
        from ray.experimental.rdt.tensor_transport_manager import (
            CommunicatorMetadata,
            TensorTransportMetadata,
        )

        meta = TensorTransportMetadata(
            tensor_meta=[((4, 4), "float32")],
            tensor_device="cuda",
        )
        comm = CommunicatorMetadata()
        received_tensors = [FakeTensor()]

        mock_store = MagicMock()
        mock_transport = MagicMock()
        mock_transport.recv_multiple_tensors.return_value = received_tensors

        with patch("ray._private.worker.global_worker") as mock_worker, \
             patch("ray.experimental.rdt.rdt_store.device_match_transport", return_value=True), \
             patch("ray.experimental.rdt.rdt_store.get_tensor_transport_manager", return_value=mock_transport):
            mock_worker.rdt_manager.rdt_store = mock_store
            __ray_recv__(None, "obj1", meta, comm, "NIXL")
            mock_store.add_object.assert_called_once_with("obj1", received_tensors)

    def test_ray_recv_error_stores_exception(self):
        from ray.experimental.rdt.rdt_store import __ray_recv__
        from ray.experimental.rdt.tensor_transport_manager import (
            CommunicatorMetadata,
            TensorTransportMetadata,
        )

        meta = TensorTransportMetadata(
            tensor_meta=[((4, 4), "float32")],
            tensor_device="cuda",
        )
        comm = CommunicatorMetadata()

        mock_store = MagicMock()
        mock_transport = MagicMock()
        mock_transport.recv_multiple_tensors.side_effect = RuntimeError("recv failed")

        with patch("ray._private.worker.global_worker") as mock_worker, \
             patch("ray.experimental.rdt.rdt_store.device_match_transport", return_value=True), \
             patch("ray.experimental.rdt.rdt_store.get_tensor_transport_manager", return_value=mock_transport):
            mock_worker.rdt_manager.rdt_store = mock_store
            __ray_recv__(None, "obj1", meta, comm, "NIXL")
            # Error should be stored as the object
            stored_error = mock_store.add_object.call_args[0][1]
            assert isinstance(stored_error, RuntimeError)

    def test_ray_abort_transport(self):
        from ray.experimental.rdt.rdt_store import __ray_abort_transport__
        from ray.experimental.rdt.tensor_transport_manager import (
            CommunicatorMetadata,
        )

        comm = CommunicatorMetadata()
        mock_transport = MagicMock()

        with patch("ray.experimental.rdt.rdt_store.get_tensor_transport_manager", return_value=mock_transport):
            __ray_abort_transport__(None, "obj1", comm, "NIXL")
            mock_transport.abort_transport.assert_called_once_with("obj1", comm)

    def test_ray_free(self):
        from ray.experimental.rdt.rdt_store import __ray_free__
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )

        meta = TensorTransportMetadata(tensor_meta=[], tensor_device=None)
        tensors = [FakeTensor()]

        mock_store = MagicMock()
        mock_store.has_object.return_value = True
        mock_store.get_object.return_value = tensors
        mock_transport = MagicMock()

        with patch("ray._private.worker.global_worker") as mock_worker, \
             patch("ray.experimental.rdt.rdt_store.get_tensor_transport_manager", return_value=mock_transport):
            mock_worker.rdt_manager.rdt_store = mock_store
            mock_worker.rdt_manager._rdt_store = mock_store
            __ray_free__(None, "obj1", "NIXL", meta)
            mock_transport.garbage_collect.assert_called_once()
            mock_store.pop_object.assert_called_once_with("obj1")

    def test_ray_free_not_found(self):
        from ray.experimental.rdt.rdt_store import __ray_free__
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )

        meta = TensorTransportMetadata(tensor_meta=[], tensor_device=None)
        mock_store = MagicMock()
        mock_store.has_object.return_value = False

        with patch("ray._private.worker.global_worker") as mock_worker, \
             patch("ray.experimental.rdt.rdt_store.get_tensor_transport_manager"):
            mock_worker.rdt_manager.rdt_store = mock_store
            mock_worker.rdt_manager._rdt_store = mock_store
            # Should not raise, just return
            __ray_free__(None, "obj1", "NIXL", meta)

    def test_ray_fetch_rdt_object(self):
        from ray.experimental.rdt.rdt_store import __ray_fetch_rdt_object__

        tensors = [FakeTensor()]
        mock_store = MagicMock()
        mock_store.wait_and_get_object.return_value = tensors

        with patch("ray._private.worker.global_worker") as mock_worker:
            mock_worker.rdt_manager.rdt_store = mock_store
            result = __ray_fetch_rdt_object__(None, "obj1")
            assert result is tensors


# ═══════════════════════════════════════════════════════════════════════════════
# 4. rdt_manager.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestRDTManager:

    def _make_manager(self):
        from ray.experimental.rdt.rdt_manager import RDTManager
        return RDTManager()

    def test_initial_state(self):
        mgr = self._make_manager()
        assert not mgr.is_managed_object("obj1")
        assert mgr.get_rdt_metadata("obj1") is None

    def test_add_rdt_ref(self):
        mgr = self._make_manager()
        ref = FakeObjectRef("obj1")
        actor = FakeActorHandle()
        mgr.add_rdt_ref(ref, actor, "NIXL")
        assert mgr.is_managed_object("obj1")
        meta = mgr.get_rdt_metadata("obj1")
        assert meta.src_actor is actor
        assert meta.tensor_transport_backend == "NIXL"
        assert meta.tensor_transport_meta is None

    def test_set_tensor_transport_metadata(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        mgr = self._make_manager()
        ref = FakeObjectRef("obj1")
        actor = FakeActorHandle()
        mgr.add_rdt_ref(ref, actor, "NIXL")

        meta = TensorTransportMetadata(tensor_meta=[], tensor_device="cuda")
        mgr.set_tensor_transport_metadata_and_trigger_queued_operations("obj1", meta)

        rdt_meta = mgr.get_rdt_metadata("obj1")
        assert rdt_meta.tensor_transport_meta is meta

    def test_set_target_buffers(self):
        mgr = self._make_manager()
        ref = FakeObjectRef("obj1")
        actor = FakeActorHandle()
        mgr.add_rdt_ref(ref, actor, "NIXL")

        buf = FakeTensor()
        mgr.set_target_buffers_for_ref(ref, [buf])
        meta = mgr.get_rdt_metadata("obj1")
        assert meta.target_buffers is not None
        assert len(meta.target_buffers) == 1

    def test_set_target_buffers_non_managed_raises(self):
        mgr = self._make_manager()
        ref = FakeObjectRef("nonexistent")
        with pytest.raises(ValueError, match="not an RDT object"):
            mgr.set_target_buffers_for_ref(ref, [FakeTensor()])

    def test_rdt_store_lazy_creation(self):
        mgr = self._make_manager()
        assert mgr._rdt_store is None
        store = mgr.rdt_store
        assert store is not None
        # Second access returns same instance
        assert mgr.rdt_store is store

    def test_shutdown(self):
        mgr = self._make_manager()
        # No thread running, should be no-op
        mgr.shutdown()

        # With a mock thread
        mock_thread = MagicMock()
        mgr._monitor_failures_thread = mock_thread
        mgr.shutdown()
        mock_thread.join.assert_called_once()
        assert mgr._monitor_failures_thread is None

    def test_queued_transfer_triggered_when_metadata_set(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        mgr = self._make_manager()
        ref = FakeObjectRef("obj1")
        src = FakeActorHandle("src")
        dst = FakeActorHandle("dst")
        mgr.add_rdt_ref(ref, src, "NIXL")

        # Queue a transfer (metadata not yet available)
        with mgr._lock:
            mgr._queued_transfers["obj1"].append(dst)

        meta = TensorTransportMetadata(tensor_meta=[], tensor_device="cuda")
        with patch.object(mgr, "trigger_out_of_band_tensor_transfer") as mock_trigger:
            mgr.set_tensor_transport_metadata_and_trigger_queued_operations("obj1", meta)
            mock_trigger.assert_called_once_with(dst, "obj1")

    def test_queued_free_triggered_when_metadata_set(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        mgr = self._make_manager()
        ref = FakeObjectRef("obj1")
        src = FakeActorHandle("src")
        mgr.add_rdt_ref(ref, src, "NIXL")

        # Queue a free
        with mgr._lock:
            mgr._queued_frees.add("obj1")

        meta = TensorTransportMetadata(tensor_meta=[], tensor_device="cuda")
        with patch.object(mgr, "free_object_primary_copy") as mock_free:
            mgr.set_tensor_transport_metadata_and_trigger_queued_operations("obj1", meta)
            mock_free.assert_called_once_with("obj1")

    def test_queue_or_free_with_metadata(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        mgr = self._make_manager()
        ref = FakeObjectRef("obj1")
        src = FakeActorHandle("src")
        meta = TensorTransportMetadata(tensor_meta=[], tensor_device="cuda")
        mgr.add_rdt_ref(ref, src, "NIXL", tensor_transport_meta=meta)

        with patch.object(mgr, "free_object_primary_copy") as mock_free:
            mgr.queue_or_free_object_primary_copy("obj1")
            mock_free.assert_called_once_with("obj1")

    def test_queue_or_free_without_metadata(self):
        mgr = self._make_manager()
        ref = FakeObjectRef("obj1")
        src = FakeActorHandle("src")
        mgr.add_rdt_ref(ref, src, "NIXL")

        mgr.queue_or_free_object_primary_copy("obj1")
        assert "obj1" in mgr._queued_frees

    def test_wait_for_tensor_transport_metadata_timeout(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        mgr = self._make_manager()
        ref = FakeObjectRef("obj1")
        src = FakeActorHandle("src")
        mgr.add_rdt_ref(ref, src, "NIXL")

        result = mgr.wait_for_tensor_transport_metadata("obj1", timeout=0.1)
        assert result is None

    def test_wait_for_tensor_transport_metadata_success(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        mgr = self._make_manager()
        ref = FakeObjectRef("obj1")
        src = FakeActorHandle("src")
        mgr.add_rdt_ref(ref, src, "NIXL")

        meta = TensorTransportMetadata(tensor_meta=[], tensor_device="cuda")

        def set_meta_later():
            time.sleep(0.1)
            mgr.set_tensor_transport_metadata_and_trigger_queued_operations("obj1", meta)

        t = threading.Thread(target=set_meta_later)
        t.start()
        result = mgr.wait_for_tensor_transport_metadata("obj1", timeout=5)
        assert result is meta
        t.join()


class TestRDTManagerTransferFlow:
    """Tests for trigger_out_of_band_tensor_transfer and related methods."""

    def _make_manager(self):
        from ray.experimental.rdt.rdt_manager import RDTManager
        return RDTManager()

    def test_intra_actor_skips_transfer(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        mgr = self._make_manager()
        actor = FakeActorHandle("same")
        ref = FakeObjectRef("obj1")
        meta = TensorTransportMetadata(tensor_meta=[], tensor_device="cuda")
        mgr.add_rdt_ref(ref, actor, "NIXL", tensor_transport_meta=meta)

        with patch("ray.experimental.rdt.util.get_tensor_transport_manager") as mock_get:
            mock_transport = MagicMock()
            mock_transport.get_communicator_metadata.return_value = MagicMock()
            mock_get.return_value = mock_transport

            # src == dst should skip
            mgr.trigger_out_of_band_tensor_transfer(actor, "obj1")
            # No send/recv should be submitted
            assert not actor.__ray_call__.called

    def test_mutation_warning(self):
        from ray.experimental.rdt.tensor_transport_manager import (
            TensorTransportMetadata,
        )
        import warnings

        mgr = self._make_manager()
        src = FakeActorHandle("src")
        dst1 = FakeActorHandle("dst1")
        ref = FakeObjectRef("obj1")
        meta = TensorTransportMetadata(tensor_meta=[], tensor_device="cuda")
        mgr.add_rdt_ref(ref, src, "NIXL", tensor_transport_meta=meta)

        mock_transport = MagicMock()
        mock_transport.__class__ = MagicMock()
        mock_transport.__class__.is_one_sided.return_value = True
        mock_transport.get_communicator_metadata.return_value = MagicMock()

        with patch("ray.experimental.rdt.util.get_tensor_transport_manager", return_value=mock_transport):
            # First: send to another actor
            mgr.trigger_out_of_band_tensor_transfer(dst1, "obj1")
            # Second: send back to src
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                mgr.trigger_out_of_band_tensor_transfer(src, "obj1")
                assert len(w) == 1
                assert "mutable" in str(w[0].message).lower()

            # Third: no duplicate warning
            dst2 = FakeActorHandle("dst2")
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                mgr.trigger_out_of_band_tensor_transfer(dst2, "obj1")
                assert len(w) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# 5. nixl_tensor_transport.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestNixlTransportMetadata:
    def test_creation(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTransportMetadata,
        )
        meta = NixlTransportMetadata(
            tensor_meta=[((4, 4), "float32")],
            tensor_device="cuda",
            nixl_serialized_descs=b"descs",
            nixl_agent_meta=b"meta",
            nixl_agent_name="agent1",
            nixl_agent_meta_version=1,
        )
        assert meta.nixl_agent_name == "agent1"

    def test_identity_eq_hash(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTransportMetadata,
        )
        m1 = NixlTransportMetadata(tensor_meta=[], nixl_agent_name="a")
        m2 = NixlTransportMetadata(tensor_meta=[], nixl_agent_name="a")
        # Object identity, not value equality
        assert m1 != m2
        assert hash(m1) != hash(m2)


class TestNixlTensorTransport:
    def _make_transport(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTensorTransport,
        )
        return NixlTensorTransport()

    def test_backend_name(self):
        t = self._make_transport()
        assert t.tensor_transport_backend() == "NIXL"

    def test_is_one_sided(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTensorTransport,
        )
        assert NixlTensorTransport.is_one_sided() is True

    def test_can_abort(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTensorTransport,
        )
        assert NixlTensorTransport.can_abort_transport() is True

    def test_send_not_implemented(self):
        t = self._make_transport()
        with pytest.raises(NotImplementedError, match="one-sided"):
            t.send_multiple_tensors([], MagicMock(), MagicMock())

    def test_get_communicator_metadata(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlCommunicatorMetadata,
        )
        t = self._make_transport()
        comm = t.get_communicator_metadata(None, None, None)
        assert isinstance(comm, NixlCommunicatorMetadata)

    def test_abort_transport(self):
        t = self._make_transport()
        t.abort_transport("obj1", MagicMock())
        assert "obj1" in t._aborted_transfer_obj_ids

    def test_add_tensor_descs_refcounting(self):
        t = self._make_transport()
        mock_agent = MagicMock()
        mock_agent.register_memory.return_value = "reg_desc_1"
        t._nixl_agent = mock_agent

        tensor = FakeTensor(data_ptr=42)
        t._add_tensor_descs([tensor])
        assert 42 in t._tensor_desc_cache
        assert t._tensor_desc_cache[42].metadata_count == 1

        # Adding same tensor again increments count
        t._add_tensor_descs([tensor])
        assert t._tensor_desc_cache[42].metadata_count == 2

    def test_garbage_collect(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTransportMetadata,
            TensorDesc,
        )
        t = self._make_transport()
        mock_agent = MagicMock()
        t._nixl_agent = mock_agent

        tensor = FakeTensor(data_ptr=42)
        desc = TensorDesc(reg_desc="reg1", metadata_count=1)
        t._tensor_desc_cache[42] = desc
        t._managed_meta_nixl["obj1"] = NixlTransportMetadata(
            tensor_meta=[], nixl_agent_name="a"
        )

        meta = NixlTransportMetadata(tensor_meta=[], nixl_agent_name="a")
        t.garbage_collect("obj1", meta, [tensor])

        assert "obj1" not in t._managed_meta_nixl
        assert 42 not in t._tensor_desc_cache
        mock_agent.deregister_memory.assert_called_once_with("reg1")
        assert t._nixl_agent_meta_version == 1

    def test_garbage_collect_refcount_not_zero(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTransportMetadata,
            TensorDesc,
        )
        t = self._make_transport()
        mock_agent = MagicMock()
        t._nixl_agent = mock_agent

        tensor = FakeTensor(data_ptr=42)
        desc = TensorDesc(reg_desc="reg1", metadata_count=2)
        t._tensor_desc_cache[42] = desc
        t._managed_meta_nixl["obj1"] = NixlTransportMetadata(
            tensor_meta=[], nixl_agent_name="a"
        )

        meta = NixlTransportMetadata(tensor_meta=[], nixl_agent_name="a")
        t.garbage_collect("obj1", meta, [tensor])

        # Still has refcount 1, should NOT deregister
        assert 42 in t._tensor_desc_cache
        assert t._tensor_desc_cache[42].metadata_count == 1
        mock_agent.deregister_memory.assert_not_called()

    def test_garbage_collect_already_freed(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTransportMetadata,
        )
        t = self._make_transport()
        meta = NixlTransportMetadata(tensor_meta=[], nixl_agent_name="a")
        # obj not in _managed_meta_nixl — should be no-op
        t.garbage_collect("nonexistent", meta, [])

    def test_get_meta_put_meta(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTransportMetadata,
        )
        t = self._make_transport()
        assert t._get_meta("obj1") is None

        meta = NixlTransportMetadata(tensor_meta=[], nixl_agent_name="a")
        t._put_meta("obj1", meta)
        assert t._get_meta("obj1") is meta

    def test_get_num_managed_meta_nixl(self):
        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTransportMetadata,
        )
        t = self._make_transport()
        assert t._get_num_managed_meta_nixl() == 0

        meta = NixlTransportMetadata(tensor_meta=[], nixl_agent_name="a")
        t._put_meta("obj1", meta)
        assert t._get_num_managed_meta_nixl() == 1


# ═══════════════════════════════════════════════════════════════════════════════
# 6. collective_tensor_transport.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestCollectiveTensorTransport:
    def test_nccl_backend(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        assert t.tensor_transport_backend() == "NCCL"

    def test_gloo_backend(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            GLOOTensorTransport,
        )
        t = GLOOTensorTransport()
        assert t.tensor_transport_backend() == "GLOO"

    def test_is_not_one_sided(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            CollectiveTensorTransport,
        )
        assert CollectiveTensorTransport.is_one_sided() is False

    def test_cannot_abort(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            CollectiveTensorTransport,
        )
        assert CollectiveTensorTransport.can_abort_transport() is False

    def test_base_class_transport_backend_raises(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            CollectiveTensorTransport,
        )
        t = CollectiveTensorTransport()
        with pytest.raises(NotImplementedError):
            t.tensor_transport_backend()

    def test_abort_transport_not_implemented(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        with pytest.raises(NotImplementedError):
            t.abort_transport("obj1", MagicMock())

    def test_garbage_collect_noop(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        # Should not raise
        t.garbage_collect("obj1", MagicMock(), [])

    def test_extract_metadata_device_mismatch(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        t1 = FakeTensor(device_type="cuda")
        t2 = FakeTensor(device_type="cpu")
        with pytest.raises(ValueError, match="same device type"):
            t.extract_tensor_transport_metadata("obj1", [t1, t2])

    def test_extract_metadata_empty(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        meta = t.extract_tensor_transport_metadata("obj1", [])
        assert meta.tensor_meta == []
        assert meta.tensor_device is None

    def _patch_get_collective_groups(self, return_value):
        """Helper to patch get_collective_groups which is lazily imported.

        The module ray.experimental.collective requires torch at import time,
        so we inject a mock module into sys.modules before patching.
        """
        import sys
        mock_module = MagicMock()
        mock_module.get_collective_groups = MagicMock(return_value=return_value)
        return patch.dict(sys.modules, {"ray.experimental.collective": mock_module})

    def test_get_communicator_no_groups_raises(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        src = FakeActorHandle("src")
        dst = FakeActorHandle("dst")

        with self._patch_get_collective_groups([]):
            with pytest.raises(ValueError, match="No communicators"):
                t.get_communicator_metadata(src, dst, "NCCL")

    def test_get_communicator_multiple_groups_raises(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        src = FakeActorHandle("src")
        dst = FakeActorHandle("dst")

        with self._patch_get_collective_groups([MagicMock(), MagicMock()]):
            with pytest.raises(ValueError, match="only support one communicator"):
                t.get_communicator_metadata(src, dst, "NCCL")

    def test_get_communicator_src_not_found(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        src = FakeActorHandle("src")
        dst = FakeActorHandle("dst")

        mock_comm = MagicMock()
        mock_comm.get_rank.side_effect = lambda a: -1 if a is src else 1

        with self._patch_get_collective_groups([mock_comm]):
            with pytest.raises(ValueError, match="Sender actor"):
                t.get_communicator_metadata(src, dst, "NCCL")

    def test_get_communicator_dst_not_found(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        src = FakeActorHandle("src")
        dst = FakeActorHandle("dst")

        mock_comm = MagicMock()
        mock_comm.get_rank.side_effect = lambda a: 0 if a is src else -1

        with self._patch_get_collective_groups([mock_comm]):
            with pytest.raises(ValueError, match="Receiver actor"):
                t.get_communicator_metadata(src, dst, "NCCL")

    def test_get_communicator_success(self):
        from ray.experimental.rdt.collective_tensor_transport import (
            CollectiveCommunicatorMetadata,
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        src = FakeActorHandle("src")
        dst = FakeActorHandle("dst")

        mock_comm = MagicMock()
        mock_comm.get_rank.side_effect = lambda a: 0 if a is src else 1
        mock_comm.name = "group1"

        with self._patch_get_collective_groups([mock_comm]):
            meta = t.get_communicator_metadata(src, dst, "NCCL")
            assert isinstance(meta, CollectiveCommunicatorMetadata)
            assert meta.src_rank == 0
            assert meta.dst_rank == 1
            assert meta.communicator_name == "group1"

    def test_send_device_mismatch(self):
        pytest.importorskip("torch")
        from ray.experimental.rdt.collective_tensor_transport import (
            CollectiveCommunicatorMetadata,
            CollectiveTransportMetadata,
            NCCLTensorTransport,
        )
        t = NCCLTensorTransport()
        t1 = FakeTensor(device_type="cuda")
        t2 = FakeTensor(device_type="cpu")
        meta = CollectiveTransportMetadata(tensor_meta=[], tensor_device="cuda")
        comm = CollectiveCommunicatorMetadata(communicator_name="g", src_rank=0, dst_rank=1)

        with pytest.raises(ValueError, match="does not match"):
            t.send_multiple_tensors([t1, t2], meta, comm)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. cuda_ipc_transport.py tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestCudaIpcTransport:
    def _make_transport(self):
        from ray.experimental.rdt.cuda_ipc_transport import CudaIpcTransport
        return CudaIpcTransport()

    def test_backend_name(self):
        t = self._make_transport()
        assert t.tensor_transport_backend == "CUDA_IPC"

    def test_is_one_sided(self):
        from ray.experimental.rdt.cuda_ipc_transport import CudaIpcTransport
        assert CudaIpcTransport.is_one_sided() is True

    def test_cannot_abort(self):
        from ray.experimental.rdt.cuda_ipc_transport import CudaIpcTransport
        assert CudaIpcTransport.can_abort_transport() is False

    def test_actor_has_transport_always_true(self):
        t = self._make_transport()
        assert t.actor_has_tensor_transport(FakeActorHandle()) is True

    def test_send_not_implemented(self):
        t = self._make_transport()
        with pytest.raises(NotImplementedError, match="one-sided"):
            t.send_multiple_tensors([], MagicMock(), MagicMock())

    def test_abort_not_implemented(self):
        t = self._make_transport()
        with pytest.raises(NotImplementedError):
            t.abort_transport("obj1", MagicMock())

    def test_garbage_collect_noop(self):
        t = self._make_transport()
        # Should not raise
        t.garbage_collect("obj1", MagicMock(), [])

    def test_get_communicator_metadata(self):
        from ray.experimental.rdt.cuda_ipc_transport import (
            CudaIpcCommunicatorMetadata,
        )
        t = self._make_transport()
        comm = t.get_communicator_metadata(None, None, None)
        assert isinstance(comm, CudaIpcCommunicatorMetadata)

    def test_recv_target_buffers_not_supported(self):
        from ray.experimental.rdt.cuda_ipc_transport import (
            CudaIpcCommunicatorMetadata,
            CudaIpcTransportMetadata,
        )
        t = self._make_transport()
        meta = CudaIpcTransportMetadata(tensor_meta=[], tensor_device="cuda")
        comm = CudaIpcCommunicatorMetadata()
        with pytest.raises(ValueError, match="does not support receiving into buffers"):
            t.recv_multiple_tensors("obj1", meta, comm, target_buffers=[FakeTensor()])

    def test_recv_empty_tensors(self):
        from ray.experimental.rdt.cuda_ipc_transport import (
            CudaIpcCommunicatorMetadata,
            CudaIpcTransportMetadata,
        )
        t = self._make_transport()
        meta = CudaIpcTransportMetadata(tensor_meta=[], tensor_device="cuda")
        comm = CudaIpcCommunicatorMetadata()
        result = t.recv_multiple_tensors("obj1", meta, comm)
        assert result == []

    def test_extract_metadata_different_gpu_raises(self):
        pytest.importorskip("torch")
        from ray.experimental.rdt.cuda_ipc_transport import CudaIpcTransport

        t = CudaIpcTransport()
        t1 = FakeTensor(device_type="cuda", device_index=0)
        t2 = FakeTensor(device_type="cuda", device_index=1)

        with patch("ray.experimental.rdt.cuda_ipc_transport.ray") as mock_ray:
            mock_ray.get_gpu_ids.return_value = [0, 1]
            mock_ctx = MagicMock()
            mock_ctx.get_node_id.return_value = "node1"
            mock_ray.get_runtime_context.return_value = mock_ctx

            with pytest.raises(ValueError, match="same GPU"):
                t.extract_tensor_transport_metadata("obj1", [t1, t2])

    def test_extract_metadata_empty(self):
        t = self._make_transport()
        meta = t.extract_tensor_transport_metadata("obj1", [])
        assert meta.tensor_meta == []
        assert meta.cuda_ipc_handles == []
        assert meta.tensor_device is None


# ═══════════════════════════════════════════════════════════════════════════════
# 8. Integration-style tests (cross-module)
# ═══════════════════════════════════════════════════════════════════════════════


class TestRDTStoreAndManagerIntegration:
    """Test interactions between RDTStore and RDTManager."""

    def test_get_rdt_object_primary_copy_not_popped(self):
        """Primary copy should be returned but not consumed."""
        from ray.experimental.rdt.rdt_manager import RDTManager

        mgr = RDTManager()
        store = mgr.rdt_store
        tensors = [FakeTensor()]
        store.add_object("obj1", tensors, is_primary=True)

        with patch("ray._private.ray_constants.RDT_FETCH_FAIL_TIMEOUT_SECONDS", 5):
            result = mgr.get_rdt_object("obj1")
        assert result is tensors
        # Object should still be in store (primary copies persist)
        assert store.has_object("obj1")

    def test_get_rdt_object_non_primary_popped(self):
        """Non-primary copy should be consumed on get."""
        from ray.experimental.rdt.rdt_manager import RDTManager

        mgr = RDTManager()
        store = mgr.rdt_store
        tensors = [FakeTensor()]
        store.add_object("obj1", tensors, is_primary=False)

        with patch("ray._private.ray_constants.RDT_FETCH_FAIL_TIMEOUT_SECONDS", 5):
            result = mgr.get_rdt_object("obj1")
        assert result is tensors
        # Object should be removed
        assert not store.has_object("obj1")


# ═══════════════════════════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-sv"])
