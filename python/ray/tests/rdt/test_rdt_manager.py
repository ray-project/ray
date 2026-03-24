"""Unit tests for RDTManager."""
import re
import sys
from dataclasses import dataclass
from typing import Any, List

import pytest

from ray.experimental import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
    register_tensor_transport,
)
from ray.experimental.rdt.rdt_manager import RDTManager, RDTMeta
from ray.experimental.rdt.tensor_transport_manager import FetchRequest
from ray.exceptions import GetTimeoutError

_BACKEND_NAME = "TEST_PIPELINE"
_TWO_SIDED_BACKEND_NAME = "TEST_TWO_SIDED"


@dataclass
class _TestCommMeta(CommunicatorMetadata):
    pass


@dataclass
class _TrackedFetchRequest(FetchRequest):
    """FetchRequest subclass that records when it is deleted."""

    def __del__(self):
        _PipelineCheckingTransport.deleted_requests.add(self.obj_id)


class _PipelineCheckingTransport(TensorTransportManager):
    """Fake one-sided transport that records the order of fetch/wait calls.

    Each fetch_multiple_tensors call appends ("fetch", obj_id) to call_log,
    and each wait_fetch_complete call appends ("wait", obj_id).  The test
    asserts that all fetch entries appear before any wait entry.

    call_log is a class-level list so the singleton instance created by
    get_tensor_transport_manager records to the same list across all tests.
    """

    call_log: List = []
    fail_on_wait: set = set()
    wait_delay: float = 0
    deleted_requests: set = set()

    def tensor_transport_backend(self) -> str:
        return _BACKEND_NAME

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor) -> bool:
        return True

    def extract_tensor_transport_metadata(self, obj_id, rdt_object):
        return TensorTransportMetadata(tensor_meta=[], tensor_device="cpu")

    def get_communicator_metadata(self, src_actor, dst_actor, backend=None):
        return _TestCommMeta()

    def fetch_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata,
        communicator_metadata,
        target_buffers=None,
    ) -> FetchRequest:
        self.__class__.call_log.append(("fetch", obj_id))
        return _TrackedFetchRequest(obj_id=obj_id, tensors=[f"val:{obj_id}"])

    def wait_fetch_complete(
        self, fetch_request: FetchRequest, timeout: float = -1
    ) -> List[Any]:
        if self.__class__.wait_delay > 0:
            import time
            time.sleep(self.__class__.wait_delay)
        self.__class__.call_log.append(("wait", fetch_request.obj_id))
        if fetch_request.obj_id in self.__class__.fail_on_wait:
            raise RuntimeError(f"wait failed for {fetch_request.obj_id}")
        return fetch_request.tensors

    def recv_multiple_tensors(self, obj_id, meta, comm_meta, target_buffers=None):
        return []

    def send_multiple_tensors(self, tensors, meta, comm_meta):
        pass

    def garbage_collect(self, obj_id, meta, tensors):
        pass

    def abort_transport(self, obj_id, comm_meta):
        pass


class _TwoSidedTransport(TensorTransportManager):
    """Fake two-sided transport (e.g. NCCL/GLOO style)."""

    def tensor_transport_backend(self) -> str:
        return _TWO_SIDED_BACKEND_NAME

    @staticmethod
    def is_one_sided() -> bool:
        return False

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor) -> bool:
        return True

    def extract_tensor_transport_metadata(self, obj_id, rdt_object):
        return TensorTransportMetadata(tensor_meta=[], tensor_device="cpu")

    def get_communicator_metadata(self, src_actor, dst_actor, backend=None):
        return _TestCommMeta()

    def fetch_multiple_tensors(self, obj_id, meta, comm_meta, target_buffers=None):
        raise NotImplementedError

    def recv_multiple_tensors(self, obj_id, meta, comm_meta, target_buffers=None):
        raise NotImplementedError

    def send_multiple_tensors(self, tensors, meta, comm_meta):
        raise NotImplementedError

    def garbage_collect(self, obj_id, meta, tensors):
        pass

    def abort_transport(self, obj_id, comm_meta):
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def register_test_transports():
    """Register both test transports once for the lifetime of the module."""
    try:
        register_tensor_transport(_BACKEND_NAME, ["cpu"], _PipelineCheckingTransport, list)
    except ValueError:
        pass  # already registered (e.g. test module loaded more than once)
    try:
        register_tensor_transport(_TWO_SIDED_BACKEND_NAME, ["cpu"], _TwoSidedTransport, list)
    except ValueError:
        pass


@pytest.fixture(autouse=True)
def clear_call_log():
    """Reset the pipeline transport's call log and error config before each test."""
    _PipelineCheckingTransport.call_log.clear()
    _PipelineCheckingTransport.fail_on_wait.clear()
    _PipelineCheckingTransport.wait_delay = 0
    _PipelineCheckingTransport.deleted_requests.clear()


def _build_manager(object_ids: List[str], backend: str = _BACKEND_NAME) -> RDTManager:
    """Return an RDTManager pre-populated with fake RDT metadata.

    Uses a real RDTStore so no Ray cluster is required.
    All objects are non-primary copies (pop_object=True in fetch_and_get_rdt_objects).
    """
    manager = RDTManager()

    meta = TensorTransportMetadata(tensor_meta=[], tensor_device="cpu")
    for obj_id in object_ids:
        manager.set_rdt_metadata(
            obj_id,
            RDTMeta(
                src_actor=None,
                tensor_transport_backend=backend,
                tensor_transport_meta=meta,
                sent_dest_actors=set(),
                sent_to_src_actor_and_others_warned=False,
                target_buffers=None,
            ),
        )

    return manager


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_fetch_and_get():
    object_ids = ["obj1", "obj2", "obj3"]
    manager = _build_manager(object_ids)
    result = manager.fetch_and_get_rdt_objects(object_ids)

    call_log = _PipelineCheckingTransport.call_log
    fetch_indices = [i for i, (kind, _) in enumerate(call_log) if kind == "fetch"]
    wait_indices = [i for i, (kind, _) in enumerate(call_log) if kind == "wait"]

    # All fetch_multiple_tensors calls must precede all wait_fetch_complete
    # calls.
    assert len(fetch_indices) == len(object_ids), f"call_log={call_log}"
    assert len(wait_indices) == len(object_ids), f"call_log={call_log}"
    assert max(fetch_indices) < min(wait_indices), (
        f"Expected all fetches before all waits, got call_log={call_log}"
    )

    # One entry per requested object ID.
    assert set(result.keys()) == set(object_ids)

    call_log = _PipelineCheckingTransport.call_log
    # Each object ID triggers exactly one fetch and one wait.
    fetched = [oid for kind, oid in call_log if kind == "fetch"]
    waited = [oid for kind, oid in call_log if kind == "wait"]

    assert sorted(fetched) == sorted(object_ids)
    assert sorted(waited) == sorted(object_ids)


def test_primary_copy_objects_skip_fetch():
    """Objects already in the store as primary copies must not trigger a fetch."""
    secondary_ids = ["secondary1", "secondary2"]
    primary_id = "primary1"
    manager = _build_manager(secondary_ids + [primary_id])

    # Add the primary-copy object to the store directly. Phase 1 of
    # fetch_and_get_rdt_objects skips objects whose is_primary_copy() returns True.
    manager.rdt_store.add_object(primary_id, ["primary_value"], is_primary=True)
    result = manager.fetch_and_get_rdt_objects(secondary_ids + [primary_id])

    call_log = _PipelineCheckingTransport.call_log
    fetched = [oid for kind, oid in call_log if kind == "fetch"]
    assert set(fetched) == set(secondary_ids), (
        f"Primary-copy objects should not be fetched; got fetched={fetched}"
    )
    # One fetch + one wait for each secondary object; zero for the primary one.
    assert len(call_log) == len(secondary_ids) * 2, f"call_log={call_log}"
    # All objects should be returned in the results.
    assert set(result.keys()) == set(secondary_ids + [primary_id])
    assert result[primary_id] == ["primary_value"]


def test_empty_object_list_returns_empty_dict():
    """Calling fetch_and_get_rdt_objects with an empty list returns an empty dict."""
    manager = _build_manager([])
    result = manager.fetch_and_get_rdt_objects([])

    assert result == {}
    assert _PipelineCheckingTransport.call_log == []


def test_two_sided_transport_raises_on_fetch_and_get_rdt_objects():
    """ray.get (use_object_store=False) must raise ValueError for two-sided transports."""
    obj_id = "two_sided_obj"
    manager = _build_manager([obj_id], backend=_TWO_SIDED_BACKEND_NAME)

    with pytest.raises(
        ValueError,
        match=re.escape(
            f"ray.get is not allowed on RDT objects using the two-sided transport {_TWO_SIDED_BACKEND_NAME}. "
            "Either use a one-sided RDT transport or pass _use_object_store=True to ray.get to fetch the object through the object store instead."
        ),
    ):
        manager.fetch_and_get_rdt_objects([obj_id], use_object_store=False)


def test_fetch_requests_deleted_on_exception():
    """If _wait_fetch raises, all FetchRequests are deleted (cleaning up resources via __del__)."""
    import gc

    object_ids = ["obj1", "obj2", "obj3"]
    manager = _build_manager(object_ids)
    _PipelineCheckingTransport.fail_on_wait.add("obj1")

    with pytest.raises(RuntimeError, match="wait failed for obj1"):
        manager.fetch_and_get_rdt_objects(object_ids)

    gc.collect()
    assert _PipelineCheckingTransport.deleted_requests == set(object_ids), (
        f"All FetchRequests must be GCed even if one fails; "
        f"deleted={_PipelineCheckingTransport.deleted_requests}"
    )


def test_object_fetch_timed_out_error():
    """fetch_and_get_rdt_objects raises ObjectFetchTimedOutError when RDT timeout is hit."""
    from ray.exceptions import ObjectFetchTimedOutError

    object_ids = ["obj1", "obj2"]
    manager = _build_manager(object_ids)
    # Make wait_fetch_complete slow enough to exceed a very short timeout.
    _PipelineCheckingTransport.wait_delay = 0.2

    with pytest.raises(ObjectFetchTimedOutError):
        # timeout=None means no user timeout, so only RDT timeout applies.
        # We monkeypatch the constant to a very small value.
        import ray._private.ray_constants as rc
        original = rc.RDT_FETCH_FAIL_TIMEOUT_SECONDS
        rc.RDT_FETCH_FAIL_TIMEOUT_SECONDS = 0.1
        try:
            manager.fetch_and_get_rdt_objects(object_ids)
        finally:
            rc.RDT_FETCH_FAIL_TIMEOUT_SECONDS = original


def test_get_timed_out_error():
    """fetch_and_get_rdt_objects raises GetTimeoutError when user timeout is hit."""
    object_ids = ["obj1", "obj2"]
    manager = _build_manager(object_ids)
    # Make wait_fetch_complete slow enough to exceed a very short timeout.
    _PipelineCheckingTransport.wait_delay = 0.2

    # Check that user timeout triggers before fetch fail timeout.
    with pytest.raises(GetTimeoutError):
        import ray._private.ray_constants as rc
        original = rc.RDT_FETCH_FAIL_TIMEOUT_SECONDS
        rc.RDT_FETCH_FAIL_TIMEOUT_SECONDS = 1
        try:
            manager.fetch_and_get_rdt_objects(object_ids, timeout=0.1)
        finally:
            rc.RDT_FETCH_FAIL_TIMEOUT_SECONDS = original


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
