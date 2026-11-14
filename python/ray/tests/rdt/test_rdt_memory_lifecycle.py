"""Unit tests for the RDT memory-info callback lifecycle wiring.

These verify that the publisher injected into :class:`RDTManager` from
``Worker.rdt_manager`` resolves the current CoreWorker at call time (holding no
strong bound CoreWorker reference) and that shutdown discards the manager, all
without needing a Ray cluster.
"""
import gc
import weakref

import pytest

from ray._private.worker import Worker


class _FakeCoreWorker:
    def set_rdt_object_memory_info(
        self, object_id_bytes, device_bytes, num_copies, num_errors
    ):
        pass


def test_rdt_memory_publisher_resolves_core_worker_at_call_time():
    worker = Worker()
    calls = []

    class Core1:
        def set_rdt_object_memory_info(self, *args):
            calls.append(("core1", args))

    class Core2:
        def set_rdt_object_memory_info(self, *args):
            calls.append(("core2", args))

    worker.core_worker = Core1()
    worker._rdt_memory_info_callback(b"id", [("cpu", 8)], 1, 0)
    assert calls == [("core1", (b"id", [("cpu", 8)], 1, 0))]

    # Replacing the core worker changes what the publisher resolves on the next
    # call: it looks up the current core worker instead of holding a bound ref.
    worker.core_worker = Core2()
    worker._rdt_memory_info_callback(b"id", [], 0, 0)
    assert calls[-1] == ("core2", (b"id", [], 0, 0))


def test_rdt_memory_publisher_does_not_retain_core_worker():
    worker = Worker()
    core_worker = _FakeCoreWorker()
    worker.core_worker = core_worker
    ref = weakref.ref(core_worker)

    # The publisher is bound to Worker, not CoreWorker, so holding it must not
    # keep the core worker alive after the worker drops its reference.
    callback = worker._rdt_memory_info_callback
    del core_worker
    del worker.core_worker
    gc.collect()

    assert ref() is None
    # The publisher still exists (bound to the still-live worker).
    assert callback is not None


def test_shutdown_rdt_manager_shuts_down_without_reset():
    # The reset happens after CoreWorker deletion in `ray.shutdown`, not here:
    # native GC callbacks still need the manager during teardown.
    worker = Worker()

    class FakeManager:
        def __init__(self):
            self.shutdown_called = False

        def shutdown(self):
            self.shutdown_called = True

    manager = FakeManager()
    worker._rdt_manager = manager
    worker.shutdown_rdt_manager()
    assert manager.shutdown_called
    assert worker._rdt_manager is manager  # not reset here


def test_rdt_manager_standalone_defaults_no_callback():
    from ray.experimental.rdt.rdt_manager import RDTManager

    manager = RDTManager()
    assert manager._memory_info_callback is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
