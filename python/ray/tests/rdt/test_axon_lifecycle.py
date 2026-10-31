"""Exercise shutdown races using real asyncio threads and a fake native runtime."""

import asyncio
import gc
import sys
import threading
import unittest
import weakref
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from ray.experimental.rdt import axon_tensor_transport as adapter


class RuntimeLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.managers = []
        self.native = Mock()
        self.native.AxonRuntime.side_effect = self.make_runtime
        self.native_patch = patch.dict("sys.modules", {"axon": self.native})
        self.native_patch.start()
        self.addCleanup(self.cleanup)

    def make_runtime(self, *args, **kwargs):
        runtime = Mock()
        runtime.get_local_address.return_value = b"address"
        runtime.stop.side_effect = lambda: self.assertIs(
            asyncio.get_running_loop(), adapter._runtime_loop
        )
        return runtime

    def manager(self, start=True):
        manager = adapter.AxonNumpyTensorTransport()
        manager._receive_timeout_s = 2
        manager._array_support = Mock(return_value=(object, Mock()))
        self.managers.append(manager)
        if start:
            manager._ensure_runtime()
        return manager

    def cleanup(self):
        for manager in self.managers:
            manager.shutdown(timeout=2)
        self.native_patch.stop()
        self.assertFalse(adapter._runtime_users)

    def test_cold_shutdown_is_idempotent_and_disallows_reuse(self):
        manager = self.manager(start=False)
        manager.shutdown()
        manager.shutdown()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            manager._ensure_runtime()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            manager.extract_tensor_transport_metadata("empty", [])
        with self.assertRaisesRegex(RuntimeError, "closed"):
            manager.recv_multiple_tensors(
                "empty", adapter.AxonTransportMetadata(tensor_meta=[]), None
            )
        self.native.AxonRuntime.assert_not_called()

    def test_shared_loop_survives_until_last_runtime_and_can_be_recreated(self):
        first, second = self.manager(), self.manager()
        loop, thread = first._loop, first._loop_thread
        self.assertIs(loop, second._loop)
        first_runtime, second_runtime = first._runtime, second._runtime
        first.shutdown()
        first.shutdown()
        first_runtime.stop.assert_called_once()
        second_runtime.stop.assert_not_called()
        self.assertTrue(thread.is_alive())
        self.assertEqual(second._submit(asyncio.sleep(0, result="alive")), "alive")
        second.shutdown()
        second_runtime.stop.assert_called_once()
        self.assertFalse(thread.is_alive())
        self.assertTrue(loop.is_closed())
        replacement = self.manager()
        self.assertIsNot(replacement._loop, loop)

    def test_shutdown_drops_exports_connections_and_runtime_reference(self):
        manager = self.manager()

        class Export:
            pass

        tensor = Export()
        reference = weakref.ref(tensor)
        manager._exports["object"] = (tensor,)
        manager._connected_workers.add("peer")
        del tensor
        manager.shutdown()
        gc.collect()
        self.assertIsNone(reference())
        self.assertIsNone(manager._runtime)
        self.assertIsNone(manager._from_dlpack)
        self.assertFalse(manager._connected_workers)
        self.assertEqual(manager._local_address, b"")
        manager.garbage_collect("object", None, [])

    def test_shutdown_waits_for_an_active_receive(self):
        manager = self.manager()
        runtime = manager._runtime
        entered, release = threading.Event(), asyncio.Event()

        async def operation():
            entered.set()
            await release.wait()
            return "received"

        def receive():
            with manager._lock:
                return manager._submit(operation())

        with ThreadPoolExecutor(2) as pool:
            work = pool.submit(receive)
            self.assertTrue(entered.wait(2))
            # A short deadline must not stop the native runtime underneath IO.
            with self.assertRaises(TimeoutError):
                manager.shutdown(timeout=0.01)
            runtime.stop.assert_not_called()
            manager._loop.call_soon_threadsafe(release.set)
            self.assertEqual(work.result(2), "received")
        manager.shutdown()
        runtime.stop.assert_called_once()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            manager._ensure_runtime()

    def test_caller_timeout_keeps_pending_rpc_alive_until_shutdown_can_drain(self):
        manager = self.manager()
        runtime = manager._runtime
        release = asyncio.Event()
        manager._receive_timeout_s = 0.01
        with self.assertRaises(TimeoutError):
            manager._submit(release.wait())
        try:
            with self.assertRaisesRegex(TimeoutError, "draining"):
                manager.shutdown(timeout=0.01)
            runtime.stop.assert_not_called()
            self.assertIs(manager._runtime, runtime)
        finally:
            manager._loop.call_soon_threadsafe(release.set)
        manager.shutdown(timeout=2)
        runtime.stop.assert_called_once()

    def test_partial_startup_failure_stops_native_runtime_on_owner_loop(self):
        runtime = self.make_runtime()
        runtime.get_local_address.side_effect = RuntimeError("startup failed")
        self.native.AxonRuntime.side_effect = None
        self.native.AxonRuntime.return_value = runtime
        manager = self.manager(start=False)
        with self.assertRaisesRegex(RuntimeError, "startup failed"):
            manager._ensure_runtime()
        manager.shutdown()
        runtime.stop.assert_called_once()
        self.assertIsNone(manager._runtime)

    def test_completed_receive_does_not_retain_its_result(self):
        manager = self.manager()

        class Buffer:
            pass

        async def receive():
            return Buffer()

        buffer = manager._submit(receive())
        reference = weakref.ref(buffer)
        del buffer
        gc.collect()
        self.assertIsNone(reference())

    def test_shutdown_timeout_during_stop_can_be_retried_without_stopping_twice(self):
        manager = self.manager()
        runtime = manager._runtime
        release = threading.Event()
        runtime.stop.side_effect = lambda: release.wait(2)
        try:
            with self.assertRaises(TimeoutError):
                manager.shutdown(timeout=0.01)
            self.assertIs(manager._runtime, runtime)
        finally:
            release.set()
        manager.shutdown(timeout=2)
        runtime.stop.assert_called_once()

    def test_concurrent_shutdown_calls_stop_once(self):
        manager = self.manager()
        runtime = manager._runtime
        with ThreadPoolExecutor(4) as pool:
            list(pool.map(lambda _: manager.shutdown(), range(4)))
        runtime.stop.assert_called_once()

    def test_shutdown_waits_for_initialization_already_in_progress(self):
        manager = self.manager(start=False)
        entered, release = threading.Event(), threading.Event()
        runtime = self.make_runtime()

        def start():
            entered.set()
            self.assertTrue(release.wait(2))

        runtime.start.side_effect = start
        self.native.AxonRuntime.side_effect = None
        self.native.AxonRuntime.return_value = runtime
        with ThreadPoolExecutor(2) as pool:
            startup = pool.submit(manager._ensure_runtime)
            self.assertTrue(entered.wait(2))
            shutdown = pool.submit(manager.shutdown)
            try:
                runtime.stop.assert_not_called()
            finally:
                release.set()
            startup.result(2)
            shutdown.result(2)
        runtime.stop.assert_called_once()
        self.assertTrue(manager._closed)

    def test_native_stop_failure_retains_resources_for_retry(self):
        manager = self.manager()
        runtime = manager._runtime
        runtime.stop.side_effect = [RuntimeError("stop failed"), None]
        with self.assertRaisesRegex(RuntimeError, "stop failed"):
            manager.shutdown()
        self.assertIs(manager._runtime, runtime)
        self.assertTrue(manager._loop_thread.is_alive())
        manager.shutdown()
        self.assertEqual(runtime.stop.call_count, 2)
        self.assertTrue(manager._closed)

    def test_shutdown_on_runtime_thread_fails_without_deadlocking(self):
        manager = self.manager()

        async def close():
            manager.shutdown()

        with self.assertRaisesRegex(RuntimeError, "inside a runtime"):
            manager._submit(close())
        self.assertFalse(manager._closed)

    def test_exit_hook_closes_started_runtimes(self):
        first, second = self.manager(), self.manager()
        adapter._shutdown_runtime_managers()
        self.assertTrue(first._closed and second._closed)

    def test_worker_shutdown_closes_shared_runtimes_without_an_rdt_manager(self):
        from ray._private.worker import Worker
        from ray.experimental.rdt import util

        first, second = self.manager(), self.manager()
        runtimes = [first._runtime, second._runtime]
        loop, thread = first._loop, first._loop_thread
        registry = {"AXON": first, "AXON_TORCH": second}
        worker = SimpleNamespace(_rdt_manager=None)
        with patch.object(util, "transport_managers", registry):
            Worker.shutdown_rdt_manager(worker)
            Worker.shutdown_rdt_manager(worker)
            self.assertFalse(registry)
        for runtime in runtimes:
            runtime.stop.assert_called_once()
        self.assertFalse(thread.is_alive())
        self.assertTrue(loop.is_closed())
        self.assertFalse(adapter._runtime_users)
        self.assertIsNone(worker._rdt_manager)

    def test_failed_worker_cleanup_retains_runtime_and_can_be_retried(self):
        from ray._private.worker import Worker
        from ray.experimental.rdt import util

        first, second = self.manager(), self.manager()
        runtime = first._runtime
        runtime.stop.side_effect = [RuntimeError("stop failed"), None]
        registry = {"AXON": first, "AXON_TORCH": second}
        worker = SimpleNamespace(_rdt_manager=Mock())
        with patch.object(util, "transport_managers", registry):
            with self.assertLogs(util.logger, level="ERROR") as logs:
                Worker.shutdown_rdt_manager(worker)
            self.assertIn("Failed to shut down tensor transport AXON", logs.output[0])
            self.assertEqual(registry, {"AXON": first})
            self.assertIs(first._runtime, runtime)
            self.assertTrue(first._loop_thread.is_alive())
            self.assertIsNone(second._runtime)
            Worker.shutdown_rdt_manager(worker)
            self.assertFalse(registry)
        self.assertEqual(runtime.stop.call_count, 2)
        self.assertEqual(worker._rdt_manager.shutdown.call_count, 2)
        self.assertFalse(adapter._runtime_users)


def test_shutdown_does_not_initialize_unused_transports(monkeypatch):
    from ray._private.worker import Worker
    from ray.experimental.rdt import util

    monkeypatch.setattr(util, "transport_managers", {})
    monkeypatch.setattr(
        util,
        "_ensure_default_transports_registered",
        Mock(side_effect=AssertionError("Shutdown initialized the registry")),
    )
    monkeypatch.setitem(sys.modules, "axon", None)
    Worker.shutdown_rdt_manager(SimpleNamespace(_rdt_manager=None))
    assert not util.transport_managers


def test_shutdown_releases_registry_lock_and_preserves_replacement(monkeypatch):
    from ray.experimental.rdt import util

    replacement = Mock()
    manager = Mock()

    def close(timeout):
        assert util.transport_managers_lock.acquire(blocking=False)
        try:
            util.transport_managers["TEST"] = replacement
        finally:
            util.transport_managers_lock.release()

    manager.shutdown.side_effect = close
    monkeypatch.setattr(util, "transport_managers", {"TEST": manager})
    util.shutdown_tensor_transport_managers()
    assert util.transport_managers == {"TEST": replacement}
    replacement.shutdown.assert_not_called()


def test_shutdown_supports_transports_without_a_cleanup_override(monkeypatch):
    from ray.experimental.rdt import util
    from ray.experimental.rdt.collective_tensor_transport import GLOOTensorTransport

    manager = GLOOTensorTransport()
    monkeypatch.setattr(util, "transport_managers", {"GLOO": manager})
    util.shutdown_tensor_transport_managers()
    assert not util.transport_managers
    assert isinstance(util.get_tensor_transport_manager("GLOO"), GLOOTensorTransport)
    assert util.get_tensor_transport_manager("GLOO") is not manager


def test_shutdown_shares_timeout_across_transports(monkeypatch):
    from ray.experimental.rdt import util

    first, second = Mock(), Mock()
    monkeypatch.setattr(util, "transport_managers", {"FIRST": first, "SECOND": second})
    monkeypatch.setattr(
        util, "time", SimpleNamespace(monotonic=Mock(side_effect=[10.0, 11.0, 16.0]))
    )
    util.shutdown_tensor_transport_managers(timeout=5.0)
    first.shutdown.assert_called_once_with(timeout=4.0)
    second.shutdown.assert_called_once_with(timeout=0.0)
    assert not util.transport_managers


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
