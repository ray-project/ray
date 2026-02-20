import sys
import time
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.progress import (
    get_progress_manager,
)
from ray.data._internal.progress.async_progress_wrapper import (
    AsyncProgressManagerWrapper,
    _DaemonThreadPoolExecutor,
)
from ray.data._internal.progress.base_progress import BaseExecutionProgressManager
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa


class BlockingProgressManager(BaseExecutionProgressManager):
    """Mock progress manager that blocks indefinitely on all operations."""

    def __init__(self):
        self.operations_called = []

    def start(self):
        self.operations_called.append("start")
        time.sleep(999)

    def refresh(self):
        self.operations_called.append("refresh")
        time.sleep(999)

    def update_total_progress(self, *args, **kwargs):
        self.operations_called.append("update_total_progress")
        time.sleep(999)

    def update_total_resource_status(self, *args, **kwargs):
        self.operations_called.append("update_total_resource_status")
        time.sleep(999)

    def update_operator_progress(self, *args, **kwargs):
        self.operations_called.append("update_operator_progress")
        time.sleep(999)

    def close_with_finishing_description(self, description, success):
        self.operations_called.append("close")
        time.sleep(999)


class ExceptionProgressManager(BaseExecutionProgressManager):
    """Mock progress manager that always raises exceptions."""

    def __init__(self):
        pass

    def start(self):
        raise RuntimeError("start failed")

    def refresh(self):
        raise RuntimeError("refresh failed")

    def update_total_progress(self, *args, **kwargs):
        raise RuntimeError("update_total_progress failed")

    def update_total_resource_status(self, *args, **kwargs):
        raise RuntimeError("update_total_resource_status failed")

    def update_operator_progress(self, *args, **kwargs):
        raise RuntimeError("update_operator_progress failed")

    def close_with_finishing_description(self, description, success):
        raise RuntimeError("close failed")


class FastProgressManager(BaseExecutionProgressManager):
    """Mock progress manager that completes instantly and tracks calls."""

    def __init__(self):
        self.operations_called = []

    def start(self):
        self.operations_called.append("start")

    def refresh(self):
        self.operations_called.append("refresh")

    def update_total_progress(self, *args, **kwargs):
        self.operations_called.append("update_total_progress")

    def update_total_resource_status(self, *args, **kwargs):
        self.operations_called.append("update_total_resource_status")

    def update_operator_progress(self, *args, **kwargs):
        self.operations_called.append("update_operator_progress")

    def close_with_finishing_description(self, description, success):
        self.operations_called.append("close")


class TestAsyncProgressManagerWrapperBehavior:
    """Unit tests for AsyncProgressManagerWrapper behavior."""

    @pytest.mark.timeout(10)
    def test_operations_are_non_blocking(self):
        """Test that all operations return immediately despite blocking manager."""
        blocking_manager = BlockingProgressManager()
        wrapper = AsyncProgressManagerWrapper(
            blocking_manager, max_workers=1, shutdown_timeout=1
        )

        # All operations should return immediately (not block for 999 seconds)
        start_time = time.time()

        wrapper.start()
        wrapper.refresh()
        wrapper.update_total_progress(10, 100)
        wrapper.update_total_resource_status("cpu: 4")

        mock_opstate = MagicMock()
        mock_resource_manager = MagicMock()
        wrapper.update_operator_progress(mock_opstate, mock_resource_manager)

        elapsed = time.time() - start_time

        assert elapsed < 5.0, f"Operations took {elapsed}s, should be instant"

        time.sleep(0.1)
        assert "start" in blocking_manager.operations_called

        wrapper.close_with_finishing_description("Test", True)

    @pytest.mark.timeout(10)
    def test_close_respects_shutdown_timeout(self):
        """Test that close() respects shutdown timeout and doesn't block indefinitely."""
        blocking_manager = BlockingProgressManager()
        wrapper = AsyncProgressManagerWrapper(
            blocking_manager, max_workers=1, shutdown_timeout=0.5
        )

        wrapper.start()
        wrapper.update_total_progress(10, 100)

        start_time = time.time()
        wrapper.close_with_finishing_description("Test", True)
        elapsed = time.time() - start_time

        assert elapsed < 5.0, f"Close took {elapsed}s, should respect 0.5s timeout"

    @pytest.mark.timeout(10)
    def test_handles_exceptions_gracefully(self):
        """Test that exceptions in operations don't crash the wrapper."""
        exception_manager = ExceptionProgressManager()
        wrapper = AsyncProgressManagerWrapper(exception_manager, max_workers=1)

        # All these should NOT raise exceptions (errors are logged, not raised)
        wrapper.start()
        wrapper.refresh()
        wrapper.update_total_progress(10, 100)
        wrapper.update_total_resource_status("cpu: 4")

        mock_opstate = MagicMock()
        mock_resource_manager = MagicMock()
        wrapper.update_operator_progress(mock_opstate, mock_resource_manager)

        wrapper.close_with_finishing_description("Complete", True)

    @pytest.mark.timeout(10)
    def test_handles_many_rapid_updates(self):
        """Test that wrapper handles many rapid updates without blocking or crashing."""
        fast_manager = FastProgressManager()
        wrapper = AsyncProgressManagerWrapper(fast_manager, max_workers=1)

        wrapper.start()

        for i in range(100):
            wrapper.update_total_progress(i, 100)

        wrapper.close_with_finishing_description("Complete", True)

        # Should not block or crash
        time.sleep(0.2)
        assert "update_total_progress" in fast_manager.operations_called

    @pytest.mark.timeout(10)
    def test_bounded_queue_prevents_memory_leak(self):
        """Test that pending updates don't grow unbounded when I/O is blocked."""
        blocking_manager = BlockingProgressManager()
        wrapper = AsyncProgressManagerWrapper(
            blocking_manager, max_workers=1, shutdown_timeout=1
        )

        # Submit many rapid updates of DIFFERENT operations while worker is blocked
        mock_opstate = MagicMock()
        mock_resource_manager = MagicMock()
        for i in range(200):
            wrapper.start()
            wrapper.refresh()
            wrapper.update_total_progress(i, 200)
            wrapper.update_total_resource_status(f"cpu: {i}")
            wrapper.update_operator_progress(mock_opstate, mock_resource_manager)

        with wrapper._lock:
            num_pending = len(wrapper._pending_futures)

        # The loop submits exactly 5 distinct keys:
        # start, refresh, update_total_progress, update_total_resource_status,
        # update_operator_progress:<id> — all extras are deduplicated by the replacement mechanism
        assert (
            num_pending == 5
        ), f"Expected exactly 5 pending futures (one per distinct operation key), got {num_pending}"

        wrapper.close_with_finishing_description("Test", True)

    @pytest.mark.timeout(10)
    def test_replaces_old_updates_with_new_ones(self):
        """Test that old pending updates are replaced by newer ones."""
        blocking_manager = BlockingProgressManager()
        wrapper = AsyncProgressManagerWrapper(
            blocking_manager, max_workers=1, shutdown_timeout=1
        )

        # Submit first update
        wrapper.update_total_progress(10, 100)
        time.sleep(0.1)

        # Get the first future
        with wrapper._lock:
            first_future = wrapper._pending_futures.get("update_total_progress")

        assert first_future is not None

        # Submit second update (should replace first)
        wrapper.update_total_progress(20, 100)
        time.sleep(0.1)

        # Get the second future
        with wrapper._lock:
            second_future = wrapper._pending_futures.get("update_total_progress")

        # Should be a different future
        assert (
            second_future is not first_future
        ), "New update should replace old update, not queue behind it"

        # First future should be cancelled (if it wasn't running yet)
        # or completed (if it was already running)
        assert (
            first_future.cancelled() or first_future.done() or first_future.running()
        ), "Old update should be cancelled or completed when replaced"

        wrapper.close_with_finishing_description("Test", True)


class TestAsyncProgressManagerWrapperIntegration:
    """Tests wrapping logic using get_progress_manager"""

    @pytest.fixture
    def mock_topology(self):
        """Create a mock Topology object that supports iteration"""
        topology = MagicMock()
        topology.values.return_value = []
        return topology

    @patch("sys.stdout.isatty", return_value=True)
    def test_wraps_tqdm_progress_manager(
        self, mock_isatty, mock_topology, restore_data_context
    ):
        """Tests that Async wrapper wraps TqdmExecutionProgressManager"""
        ctx = DataContext.get_current()
        ctx.use_ray_tqdm = True
        ctx.enable_progress_bars = True
        ctx.enable_rich_progress_bars = False
        ctx.enable_operator_progress_bars = True
        ctx.enable_async_progress_manager_wrapper = True

        manager = get_progress_manager(ctx, "test_id", mock_topology, False)

        assert isinstance(manager, AsyncProgressManagerWrapper)
        manager.close_with_finishing_description("Test complete", True)

    @patch("sys.stdout.isatty", return_value=True)
    def test_wrap_rich_progress_manager(
        self, mock_isatty, mock_topology, restore_data_context
    ):
        """Tests that Async wrapper wraps RichExecutionProgressManager"""
        ctx = DataContext.get_current()
        ctx.use_ray_tqdm = False
        ctx.enable_progress_bars = True
        ctx.enable_rich_progress_bars = True
        ctx.enable_async_progress_manager_wrapper = True
        manager = get_progress_manager(ctx, "test_id", mock_topology, False)

        assert isinstance(manager, AsyncProgressManagerWrapper)
        manager.close_with_finishing_description("Test complete", True)

    @patch("sys.stdout.isatty", return_value=False)
    def test_no_wrap_logging_progress_manager(
        self, mock_isatty, mock_topology, restore_data_context
    ):
        """Tests that Async wrapper doesn't wrap LoggingExecutionProgressManager"""
        ctx = DataContext.get_current()
        ctx.use_ray_tqdm = False
        ctx.enable_progress_bars = True
        ctx.enable_async_progress_manager_wrapper = True

        with patch("ray._private.worker.global_worker") as mock_worker:
            mock_worker.mode = ray._private.worker.SCRIPT_MODE

            manager = get_progress_manager(ctx, "test_id", mock_topology, False)

            assert not isinstance(manager, AsyncProgressManagerWrapper)

    @patch("sys.stdout.isatty", return_value=False)
    def test_no_wrap_noop_progress_manager(
        self, mock_isatty, mock_topology, restore_data_context
    ):
        """Tests that Async wrapper does not wrap NoopExecutionProgressManager"""
        ctx = DataContext.get_current()
        ctx.enable_progress_bars = False
        ctx.enable_async_progress_manager_wrapper = True
        manager = get_progress_manager(ctx, "test_id", mock_topology, False)

        assert not isinstance(manager, AsyncProgressManagerWrapper)

    @patch("sys.stdout.isatty", return_value=True)
    def test_does_not_wrap_when_disabled(
        self, mock_isatty, mock_topology, restore_data_context
    ):
        """Test that wrapper is NOT used when enable_async_progress_manager_wrapper is False"""
        ctx = DataContext.get_current()
        ctx.use_ray_tqdm = True
        ctx.enable_progress_bars = True
        ctx.enable_async_progress_manager_wrapper = False

        manager = get_progress_manager(ctx, "test_id", mock_topology, False)

        assert not isinstance(manager, AsyncProgressManagerWrapper)


@pytest.mark.timeout(20)
def test_async_progress_updater_non_blocking():
    """Test that slow tqdm updates don't block the streaming executor.

    If tqdm.update or tqdm.close blocks indefinitely, this test will timeout after 5 seconds.
    If the async progress updater works (including cleanup), the test completes quickly.
    """
    import time
    from unittest.mock import patch

    def blocking_tqdm_operation(self, *args, **kwargs):
        """Simulate completely blocked terminal I/O"""
        time.sleep(999)  # Sleep indefinitely to simulate blocked terminal
        pass

    try:
        import tqdm

        # Mock update to block indefinitely
        with patch.object(tqdm.tqdm, "update", blocking_tqdm_operation):
            # Create and process a dataset that will trigger progress updates
            ds = ray.data.range(100, override_num_blocks=10)
            result = ds.map_batches(lambda batch: batch).take_all()

            # If we reach this point, the async progress updater worked for both
            # execution and cleanup
            assert len(result) == 100

    except ImportError:
        # Skip test if tqdm not available
        pytest.skip("tqdm not available")


def test_worker_threads_are_daemon():
    executor = _DaemonThreadPoolExecutor(max_workers=2)
    future = executor.submit(lambda: None)
    future.result(timeout=2)
    for t in executor._threads:
        assert t.daemon, f"Thread {t.name} is not a daemon thread"
    executor.shutdown(wait=True)


def test_submit_after_shutdown_raises():
    executor = _DaemonThreadPoolExecutor(max_workers=1)
    executor.shutdown(wait=True)
    with pytest.raises(
        RuntimeError, match="cannot schedule new futures after shutdown"
    ):
        executor.submit(lambda: None)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
