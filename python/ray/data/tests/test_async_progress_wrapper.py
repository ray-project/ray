import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.progress import (
    get_progress_manager,
)
from ray.data._internal.progress.async_progress_wrapper import (
    AsyncProgressManagerWrapper,
)
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa


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
        manager.close()

    @patch("sys.stdout.isatty", return_value=True)
    def test_wrap_rich_progress_manager(
        self, mock_isatty, mock_topology, restore_data_context
    ):
        """Tests that Async wrapper wrap NoopExecutionProgressManager"""
        ctx = DataContext.get_current()
        ctx.use_ray_tqdm = False
        ctx.enable_progress_bars = True
        ctx.enable_rich_progress_bars = True
        ctx.enable_async_progress_manager_wrapper = True
        manager = get_progress_manager(ctx, "test_id", mock_topology, False)

        assert isinstance(manager, AsyncProgressManagerWrapper)
        manager.close()

    def test_no_wrap_logging_progress_manager(
        self, mock_isatty, mock_topology, restore_data_context
    ):
        """Tests that Async wrapper doen't wrap LoggingExecutionProgressManager"""
        ctx = DataContext.get_current()
        ctx.use_ray_tqdm = False
        ctx.enable_progress_bars = True
        ctx.enable_async_progress_manager_wrapper = True

        with patch("sys.stdout.isatty", return_value=False):
            with patch("ray._private.worker.global_worker") as mock_worker:
                mock_worker.mode = ray._private.worker.SCRIPT_MODE

                manager = get_progress_manager(ctx, "test_id", mock_topology, False)

                assert not isinstance(manager, AsyncProgressManagerWrapper)

    def test_no_wrap_noop_progress_manager(
        self, mock_isatty, mock_topology, restore_data_context
    ):
        """Tests that Async wrapper ignored NoopExecutionProgressManager"""
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
