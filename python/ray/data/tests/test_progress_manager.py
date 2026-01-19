import builtins
from unittest.mock import MagicMock, Mock, patch

import pytest

import ray
from ray.data._internal.progress import get_progress_manager
from ray.data._internal.progress.base_progress import (
    NoopExecutionProgressManager,
)
from ray.data._internal.progress.logging_progress import (
    LoggingExecutionProgressManager,
)
from ray.data._internal.progress.rich_progress import (
    RichExecutionProgressManager,
)
from ray.data._internal.progress.tqdm_progress import (
    TqdmExecutionProgressManager,
)
from ray.data.context import DataContext


class TestGetProgressManager:
    @pytest.fixture
    def mock_ctx(self):
        """Create a mock DataContext with default settings."""
        ctx = Mock(spec=DataContext)
        ctx.enable_progress_bars = True
        ctx.enable_operator_progress_bars = True
        ctx.enable_rich_progress_bars = True
        ctx.use_ray_tqdm = False
        return ctx

    @pytest.fixture
    def mock_topology(self):
        """Create a mock Topology object that supports iteration."""
        topology = MagicMock()
        # Make it iterable by having .values() return an empty list
        topology.values.return_value = []
        return topology

    @pytest.fixture
    def setup_ray_worker(self):
        """Setup Ray worker state."""
        with patch("ray._private.worker.global_worker") as mock_worker:
            mock_worker.mode = ray._private.worker.WORKER_MODE
            yield mock_worker

    def test_progress_bars_disabled_uses_noop(self, mock_ctx, mock_topology):
        """Test that NoopExecutionProgressManager is returned when progress bars are disabled."""
        mock_ctx.enable_progress_bars = False

        manager = get_progress_manager(mock_ctx, "test_id", mock_topology, False)

        assert isinstance(manager, NoopExecutionProgressManager)

    @patch("ray.data._internal.progress.logger")
    def test_operator_progress_disabled_logs_warning(
        self, mock_logger, mock_ctx, mock_topology
    ):
        """Test warning when operator progress bars are disabled."""
        mock_ctx.enable_operator_progress_bars = False

        with patch("sys.stdout.isatty", return_value=True):
            manager = get_progress_manager(mock_ctx, "test_id", mock_topology, False)

            # should still create some non-noop progress manager.
            assert not isinstance(manager, NoopExecutionProgressManager)
            mock_logger.warning.assert_any_call(
                "Progress bars for operators disabled. To enable, "
                "set `ray.data.DataContext.get_current()."
                "enable_operator_progress_bars = True`."
            )

    @patch("sys.stdout.isatty", return_value=False)
    def test_non_atty_uses_logging_progress(self, mock_isatty, mock_ctx, mock_topology):
        """Test that LoggingExecutionProgressManager is used for non-interactive terminals."""
        mock_ctx.use_ray_tqdm = False

        with patch("ray._private.worker.global_worker") as mock_worker:
            mock_worker.mode = ray._private.worker.SCRIPT_MODE

            manager = get_progress_manager(mock_ctx, "test_id", mock_topology, False)

            assert isinstance(manager, LoggingExecutionProgressManager)

    @patch("sys.stdout.isatty", return_value=False)
    def test_ray_tqdm_in_worker_force_uses_tqdm_progress(
        self, mock_isatty, mock_ctx, mock_topology, setup_ray_worker
    ):
        """Test that TqdmExecutionProgressManager is used when use_ray_tqdm is True in Ray worker."""
        mock_ctx.use_ray_tqdm = True
        mock_ctx.enable_rich_progress_bars = False

        manager = get_progress_manager(mock_ctx, "test_id", mock_topology, False)

        assert isinstance(manager, TqdmExecutionProgressManager)

    @patch("sys.stdout.isatty", return_value=True)
    def test_tqdm_when_rich_disabled(self, mock_isatty, mock_ctx, mock_topology):
        """Test that TqdmExecutionProgressManager is used when rich is disabled."""
        mock_ctx.enable_rich_progress_bars = False
        mock_ctx.use_ray_tqdm = False

        manager = get_progress_manager(mock_ctx, "test_id", mock_topology, False)

        assert isinstance(manager, TqdmExecutionProgressManager)

    @patch("sys.stdout.isatty", return_value=True)
    def test_tqdm_when_use_ray_tqdm_enabled(self, mock_isatty, mock_ctx, mock_topology):
        """Test that TqdmExecutionProgressManager is used when use_ray_tqdm is True,
        even if RichExecutionProgressManager is enabled."""
        mock_ctx.enable_rich_progress_bars = True
        mock_ctx.use_ray_tqdm = True

        manager = get_progress_manager(mock_ctx, "test_id", mock_topology, False)

        assert isinstance(manager, TqdmExecutionProgressManager)

    @patch("sys.stdout.isatty", return_value=True)
    def test_rich_progress_default(self, mock_isatty, mock_ctx, mock_topology):
        """Test that RichExecutionProgressManager is used by default in interactive terminal."""
        manager = get_progress_manager(mock_ctx, "test_id", mock_topology, False)

        assert isinstance(manager, RichExecutionProgressManager)

    @patch("sys.stdout.isatty", return_value=True)
    @patch("ray.data._internal.progress.logger")
    def test_rich_import_error_fallback(
        self, mock_logger, mock_isatty, mock_ctx, mock_topology
    ):
        """Test fallback to NoopExecutionProgressManager when rich import fails."""
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "rich_progress" in name:
                raise ImportError("No module named 'rich'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            manager = get_progress_manager(mock_ctx, "test_id", mock_topology, False)

            assert isinstance(manager, NoopExecutionProgressManager)
            mock_logger.warning.assert_any_call(
                "[dataset]: Run `pip install rich` to enable progress reporting."
            )

    @pytest.mark.parametrize(
        "enable_progress,enable_op_progress,expected_type",
        [
            (False, False, NoopExecutionProgressManager),
            (False, True, NoopExecutionProgressManager),
            (True, False, RichExecutionProgressManager),
            (True, True, RichExecutionProgressManager),
        ],
    )
    @patch("sys.stdout.isatty", return_value=True)
    def test_progress_toggle_flag_combinations(
        self,
        mock_isatty,
        mock_ctx,
        mock_topology,
        enable_progress,
        enable_op_progress,
        expected_type,
    ):
        """Test various combinations of progress bar settings."""
        mock_ctx.enable_progress_bars = enable_progress
        mock_ctx.enable_operator_progress_bars = enable_op_progress

        manager = get_progress_manager(mock_ctx, "test_id", mock_topology, False)

        assert isinstance(manager, expected_type)


class TestLoggingProgressManager:
    @pytest.fixture
    def mock_ctx(self):
        """Create a mock DataContext with default settings."""
        ctx = Mock(spec=DataContext)
        ctx.enable_progress_bars = True
        ctx.enable_operator_progress_bars = True
        ctx.enable_rich_progress_bars = True
        ctx.use_ray_tqdm = False
        return ctx

    @pytest.fixture
    def mock_topology(self):
        """Create a mock Topology object that supports iteration."""
        topology = MagicMock()
        # Make it iterable by having .values() return an empty list
        topology.values.return_value = []
        return topology

    @patch("sys.stdout.isatty", return_value=False)
    @patch("ray.data._internal.progress.logging_progress.logger")
    def test_logging_progress_manager_properly_logs_per_interval(
        self, mock_logger, mock_isatty, mock_topology
    ):
        """Test logging progress manager logs correct output based on time intervals."""
        current_time = 0
        pg = LoggingExecutionProgressManager(
            "dataset_123", mock_topology, False, False, _get_time=lambda: current_time
        )

        # Initial logging of progress
        mock_logger.info.reset_mock()
        pg.refresh()
        mock_logger.info.assert_any_call("======= Running Dataset: dataset_123 =======")
        mock_logger.info.assert_any_call("Total Progress: 0/?")

        # Only 5 seconds passed from previous log, so logging doesn't occur
        current_time += 5
        mock_logger.info.reset_mock()
        pg.update_total_progress(1, 10)
        pg.refresh()
        assert mock_logger.info.call_count == 0

        # 10 seconds has passed, so must log previous progress.
        current_time += 10
        mock_logger.info.reset_mock()
        pg.refresh()
        mock_logger.info.assert_any_call("======= Running Dataset: dataset_123 =======")
        mock_logger.info.assert_any_call("Total Progress: 1/10")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
