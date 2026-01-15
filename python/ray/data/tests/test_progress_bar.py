import functools
import logging
from unittest.mock import patch

import pytest
from pytest import fixture

import ray
from ray.data._internal.progress.progress_bar import ProgressBar


@fixture(params=[True, False])
def enable_tqdm_ray(request):
    context = ray.data.DataContext.get_current()
    original_use_ray_tqdm = context.use_ray_tqdm
    context.use_ray_tqdm = request.param
    yield request.param
    context.use_ray_tqdm = original_use_ray_tqdm


def test_progress_bar(enable_tqdm_ray):
    total = 10
    # Used to record the total value of the bar at close
    total_at_close = 0

    def patch_close(bar):
        nonlocal total_at_close
        total_at_close = 0
        original_close = bar.close

        @functools.wraps(original_close)
        def wrapped_close():
            nonlocal total_at_close
            total_at_close = bar.total

        bar.close = wrapped_close

    # Test basic usage
    with patch("sys.stdout.isatty", return_value=True):
        pb = ProgressBar("", total, "unit", enabled=True)
        assert pb._bar is not None
        patch_close(pb._bar)
        for _ in range(total):
            pb.update(1)
        pb.close()

        assert pb._progress == total
        assert total_at_close == total

    # Test if update() exceeds the original total, the total will be updated.
    with patch("sys.stdout.isatty", return_value=True):
        pb = ProgressBar("", total, "unit", enabled=True)
        assert pb._bar is not None
        patch_close(pb._bar)
        new_total = total * 2
        for _ in range(new_total):
            pb.update(1)
        pb.close()

        assert pb._progress == new_total
        assert total_at_close == new_total

    # Test that if the bar is not complete at close(), the total will be updated.
    with patch("sys.stdout.isatty", return_value=True):
        pb = ProgressBar("", total, "unit")
        assert pb._bar is not None
        patch_close(pb._bar)
        new_total = total // 2
        for _ in range(new_total):
            pb.update(1)
        pb.close()

        assert pb._progress == new_total
        assert total_at_close == new_total

    # Test updating the total
    with patch("sys.stdout.isatty", return_value=True):
        pb = ProgressBar("", total, "unit", enabled=True)
        assert pb._bar is not None
        patch_close(pb._bar)
        new_total = total * 2
        pb.update(0, new_total)

        assert pb._bar.total == new_total
        pb.update(total + 1, total)
        assert pb._bar.total == total + 1
        pb.close()


@pytest.mark.parametrize(
    "name, expected_description, max_line_length, should_emit_warning",
    [
        ("Op", "Op", 2, False),
        ("Op->Op", "Op->Op", 5, False),
        ("Op->Op->Op", "Op->...->Op", 9, True),
        ("Op->Op->Op", "Op->Op->Op", 10, False),
        # Test case for https://github.com/ray-project/ray/issues/47679.
        ("spam", "spam", 1, False),
    ],
)
def test_progress_bar_truncates_chained_operators(
    name,
    expected_description,
    max_line_length,
    should_emit_warning,
    caplog,
    propagate_logs,
):
    with patch("sys.stdout.isatty", return_value=True):
        with patch.object(ProgressBar, "MAX_NAME_LENGTH", max_line_length):
            pb = ProgressBar(name, None, "unit")

        assert pb.get_description() == expected_description
        if should_emit_warning:
            assert any(
                record.levelno == logging.WARNING
                and "Truncating long operator name" in record.message
                for record in caplog.records
            ), caplog.records


def test_progress_bar_non_interactive_terminal(enable_tqdm_ray):
    """Test progress bar behavior in non-interactive terminals."""
    import ray._private.worker as worker

    total = 100

    # Mock non-interactive terminal on driver (not in worker)
    with patch("sys.stdout.isatty", return_value=False):
        # On driver with non-interactive terminal, always falls back to logging
        pb = ProgressBar("test", total, "unit", enabled=True)
        assert pb._bar is None
        assert pb._use_logging is True

    # Mock non-interactive terminal in Ray worker with tqdm_ray
    with patch("sys.stdout.isatty", return_value=False), patch.object(
        worker.global_worker, "mode", worker.WORKER_MODE
    ):
        pb = ProgressBar("test", total, "unit", enabled=True)
        if enable_tqdm_ray:
            # tqdm_ray works in workers by sending JSON to driver
            assert pb._bar is not None
            assert pb._use_logging is False
        else:
            # Without tqdm_ray, falls back to logging even in worker
            assert pb._bar is None
            assert pb._use_logging is True

    # Mock interactive terminal
    with patch("sys.stdout.isatty", return_value=True):
        # With enabled=True, progress bar should be enabled in interactive terminal
        pb = ProgressBar("test", total, "unit", enabled=True)
        assert pb._bar is not None
        assert pb._use_logging is False


@patch("ray.data._internal.progress.progress_bar.logger")
def test_progress_bar_logging_in_non_interactive_terminal_with_total(mock_logger):
    """Test that progress is logged in non-interactive terminals with known total."""
    total = 10

    # Mock time to ensure logging occurs
    with patch(
        "ray.data._internal.progress.progress_bar.time.time", side_effect=[0, 10]
    ), patch("sys.stdout.isatty", return_value=False):
        pb = ProgressBar("test", total, "unit")
        # On driver with non-interactive terminal, falls back to logging
        assert pb._bar is None
        assert pb._use_logging is True

        # Reset mock to clear the "progress bar disabled" log call
        mock_logger.info.reset_mock()

        # Update progress - should log
        pb.update(5)
        # Verify logger.info was called exactly twice with expected messages
        assert mock_logger.info.call_count == 2
        mock_logger.info.assert_any_call("=== Ray Data Progress {test} ===")
        mock_logger.info.assert_any_call("test: Progress Completed 5 / 10")


@patch("ray.data._internal.progress.progress_bar.logger")
def test_progress_bar_logging_in_non_interactive_terminal_without_total(mock_logger):
    """Test that progress is logged in non-interactive terminals with unknown total."""

    # Mock time to ensure logging occurs
    with patch(
        "ray.data._internal.progress.progress_bar.time.time", side_effect=[0, 10]
    ), patch("sys.stdout.isatty", return_value=False):
        pb = ProgressBar("test2", None, "unit")
        # On driver with non-interactive terminal, falls back to logging
        assert pb._bar is None
        assert pb._use_logging is True

        # Reset mock to clear the "progress bar disabled" log call
        mock_logger.info.reset_mock()

        # Update progress - should log
        pb.update(3)

        # Verify logger.info was called exactly twice with expected messages
        assert mock_logger.info.call_count == 2
        mock_logger.info.assert_any_call("=== Ray Data Progress {test2} ===")
        mock_logger.info.assert_any_call("test2: Progress Completed 3 / ?")


def test_progress_bar_disabled():
    """Test that progress bar is disabled when enabled=False."""
    with patch("sys.stdout.isatty", return_value=True):
        pb = ProgressBar("test", 100, "unit", enabled=False)
        assert pb._bar is None
        assert pb._use_logging is False


def test_progress_bar_tqdm_not_installed(enable_tqdm_ray):
    """Test behavior when tqdm package is not installed."""
    with patch("sys.stdout.isatty", return_value=True):
        # Mock tqdm not being installed
        with patch("ray.data._internal.progress.progress_bar.tqdm", None):
            pb = ProgressBar("test", 100, "unit", enabled=True)
            if enable_tqdm_ray:
                # tqdm_ray still works (part of Ray, no tqdm dependency)
                assert pb._bar is not None
                assert pb._use_logging is False
            else:
                # No progress bar available (interactive, so no logging fallback)
                assert pb._bar is None
                assert pb._use_logging is False


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
