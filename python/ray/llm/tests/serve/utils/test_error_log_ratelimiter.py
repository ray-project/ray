"""Tests for the fatal-engine-error log rate limiter in server_utils."""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from vllm.v1.engine.exceptions import EngineDeadError

from ray.llm._internal.serve.utils import server_utils

COOLDOWN = 10.0


def _make_fatal_error() -> Exception:
    return EngineDeadError("engine is dead")


def _make_ray_wrapped_fatal_error() -> Exception:
    """Simulate Ray's RayTaskError wrapping."""

    class WrappedEngineDeadError(EngineDeadError):
        pass

    return WrappedEngineDeadError("engine is dead")


@contextmanager
def _patched_logger_and_time(start: float = 100.0):
    """Patch ``time.monotonic`` and the server_utils logger.

    Yields (mock_logger, mock_time) via mock_time.return_value = <new_value>.
    """
    mock_time = MagicMock(return_value=start)
    with (
        patch.object(server_utils, "logger") as mock_logger,
        patch("time.monotonic", mock_time),
    ):
        yield mock_logger, mock_time


@pytest.fixture
def handler():
    return server_utils._FatalEngineErrorLogHandler(cooldown_s=COOLDOWN)


class TestIsFatalEngineError:
    """Tests if fatal engine errors are identified and other errors are untouched."""

    def test_bare_engine_dead_error(self):
        assert server_utils._is_fatal_engine_error(_make_fatal_error())

    def test_ray_wrapped_engine_dead_error(self):
        assert server_utils._is_fatal_engine_error(_make_ray_wrapped_fatal_error())

    def test_non_fatal_exception(self):
        assert not server_utils._is_fatal_engine_error(ValueError("hi"))

    def test_runtime_error_is_not_fatal(self):
        assert not server_utils._is_fatal_engine_error(RuntimeError("error"))


class TestFatalEngineErrorLogHandler:
    """Checks that the handler suppresses duplicate logs on fatal errors
    within cooldown_s and keeps other logs undisturbed.
    """

    def test_first_fatal_logs_full_traceback(self, handler):
        exc = _make_fatal_error()
        with _patched_logger_and_time() as (mock_logger, _):
            handler.log(exc, request_id="req-1", status_code=500)

        mock_logger.error.assert_called_once()
        assert mock_logger.error.call_args.kwargs["exc_info"] is exc

    def test_suppressed_within_cooldown(self, handler):
        exc = _make_fatal_error()
        with _patched_logger_and_time() as (mock_logger, _):
            handler.log(exc, request_id="req-1", status_code=500)
            handler.log(exc, request_id="req-2", status_code=500)
            handler.log(exc, request_id="req-3", status_code=500)
            handler.log(exc, request_id="req-4", status_code=500)

        mock_logger.error.assert_called_once()

    def test_summary_emitted_after_cooldown(self, handler):
        exc = _make_fatal_error()
        with _patched_logger_and_time() as (mock_logger, mock_time):
            handler.log(exc, request_id="req-1", status_code=500)
            for i in range(5):
                handler.log(exc, request_id=f"req-s{i}", status_code=500)

            mock_time.return_value = 100.0 + COOLDOWN + 1
            handler.log(exc, request_id="req-trigger", status_code=500)

        assert mock_logger.error.call_count == 2
        summary_call = mock_logger.error.call_args_list[1]
        assert "Suppressed" in summary_call[0][0]
        assert summary_call[0][1] == 6

    def test_reset_after_quiet_period_logs_full_traceback(self, handler):
        """Tests that after 2x cooldown of silence, the next fatal error should
        be treated as a new event and log a full traceback again."""
        exc = _make_fatal_error()
        with _patched_logger_and_time() as (mock_logger, mock_time):
            # First crash gives full traceback
            handler.log(exc, request_id="req-1", status_code=500)
            assert mock_logger.error.call_count == 1
            assert mock_logger.error.call_args.kwargs["exc_info"] is exc

            # Suppress a few within cooldown
            for i in range(3):
                handler.log(exc, request_id=f"req-s{i}", status_code=500)
            assert mock_logger.error.call_count == 1

            # Move time past 2x cooldown
            mock_time.return_value = 100.0 + 2 * COOLDOWN + 1

            # Second crash should get a fresh full traceback
            handler.log(exc, request_id="req-new-crash", status_code=500)

        assert mock_logger.error.call_count == 2
        new_crash_call = mock_logger.error.call_args_list[1]
        assert new_crash_call.kwargs["exc_info"] is exc

    def test_non_fatal_500_logs_every_call(self, handler):
        with _patched_logger_and_time() as (mock_logger, _):
            for i in range(5):
                handler.log(ValueError("a"), request_id=f"r{i}", status_code=500)

        assert mock_logger.error.call_count == 5

    def test_non_fatal_4xx_logs_every_call(self, handler):
        with _patched_logger_and_time() as (mock_logger, _):
            for i in range(5):
                handler.log(ValueError("a"), request_id=f"r{i}", status_code=400)

        assert mock_logger.warning.call_count == 5


class TestFatalEngineErrorRateLimiting:
    """
    Verify internal state transitions and rate limiting invariants.
    """

    def test_initial_state(self, handler):
        assert not handler._first_logged
        assert handler._suppressed_count == 0
        assert handler._last_summary_time == 0.0

    def test_first_fatal_sets_state(self, handler):
        with _patched_logger_and_time():
            handler.log(
                _make_fatal_error(),
                request_id="r1",
                status_code=500,
            )

        assert handler._first_logged
        assert handler._suppressed_count == 0
        assert handler._last_summary_time == 100.0

    def test_suppressed_count_increments(self, handler):
        exc = _make_fatal_error()
        handler.log(exc, request_id="r1", status_code=500)
        handler.log(exc, request_id="r2", status_code=500)
        handler.log(exc, request_id="r3", status_code=500)
        handler.log(exc, request_id="r4", status_code=500)

        assert handler._suppressed_count == 3

    def test_cooldown_resets_count(self, handler):
        exc = _make_fatal_error()
        with _patched_logger_and_time() as (_, mock_time):
            handler.log(exc, request_id="r1", status_code=500)
            for i in range(5):
                handler.log(exc, request_id=f"r{i+2}", status_code=500)
            assert handler._suppressed_count == 5
            assert handler._last_summary_time == 100

            mock_time.return_value = 100.0 + COOLDOWN + 1
            handler.log(exc, request_id="r-trigger", status_code=500)

        assert handler._suppressed_count == 0
        assert handler._last_summary_time == 100.0 + COOLDOWN + 1

    def test_multiple_cooldown_windows(self, handler):
        exc = _make_fatal_error()
        with _patched_logger_and_time() as (mock_logger, mock_time):
            handler.log(exc, request_id="r1", status_code=500)
            handler.log(exc, request_id="r2", status_code=500)
            handler.log(exc, request_id="r3", status_code=500)
            assert handler._suppressed_count == 2

            mock_time.return_value = 100.0 + COOLDOWN + 1
            handler.log(exc, request_id="r4", status_code=500)
            assert handler._suppressed_count == 0

            handler.log(exc, request_id="r5", status_code=500)
            assert handler._suppressed_count == 1

            mock_time.return_value = 100.0 + (COOLDOWN + 1) * 2
            handler.log(exc, request_id="r6", status_code=500)
            assert handler._suppressed_count == 0

        assert mock_logger.error.call_count == 3

    def test_non_fatal_does_not_affect_fatal_state(self, handler):
        handler.log(ValueError("a"), request_id="r1", status_code=500)
        handler.log(ValueError("b"), request_id="r2", status_code=400)

        assert not handler._first_logged
        assert handler._suppressed_count == 0
        assert handler._last_summary_time == 0


class TestGetResponseForError:
    """Checks that get_response_for_error routes through the fatal error log handler."""

    def test_returns_error_response_and_invokes_fatal_handler(self):
        exc = _make_fatal_error()
        with patch.object(
            server_utils._fatal_error_log_handler, "log", autospec=True
        ) as mock_log:
            resp = server_utils.get_response_for_error(exc, request_id="rid")

        mock_log.assert_called_once_with(exc, "rid", 500)
        assert resp.error is not None
        assert "rid" in resp.error.message


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
