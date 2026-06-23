"""Tests for UserCallback lifecycle hooks (before_run / after_run).

Validates backward compatibility and correct bridging via UserCallbackHandler.
"""

import sys
from typing import List
from unittest.mock import MagicMock

import pytest

from ray.train import Checkpoint
from ray.train.v2._internal.callbacks.user_callback import UserCallbackHandler
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2.api.callback import UserCallback
from ray.train.v2.api.result import Result

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_run_context() -> TrainRunContext:
    """Create a mock TrainRunContext."""
    ctx = MagicMock(spec=TrainRunContext)
    ctx.run_id = "test-run-id"
    ctx.run_config = MagicMock()
    ctx.run_config.name = "test-run"
    ctx.scaling_config = MagicMock()
    return ctx


def _make_result(error: bool = False) -> Result:
    """Create a mock Result."""
    result = MagicMock(spec=Result)
    result.error = RuntimeError("boom") if error else None
    result.metrics = {"loss": 0.1}
    result.checkpoint = MagicMock(spec=Checkpoint)
    return result


# ---------------------------------------------------------------------------
# Full-lifecycle callback (implements all 4 hooks)
# ---------------------------------------------------------------------------


class FullLifecycleCallback(UserCallback):
    """A callback that records all lifecycle hook invocations."""

    def __init__(self):
        self.calls: List[str] = []

    def before_run(self, run_context: TrainRunContext) -> None:
        self.calls.append(("before_run", run_context))

    def after_report(self, run_context, metrics, checkpoint):
        self.calls.append(("after_report", run_context, metrics, checkpoint))

    def after_exception(self, run_context, worker_exceptions):
        self.calls.append(("after_exception", run_context, worker_exceptions))

    def after_run(self, run_context: TrainRunContext, result: Result) -> None:
        self.calls.append(("after_run", run_context, result))


# ---------------------------------------------------------------------------
# Backward-compatible callback (only old hooks, no before_run/after_run)
# ---------------------------------------------------------------------------


class LegacyCallback(UserCallback):
    """Simulates an existing UserCallback subclass that only implements
    after_report and after_exception. Verifies that before_run and after_run
    default to no-op without breaking."""

    def __init__(self):
        self.report_count = 0
        self.exception_count = 0

    def after_report(self, run_context, metrics, checkpoint):
        self.report_count += 1

    def after_exception(self, run_context, worker_exceptions):
        self.exception_count += 1


# ---------------------------------------------------------------------------
# Tests: backward compatibility
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    """Existing UserCallback subclasses must not break with the new hooks."""

    def test_legacy_callback_default_before_run_is_noop(self):
        """before_run should not raise on a subclass that doesn't override it."""
        cb = LegacyCallback()
        ctx = _make_run_context()
        # Should not raise
        cb.before_run(run_context=ctx)

    def test_legacy_callback_default_after_run_is_noop(self):
        """after_run should not raise on a subclass that doesn't override it."""
        cb = LegacyCallback()
        ctx = _make_run_context()
        result = _make_result()
        # Should not raise
        cb.after_run(run_context=ctx, result=result)


# ---------------------------------------------------------------------------
# Tests: UserCallbackHandler bridging
# ---------------------------------------------------------------------------


class TestUserCallbackHandlerBridge:
    """Verify UserCallbackHandler correctly bridges ControllerCallback hooks
    to UserCallback hooks."""

    def test_after_controller_start_calls_before_run(self):
        ctx = _make_run_context()
        cb = FullLifecycleCallback()
        handler = UserCallbackHandler([cb], ctx)

        handler.after_controller_start(ctx)

        assert len(cb.calls) == 1
        assert cb.calls[0][0] == "before_run"
        assert cb.calls[0][1] is ctx

    def test_after_controller_finish_calls_after_run(self):
        ctx = _make_run_context()
        result = _make_result()
        cb = FullLifecycleCallback()
        handler = UserCallbackHandler([cb], ctx)

        handler.after_controller_finish(result)

        assert len(cb.calls) == 1
        assert cb.calls[0][0] == "after_run"
        assert cb.calls[0][1] is ctx
        assert cb.calls[0][2] is result

    def test_multiple_callbacks_all_invoked(self):
        ctx = _make_run_context()
        cb1 = FullLifecycleCallback()
        cb2 = FullLifecycleCallback()
        handler = UserCallbackHandler([cb1, cb2], ctx)

        handler.after_controller_start(ctx)

        assert len(cb1.calls) == 1
        assert len(cb2.calls) == 1

    def test_handler_implements_controller_callback(self):
        """UserCallbackHandler must implement ControllerCallback to bridge
        before_run / after_run."""
        ctx = _make_run_context()
        handler = UserCallbackHandler([], ctx)
        assert isinstance(handler, ControllerCallback)

    def test_after_controller_finish_with_error_result(self):
        """after_run should be called even when training failed."""
        ctx = _make_run_context()
        result = _make_result(error=True)
        cb = FullLifecycleCallback()
        handler = UserCallbackHandler([cb], ctx)

        handler.after_controller_finish(result)

        assert len(cb.calls) == 1
        assert cb.calls[0][0] == "after_run"
        assert cb.calls[0][2].error is not None


# ---------------------------------------------------------------------------
# Tests: legacy callback through handler
# ---------------------------------------------------------------------------


class TestLegacyCallbackThroughHandler:
    """Legacy callbacks (without before_run/after_run) should work through
    the handler without errors."""

    def test_legacy_before_run_via_handler(self):
        ctx = _make_run_context()
        cb = LegacyCallback()
        handler = UserCallbackHandler([cb], ctx)
        # Should not raise
        handler.after_controller_start(ctx)

    def test_legacy_after_run_via_handler(self):
        ctx = _make_run_context()
        result = _make_result()
        cb = LegacyCallback()
        handler = UserCallbackHandler([cb], ctx)
        # Should not raise
        handler.after_controller_finish(result)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
