import pytest

from ray.serve._private import tracing_utils


class TestIsTracingEnabled:
    """Tests for the is_tracing_enabled() mutable flag."""

    def setup_method(self):
        """Reset the flag before each test."""
        self._original = tracing_utils._tracing_enabled
        tracing_utils._tracing_enabled = False

    def teardown_method(self):
        """Restore the flag after each test."""
        tracing_utils._tracing_enabled = self._original

    def test_disabled_by_default(self):
        """_tracing_enabled is False before setup_tracing is called."""
        assert tracing_utils.is_tracing_enabled() is False

    def test_enabled_after_flag_set(self):
        """is_tracing_enabled() returns True when flag is set and trace exists."""
        tracing_utils._tracing_enabled = True
        if tracing_utils.trace is not None:
            assert tracing_utils.is_tracing_enabled() is True
        else:
            # If opentelemetry is not installed, should still be False
            assert tracing_utils.is_tracing_enabled() is False

    def test_disabled_when_flag_false(self):
        """is_tracing_enabled() returns False when flag is explicitly False."""
        tracing_utils._tracing_enabled = False
        assert tracing_utils.is_tracing_enabled() is False
