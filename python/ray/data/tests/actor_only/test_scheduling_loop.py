"""Unit tests for the actor-only scheduling loop's wiring: executor selection
and shutdown ordering."""
import sys
from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.streaming_executor import (
    StreamingExecutor,
    get_streaming_executor_cls,
)
from ray.data._internal.experimental.execution.streaming_executor import (
    ExperimentalStreamingExecutor,
)
from ray.data.context import DataContext


def test_executor_is_selected_per_call_from_the_flag(monkeypatch):
    monkeypatch.setenv("RAY_DATA_ACTOR_ONLY_BACKEND", "1")
    assert get_streaming_executor_cls() is ExperimentalStreamingExecutor
    monkeypatch.setenv("RAY_DATA_ACTOR_ONLY_BACKEND", "0")
    assert get_streaming_executor_cls() is StreamingExecutor


def test_executor_selection_honors_dataset_override(monkeypatch):
    monkeypatch.setenv("RAY_DATA_ACTOR_ONLY_BACKEND", "1")
    ctx = DataContext.get_current().copy()
    ctx._actor_only_backend_override = False
    assert get_streaming_executor_cls(ctx) is StreamingExecutor
    assert get_streaming_executor_cls() is ExperimentalStreamingExecutor


def test_shutdown_flushes_metrics_after_the_loop_stops(monkeypatch):
    """The final metrics flush must not race the scheduling loop: it runs after
    the base shutdown (which stops the loop) and before the bank is cleared."""
    calls = []
    monkeypatch.setattr(
        StreamingExecutor,
        "shutdown",
        lambda self, force, exception=None: calls.append("base_shutdown"),
    )
    executor = ExperimentalStreamingExecutor.__new__(ExperimentalStreamingExecutor)
    executor._topology = {}
    executor._execution_started = True
    executor._shutdown = False
    executor._actor_only_metrics = MagicMock()
    executor._actor_only_metrics.maybe_update.side_effect = (
        lambda **kwargs: calls.append("flush")
    )
    executor._resource_bank = MagicMock()
    executor._resource_bank.finalize_dataset.side_effect = (
        lambda **kwargs: calls.append("finalize")
    )

    executor.shutdown(force=False)

    assert calls == ["base_shutdown", "flush", "finalize"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
