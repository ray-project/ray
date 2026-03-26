"""Tests for interactive command handler."""

import pytest

# We need to test handle_command, but it's defined inside run_interactive.
# Instead, test the helper functions that are module-level.

from ray.llm._internal.serve.benchmark.interactive import (
    RuntimeState,
    _percentile,
    _summarize_metrics,
)
from ray.llm._internal.serve.benchmark.multiturn_bench import TurnMetric


class TestSummarizeMetrics:
    def test_empty_metrics(self):
        result = _summarize_metrics([], 10.0)
        assert result["requests"] == 0

    def test_basic_metrics(self):
        metrics = [
            TurnMetric(
                session_id="s0",
                turn=0,
                ttft_ms=10.0,
                fc_ms=20.0,
                tpot_ms=5.0,
                latency_ms=100.0,
                input_tokens=50,
                output_tokens=20,
                start_time_ms=0.0,
            ),
            TurnMetric(
                session_id="s1",
                turn=0,
                ttft_ms=15.0,
                fc_ms=25.0,
                tpot_ms=6.0,
                latency_ms=120.0,
                input_tokens=60,
                output_tokens=25,
                start_time_ms=100.0,
            ),
        ]
        result = _summarize_metrics(metrics, 2.0)
        assert result["requests"] == 2
        assert result["avg_ttft_ms"] == pytest.approx(12.5, abs=0.1)
        assert result["throughput_tok_s"] == pytest.approx(22.5, abs=0.1)


class TestRuntimeState:
    def test_default_init(self):
        state = RuntimeState()
        assert state.current_qps == 0.0
        assert state.measurement_metrics == []
        assert state.last_window_metrics == []
        assert state.measurement_active is False


class TestPercentile:
    def test_empty(self):
        assert _percentile([], 50) == 0.0

    def test_basic(self):
        assert _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 50) == pytest.approx(
            3.0, abs=0.1
        )
