"""Tests for interactive command handler, _build_spec, and helpers."""

import argparse
import asyncio
import json
import os
import sys
import tempfile

import pytest

from ray.llm._internal.serve.benchmark.interactive import (
    CommandHandler,
    RuntimeState,
    _build_spec,
    _save_window_result,
)
from ray.llm._internal.serve.benchmark.metrics import percentile, summarize_metrics
from ray.llm._internal.serve.benchmark.models import TurnMetric

# ============================================================================
# Helpers
# ============================================================================


def _make_args(**overrides) -> argparse.Namespace:
    """Create a minimal args namespace for interactive mode."""
    defaults = dict(
        base_url="http://127.0.0.1:8000",
        model="test-model",
        tokenizer=None,
        first_chunk_threshold=16,
        isl=1000,
        hit_rate=0.5,
        num_turns=1,
        osl=50,
        shared_system_prompt_ratio=1.0,
        save_result=None,
        save_dir=None,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _make_spec_for_handler():
    """Build a resolved spec for use in CommandHandler tests."""
    args = _make_args()
    return _build_spec(args)


def _make_handler(**runtime_overrides) -> CommandHandler:
    """Create a CommandHandler with default state for testing."""
    args = _make_args()
    spec = _build_spec(args)
    workload = {"spec": spec, "shared_system_text": ""}
    runtime = RuntimeState(save_dir="/tmp/test_bench", **runtime_overrides)
    return CommandHandler(
        runtime=runtime,
        workload=workload,
        args=args,
    )


def _run(coro):
    """Run an async coroutine synchronously."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _make_metric(**overrides) -> TurnMetric:
    defaults = dict(
        session_id="s0",
        turn=0,
        ttft_ms=10.0,
        fc_ms=20.0,
        itl_ms=5.0,
        latency_ms=100.0,
        input_tokens=50,
        output_tokens=20,
        start_time_ms=0.0,
    )
    defaults.update(overrides)
    return TurnMetric(**defaults)


# ============================================================================
# _summarize_metrics
# ============================================================================


class TestSummarizeMetrics:
    def test_empty_metrics(self):
        result = summarize_metrics([], 10.0)
        assert result["requests"] == 0

    def test_basic_metrics(self):
        metrics = [
            _make_metric(ttft_ms=10.0, output_tokens=20),
            _make_metric(
                session_id="s1",
                ttft_ms=15.0,
                fc_ms=25.0,
                itl_ms=6.0,
                latency_ms=120.0,
                input_tokens=60,
                output_tokens=25,
                start_time_ms=100.0,
            ),
        ]
        result = summarize_metrics(metrics, 2.0)
        assert result["requests"] == 2
        assert result["avg_ttft_ms"] == pytest.approx(12.5, abs=0.1)
        assert result["throughput_tok_s"] == pytest.approx(22.5, abs=0.1)


# ============================================================================
# RuntimeState
# ============================================================================


class TestRuntimeState:
    def test_default_init(self):
        state = RuntimeState()
        assert state.current_qps == 0.0
        assert state.measurement_metrics == []
        assert state.last_window_metrics == []
        assert state.measurement_active is False


# ============================================================================
# percentile (from multiturn_bench)
# ============================================================================


class TestPercentile:
    def test_empty(self):
        assert percentile([], 50) == 0.0

    def test_basic(self):
        assert percentile([1.0, 2.0, 3.0, 4.0, 5.0], 50) == pytest.approx(3.0, abs=0.1)


# ============================================================================
# _build_spec
# ============================================================================


class TestBuildSpec:
    def test_basic_build(self):
        args = _make_args()
        spec = _build_spec(args)
        assert spec.isl == 1000
        assert spec.hit_rate == 0.5
        assert spec.num_turns == 1
        assert spec.osl == 50

    def test_overrides(self):
        args = _make_args()
        spec = _build_spec(args, {"isl": 2000, "osl": 100})
        assert spec.isl == 2000
        assert spec.osl == 100
        assert spec.num_turns == 1  # unchanged

    def test_override_num_turns(self):
        args = _make_args(
            isl=5600, hit_rate=0.8, osl=140, shared_system_prompt_ratio=1.0
        )
        spec = _build_spec(args, {"num_turns": 5})
        assert spec.num_turns == 5

    def test_override_hit_rate(self):
        args = _make_args()
        spec = _build_spec(args, {"hit_rate": 0.8})
        assert spec.hit_rate == 0.8

    def test_always_has_request_rate(self):
        """_build_spec always sets request_rate=1.0 for spec resolution."""
        args = _make_args()
        spec = _build_spec(args)
        assert spec.request_rate == 1.0


# ============================================================================
# _save_window_result
# ============================================================================


class TestSaveWindowResult:
    def test_saves_valid_json(self):
        args = _make_args()
        spec = _build_spec(args)
        metrics = [_make_metric(), _make_metric(session_id="s1")]

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            _save_window_result(path, args, spec, metrics, 2.0, runtime_qps=5.0)
            with open(path) as f:
                data = json.load(f)
            assert data["mode"] == "interactive_rate"
            assert data["config"]["runtime_qps"] == 5.0
            assert data["config"]["first_chunk_threshold"] == 16
            assert "chunk_size" not in data["config"]
            assert data["window"]["requests"] == 2
            assert len(data["raw_metrics"]) == 2
            # spec should use summary() format, not asdict
            assert "effective_isl" in data["spec"]
            assert "_u" not in data["spec"]
        finally:
            os.unlink(path)


# ============================================================================
# CommandHandler
# ============================================================================


class TestCommandHandler:
    def test_empty_command(self):
        h = _make_handler()
        assert _run(h.handle("")) == "empty command"
        assert _run(h.handle("  ")) == "empty command"

    def test_help(self):
        h = _make_handler()
        resp = _run(h.handle("help"))
        assert "rate <qps>" in resp
        assert "workload" in resp

    def test_unknown_command(self):
        h = _make_handler()
        resp = _run(h.handle("foobar"))
        assert "Unknown command" in resp

    # --- rate ---
    def test_rate_set(self):
        h = _make_handler()
        resp = _run(h.handle("rate 5.5"))
        assert "5.500" in resp
        assert h.runtime.current_qps == 5.5
        assert h.rate_changed.is_set()

    def test_rate_zero(self):
        h = _make_handler()
        resp = _run(h.handle("rate 0"))
        assert "0.000" in resp
        assert h.runtime.current_qps == 0.0

    def test_rate_negative(self):
        h = _make_handler()
        resp = _run(h.handle("rate -1"))
        assert "non-negative" in resp

    def test_rate_invalid(self):
        h = _make_handler()
        resp = _run(h.handle("rate abc"))
        assert "non-negative" in resp

    def test_rate_missing_arg(self):
        h = _make_handler()
        resp = _run(h.handle("rate"))
        assert "Usage" in resp

    # --- start ---
    def test_start(self):
        h = _make_handler()
        resp = _run(h.handle("start"))
        assert "started" in resp.lower()
        assert h.runtime.measurement_active is True
        assert h.runtime.measurement_start_ns is not None
        assert h.runtime.measurement_metrics == []

    # --- measure ---
    def test_measure(self):
        h = _make_handler()
        resp = _run(h.handle("measure 100"))
        assert "100" in resp
        assert h.runtime.measurement_active is True
        assert h.runtime.measurement_target_requests == 100

    def test_measure_invalid(self):
        h = _make_handler()
        assert "positive integer" in _run(h.handle("measure 0"))
        assert "positive integer" in _run(h.handle("measure -5"))
        assert "positive integer" in _run(h.handle("measure abc"))

    def test_measure_missing_arg(self):
        h = _make_handler()
        assert "Usage" in _run(h.handle("measure"))

    # --- stop ---
    def test_stop_when_active(self):
        h = _make_handler()
        _run(h.handle("start"))
        h.runtime.measurement_metrics = [_make_metric()]
        resp = _run(h.handle("stop"))
        assert "stopped" in resp.lower()
        assert h.runtime.measurement_active is False
        assert len(h.runtime.last_window_metrics) == 1

    def test_stop_when_inactive(self):
        h = _make_handler()
        resp = _run(h.handle("stop"))
        assert "not active" in resp.lower()

    # --- status ---
    def test_status(self):
        h = _make_handler(current_qps=3.0)
        h.runtime.total_completed = 42
        resp = _run(h.handle("status"))
        assert "qps=3.00" in resp
        assert "completed=42" in resp
        assert "workload:" in resp

    def test_status_clears_notice(self):
        h = _make_handler()
        h.runtime.last_notice = "some notice"
        resp = _run(h.handle("status"))
        assert "some notice" in resp
        # Second call should not have the notice
        resp2 = _run(h.handle("status"))
        assert "some notice" not in resp2

    # --- save-dir ---
    def test_save_dir(self):
        h = _make_handler()
        resp = _run(h.handle("save-dir /tmp/newdir"))
        assert "/tmp/newdir" in resp
        assert h.runtime.save_dir == "/tmp/newdir"

    def test_save_dir_missing_arg(self):
        h = _make_handler()
        assert "Usage" in _run(h.handle("save-dir"))

    # --- save ---
    def test_save_no_data(self):
        h = _make_handler()
        resp = _run(h.handle("save"))
        assert "No measured" in resp

    def test_save_with_data(self):
        h = _make_handler()
        h.runtime.last_window_metrics = [_make_metric()]
        h.runtime.last_window_elapsed_s = 1.0

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            resp = _run(h.handle(f"save {path}"))
            assert "Saved" in resp
            with open(path) as f:
                data = json.load(f)
            assert data["window"]["requests"] == 1
        finally:
            os.unlink(path)

    # --- workload (query) ---
    def test_workload_query(self):
        h = _make_handler()
        resp = _run(h.handle("workload"))
        assert "isl=1000" in resp
        assert "osl=50" in resp

    # --- workload (update) ---
    def test_workload_update(self):
        h = _make_handler()
        resp = _run(h.handle("workload isl=2000 osl=100"))
        assert "Workload updated" in resp
        assert h.workload["spec"].isl == 2000
        assert h.workload["spec"].osl == 100
        assert h.workload_changed.is_set()

    def test_workload_partial_update(self):
        h = _make_handler()
        _run(h.handle("workload isl=2000"))
        assert h.workload["spec"].isl == 2000
        assert h.workload["spec"].osl == 50  # unchanged

    def test_workload_aliases(self):
        h = _make_handler()
        resp = _run(h.handle("workload hit-rate=0.8"))
        assert "Workload updated" in resp
        assert h.workload["spec"].hit_rate == 0.8

    def test_workload_bad_token(self):
        h = _make_handler()
        resp = _run(h.handle("workload notaparam"))
        assert "Error" in resp
        assert "bad token" in resp

    def test_workload_unknown_param(self):
        h = _make_handler()
        resp = _run(h.handle("workload foo=123"))
        assert "Error" in resp
        assert "unknown param" in resp

    def test_workload_invalid_value(self):
        h = _make_handler()
        resp = _run(h.handle("workload isl=abc"))
        assert "Error" in resp
        assert "invalid value" in resp

    # --- quit ---
    def test_quit(self):
        h = _make_handler()
        resp = _run(h.handle("quit"))
        assert "Stopping" in resp
        assert h.stop_event.is_set()

    def test_exit(self):
        h = _make_handler()
        resp = _run(h.handle("exit"))
        assert "Stopping" in resp
        assert h.stop_event.is_set()

    # --- case insensitivity ---
    def test_case_insensitive(self):
        h = _make_handler()
        assert "rate <qps>" in _run(h.handle("HELP"))
        resp = _run(h.handle("RATE 1.0"))
        assert "1.000" in resp


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
