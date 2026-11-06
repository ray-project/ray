"""Unit tests for the R-value matrix orchestration (no cluster required)."""

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT / "python") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "python"))

benchmark_harness = pytest.importorskip("ray.data.benchmarks.cbo_benchmark_harness")
cbo_benchmark = pytest.importorskip("ray.data.benchmarks.cbo_benchmark")


class _ScriptedRunner:
    """Returns pre-scripted durations, cycling when a setting runs out."""

    def __init__(self, schedule):
        self.schedule = {key: list(values) for key, values in schedule.items()}
        self.counts = {}
        self.calls = []

    def __call__(
        self,
        num_rows,
        transform_ops,
        enable_cbo,
        payload_bytes_per_row=0,
        forced_reservation_ratio=None,
        memory_poll_interval_s=None,
    ):
        key = "derived" if enable_cbo else f"r={forced_reservation_ratio:g}"
        self.calls.append(key)
        values = self.schedule[key]
        index = self.counts.get(key, 0)
        self.counts[key] = index + 1
        metrics = benchmark_harness.RunMetrics(
            duration_s=values[index % len(values)],
            enable_cbo=enable_cbo,
            reservation_ratio=(0.33 if enable_cbo else float(forced_reservation_ratio)),
        )
        return metrics


def _workload():
    return benchmark_harness.build_parameterized_workloads([1000], [2])[0]


def test_matrix_warmup_interleaves_settings_and_picks_significant_best(monkeypatch):
    runner = _ScriptedRunner(
        {"derived": [9.0], "r=0.3": [8.0], "r=0.5": [10.0], "r=0.7": [9.5]}
    )
    monkeypatch.setattr(cbo_benchmark, "run_parameterized_workload", runner)

    results = cbo_benchmark.run_r_value_matrix_experiment(
        [_workload()],
        sweep_ratios=[0.3, 0.7],
        repetitions=3,
        log_fn=lambda _: None,
    )
    result = results[0]

    # One discarded warmup per setting, then 4 settings + 1 duplicate per block.
    assert runner.calls[:2] == ["derived", "r=0.5"]
    assert len(runner.calls) == 2 + 3 * 5
    assert result.blocks == 3
    assert result.best_ratio == pytest.approx(0.3)
    assert result.best_is_significant is True
    assert result.r_error == pytest.approx(0.03, abs=1e-6)
    assert result.derived_vs_default_pct == pytest.approx(-10.0)
    assert result.derived_vs_default_significant is True
    # The default ratio is always swept, even when not requested explicitly.
    assert [entry["ratio"] for entry in result.sweep_detail] == [0.3, 0.7, 0.5]
    assert result.default_duration_s == pytest.approx(10.0)


def test_matrix_does_not_call_noise_a_winner(monkeypatch):
    # ``r=0.45`` is 1% faster per block, but repeating the default ratio twice
    # inside a block shows a ~6% same-setting spread, so the win is noise.
    runner = _ScriptedRunner(
        {
            "derived": [10.0],
            "r=0.45": [9.9],
            "r=0.5": [10.0, 10.6, 9.8, 10.4],
        }
    )
    monkeypatch.setattr(cbo_benchmark, "run_parameterized_workload", runner)

    result = cbo_benchmark.run_r_value_matrix_experiment(
        [_workload()],
        sweep_ratios=[0.45],
        repetitions=3,
        log_fn=lambda _: None,
    )[0]

    assert result.noise_floor_pct > 5.0
    assert result.best_ratio == pytest.approx(0.5)
    assert result.best_is_significant is False
    sweep_entry = next(
        entry for entry in result.sweep_detail if entry["ratio"] == pytest.approx(0.45)
    )
    assert sweep_entry["significant"] is False


def test_matrix_marks_single_block_as_not_significant(monkeypatch):
    # A single pair has no spread estimate, so it cannot become "best".
    runner = _ScriptedRunner({"derived": [9.0], "r=0.3": [5.0], "r=0.5": [10.0]})
    monkeypatch.setattr(cbo_benchmark, "run_parameterized_workload", runner)

    result = cbo_benchmark.run_r_value_matrix_experiment(
        [_workload()],
        sweep_ratios=[0.3],
        repetitions=1,
        log_fn=lambda _: None,
    )[0]

    assert result.blocks == 1
    assert result.best_ratio == pytest.approx(0.5)
    assert result.best_is_significant is False
    assert result.derived_vs_default_ci95 is None
    assert result.derived_vs_default_significant is False
