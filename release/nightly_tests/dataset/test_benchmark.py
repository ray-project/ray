"""Tests for the Ray-dependent wiring in ``benchmark.py``.

The check logic itself lives in ``benchmark_health_checks.py`` and is tested in
``release/ray_release/tests/test_benchmark_health_checks.py`` without Ray. These
tests need an environment where ``ray`` is importable. Run them from the
repository root with ``pytest release/nightly_tests/dataset/test_benchmark.py``.
"""
import json
import os
from unittest.mock import patch

import pytest

from ray.data import DataContext
from release.nightly_tests.dataset import benchmark as benchmark_module
from release.nightly_tests.dataset.benchmark import Benchmark, BenchmarkMetric


@pytest.fixture(autouse=True)
def no_prometheus_wait():
    with patch.object(
        benchmark_module, "_wait_for_prometheus_to_catch_up", return_value=True
    ) as wait:
        yield wait


def _all_checks_off(**overrides):
    kwargs = dict(
        fail_on_worker_oom=False,
        fail_on_unexpected_worker_failure=False,
        fail_on_dead_nodes=False,
        max_object_store_utilization=None,
    )
    kwargs.update(overrides)
    return Benchmark(**kwargs)


def _with_run_window(benchmark, start=100.0, end=200.0):
    benchmark._first_case_start_unix_time = start
    benchmark._last_case_end_unix_time = end
    return benchmark


def test_benchmark_configures_resource_manager_debugging(monkeypatch):
    context = DataContext.get_current()
    monkeypatch.setattr(context, "debug_resource_manager", False)

    _all_checks_off(debug_progress_manager=True)
    assert context.debug_resource_manager is True

    _all_checks_off(debug_progress_manager=False)
    assert context.debug_resource_manager is False


def test_py_modules_ship_the_health_checks_module():
    modules = benchmark_module.benchmark_py_modules()
    basenames = [os.path.basename(m) for m in modules]
    assert "benchmark.py" in basenames
    assert "benchmark_health_checks.py" in basenames
    assert all(os.path.exists(m) for m in modules), modules


def test_node_sample_times_fail_closed_but_tolerate_no_series():
    with patch.object(benchmark_module, "_query_prometheus", return_value=None):
        with pytest.raises(RuntimeError, match="node scrape timestamps"):
            benchmark_module._get_node_sample_unix_times()

    with patch.object(benchmark_module, "_query_prometheus", return_value=[]):
        assert benchmark_module._get_oldest_node_sample_unix_time() is None

    series = [{"metric": {}, "value": [1, "30.5"]}, {"metric": {}, "value": [1, "12"]}]
    with patch.object(benchmark_module, "_query_prometheus", return_value=series):
        assert benchmark_module._get_oldest_node_sample_unix_time() == 12.0


def test_worker_failure_query_fails_closed_when_prometheus_is_unavailable():
    with patch.object(benchmark_module, "_query_prometheus", return_value=None):
        with pytest.raises(RuntimeError, match="Failed to query Prometheus"):
            benchmark_module._get_positive_worker_failure_metrics("ray_x_total")


def test_object_store_query_uses_wrapper_expression_over_run_window():
    with patch.object(
        benchmark_module, "_session_label", return_value='SessionName="s"'
    ), patch.object(
        benchmark_module,
        "_query_prometheus_range",
        return_value=[{"metric": {}, "values": [[1, "0.4"], [2, "0.7"]]}],
    ) as query:
        peak = benchmark_module._get_peak_object_store_utilization(100.0, 200.5)

    assert peak == 0.7
    (query_str, start, end), _ = query.call_args
    assert "sum(ray_object_store_memory{" in query_str
    assert 'ray_resources{Name="object_store_memory"' in query_str
    assert (start, end) == (100.0, 200.5)


def test_write_result_runs_object_store_guard_after_export(
    tmp_path, monkeypatch, no_prometheus_wait
):
    output_path = tmp_path / "result.json"
    monkeypatch.setenv("TEST_OUTPUT_JSON", str(output_path))
    benchmark = _with_run_window(_all_checks_off(max_object_store_utilization=0.5))
    benchmark.result = {
        "main": {BenchmarkMetric.OBJECT_STORE_MEMORY_UTILIZATION_PEAK.value: 0.1}
    }

    with patch.object(
        benchmark_module, "_get_peak_object_store_utilization", return_value=0.6
    ) as peak:
        with pytest.raises(AssertionError, match="Peak object-store utilization"):
            benchmark.write_result()

    peak.assert_called_once_with(100.0, 200.0)
    no_prometheus_wait.assert_called_once_with(200.0)
    assert json.loads(output_path.read_text()) == benchmark.result


def test_write_result_reports_dead_nodes(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OUTPUT_JSON", str(tmp_path / "result.json"))
    benchmark = _all_checks_off(fail_on_dead_nodes=True)

    with patch.object(
        benchmark_module, "_get_unexpectedly_dead_nodes", return_value=["n1"]
    ):
        with pytest.raises(AssertionError, match="Dead nodes found"):
            benchmark.write_result()


def test_write_result_waits_for_prometheus_before_worker_checks(
    tmp_path, monkeypatch, no_prometheus_wait
):
    monkeypatch.setenv("TEST_OUTPUT_JSON", str(tmp_path / "result.json"))
    benchmark = _with_run_window(_all_checks_off(fail_on_worker_oom=True))

    with patch.object(
        benchmark_module, "_get_positive_worker_failure_metrics", return_value=[]
    ) as query:
        benchmark.write_result()

    no_prometheus_wait.assert_called_once_with(200.0)
    query.assert_called_once_with(benchmark_module.health_checks.WORKER_OOM_METRIC)


def test_write_result_skips_checks_when_a_case_failed(tmp_path, monkeypatch):
    """A failing benchmark case must surface its own error, not a health check."""
    monkeypatch.setenv("TEST_OUTPUT_JSON", str(tmp_path / "result.json"))
    benchmark = _all_checks_off(fail_on_dead_nodes=True)

    def boom():
        raise ValueError("workload failed")

    with patch.object(benchmark_module, "get_state_from_address"), patch.object(
        benchmark_module, "_get_spilled_bytes_total", return_value=0
    ), patch.object(benchmark_module, "ObjectStoreMemorySampler"), patch.object(
        benchmark_module.ray, "get_runtime_context"
    ):
        with pytest.raises(ValueError, match="workload failed"):
            benchmark.run_fn("main", boom)

    with patch.object(
        benchmark_module, "_get_unexpectedly_dead_nodes", return_value=["n1"]
    ) as dead_nodes:
        benchmark.write_result()
    dead_nodes.assert_not_called()


def test_write_result_skips_checks_when_a_post_case_assertion_failed(
    tmp_path, monkeypatch
):
    """Assertions raised after the workload (e.g. head-node memory) count too."""
    monkeypatch.setenv("TEST_OUTPUT_JSON", str(tmp_path / "result.json"))
    benchmark = _all_checks_off(
        fail_on_dead_nodes=True,
        max_head_node_memory_bytes=1,
        max_sched_loop_duration_s=None,
    )

    with patch.object(benchmark_module, "get_state_from_address"), patch.object(
        benchmark_module, "_get_spilled_bytes_total", return_value=0
    ), patch.object(benchmark_module, "ObjectStoreMemorySampler"), patch.object(
        benchmark_module.ray, "get_runtime_context"
    ), patch.object(
        benchmark_module, "_get_peak_head_node_memory_used_bytes", return_value=2.0
    ):
        with pytest.raises(AssertionError, match="head-node physical memory"):
            benchmark.run_fn("main", lambda: None)

    with patch.object(
        benchmark_module, "_get_unexpectedly_dead_nodes", return_value=["n1"]
    ) as dead_nodes:
        benchmark.write_result()
    dead_nodes.assert_not_called()


def test_invalid_limits():
    with pytest.raises(ValueError, match="must be nonnegative"):
        Benchmark(max_object_store_utilization=-0.1)
    with pytest.raises(ValueError, match="greater than 0"):
        Benchmark(max_head_node_memory_bytes=0)
