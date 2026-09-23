"""Tests for release/nightly_tests/dataset/benchmark_health_checks.py.

The module under test has no ``ray`` dependency, so these run in the release
tooling CI (``release_unit``) where Ray is not built.
"""
import sys

import benchmark_health_checks as hc
import pytest


def _series(name, metric_type, value):
    return {"metric": {"Name": name, "Type": metric_type}, "value": [1, str(value)]}


def test_check_worker_failures_ignores_idle_worker_evictions():
    queries = []

    def query(metric):
        queries.append(metric)
        if metric == hc.WORKER_OOM_METRIC:
            return [_series("idle", hc.IDLE_WORKER_EVICTION_METRIC_TYPE, 3)]
        return []

    failures = hc.check_worker_failures(
        fail_on_worker_oom=True,
        fail_on_unexpected_worker_failure=True,
        query_positive_counters=query,
    )
    assert failures == []
    assert queries == [hc.WORKER_OOM_METRIC, hc.UNEXPECTED_WORKER_FAILURE_METRIC]


def test_check_worker_failures_reports_each_enabled_check():
    def query(metric):
        if metric == hc.WORKER_OOM_METRIC:
            return [_series("ReadFiles", "MemoryManager.TaskEviction.Total", 2)]
        return [_series("MapWorker", "Raylet.UnexpectedActorFailure.Total", 1)]

    failures = hc.check_worker_failures(
        fail_on_worker_oom=True,
        fail_on_unexpected_worker_failure=True,
        query_positive_counters=query,
    )
    assert len(failures) == 2
    assert "OOM worker kills detected" in failures[0]
    assert "ReadFiles (MemoryManager.TaskEviction.Total): 2" in failures[0]
    assert "Unexpected worker failures detected" in failures[1]
    assert "MapWorker (Raylet.UnexpectedActorFailure.Total): 1" in failures[1]


def test_check_worker_failures_skips_disabled_checks():
    def query(metric):
        raise AssertionError(f"should not query {metric}")

    assert (
        hc.check_worker_failures(
            fail_on_worker_oom=False,
            fail_on_unexpected_worker_failure=False,
            query_positive_counters=query,
        )
        == []
    )


def test_check_worker_failures_propagates_query_errors():
    def query(metric):
        raise RuntimeError("prometheus down")

    with pytest.raises(RuntimeError, match="prometheus down"):
        hc.check_worker_failures(
            fail_on_worker_oom=True,
            fail_on_unexpected_worker_failure=False,
            query_positive_counters=query,
        )


def test_find_unexpectedly_dead_nodes():
    nodes = [
        {"NodeID": "alive", "Alive": True},
        {"NodeID": "expected", "Alive": False, "DeathReason": "EXPECTED"},
        {"NodeID": "unexpected", "Alive": False, "DeathReason": "UNEXPECTED"},
        {"NodeID": "no-reason", "Alive": False},
    ]
    assert hc.find_unexpectedly_dead_nodes(nodes, {"UNEXPECTED"}) == ["unexpected"]
    assert hc.find_unexpectedly_dead_nodes(nodes, {"UNEXPECTED", None}) == [
        "unexpected",
        "no-reason",
    ]


def test_peak_metric_value():
    assert hc.peak_metric_value([]) is None
    assert hc.peak_metric_value([{"metric": {}, "values": []}]) is None
    series = [
        {"metric": {}, "values": [[1, "0.2"], [2, "NaN"], [3, "0.9"]]},
        {"metric": {}, "values": [[1, "0.5"]]},
    ]
    assert hc.peak_metric_value(series) == 0.9


@pytest.mark.parametrize(
    "peak, limit, expect_failure",
    [
        (0.6, 0.5, True),
        (0.5, 0.5, False),
        (None, 0.5, False),  # no samples: pass, like the job wrapper
        (0.9, None, False),  # check disabled
    ],
)
def test_check_object_store_utilization(peak, limit, expect_failure):
    failures = hc.check_object_store_utilization(peak, limit)
    assert bool(failures) is expect_failure
    if expect_failure:
        assert "60.0%" in failures[0] and "50.0%" in failures[0]


class _FakeClock:
    def __init__(self):
        self.t = 1000.0
        self.slept = []

    def now(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


def test_wait_for_metrics_to_catch_up_polls_until_all_nodes_are_fresh():
    clock = _FakeClock()
    end = 1000.0
    samples = iter([None, end + 3, end + hc.METRICS_REPORT_INTERVAL_S])

    assert hc.wait_for_metrics_to_catch_up(
        workload_end_unix_time=end,
        oldest_node_sample_unix_time=lambda: next(samples),
        timeout_s=60,
        poll_interval_s=5,
        sleep=clock.sleep,
        now=clock.now,
    )
    assert clock.slept == [5, 5]


def test_wait_for_metrics_to_catch_up_times_out_on_stale_node():
    clock = _FakeClock()

    assert not hc.wait_for_metrics_to_catch_up(
        workload_end_unix_time=1000.0,
        oldest_node_sample_unix_time=lambda: 990.0,  # a dead node never advances
        timeout_s=20,
        poll_interval_s=5,
        sleep=clock.sleep,
        now=clock.now,
    )
    assert clock.slept == [5, 5, 5, 5]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
