"""Flag-equivalence: the columnar (array) autoscaling path must produce the SAME
get_total_num_requests as the cloudpickle/object path on identical data.

Builds two DeploymentAutoscalingStates from the same replica reports — one fed the
object path (_replica_metrics -> _calculate_total_requests_aggregate_mode), one fed
the columnar path (_replica_running_arrays -> _columnar_aggregate_total_requests) —
and asserts the aggregate-mode totals match exactly for all aggregation functions.

Time is pinned so the time-weighted average's last_window_s is identical for both
paths (the merge math itself is verified separately in test for the array merge).
"""
import random
import sys

import pytest

import ray.serve._private.autoscaling_state as A
from ray.serve._private import autoscaling_metrics_codec as codec
from ray.serve._private.autoscaling_state import DeploymentAutoscalingState
from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    DeploymentID,
    ReplicaID,
    ReplicaMetricReport,
    TimeStampedValue,
)
from ray.serve.config import AggregationFunction, AutoscalingConfig

NOW = 1000.0
DEP = DeploymentID("D", "default")


def _make_reports(rng):
    reports = []
    for i in range(rng.randint(1, 8)):
        rid = ReplicaID(f"r{i}", DEP)
        npts = rng.randint(1, 6)
        series = [
            TimeStampedValue(
                round(NOW - 6.0 * (npts - 1 - j), 2), float(rng.randint(0, 9))
            )
            for j in range(npts)
        ]
        reports.append(
            ReplicaMetricReport(
                replica_id=rid,
                aggregated_metrics={RUNNING_REQUESTS_KEY: 0.0},
                metrics={RUNNING_REQUESTS_KEY: series},
                timestamp=NOW,
            )
        )
    return reports


def _object_state(reports, agg):
    st = DeploymentAutoscalingState(DEP)
    st._config = AutoscalingConfig(
        min_replicas=1,
        max_replicas=1000,
        target_ongoing_requests=1,
        aggregation_function=agg,
    )
    for rep in reports:
        st._replica_metrics[rep.replica_id] = rep  # object path; arrays left empty
    st._running_replicas = {rep.replica_id for rep in reports}
    return st


def _columnar_state(reports, agg):
    st = DeploymentAutoscalingState(DEP)
    st._config = AutoscalingConfig(
        min_replicas=1,
        max_replicas=1000,
        target_ongoing_requests=1,
        aggregation_function=agg,
    )
    for rep in reports:
        rid, ts_arr, val_arr, ts_ = codec.decode_replica_running_requests(
            codec.encode(rep)
        )
        st._replica_running_arrays[rid] = (ts_arr, val_arr, ts_)
    st._running_replicas = {rep.replica_id for rep in reports}
    return st


@pytest.mark.parametrize(
    "agg", [AggregationFunction.MEAN, AggregationFunction.MAX, AggregationFunction.MIN]
)
def test_columnar_equals_object_total_requests(agg, monkeypatch):
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    rng = random.Random(0)
    for _ in range(300):
        reports = _make_reports(rng)
        ref = _object_state(reports, agg)._calculate_total_requests_aggregate_mode()
        arr = _columnar_state(reports, agg)._columnar_aggregate_total_requests()
        assert abs(ref - arr) < 1e-9, f"{agg}: object={ref} columnar={arr}"


def test_columnar_empty_is_zero(monkeypatch):
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    st = DeploymentAutoscalingState(DEP)
    st._config = AutoscalingConfig(
        min_replicas=1, max_replicas=10, target_ongoing_requests=1
    )
    st._running_replicas = set()
    assert st._columnar_aggregate_total_requests() == 0.0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
