"""Reviewer-flagged correctness (mixed columnar + cloudpickle stores).

During a rolling upgrade the columnar (array) and cloudpickle (object) metric stores
can BOTH hold data: the controller wire-detects the format from the frame magic. The
aggregation must count both, never double-count, and be exact for every aggregation
function.
"""
import sys

import pytest

import ray.serve._private.autoscaling_state as A
from ray.serve._private import autoscaling_metrics_codec as codec
from ray.serve._private.autoscaling_state import DeploymentAutoscalingState
from ray.serve._private.common import (
    RUNNING_REQUESTS_KEY,
    DeploymentHandleSource,
    DeploymentID,
    HandleMetricReport,
    ReplicaID,
    ReplicaMetricReport,
    TimeStampedValue,
)
from ray.serve.config import AggregationFunction, AutoscalingConfig

NOW = 1000.0
DEP = DeploymentID("D", "default")


def _cfg(agg):
    return AutoscalingConfig(
        min_replicas=1,
        max_replicas=1000,
        target_ongoing_requests=1,
        aggregation_function=agg,
    )


def _state(agg=AggregationFunction.MEAN):
    st = DeploymentAutoscalingState(DEP)
    st._config = _cfg(agg)
    return st


def _handle_report(hid, queued):
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id=hid,
        actor_id="a",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=queued,
        metrics={RUNNING_REQUESTS_KEY: {}},
        timestamp=NOW,
    )


def test_handle_cross_format_staleness_guard():
    """A delayed report in one wire format must not overwrite fresher data the other
    wrote. A handle flips object<->columnar as it crosses the columnar width gate, so
    _handle_report_ts is a unified per-handle last-accepted timestamp gating BOTH ingest
    paths. Regression for the mixed-rollout stale-overwrite bug."""
    st = _state()
    hid = "h0"

    def _rep(ts):
        return HandleMetricReport(
            deployment_id=DEP,
            handle_id=hid,
            actor_id="a",
            handle_source=DeploymentHandleSource.PROXY,
            queued_requests=[TimeStampedValue(NOW, 1.0)],
            metrics={RUNNING_REQUESTS_KEY: {}},
            timestamp=ts,
        )

    # Fresh columnar report @ NOW+10 -> lands in the array store.
    st.record_columnar_metrics_for_handle(
        codec.decode_handle_flat(codec.encode(_rep(NOW + 10)))
    )
    assert hid in st._handle_arrays
    assert st._handle_report_ts[hid] == NOW + 10

    # STALE object report @ NOW+1 must be rejected: object store stays empty, columnar
    # data preserved, gate unchanged.
    st.record_request_metrics_for_handle(_rep(NOW + 1))
    assert hid not in st._handle_requests
    assert hid in st._handle_arrays
    assert st._handle_report_ts[hid] == NOW + 10

    # Fresh object report @ NOW+20 is accepted -> clears columnar, updates the gate.
    st.record_request_metrics_for_handle(_rep(NOW + 20))
    assert hid in st._handle_requests
    assert hid not in st._handle_arrays
    assert st._handle_report_ts[hid] == NOW + 20


def _handle_report_running(hid, replica_str, running, queued):
    """HandleMetricReport carrying per-replica RUNNING timeseries + queued (the
    handle-collection default). The sibling _handle_report only covers queued."""
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id=hid,
        actor_id="a",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=queued,
        metrics={RUNNING_REQUESTS_KEY: {replica_str: running}},
        timestamp=NOW,
    )


@pytest.mark.parametrize(
    "agg", [AggregationFunction.MEAN, AggregationFunction.MAX, AggregationFunction.MIN]
)
def test_empty_object_running_series_does_not_suppress_columnar_handle(
    agg, monkeypatch
):
    """Regression (@cursor): in a mixed rollout an object (cloudpickle) replica that
    reports RUNNING_REQUESTS_KEY with an EMPTY series must NOT flip
    metrics_collected_on_replicas and suppress columnar handle-side running (which
    carries the real load). The empty series holds no data; the total must include
    the handle running and match the all-object twin for every aggregation function."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    live = ReplicaID("r_live", DEP)  # running; running reported on a handle
    idle = ReplicaID("r_idle", DEP)  # running; reports an EMPTY running series
    live_str, idle_str = live.to_full_id_str(), idle.to_full_id_str()
    running = [TimeStampedValue(NOW - 6, 4.0), TimeStampedValue(NOW, 6.0)]
    queued = [TimeStampedValue(NOW - 6, 2.0), TimeStampedValue(NOW, 3.0)]
    handle = _handle_report_running("h0", live_str, running, queued)
    empty_rep = ReplicaMetricReport(
        replica_id=idle,
        metrics={RUNNING_REQUESTS_KEY: []},  # present-but-empty: the flag trigger
        timestamp=NOW,
    )

    # All-object twin (reference).
    ref = _state(agg)
    ref._handle_requests["h0"] = handle
    ref._replica_metrics[idle] = empty_rep
    ref._running_replicas = {live, idle}
    ref._cached_running_replica_strs = {live_str, idle_str}
    ref_total = ref._calculate_total_requests_aggregate_mode()

    # Mixed: columnar handle running + object empty-series replica.
    mix = _state(agg)
    mix.record_columnar_metrics_for_handle(
        codec.decode_handle_flat(codec.encode(handle))
    )
    mix._replica_metrics[idle] = empty_rep
    mix._running_replicas = {live, idle}
    mix._cached_running_replica_strs = {live_str, idle_str}
    mix_total = mix._calculate_total_requests_aggregate_mode()

    # Handle running must not be suppressed by the empty object series.
    assert mix_total > 0.0
    assert abs(ref_total - mix_total) < 1e-9


def test_queued_from_both_stores(monkeypatch):
    """C: _get_queued_requests includes columnar handle queued, not just object."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    q = [TimeStampedValue(NOW - 6, 2.0), TimeStampedValue(NOW, 2.0)]
    obj_h, col_h = _handle_report("h_obj", q), _handle_report("h_col", q)
    ref = _state()
    ref._handle_requests["h_obj"] = obj_h
    ref._handle_requests["h_col"] = col_h
    ref._running_replicas, ref._cached_running_replica_strs = set(), set()
    ref_q = ref._get_queued_requests()
    mix = _state()
    mix._handle_requests["h_obj"] = obj_h
    mix.record_columnar_metrics_for_handle(
        codec.decode_handle_flat(codec.encode(col_h))
    )
    mix._running_replicas, mix._cached_running_replica_strs = set(), set()
    assert mix._get_queued_requests() > 0.0
    assert abs(ref_q - mix._get_queued_requests()) < 1e-9


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
