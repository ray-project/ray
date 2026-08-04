"""Custom autoscaling metrics over the columnar path.

The codec is lossless (encodes every metric), so custom metrics ride the columnar
wire. The controller decodes ALL metrics (decode_replica_all_metrics) into a custom
array store, and _get_aggregated_custom_metrics / _get_raw_custom_metrics read it --
producing identical results to the cloudpickle/object path.
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
METRICS = [RUNNING_REQUESTS_KEY, "gpu_util", "queue_depth"]


def _cfg(agg):
    return AutoscalingConfig(
        min_replicas=1,
        max_replicas=1000,
        target_ongoing_requests=1,
        aggregation_function=agg,
    )


def _report(i, rng):
    m = {}
    for name in METRICS:
        if name != RUNNING_REQUESTS_KEY and rng.random() < 0.3:
            continue
        npts = rng.randint(1, 5)
        m[name] = [
            TimeStampedValue(
                round(NOW - 6.0 * (npts - 1 - j), 2), float(rng.randint(0, 9))
            )
            for j in range(npts)
        ]
    return ReplicaMetricReport(
        replica_id=ReplicaID(f"r{i}", DEP),
        aggregated_metrics=dict.fromkeys(m, 0.0),
        metrics=m,
        timestamp=NOW,
    )


def _pairs(ts):
    return [(round(p.timestamp, 4), round(p.value, 4)) for p in ts]


@pytest.mark.parametrize(
    "agg", [AggregationFunction.MEAN, AggregationFunction.MAX, AggregationFunction.MIN]
)
def test_columnar_custom_metrics_equal_object(agg, monkeypatch):
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    rng = random.Random(0)
    for _ in range(150):
        n = rng.randint(1, 5)
        reports = [_report(i, rng) for i in range(n)]
        running = {r.replica_id for r in reports}
        obj = DeploymentAutoscalingState(DEP)
        obj._config = _cfg(agg)
        for r in reports:
            obj.record_request_metrics_for_replica(r)
        obj._running_replicas = running
        col = DeploymentAutoscalingState(DEP)
        col._config = _cfg(agg)
        for r in reports:
            rid, ma, t = codec.decode_replica_all_metrics(codec.encode(r))
            col.record_columnar_metrics_for_replica(rid, ma, t)
        col._running_replicas = running
        ao, ac = (
            obj._get_aggregated_custom_metrics(),
            col._get_aggregated_custom_metrics(),
        )
        assert set(ao) == set(ac)
        for mt in ao:
            assert set(ao[mt]) == set(ac[mt])
            for rid in ao[mt]:
                assert abs(ao[mt][rid] - ac[mt][rid]) < 1e-9
        ro, rc = obj._get_raw_custom_metrics(), col._get_raw_custom_metrics()
        assert set(ro) == set(rc)
        for mt in ro:
            for rid in ro[mt]:
                assert _pairs(ro[mt][rid]) == _pairs(rc[mt][rid])


def test_decode_all_metrics_roundtrip():
    rep = _report(0, random.Random(5))
    rid, ma, t = codec.decode_replica_all_metrics(codec.encode(rep))
    assert rid == rep.replica_id
    assert t == rep.timestamp
    assert set(ma) == set(rep.metrics)
    for name, series in rep.metrics.items():
        ts, val = ma[name]
        got = [TimeStampedValue(float(ts[k]), float(val[k])) for k in range(ts.size)]
        assert _pairs(got) == _pairs(series)


def test_columnar_custom_dedup_at_write(monkeypatch):
    """A replica switching wire format keeps its metrics in exactly one store."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    st = DeploymentAutoscalingState(DEP)
    st._config = _cfg(AggregationFunction.MEAN)
    rid = ReplicaID("r0", DEP)
    rep = ReplicaMetricReport(
        replica_id=rid,
        aggregated_metrics={RUNNING_REQUESTS_KEY: 0.0, "gpu_util": 0.0},
        metrics={
            RUNNING_REQUESTS_KEY: [TimeStampedValue(NOW, 2.0)],
            "gpu_util": [TimeStampedValue(NOW, 0.7)],
        },
        timestamp=NOW,
    )
    r, ma, t = codec.decode_replica_all_metrics(codec.encode(rep))
    st.record_columnar_metrics_for_replica(r, ma, t)
    assert "gpu_util" in st._replica_custom_arrays[rid]
    assert rid in st._replica_running_arrays
    nxt = ReplicaMetricReport(
        replica_id=rid,
        aggregated_metrics={RUNNING_REQUESTS_KEY: 0.0},
        metrics={RUNNING_REQUESTS_KEY: [TimeStampedValue(NOW, 1.0)]},
        timestamp=NOW + 5,
    )
    st.record_request_metrics_for_replica(nxt)
    assert rid not in st._replica_custom_arrays
    assert rid not in st._replica_running_arrays
    assert rid in st._replica_metrics


def test_columnar_replica_report_ordering_and_running_clear():
    """record_columnar_metrics_for_replica must (1) drop a stale running array when a
    newer report omits running_requests, and (2) reject out-of-order (older) reports via
    a report-level timestamp guard covering BOTH the running and custom stores --
    mirroring the object path. Regression for the columnar replica-ingest path."""
    st = DeploymentAutoscalingState(DEP)
    st._config = _cfg(AggregationFunction.MEAN)
    rid = ReplicaID("r0", DEP)

    def ingest(metrics, ts):
        rep = ReplicaMetricReport(
            replica_id=rid,
            aggregated_metrics=dict.fromkeys(metrics, 0.0),
            metrics=metrics,
            timestamp=ts,
        )
        r, ma, t = codec.decode_replica_all_metrics(codec.encode(rep))
        st.record_columnar_metrics_for_replica(r, ma, t)

    ingest(
        {
            RUNNING_REQUESTS_KEY: [TimeStampedValue(NOW, 2.0)],
            "gpu_util": [TimeStampedValue(NOW, 0.7)],
        },
        NOW,
    )
    assert rid in st._replica_running_arrays
    assert "gpu_util" in st._replica_custom_arrays[rid]

    # (1) A newer report omitting running_requests drops the stale running array.
    ingest({"gpu_util": [TimeStampedValue(NOW + 6, 0.9)]}, NOW + 6)
    assert rid not in st._replica_running_arrays
    assert "gpu_util" in st._replica_custom_arrays[rid]

    # (2) An out-of-order (older) report is rejected -- nothing overwritten.
    ingest({"gpu_util": [TimeStampedValue(NOW, 0.1)]}, NOW - 100)
    assert st._replica_report_ts[rid] == NOW + 6

    # (3) Cross-format: a STALE cloudpickle (object) report must not wipe fresher
    # columnar data. The unified _replica_report_ts gate rejects it even though the
    # object store is empty (the earlier columnar report cleared it).
    st.record_request_metrics_for_replica(
        ReplicaMetricReport(
            replica_id=rid,
            aggregated_metrics={RUNNING_REQUESTS_KEY: 0.0},
            metrics={RUNNING_REQUESTS_KEY: [TimeStampedValue(NOW, 9.0)]},
            timestamp=NOW + 1,
        )
    )
    assert rid not in st._replica_metrics  # stale object rejected
    assert "gpu_util" in st._replica_custom_arrays[rid]  # columnar preserved
    assert st._replica_report_ts[rid] == NOW + 6

    # (4) A fresh object report (newer than the columnar) is accepted and clears the
    # columnar store, updating the unified gate.
    st.record_request_metrics_for_replica(
        ReplicaMetricReport(
            replica_id=rid,
            aggregated_metrics={RUNNING_REQUESTS_KEY: 0.0},
            metrics={RUNNING_REQUESTS_KEY: [TimeStampedValue(NOW, 9.0)]},
            timestamp=NOW + 20,
        )
    )
    assert rid in st._replica_metrics
    assert rid not in st._replica_running_arrays
    assert rid not in st._replica_custom_arrays
    assert st._replica_report_ts[rid] == NOW + 20


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
