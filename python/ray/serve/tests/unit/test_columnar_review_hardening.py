"""Review-hardening tests for the columnar autoscaling-metrics ingest.

Covers the gaps flagged in review: round-trip losslessness and the randomized
merge-vs-Cython harness (formerly __main__ blocks CI never ran), randomized
equivalence through the PRODUCTION handle-array path (decode_handle_flat ->
record_columnar_metrics_for_handle -> fused aggregate), corrupt-frame rejection at
ingest, columnar store pruning/cleanup, and the producer-side encode gate.
"""
import random
import sys
from unittest import mock

import numpy as np
import pytest

import ray.serve._private.autoscaling_state as A
from ray.serve._private import (
    autoscaling_metrics_codec as codec,
    autoscaling_metrics_merge as merge,
)
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
from ray.serve._private.metrics_utils import (
    aggregate_timeseries,
    merge_instantaneous_total,
)
from ray.serve.config import AggregationFunction, AutoscalingConfig

NOW = 1000.0
DEP = DeploymentID("D", "default")


def _cfg(agg=AggregationFunction.MEAN):
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


def _rich_handle_report():
    """2 metrics, ragged point counts, 3 replicas, queued series (round-trip case)."""
    metrics = {
        RUNNING_REQUESTS_KEY: {
            f"D#r{r}": [
                TimeStampedValue(1.0 + i, float((r + i) % 7)) for i in range(r % 4 + 1)
            ]
            for r in range(3)
        },
        "custom_load": {
            f"D#r{r}": [TimeStampedValue(2.0 + i, 0.5 * (r + i)) for i in range(2)]
            for r in range(3)
        },
    }
    agg = {
        RUNNING_REQUESTS_KEY: {f"D#r{r}": float(r) for r in range(3)},
        "custom_load": {f"D#r{r}": 0.1 * r for r in range(3)},
    }
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id="h7",
        actor_id="act1",
        handle_source=DeploymentHandleSource.PROXY,
        aggregated_queued_requests=3.5,
        queued_requests=[TimeStampedValue(9.0 + i, float(i)) for i in range(4)],
        aggregated_metrics=agg,
        metrics=metrics,
        timestamp=123.5,
    )


def _rich_replica_report():
    return ReplicaMetricReport(
        replica_id=ReplicaID("r42", DEP),
        aggregated_metrics={RUNNING_REQUESTS_KEY: 4.0, "custom_load": 1.25},
        metrics={
            RUNNING_REQUESTS_KEY: [
                TimeStampedValue(1.0 + i, float(i)) for i in range(5)
            ],
            "custom_load": [TimeStampedValue(2.0 + i, 0.5 * i) for i in range(3)],
        },
        timestamp=77.0,
    )


# ---- round-trip losslessness (formerly the codec __main__ block) ----


def test_handle_roundtrip_lossless():
    hr = _rich_handle_report()
    assert codec._handle_eq(hr, codec.reconstruct(codec.encode(hr)))


def test_replica_roundtrip_lossless():
    rr = _rich_replica_report()
    assert codec._replica_eq(rr, codec.reconstruct(codec.encode(rr)))


# ---- randomized merge-vs-Cython harness (formerly the merge __main__ block) ----


def _to_arrays(tl):
    ts, val, offs = [], [], [0]
    for s in tl:
        ts += [p.timestamp for p in s]
        val += [p.value for p in s]
        offs.append(len(ts))
    return np.array(ts, "f8"), np.array(val, "f8"), np.array(offs, "i8")


def test_array_merge_matches_object_kernels():
    """The numpy merge/aggregate must match the object-list Cython kernels exactly,
    for every aggregation function, on randomized ragged inputs."""
    rng = random.Random(7)
    for _ in range(600):
        tl = []
        for _ in range(rng.randint(1, 7)):
            tss = sorted(
                {
                    round(rng.uniform(88, 96) + j * rng.uniform(0.03, 0.6), 2)
                    for j in range(rng.randint(1, 10))
                }
            )
            tl.append([TimeStampedValue(t, float(rng.randint(0, 12))) for t in tss])
        merged = merge_instantaneous_total(tl)
        ref_merge = [(round(p.timestamp, 2), p.value) for p in merged]
        ts, val, offs = _to_arrays(tl)
        mts, mtot = merge.merge_instantaneous_total_arrays(ts, val, offs)
        assert ref_merge == [(round(float(t), 2), float(v)) for t, v in zip(mts, mtot)]
        now = 100.0
        lw = max(now - merged[-1].timestamp, 1e-3) if merged else 1e-3
        ws = None
        ne = [s for s in tl if s]
        if merged and len(ne) > 1:
            a = max(s[0].timestamp for s in ne)
            if a <= merged[-1].timestamp:
                ws = max(a, merged[0].timestamp)
        for fn in (
            AggregationFunction.MEAN,
            AggregationFunction.MAX,
            AggregationFunction.MIN,
        ):
            ref_v = (
                aggregate_timeseries(merged, fn, last_window_s=lw, window_start=ws)
                or 0.0
            )
            arr_v = merge.merge_and_aggregate_arrays(ts, val, offs, now, fn.value)
            assert abs(ref_v - arr_v) < 1e-9, (fn, ref_v, arr_v)


def test_array_path_equals_production_object_path_unrounded_timestamps():
    """Array and object paths must agree on real (unrounded) time.time()-style stamps.

    test_array_merge_matches_object_kernels generates timestamps already rounded to 2
    decimals, which makes every round() inside the array path a no-op -- it cannot see a
    rounding divergence. Production timestamps are not 2-decimal.

    The reference here is the production method itself, not a reimplementation of its
    window logic, and time.time() is pinned because that method reads it internally.
    """
    rng = random.Random(11)
    for _ in range(200):
        tl = []
        for _ in range(rng.randint(1, 5)):
            base = 88.0 + rng.random()
            tss = sorted(
                {
                    base + j * (0.031 + rng.random() * 0.4)
                    for j in range(rng.randint(1, 8))
                }
            )
            tl.append([TimeStampedValue(t, float(rng.randint(0, 12))) for t in tss])

        das = DeploymentAutoscalingState(DeploymentID(name="d", app_name="a"))
        das._config = AutoscalingConfig(min_replicas=1, max_replicas=10)
        ts, val, offs = _to_arrays(tl)

        now = 100.0
        with mock.patch("time.time", return_value=now):
            expected = das._merge_and_aggregate_timeseries(list(tl))
            actual = merge.merge_and_aggregate_arrays(ts, val, offs, now, "mean")
        assert abs(expected - actual) < 1e-9, (expected, actual, tl)

        # A lone series is passed through untouched by the object path, so the array
        # path must not perturb its timestamps either.
        if len([s for s in tl if s]) == 1:
            mts, _ = merge.merge_instantaneous_total_arrays(ts, val, offs)
            merged = merge_instantaneous_total(tl)
            assert [float(p.timestamp) for p in merged] == [float(x) for x in mts]


def test_merge_emits_event_for_change_inside_one_bucket():
    """Regression: a source changing value twice inside ONE 10ms bucket must still emit
    an event. Rounding and collapsing before LOCF change detection nets the change to
    zero and drops it -- this input emptied the merge entirely, which makes
    merge_and_aggregate_arrays short-circuit to 0.0 and the deployment read no load."""
    tl = [
        [TimeStampedValue(0.7608, 0.0)],
        [TimeStampedValue(0.3669, 3.0), TimeStampedValue(0.3684, 0.0)],
    ]
    ts, val, offs = _to_arrays(tl)
    mts, mtot = merge.merge_instantaneous_total_arrays(ts, val, offs)
    ref = merge_instantaneous_total(tl)
    assert len(ref) == 1 and len(mts) == 1
    assert abs(float(mts[0]) - ref[0].timestamp) < 1e-9
    assert abs(float(mtot[0]) - ref[0].value) < 1e-9


def test_array_merge_matches_object_kernels_dense_buckets():
    """Both harnesses above keep points >=10ms apart -- one pre-rounds timestamps to 2
    decimals, the other steps by >=0.031s -- so no two points of a source ever land in
    the same rounding bucket and the collapse path goes untested. This one packs several
    points per bucket, which is where change detection and rounding can disagree."""
    rng = random.Random(19)
    for _ in range(400):
        tl = []
        for _ in range(rng.randint(2, 5)):
            t = 88.0 + rng.random()
            s = []
            for _ in range(rng.randint(1, 9)):
                t += rng.choice([0.0005, 0.001, 0.003, 0.02, 0.5])
                s.append(TimeStampedValue(t, float(rng.choice([0, 1, 2, 3]))))
            tl.append(s)
        ts, val, offs = _to_arrays(tl)
        mts, mtot = merge.merge_instantaneous_total_arrays(ts, val, offs)
        ref = merge_instantaneous_total(tl)
        assert len(mts) == len(ref), (len(mts), len(ref), tl)
        for i, p in enumerate(ref):
            assert abs(float(mts[i]) - p.timestamp) < 1e-9, (i, tl)
            assert abs(float(mtot[i]) - p.value) < 1e-9, (i, tl)


def test_round_10ms_matches_c_round_on_ties():
    """The kernel rounds with C round() (half away from zero); np.round is half-to-even,
    so the two disagree on exact .5 ties at the 10ms scale."""
    ties = np.array([0.125, 1700000000.125])
    assert [float(x) for x in merge._round_10ms(ties)] == [0.13, 1700000000.13]
    assert float(np.round(ties[0], 2)) == 0.12


# ---- randomized equivalence through the PRODUCTION handle-array path ----


def _random_handle_report(hid, rng, n_replicas):
    running = {}
    aggd = {}
    for r in range(n_replicas):
        npts = rng.randint(1, 5)
        key = f"D#{hid}r{r}"
        running[key] = [
            TimeStampedValue(
                round(NOW - 6.0 * (npts - 1 - j) - rng.random(), 2),
                float(rng.randint(0, 9)),
            )
            for j in range(npts)
        ]
        aggd[key] = 0.0
    nq = rng.randint(0, 4)
    queued = [
        TimeStampedValue(round(NOW - 3.0 * (nq - 1 - j), 2), float(rng.randint(0, 5)))
        for j in range(nq)
    ]
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id=hid,
        actor_id=f"actor-{hid}",
        handle_source=DeploymentHandleSource.PROXY,
        aggregated_queued_requests=0.0,
        queued_requests=queued,
        aggregated_metrics={RUNNING_REQUESTS_KEY: aggd},
        metrics={RUNNING_REQUESTS_KEY: running},
        timestamp=NOW,
    )


@pytest.mark.parametrize(
    "agg", [AggregationFunction.MEAN, AggregationFunction.MAX, AggregationFunction.MIN]
)
def test_fused_handle_path_equals_object_path(agg, monkeypatch):
    """The live producer->controller columnar handle path (encode ->
    decode_handle_flat -> record_columnar_metrics_for_handle -> fused array
    aggregate) totals the SAME as the object path on identical reports."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    rng = random.Random(29)
    for _ in range(150):
        reports = [
            _random_handle_report(f"h{i}", rng, rng.randint(1, 6))
            for i in range(rng.randint(1, 4))
        ]
        keys = set()
        for rep in reports:
            keys |= set(rep.metrics[RUNNING_REQUESTS_KEY].keys())

        ref = _state(agg)
        ref._cached_running_replica_strs = keys
        for rep in reports:
            ref._handle_requests[rep.handle_id] = rep
        ref_total = ref._calculate_total_requests_aggregate_mode()

        col = _state(agg)
        col._cached_running_replica_strs = keys
        for rep in reports:
            col.record_columnar_metrics_for_handle(
                codec.decode_handle_flat(codec.encode(rep))
            )
        col_total = col._calculate_total_requests_aggregate_mode()
        assert abs(ref_total - col_total) < 1e-9, (agg, ref_total, col_total)


# ---- pure-object regression: empty replica series must not suppress handles ----


def test_object_empty_replica_series_does_not_suppress_handle_running(monkeypatch):
    """A present-but-EMPTY replica running series carries no data, so handle-collected
    running requests must still be counted (empty series are filtered before the
    metrics_collected_on_replicas decision)."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    st = _state()
    rid = ReplicaID("r0", DEP)
    st._replica_metrics[rid] = ReplicaMetricReport(
        replica_id=rid,
        aggregated_metrics={RUNNING_REQUESTS_KEY: 0.0},
        metrics={RUNNING_REQUESTS_KEY: []},  # present but empty
        timestamp=NOW,
    )
    st._running_replicas = {rid}
    rep = _random_handle_report("h0", random.Random(5), 3)
    st._cached_running_replica_strs = set(rep.metrics[RUNNING_REQUESTS_KEY].keys())
    st._handle_requests[rep.handle_id] = rep
    assert st._calculate_total_requests_aggregate_mode() > 0.0


# ---- corrupt/truncated frames fail at ingest, with ValueError ----


def test_decode_rejects_garbage_zlib():
    with pytest.raises(ValueError, match="corrupt"):
        codec.decode(b"SCR1" + b"this-is-not-zlib-data")


def test_decode_rejects_truncated_frame():
    buf = codec.encode(_rich_handle_report())
    with pytest.raises(ValueError):
        codec.decode(buf[: len(buf) // 2])


def _tampered_handle_frame(entries_rows, n_points, agg_len, replica_keys, names):
    arrays = {
        "entries": np.array(entries_rows, dtype="<i8").reshape(-1, 4),
        "agg": np.zeros(agg_len, dtype="<f8"),
        "ts": np.arange(n_points, dtype="<f8"),
        "val": np.ones(n_points, dtype="<f8"),
        "q_ts": np.zeros(0, dtype="<f8"),
        "q_val": np.zeros(0, dtype="<f8"),
    }
    descriptors, blob = codec._pack(arrays)
    header = {
        "type": "handle",
        "deployment": ["D", "default"],
        "handle_id": "h",
        "actor_id": "a",
        "handle_source": "PROXY",
        "timestamp": NOW,
        "aggregated_queued_requests": 0.0,
        "metric_names": names,
        "replica_keys": replica_keys,
        "arrays": descriptors,
    }
    return codec._frame(header, blob)


def test_decode_rejects_out_of_bounds_ragged_index():
    # entries row points past the point arrays -> must fail AT DECODE, not later
    # inside the control loop's merge.
    buf = _tampered_handle_frame(
        [[0, 0, 0, 99]], n_points=2, agg_len=1, replica_keys=["k"], names=["m"]
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)


def test_decode_rejects_bad_metric_or_replica_index():
    buf = _tampered_handle_frame(
        [[5, 0, 0, 1]], n_points=2, agg_len=1, replica_keys=["k"], names=["m"]
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)
    buf = _tampered_handle_frame(
        [[0, 3, 0, 1]], n_points=2, agg_len=1, replica_keys=["k"], names=["m"]
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)


# ---- columnar store pruning / cleanup ----


def _recorded_state(rep, monkeypatch, now=NOW + 3.0):
    monkeypatch.setattr(A.time, "time", lambda: now)
    st = _state()
    st.record_columnar_metrics_for_handle(codec.decode_handle_flat(codec.encode(rep)))
    return st


def test_drop_stale_handle_metrics_prunes_columnar_dead_actor(monkeypatch):
    rep = _random_handle_report("h1", random.Random(1), 2)
    st = _recorded_state(rep, monkeypatch)
    assert "h1" in st._handle_arrays
    st.drop_stale_handle_metrics(alive_serve_actor_ids=set())  # actor-h1 is dead
    assert "h1" not in st._handle_arrays


def test_drop_stale_handle_metrics_prunes_columnar_timeout(monkeypatch):
    rep = _random_handle_report("h1", random.Random(1), 2)
    st = _recorded_state(rep, monkeypatch)
    monkeypatch.setattr(A.time, "time", lambda: NOW + 1e6)  # long past any timeout
    st.drop_stale_handle_metrics(alive_serve_actor_ids={"actor-h1"})
    assert "h1" not in st._handle_arrays


def test_stale_columnar_handle_report_rejected(monkeypatch):
    fresh = _random_handle_report("h1", random.Random(1), 2)
    st = _recorded_state(fresh, monkeypatch)
    stale = _random_handle_report("h1", random.Random(2), 2)
    stale = HandleMetricReport(**{**stale.__dict__, "timestamp": NOW - 5.0})
    st.record_columnar_metrics_for_handle(codec.decode_handle_flat(codec.encode(stale)))
    # The delayed report must not overwrite the fresher one.
    assert st._handle_arrays["h1"]["timestamp"] == NOW
    assert st._handle_report_ts["h1"] == NOW


def test_on_replica_stopped_clears_columnar_stores(monkeypatch):
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    st = _state()
    rid = ReplicaID("r0", DEP)
    st._replica_running_arrays[rid] = (np.array([NOW]), np.array([1.0]), NOW)
    st._replica_custom_arrays[rid] = {"m": (np.array([NOW]), np.array([1.0]))}
    st._replica_report_ts[rid] = NOW
    st.on_replica_stopped(rid)
    assert rid not in st._replica_running_arrays
    assert rid not in st._replica_custom_arrays
    assert rid not in st._replica_report_ts


# ---- producer-side gate (the branch inversion the review flagged) ----


def _wide_report(width):
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id="h",
        actor_id="a",
        handle_source=DeploymentHandleSource.PROXY,
        aggregated_queued_requests=0.0,
        queued_requests=[],
        aggregated_metrics={RUNNING_REQUESTS_KEY: {}},
        metrics={
            RUNNING_REQUESTS_KEY: {
                f"D#r{i}": [TimeStampedValue(NOW, 1.0)] for i in range(width)
            }
        },
        timestamp=NOW,
    )


def test_should_encode_columnar_requires_numpy(monkeypatch):
    """serve-minimal (no numpy) producers must fall back to objects, not crash."""
    monkeypatch.setattr(codec, "RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER", True)
    monkeypatch.setattr(codec, "RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS", 8)
    monkeypatch.setattr(codec, "np", None)
    assert codec.should_encode_columnar(_wide_report(128)) is False


def test_should_encode_columnar_requires_aggregate_mode(monkeypatch):
    """In simple mode the controller only reconstruct()s columnar frames (pure
    overhead), so producers must stay on the object path."""
    monkeypatch.setattr(codec, "RAY_SERVE_COLUMNAR_METRICS_MIN_REPLICAS", 8)
    monkeypatch.setattr(codec, "RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER", False)
    monkeypatch.setattr(codec, "RAY_SERVE_ENABLE_DIRECT_INGRESS", False)
    assert codec.should_encode_columnar(_wide_report(128)) is False
    monkeypatch.setattr(codec, "RAY_SERVE_ENABLE_DIRECT_INGRESS", True)
    assert codec.should_encode_columnar(_wide_report(128)) is True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
