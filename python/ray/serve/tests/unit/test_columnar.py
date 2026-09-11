"""Columnar autoscaling metrics: the SCR1 wire format and the paths that consume it.

Codec coverage comes first (framing, wire detection, which reports are encoded
columnar, and decode rejection), then the plumbing that decodes frames into the array
stores and aggregates them.
"""

import random
import sys
from functools import partial
from unittest import mock
from unittest.mock import MagicMock

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
from ray.serve._private.controller import ServeController
from ray.serve._private.controller_health_metrics_tracker import (
    ControllerHealthMetricsTracker,
)
from ray.serve._private.metrics_utils import (
    aggregate_timeseries,
    merge_instantaneous_total,
)
from ray.serve._private.utils import compress_metric_report, decompress_metric_report
from ray.serve.config import AggregationFunction, AutoscalingConfig

# --------------------------------------------------------------------------
# shared fixtures
# --------------------------------------------------------------------------

DEP = DeploymentID("D", "default")

NOW = 1000.0


def _replica_report():
    return ReplicaMetricReport(
        replica_id=ReplicaID("r0", DEP),
        metrics={
            RUNNING_REQUESTS_KEY: [
                TimeStampedValue(1.0, 2.0),
                TimeStampedValue(2.0, 4.0),
            ]
        },
        timestamp=100.0,
    )


def _handle_report():
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id="h0",
        actor_id="a0",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=[TimeStampedValue(1.0, 1.0)],
        metrics={RUNNING_REQUESTS_KEY: {"D#r0": [TimeStampedValue(1.0, 3.0)]}},
        timestamp=100.0,
    )


def _handle_report_width(n):
    """Handle report whose running-requests metric spans n replica keys."""
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id="h0",
        actor_id="a0",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=[TimeStampedValue(1.0, 1.0)],
        metrics={
            RUNNING_REQUESTS_KEY: {
                f"D#r{i}": [TimeStampedValue(1.0, 3.0)] for i in range(n)
            }
        },
        timestamp=100.0,
    )


MAKERS = [_replica_report, _handle_report]


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
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id="h7",
        actor_id="act1",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=[TimeStampedValue(9.0 + i, float(i)) for i in range(4)],
        metrics=metrics,
        timestamp=123.5,
    )


def _tampered_handle_frame(
    entries_rows, n_points, replica_keys, names, frame_type="handle"
):
    arrays = {
        "entries": np.array(entries_rows, dtype="<i8").reshape(-1, 4),
        "ts": np.arange(n_points, dtype="<f8"),
        "val": np.ones(n_points, dtype="<f8"),
        "q_ts": np.zeros(0, dtype="<f8"),
        "q_val": np.zeros(0, dtype="<f8"),
    }
    descriptors, blob = codec._pack(arrays)
    header = {
        "type": frame_type,
        "deployment": ["D", "default"],
        "handle_id": "h",
        "actor_id": "a",
        "handle_source": "PROXY",
        "timestamp": NOW,
        "metric_names": names,
        "replica_keys": replica_keys,
        "arrays": descriptors,
    }
    return codec._frame(header, blob)


# --------------------------------------------------------------------------
# codec: framing, wire detection, encode selection, decode rejection
# --------------------------------------------------------------------------


def test_columnar_frame_detected_in_o1():
    buf = codec.encode(_handle_report())
    assert buf[:4] == b"SCR1"  # magic visible without decompressing
    assert codec.is_columnar(buf) is True


@pytest.mark.parametrize("make", MAKERS)
def test_cloudpickle_frame_not_columnar(make):
    """The real cloudpickle producer wire must not be misread as columnar."""
    buf = compress_metric_report(make())
    assert codec.is_columnar(buf) is False
    # and it still round-trips through the cloudpickle consumer
    assert decompress_metric_report(buf).timestamp == make().timestamp


def test_short_buffers_not_columnar():
    for b in (b"", b"S", b"SCR", b"\x78\x9c"):
        assert codec.is_columnar(b) is False


def test_decode_rejects_garbage_zlib():
    with pytest.raises(ValueError, match="corrupt"):
        codec.decode(b"SCR1" + b"this-is-not-zlib-data")


def test_decode_rejects_truncated_frame():
    buf = codec.encode(_rich_handle_report())
    with pytest.raises(ValueError):
        codec.decode(buf[: len(buf) // 2])


def test_decode_rejects_out_of_bounds_ragged_index():
    # entries row points past the point arrays -> must fail AT DECODE, not later
    # inside the control loop's merge.
    buf = _tampered_handle_frame(
        [[0, 0, 0, 99]], n_points=2, replica_keys=["k"], names=["m"]
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)


def test_decode_rejects_bad_metric_or_replica_index():
    buf = _tampered_handle_frame(
        [[5, 0, 0, 1]], n_points=2, replica_keys=["k"], names=["m"]
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)
    buf = _tampered_handle_frame(
        [[0, 3, 0, 1]], n_points=2, replica_keys=["k"], names=["m"]
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)


def test_should_encode_columnar_takes_every_handle_report():
    """Columnar is chosen by report type alone: handle reports carry every replica the
    handle routes to, replica reports carry one and stay on the Python-object path."""
    assert codec.should_encode_columnar(_handle_report_width(1)) is True
    assert codec.should_encode_columnar(_handle_report_width(128)) is True
    assert codec.should_encode_columnar(_replica_report()) is False


def test_encode_decode_round_trip_preserves_every_series():
    """The codec's own contract: metadata, the ragged per-replica series and the queued
    series all survive a round trip. TimeStampedValue declares value compare=False, so
    compare timestamps and values explicitly rather than the objects."""
    rep = _rich_handle_report()
    frame = codec.encode(rep)
    flat = codec.decode_handle_flat(frame)
    assert flat["handle_id"] == rep.handle_id
    assert flat["deployment_id"] == rep.deployment_id
    assert flat["actor_id"] == rep.actor_id
    assert flat["handle_source"] == rep.handle_source.value
    assert flat["timestamp"] == rep.timestamp
    assert [(p.timestamp, p.value) for p in rep.queued_requests] == list(
        zip(flat["q_ts"].tolist(), flat["q_val"].tolist())
    )
    names = codec.decode(frame)["header"]["metric_names"]
    assert names[flat["mi"]] == RUNNING_REQUESTS_KEY
    got = {}
    for metric_idx, key_idx, off, n in flat["entries"].tolist():
        got[(names[metric_idx], flat["replica_keys"][key_idx])] = list(
            zip(flat["ts"][off : off + n].tolist(), flat["val"][off : off + n].tolist())
        )
    assert got == {
        (metric, key): [(p.timestamp, p.value) for p in series]
        for metric, per_replica in rep.metrics.items()
        for key, series in per_replica.items()
    }


def test_decode_validates_every_frame_whatever_the_header_says():
    """The bounds check must not be skippable. It used to sit behind an "is this a handle
    frame" guard, so a tampered header type dodged it and the merge read past the array."""
    buf = _tampered_handle_frame(
        [[0, 0, 0, 99]], n_points=2, replica_keys=["k"], names=["m"], frame_type="nope"
    )
    with pytest.raises(ValueError, match="ragged index"):
        codec.decode(buf)


def test_decode_rejects_negative_ragged_index():
    """A negative index must fail rather than wrap: Python would resolve -1 to the last
    metric or replica key and silently mis-attribute the series."""
    for row in ([[-1, 0, 0, 1]], [[0, -1, 0, 1]], [[0, 0, -1, 1]], [[0, 0, 0, -1]]):
        buf = _tampered_handle_frame(row, n_points=2, replica_keys=["k"], names=["m"])
        with pytest.raises(ValueError, match="ragged index"):
            codec.decode(buf)


# --------------------------------------------------------------------------
# plumbing fixtures
# --------------------------------------------------------------------------


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


def _to_arrays(tl):
    ts, val, offs = [], [], [0]
    for s in tl:
        ts += [p.timestamp for p in s]
        val += [p.value for p in s]
        offs.append(len(ts))
    return np.array(ts, "f8"), np.array(val, "f8"), np.array(offs, "i8")


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
        queued_requests=queued,
        metrics={RUNNING_REQUESTS_KEY: running},
        timestamp=NOW,
    )


def _recorded_state(rep, monkeypatch, now=NOW + 3.0):
    monkeypatch.setattr(A.time, "time", lambda: now)
    st = _state()
    st.record_columnar_metrics_for_handle(codec.decode_handle_flat(codec.encode(rep)))
    return st


class _Observer:
    """Non-callable stand-in for a Histogram: only `.observe` may be invoked, so passing
    the metric object where a bound observer belongs fails instead of being absorbed."""

    def __init__(self):
        self.observed = []

    def observe(self, value, tags=None):
        self.observed.append((value, tags))


def _handle_report_queued(hid, queued):
    return HandleMetricReport(
        deployment_id=DEP,
        handle_id=hid,
        actor_id="a",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=queued,
        metrics={RUNNING_REQUESTS_KEY: {}},
        timestamp=NOW,
    )


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


# --------------------------------------------------------------------------
# plumbing: array stores, aggregation, controller dispatch
# --------------------------------------------------------------------------


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


def test_round_10ms_matches_c_round_on_ties():
    """The kernel rounds with C round() (half away from zero); np.round is half-to-even,
    so the two disagree on exact .5 ties at the 10ms scale."""
    ties = np.array([0.125, 1700000000.125])
    assert [float(x) for x in merge._round_10ms(ties)] == [0.13, 1700000000.13]
    assert float(np.round(ties[0], 2)) == 0.12


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
        ref_total = ref.get_total_num_requests()

        col = _state(agg)
        col._cached_running_replica_strs = keys
        for rep in reports:
            col.record_columnar_metrics_for_handle(
                codec.decode_handle_flat(codec.encode(rep))
            )
        col_total = col.get_total_num_requests()
        assert abs(ref_total - col_total) < 1e-9, (agg, ref_total, col_total)


def test_object_empty_replica_series_does_not_suppress_handle_running(monkeypatch):
    """A present-but-EMPTY replica running series carries no data, so handle-collected
    running requests must still be counted (empty series are filtered before the
    metrics_collected_on_replicas decision)."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    st = _state()
    rid = ReplicaID("r0", DEP)
    st._replica_metrics[rid] = ReplicaMetricReport(
        replica_id=rid,
        metrics={RUNNING_REQUESTS_KEY: []},  # present but empty
        timestamp=NOW,
    )
    st._running_replicas = {rid}
    rep = _random_handle_report("h0", random.Random(5), 3)
    st._cached_running_replica_strs = set(rep.metrics[RUNNING_REQUESTS_KEY].keys())
    st._handle_requests[rep.handle_id] = rep
    assert st.get_total_num_requests() > 0.0


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


def test_columnar_handle_drops_are_logged(monkeypatch):
    """The array store is now the only thing that drops handles, so it has to emit the
    operator-facing lines the object store did. Both reasons, both log levels."""
    for dead_actor, level in ((True, "debug"), (False, "info")):
        rep = _random_handle_report("h1", random.Random(1), 2)
        st = _recorded_state(rep, monkeypatch)
        # Guard against a vacuous pass: the log is gated on peak requests.
        assert A._columnar_peak_requests(st._handle_arrays["h1"]) > 0
        log = mock.Mock()
        monkeypatch.setattr(A.logger, level, log)
        if dead_actor:
            st.drop_stale_handle_metrics(alive_serve_actor_ids=set())
        else:
            monkeypatch.setattr(A.time, "time", lambda: NOW + 1e6)
            st.drop_stale_handle_metrics(alive_serve_actor_ids={"actor-h1"})
        assert "h1" not in st._handle_arrays
        assert log.call_count == 1, (dead_actor, log.call_args_list)
        assert "h1" in log.call_args[0][0]


def test_stale_columnar_handle_report_rejected(monkeypatch):
    fresh = _random_handle_report("h1", random.Random(1), 2)
    st = _recorded_state(fresh, monkeypatch)
    stale = _random_handle_report("h1", random.Random(2), 2)
    stale = HandleMetricReport(**{**stale.__dict__, "timestamp": NOW - 5.0})
    st.record_columnar_metrics_for_handle(codec.decode_handle_flat(codec.encode(stale)))
    # The delayed report must not overwrite the fresher one.
    assert st._handle_arrays["h1"]["timestamp"] == NOW
    assert st._handle_report_ts["h1"] == NOW


def test_columnar_ingest_records_delay_through_the_real_helper():
    """The routing tests below mock `self`, so they never execute _record_metrics_delay
    and cannot see a wrong argument shape. Run the real helper against a non-callable
    observer, which is what a Histogram is."""
    s = MagicMock()
    s._record_metrics_delay = partial(ServeController._record_metrics_delay, s)
    s.handle_metrics_delay_histogram = _Observer()
    ServeController.record_autoscaling_metrics_from_handle(
        s, codec.encode(_handle_report())
    )
    assert len(s.handle_metrics_delay_histogram.observed) == 1
    delay_ms, tags = s.handle_metrics_delay_histogram.observed[0]
    assert delay_ms > 0
    assert tags == {"deployment": "D", "application": "default"}


def test_handle_columnar_uses_fast_store():
    """Columnar bytes route to the array store, never the object store, and arrive
    DECODED: asserting the call alone passes even if the raw frame is forwarded."""
    s = MagicMock()
    rep = _handle_report()
    ServeController.record_autoscaling_metrics_from_handle(s, codec.encode(rep))
    asm = s.autoscaling_state_manager
    asm.record_columnar_metrics_for_handle.assert_called_once()
    asm.record_request_metrics_for_handle.assert_not_called()
    (payload,) = asm.record_columnar_metrics_for_handle.call_args[0]
    assert payload["handle_id"] == rep.handle_id
    assert payload["deployment_id"] == rep.deployment_id
    assert payload["timestamp"] == rep.timestamp


def test_handle_cloudpickle_uses_object_store():
    s = MagicMock()
    ServeController.record_autoscaling_metrics_from_handle(
        s, compress_metric_report(_handle_report())
    )
    asm = s.autoscaling_state_manager
    asm.record_columnar_metrics_for_handle.assert_not_called()
    asm.record_request_metrics_for_handle.assert_called_once()


def test_handle_cross_format_staleness_guard():
    """A delayed report in one wire format must not overwrite fresher data the other
    wrote. A producer on a pre-columnar version still sends objects, so _handle_report_ts
    is a unified per-handle last-accepted timestamp gating BOTH ingest paths. Regression
    for the mixed-rollout stale-overwrite bug."""
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
    ref_total = ref.get_total_num_requests()

    # Mixed: columnar handle running + object empty-series replica.
    mix = _state(agg)
    mix.record_columnar_metrics_for_handle(
        codec.decode_handle_flat(codec.encode(handle))
    )
    mix._replica_metrics[idle] = empty_rep
    mix._running_replicas = {live, idle}
    mix._cached_running_replica_strs = {live_str, idle_str}
    mix_total = mix.get_total_num_requests()

    # Handle running must not be suppressed by the empty object series.
    assert mix_total > 0.0
    assert abs(ref_total - mix_total) < 1e-9


def test_queued_from_both_stores(monkeypatch):
    """C: _get_queued_requests includes columnar handle queued, not just object."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    q = [TimeStampedValue(NOW - 6, 2.0), TimeStampedValue(NOW, 2.0)]
    obj_h, col_h = _handle_report_queued("h_obj", q), _handle_report_queued("h_col", q)
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


@pytest.mark.parametrize(
    "agg", [AggregationFunction.MEAN, AggregationFunction.MAX, AggregationFunction.MIN]
)
def test_replica_running_suppresses_columnar_handle_running(agg, monkeypatch):
    """Replica-reported running wins over handle-reported running, as on the object
    path. This is the mixed branch every direct-ingress and metrics-on-replica
    deployment takes on every tick, and counting both would double the total."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    rid = ReplicaID("r0", DEP)
    rid_str = rid.to_full_id_str()
    replica_running = [TimeStampedValue(NOW - 6, 1.0), TimeStampedValue(NOW, 2.0)]
    handle_running = [TimeStampedValue(NOW - 6, 7.0), TimeStampedValue(NOW, 9.0)]
    queued = [TimeStampedValue(NOW - 6, 2.0), TimeStampedValue(NOW, 3.0)]
    handle = _handle_report_running("h0", rid_str, handle_running, queued)
    replica_report = ReplicaMetricReport(
        replica_id=rid,
        metrics={RUNNING_REQUESTS_KEY: replica_running},
        timestamp=NOW,
    )

    def _build(columnar):
        st = _state(agg)
        st.record_request_metrics_for_replica(replica_report)
        if columnar:
            st.record_columnar_metrics_for_handle(
                codec.decode_handle_flat(codec.encode(handle))
            )
        else:
            st._handle_requests["h0"] = handle
        st._running_replicas = [rid]
        st._cached_running_replica_strs = {rid_str}
        return st.get_total_num_requests()

    mixed, all_object = _build(True), _build(False)
    assert abs(mixed - all_object) < 1e-9, (agg, mixed, all_object)
    # Guard against a vacuous pass: handle running is strictly larger, so a total that
    # included it would exceed the replica-only twin.
    ref = _state(agg)
    ref.record_request_metrics_for_replica(replica_report)
    ref.record_columnar_metrics_for_handle(
        codec.decode_handle_flat(codec.encode(_handle_report_queued("h0", queued)))
    )
    ref._running_replicas = [rid]
    ref._cached_running_replica_strs = {rid_str}
    assert abs(mixed - ref.get_total_num_requests()) < 1e-9


@pytest.mark.parametrize(
    "agg", [AggregationFunction.MEAN, AggregationFunction.MAX, AggregationFunction.MIN]
)
@pytest.mark.parametrize("stop_at", [0, 2, 4])
@pytest.mark.parametrize("wide", [False, True])
def test_columnar_handle_masks_replicas_that_stopped(agg, stop_at, wide, monkeypatch):
    """A handle lags the running set on every scale-down, so its frame still names a
    stopped replica. That replica's points must be dropped, and the survivors must total
    exactly what the object path totals. Covers the gather branch of
    _handle_running_columnar_blocks, which the whole-block fast path skips.

    stop_at moves the stopped replica through the frame: only when it is last are the
    survivors a contiguous prefix and the gather the identity permutation, so a middle
    or first stop is what actually exercises the index arithmetic. `wide` drives the
    same fixture down both implementations of that masking, which a width threshold
    otherwise hides from a small fixture.
    """
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    monkeypatch.setattr(A, "_GATHER_MIN_REPLICAS", 0 if wide else 1000)
    live = [ReplicaID(f"r{i}", DEP) for i in range(4)]
    gone = ReplicaID("r_gone", DEP)
    order = live[:stop_at] + [gone] + live[stop_at:]
    running = {
        r.to_full_id_str(): [
            TimeStampedValue(NOW - 6, float(i + 1)),
            TimeStampedValue(NOW, float(i + 2)),
        ]
        for i, r in enumerate(order)
    }
    handle = HandleMetricReport(
        deployment_id=DEP,
        handle_id="h0",
        actor_id="a",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=[TimeStampedValue(NOW, 1.0)],
        metrics={RUNNING_REQUESTS_KEY: running},
        timestamp=NOW,
    )
    live_strs = {r.to_full_id_str() for r in live}

    ref = _state(agg)
    ref._handle_requests["h0"] = handle
    ref._cached_running_replica_strs = live_strs
    col = _state(agg)
    col.record_columnar_metrics_for_handle(
        codec.decode_handle_flat(codec.encode(handle))
    )
    col._cached_running_replica_strs = live_strs
    assert abs(ref.get_total_num_requests() - col.get_total_num_requests()) < 1e-9
    # Guard against a vacuous pass: the stopped replica carries the largest series, so
    # counting it would move the total.
    col_all = _state(agg)
    col_all.record_columnar_metrics_for_handle(
        codec.decode_handle_flat(codec.encode(handle))
    )
    col_all._cached_running_replica_strs = live_strs | {gone.to_full_id_str()}
    assert col_all.get_total_num_requests() > col.get_total_num_requests()


def test_masked_running_blocks_follow_the_running_set(monkeypatch):
    """The masking is memoized against the running-set generation, so a replica coming
    back must invalidate it. A cache that never expires would keep reporting the
    scaled-down total forever."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    replicas = [ReplicaID(f"r{i}", DEP) for i in range(3)]
    rep = HandleMetricReport(
        deployment_id=DEP,
        handle_id="h0",
        actor_id="a",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=[],
        metrics={
            RUNNING_REQUESTS_KEY: {
                # Distinct per replica, so every subset totals differently.
                r.to_full_id_str(): [TimeStampedValue(NOW, float(1 << i))]
                for i, r in enumerate(replicas)
            }
        },
        timestamp=NOW,
    )
    st = _state()
    st.record_columnar_metrics_for_handle(codec.decode_handle_flat(codec.encode(rep)))
    # Two different subsets, so both miss the whole-block fast path and both have to
    # consult the memo. A cache that never expires would serve the first for the second.
    st.update_running_replica_ids([replicas[0], replicas[1]])
    keeps_r1 = st.get_total_num_requests()
    st.update_running_replica_ids([replicas[0], replicas[2]])
    keeps_r2 = st.get_total_num_requests()
    assert abs(keeps_r1 - keeps_r2) > 0.5, (keeps_r1, keeps_r2)
    st.update_running_replica_ids([replicas[0], replicas[1]])
    assert abs(st.get_total_num_requests() - keeps_r1) < 1e-9


def test_empty_running_rows_keep_the_whole_block_fast_path(monkeypatch):
    """A replica that reported no running points must not appear in run_keys, or it
    would drop the handle onto the masking branch for data it does not even carry."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    live, idle = ReplicaID("r0", DEP), ReplicaID("r_idle", DEP)
    rep = HandleMetricReport(
        deployment_id=DEP,
        handle_id="h0",
        actor_id="a",
        handle_source=DeploymentHandleSource.PROXY,
        queued_requests=[TimeStampedValue(NOW, 1.0)],
        metrics={
            RUNNING_REQUESTS_KEY: {
                live.to_full_id_str(): [TimeStampedValue(NOW, 3.0)],
                idle.to_full_id_str(): [],
            }
        },
        timestamp=NOW,
    )
    st = _state()
    st.record_columnar_metrics_for_handle(codec.decode_handle_flat(codec.encode(rep)))
    assert st._handle_arrays["h0"]["run_keys"] == [live.to_full_id_str()]


def test_replica_running_memo_invalidates_on_a_new_report(monkeypatch):
    """The memo is keyed on the stored report's timestamp, so a fresh report must be
    converted again rather than served from the cache."""
    monkeypatch.setattr(A.time, "time", lambda: NOW + 3.0)
    rid = ReplicaID("r0", DEP)
    st = _state()
    st._running_replicas = [rid]
    st._cached_running_replica_strs = {rid.to_full_id_str()}

    def _report(ts, value):
        return ReplicaMetricReport(
            replica_id=rid,
            metrics={RUNNING_REQUESTS_KEY: [TimeStampedValue(ts, value)]},
            timestamp=ts,
        )

    st.record_request_metrics_for_replica(_report(NOW, 2.0))
    first = st._replica_running_blocks()
    assert st._replica_running_arrays[rid][0] == NOW
    # Cache hit: the same report must hand back the very same arrays.
    assert st._replica_running_blocks()[0][0] is first[0][0]
    st.record_request_metrics_for_replica(_report(NOW + 5, 9.0))
    after = st._replica_running_blocks()
    assert st._replica_running_arrays[rid][0] == NOW + 5
    assert float(after[0][1][0]) == 9.0
    # A stopped replica takes its memo with it.
    st.on_replica_stopped(rid)
    assert rid not in st._replica_running_arrays


def test_aggregate_arrays_rejects_an_unknown_function():
    """The object kernel raises on an unrecognized aggregation function; the array form
    must not quietly reduce with min instead."""
    mts, mtot = np.array([1.0, 2.0, 3.0]), np.array([5.0, 9.0, 1.0])
    with pytest.raises(ValueError, match="Invalid aggregation function"):
        merge.aggregate_arrays(mts, mtot, "p90", None, 1.0)
    assert merge.aggregate_arrays(mts, mtot, "max", None, 1.0) == 9.0
    assert merge.aggregate_arrays(mts, mtot, "min", None, 1.0) == 1.0


def test_ingest_cpu_fraction_is_a_windowed_rate():
    """Cumulative-over-uptime could never show a controller that saturates late. The
    fraction is anchored on control-loop samples, so it tracks the recent window."""
    tracker = ControllerHealthMetricsTracker()
    tracker.controller_start_time = 0.0
    # Before any loop sample anchors a window, it falls back to cumulative over uptime.
    tracker.record_handle_ingest(500.0)
    with mock.patch("time.time", return_value=1000.0):
        assert abs(tracker.collect_metrics().ingest_cpu_fraction - 0.0005) < 1e-9
    with mock.patch("time.time", return_value=1000.0):
        tracker.record_loop_duration(0.1)  # window anchor
    tracker.record_handle_ingest(500.0)  # 0.5s of ingest inside the window
    with mock.patch("time.time", return_value=1001.0):
        metrics = tracker.collect_metrics()
    assert abs(metrics.ingest_cpu_fraction - 0.5) < 1e-6
    assert metrics.handle_reports_received == 2


def test_columnar_decode_is_timed_apart_from_cloudpickle():
    """Blending the two codecs into one deque hides the difference the format exists to
    make, so each wire format reports its own decode time."""
    tracker = ControllerHealthMetricsTracker()
    tracker.record_decompress(4.0)
    tracker.record_columnar_decode(1.0)
    metrics = tracker.collect_metrics()
    assert metrics.metrics_decompress_duration_ms.mean == 4.0
    assert metrics.columnar_decode_duration_ms.mean == 1.0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
