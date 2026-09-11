"""Equivalence tests for the columnar timeseries kernels.

`autoscaling_metrics_merge` is a flat-array port of the object kernels in
`metrics_utils`. Every test here pins the port against the original on the same
inputs, since the controller's autoscaling decisions depend on them agreeing.
"""

import random
import sys
from unittest import mock

import numpy as np
import pytest

from ray.serve._private import autoscaling_metrics_merge as merge
from ray.serve._private.autoscaling_state import DeploymentAutoscalingState
from ray.serve._private.common import DeploymentID, TimeStampedValue
from ray.serve._private.metrics_utils import (
    aggregate_timeseries,
    merge_instantaneous_total,
)
from ray.serve.config import AggregationFunction, AutoscalingConfig

DEP = DeploymentID("D", "default")


NOW = 1000.0


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


def test_round_10ms_matches_c_round_on_ties():
    """The kernel rounds with C round() (half away from zero); np.round is half-to-even,
    so the two disagree on exact .5 ties at the 10ms scale."""
    ties = np.array([0.125, 1700000000.125])
    assert [float(x) for x in merge._round_10ms(ties)] == [0.13, 1700000000.13]
    assert float(np.round(ties[0], 2)) == 0.12


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


def test_aggregate_arrays_rejects_an_unknown_function():
    """The object kernel raises on an unrecognized aggregation function; the array form
    must not quietly reduce with min instead."""
    mts, mtot = np.array([1.0, 2.0, 3.0]), np.array([5.0, 9.0, 1.0])
    with pytest.raises(ValueError, match="Invalid aggregation function"):
        merge.aggregate_arrays(mts, mtot, "p90", None, 1.0)
    assert merge.aggregate_arrays(mts, mtot, "max", None, 1.0) == 9.0
    assert merge.aggregate_arrays(mts, mtot, "min", None, 1.0) == 1.0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
