"""Array-native reference for the autoscaling decision-time merge.

This is the EXACT spec for an array-input port of the two _raylet Cython kernels
(`merge_instantaneous_total_cython`, `time_weighted_average_cython`), operating on
the columnar layout — flat float64 `ts`/`val` arrays + CSR-style per-source
`offsets` — with NO per-point Python objects. Verified to match the existing
object-list kernels exactly (see __main__).

The point of this file: porting these two functions to take array buffers (typed
memoryviews + offsets) instead of List[TimeStampedValue] is what makes Option C
zero-copy END TO END — ingest stores per-source arrays, and the decision-time
merge consumes them directly. Re-deriving the merge in pure numpy is error-prone
(the semantics below were only nailed after several wrong attempts); this reference
exists so the Cython port can reuse the proven algorithm with exact semantics.

Pinned semantics of merge_instantaneous_total_cython (verified 8000/8000):
  1. Drop empty sources.
  2. If exactly ONE active source -> return it as-is (identity; no dedup, no merge).
  3. Else, per source: round ts to 2 decimals (10ms); collapse same-rounded-ts
     keeping the last value; then keep only points where the value CHANGED from that
     source's previous value (LOCF), recording the delta (first point delta = value).
  4. Merge all per-source change-points by timestamp (sum deltas at equal ts),
     cumsum -> instantaneous total. Keep EVERY change-point timestamp (an event
     survives if any source changed there, even if deltas net to zero).
"""
from typing import Tuple

import numpy as np


def merge_instantaneous_total_arrays(
    ts: np.ndarray, val: np.ndarray, offsets: np.ndarray
) -> Tuple[np.ndarray, np.ndarray]:
    """Columnar form of merge_instantaneous_total. `offsets` is CSR: source i is
    ts[offsets[i]:offsets[i+1]]. Returns (merged_ts, merged_total) float64 arrays."""
    active = [
        (offsets[i], offsets[i + 1])
        for i in range(len(offsets) - 1)
        if offsets[i + 1] > offsets[i]
    ]
    if not active:
        return np.zeros(0), np.zeros(0)
    if len(active) == 1:
        a, b = active[0]
        return np.round(ts[a:b], 2), val[a:b].astype(float)

    ev_ts, ev_d = [], []
    for a, b in active:
        st = np.round(ts[a:b], 2)
        sv = val[a:b]
        # collapse same-rounded-ts within source, keep last value
        keep_last = np.ones(len(st), dtype=bool)
        keep_last[:-1] = st[1:] != st[:-1]
        st, sv = st[keep_last], sv[keep_last]
        # LOCF: keep only value changes vs previous (baseline 0); delta = v - prev
        prev = np.concatenate(([0.0], sv[:-1]))
        delta = sv - prev
        changed = delta != 0
        ev_ts.append(st[changed])
        ev_d.append(delta[changed])
    all_ts = np.concatenate(ev_ts)
    all_d = np.concatenate(ev_d)
    uts, inv = np.unique(all_ts, return_inverse=True)  # groups equal ts
    summed = np.zeros(len(uts))
    np.add.at(summed, inv, all_d)
    return uts, np.cumsum(summed)


def time_weighted_average_arrays(
    mts: np.ndarray, mtot: np.ndarray, window_start, last_window_s: float
) -> float:
    """Columnar form of time_weighted_average (MEAN), right-continuous/LOCF.

    Integrates the step function over [window_start, end], end = last event +
    last_window_s. The value active on each segment is the LOCF total at the
    segment's start, so a window_start that falls BETWEEN events still counts the
    value carried forward from the prior event (not skipped)."""
    if mts.size == 0:
        return 0.0
    ws = mts[0] if window_start is None else window_start
    end = mts[-1] + last_window_s
    if end <= ws:
        return 0.0
    after = mts[mts > ws]  # event times strictly inside the window
    starts = np.concatenate(([ws], after))
    ends = np.concatenate((after, [end]))
    si = (
        np.searchsorted(mts, starts, side="right") - 1
    )  # last event <= each start (LOCF)
    active = np.where(si >= 0, mtot[np.clip(si, 0, mtot.size - 1)], 0.0)
    durs = ends - starts
    return float(np.dot(active, durs) / durs.sum())


def aggregate_arrays(mts, mtot, agg_function, window_start, last_window_s) -> float:
    """Columnar form of aggregate_timeseries. agg_function is AggregationFunction
    (str enum: 'mean'|'max'|'min')."""
    if mts.size == 0:
        return 0.0
    if agg_function == "mean":
        return time_weighted_average_arrays(mts, mtot, window_start, last_window_s)
    # max/min over event values, filtered by window_start (matches aggregate_timeseries)
    vals = mtot if window_start is None else mtot[mts >= window_start]
    if vals.size == 0:
        return 0.0
    return float(vals.max() if agg_function == "max" else vals.min())


def merge_and_aggregate_arrays(
    ts, val, offsets, now: float, agg_function="mean"
) -> float:
    """Columnar form of DeploymentAutoscalingState._merge_and_aggregate_timeseries."""
    mts, mtot = merge_instantaneous_total_arrays(ts, val, offsets)
    if mts.size == 0:
        return 0.0
    last_window_s = now - mts[-1]
    if last_window_s <= 0:
        last_window_s = 1e-3
    window_start = None
    n_active = int(np.sum(np.diff(offsets) > 0))
    if n_active > 1:
        aligned = max(
            round(float(ts[offsets[i]]), 2)
            for i in range(len(offsets) - 1)
            if offsets[i + 1] > offsets[i]
        )
        if aligned <= mts[-1]:
            window_start = max(aligned, mts[0])
    return aggregate_arrays(mts, mtot, agg_function, window_start, last_window_s)


if __name__ == "__main__":
    import random

    from ray.serve._private.common import TimeStampedValue
    from ray.serve._private.metrics_utils import (
        aggregate_timeseries,
        merge_instantaneous_total,
    )
    from ray.serve.config import AggregationFunction

    def to_arrays(tl):
        ts, val, offs = [], [], [0]
        for s in tl:
            ts += [p.timestamp for p in s]
            val += [p.value for p in s]
            offs.append(len(ts))
        return np.array(ts, "f8"), np.array(val, "f8"), np.array(offs, "i8")

    rng = random.Random(7)
    merge_ok = avg_ok = 0
    max_avg_err = 0.0
    N = 8000
    for _ in range(N):
        tl = []
        for _ in range(rng.randint(1, 7)):
            tss = sorted(
                {
                    round(rng.uniform(88, 96) + j * rng.uniform(0.03, 0.6), 2)
                    for j in range(rng.randint(1, 10))
                }
            )
            tl.append([TimeStampedValue(t, float(rng.randint(0, 12))) for t in tss])
        # reference (object-list Cython kernels)
        merged = merge_instantaneous_total(tl)
        ref_merge = [(round(p.timestamp, 2), p.value) for p in merged]
        # array form
        ts, val, offs = to_arrays(tl)
        mts, mtot = merge_instantaneous_total_arrays(ts, val, offs)
        arr_merge = [(round(float(t), 2), float(v)) for t, v in zip(mts, mtot)]
        if ref_merge == arr_merge:
            merge_ok += 1
        # full merge+aggregate vs reference, for ALL aggregation functions
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
                (
                    aggregate_timeseries(merged, fn, last_window_s=lw, window_start=ws)
                    or 0.0
                )
                if merged
                else 0.0
            )
            arr_v = merge_and_aggregate_arrays(ts, val, offs, now, fn.value)
            err = abs(ref_v - arr_v)
            max_avg_err = max(max_avg_err, err)
            avg_ok += err < 1e-9
    total_agg = N * 3
    print(
        f"N={N}  merge exact: {merge_ok}/{N}  agg<1e-9: {avg_ok}/{total_agg}  max|agg err|={max_avg_err:.2e}"
    )
    assert (
        merge_ok == N and avg_ok == total_agg
    ), "array reference does not match Cython kernels"
    print(
        "ARRAY MERGE + ALL AGGREGATIONS (mean/max/min) MATCH THE CYTHON KERNELS EXACTLY"
    )
