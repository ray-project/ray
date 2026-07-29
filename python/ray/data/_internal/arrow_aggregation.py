"""Protocol for vectorizing an ``AggregateFn`` as Arrow hash-aggregate ops.

An aggregation opts into the vectorized (shuffle-v2) aggregate path by returning
an :class:`ArrowAggSpec` from ``AggregateFn._arrow_agg_spec``; returning ``None``
(the default) routes it to the Python engine.  The driver that consumes these
lives in ``hash_aggregate_v2.py``.

Two execution shapes are supported:

* **Reduction** (``single_phase=False``) -- the aggregation decomposes into a
  bounded, mergeable partial (distributive/algebraic: sum, count, min, max,
  mean, the percentages).  The map emits ``components`` via ``map_specs``; the
  reduce merges them across shards via ``merge_specs`` and then ``finalize``s.

* **Collection** (``single_phase=True``) -- no bounded partial (count_distinct,
  unique).  The map ships the raw source column; the reduce runs ``merge_specs``
  (a ``distinct``/``count_distinct`` kernel) over it in one grouped pass.
  ``map_specs``/``prep`` are unused.  Only supported for *grouped* queries (a
  global collection can't use the empty-key hash kernels -> Python engine).
"""
from typing import Callable, List, NamedTuple, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc


class ArrowAggOptions(NamedTuple):
    """PyArrow aggregate options derived from a single agg's ``ignore_nulls``."""

    # Aggregating actual values (sum/min/max/mean): skip nulls per ignore_nulls,
    # min_count=1 so an empty / all-null group yields null (not 0).
    value_opts: "pc.ScalarAggregateOptions"
    # Counting values: only-valid vs all rows, per ignore_nulls.
    count_opts: "pc.CountOptions"
    # Summing count / 0-1-indicator columns: min_count=0 so an empty group
    # yields 0, never null (counts and percentages have no "missing" state).
    zero_sum_opts: "pc.ScalarAggregateOptions"
    ignore_nulls: bool


def arrow_agg_options(ignore_nulls: bool) -> ArrowAggOptions:
    return ArrowAggOptions(
        value_opts=pc.ScalarAggregateOptions(skip_nulls=ignore_nulls, min_count=1),
        count_opts=pc.CountOptions(mode="only_valid" if ignore_nulls else "all"),
        zero_sum_opts=pc.ScalarAggregateOptions(min_count=0),
        ignore_nulls=ignore_nulls,
    )


# map_specs / merge_specs return lists of PyArrow ``TableGroupBy.aggregate``
# specs -- e.g. ``(col, "sum", options)`` or ``([], "count_all")``.
_Specs = List[tuple]


class ArrowAggSpec(NamedTuple):
    # Per-agg column suffixes present AFTER the reduce group_by (what
    # ``finalize`` reads).  The driver prefixes ``__agg{i}_`` so two aggs on the
    # same column never collide.
    components: Tuple[str, ...]
    # (reduce_input_cols, opts) -> specs producing exactly len(components) cols.
    #   reduction: reduce_input_cols are the map's emitted component columns.
    #   collection: reduce_input_cols is (raw source column,).
    merge_specs: Callable[[Tuple[str, ...], ArrowAggOptions], _Specs]
    # (merged_table, component_cols) -> the output column array.
    finalize: Callable
    single_phase: bool = False
    # Reduction only: (i, source_col, opts) -> specs producing ``components``.
    map_specs: Optional[Callable[[int, Optional[str], ArrowAggOptions], _Specs]] = None
    # Reduction only: (i, source_col, block) -> block with a derived col appended.
    prep: Optional[Callable] = None


# --- reusable finalizers (multi-line; referenced from AggregateFn specs) ------
_NULL_F = pa.scalar(None, pa.float64())


def _finalize_mean(merged: "pa.Table", comp: Tuple[str, ...]):
    s, cnt = comp
    denom = pc.cast(merged[cnt], pa.float64())
    denom = pc.if_else(pc.equal(denom, 0.0), _NULL_F, denom)  # count 0 -> null
    return pc.divide(pc.cast(merged[s], pa.float64()), denom)


def _finalize_pct(merged: "pa.Table", comp: Tuple[str, ...]):
    num, den = comp
    n = pc.cast(merged[num], pa.float64())
    d = pc.cast(merged[den], pa.float64())
    safe = pc.if_else(pc.greater(d, 0.0), d, pa.scalar(1.0))
    pct = pc.multiply(pc.divide(n, safe), 100.0)
    return pc.if_else(pc.equal(d, 0.0), _NULL_F, pct)  # den 0 -> null


# --- spec builders (an AggregateFn picks one in its _arrow_agg_spec) ----------
def sum_spec() -> ArrowAggSpec:
    return ArrowAggSpec(
        components=("sum",),
        map_specs=lambda i, col, o: [(col, "sum", o.value_opts)],
        merge_specs=lambda cols, o: [(cols[0], "sum", o.value_opts)],
        finalize=lambda merged, c: merged[c[0]],
    )


def count_spec() -> ArrowAggSpec:
    return ArrowAggSpec(
        components=("cnt",),
        # global Count() has no target column -> count_all (count rows).
        map_specs=lambda i, col, o: [
            ([], "count_all") if col is None else (col, "count", o.count_opts)
        ],
        merge_specs=lambda cols, o: [
            (cols[0], "sum", o.zero_sum_opts)
        ],  # add per-shard counts
        finalize=lambda merged, c: pc.cast(merged[c[0]], pa.int64()),
    )


def minmax_spec(kind: str) -> ArrowAggSpec:
    # min/max merge with themselves (associative); output passes through.
    return ArrowAggSpec(
        components=("mm",),
        map_specs=lambda i, col, o: [(col, kind, o.value_opts)],
        merge_specs=lambda cols, o: [(cols[0], kind, o.value_opts)],
        finalize=lambda merged, c: merged[c[0]],
    )


def mean_spec() -> ArrowAggSpec:
    # sum/count kept separate so the reduce can merge then divide.
    return ArrowAggSpec(
        components=("sum", "cnt"),
        map_specs=lambda i, col, o: [
            (col, "sum", o.value_opts),
            (col, "count", o.count_opts),
        ],
        merge_specs=lambda cols, o: [
            (cols[0], "sum", o.value_opts),
            (cols[1], "sum", o.zero_sum_opts),
        ],
        finalize=_finalize_mean,
    )


def missing_pct_spec() -> ArrowAggSpec:
    # numerator = #(null or nan) via a 0/1 indicator; denominator = #rows.
    return ArrowAggSpec(
        components=("num", "den"),
        prep=lambda i, col, block: block.append_column(
            f"__d{i}_miss",
            pc.cast(pc.is_null(block[col], nan_is_null=True), pa.int64()),
        ),
        map_specs=lambda i, col, o: [
            (f"__d{i}_miss", "sum", o.zero_sum_opts),
            ([], "count_all"),
        ],
        merge_specs=lambda cols, o: [
            (cols[0], "sum", o.zero_sum_opts),
            (cols[1], "sum", o.zero_sum_opts),
        ],
        finalize=_finalize_pct,
    )


def zero_pct_spec() -> ArrowAggSpec:
    # numerator = #zeros; denominator = #non-null (ignore_nulls) or #rows.
    return ArrowAggSpec(
        components=("num", "den"),
        prep=lambda i, col, block: block.append_column(
            f"__d{i}_zero", pc.cast(pc.equal(block[col], 0), pa.int64())
        ),
        map_specs=lambda i, col, o: [
            (f"__d{i}_zero", "sum", o.zero_sum_opts),
            (col, "count", o.count_opts) if o.ignore_nulls else ([], "count_all"),
        ],
        merge_specs=lambda cols, o: [
            (cols[0], "sum", o.zero_sum_opts),
            (cols[1], "sum", o.zero_sum_opts),
        ],
        finalize=_finalize_pct,
    )


def distinct_spec(kernel: str, finalize: Callable) -> ArrowAggSpec:
    """Single-phase collection agg: the reduce runs ``kernel`` (``distinct`` or
    ``count_distinct``) over the raw source column in one grouped pass.  ``mode``
    (only_valid vs all, via count_opts) carries ``ignore_nulls`` into the kernel."""
    return ArrowAggSpec(
        single_phase=True,
        components=("v",),
        merge_specs=lambda cols, o: [(cols[0], kernel, o.count_opts)],
        finalize=finalize,
    )
