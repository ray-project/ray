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


# raw_agg_specs / merge_specs return lists of PyArrow ``TableGroupBy.aggregate``
# specs e.g. ``(col, "sum", options)`` or ``([], "count_all")``.
_Specs = List[tuple]


class ArrowAggSpec(NamedTuple):
    # Per-agg column suffixes present AFTER the reduce group_by (what
    # ``finalize`` reads).  The driver prefixes ``__agg{i}_`` so two aggs on the
    # same column never collide.
    components: Tuple[str, ...]
    # (agg_index, source_col, options) -> specs that aggregate RAW values into
    # ``components``.  Used both by the two-phase map (to pre-aggregate) and by
    # the single-phase reduce (to aggregate the raw rows a collection query
    # shipped, correct because the shuffle co-locates a group's rows).
    raw_agg_specs: Callable[[int, Optional[str], ArrowAggOptions], _Specs]
    # (merged_table, component_cols) -> the output column array.
    finalize: Callable
    # True for collection-valued aggs (distinct set): no bounded mergeable
    # partial, so the query runs single-phase (see the module docstring for
    # the two shapes and the mixing/keyless fallback rules).
    collection: bool = False
    # Reduction only: (component_cols, options) -> specs merging the map's
    # partial ``components`` across shards.  Unused (None) for collection aggs.
    merge_specs: Optional[Callable[[Tuple[str, ...], ArrowAggOptions], _Specs]] = None
    # (agg_index, source_col, block) -> block with a derived indicator column
    # appended (e.g. percentages).
    prep: Optional[Callable] = None


# --- reusable finalizers (multi-line; referenced from AggregateFn specs) ------
_NULL_FLOAT = pa.scalar(None, pa.float64())


def _finalize_mean(merged: "pa.Table", component_cols: Tuple[str, ...]):
    sum_col, count_col = component_cols
    denominator = pc.cast(merged[count_col], pa.float64())
    # count 0 -> null (empty / all-null group has no mean).
    denominator = pc.if_else(pc.equal(denominator, 0.0), _NULL_FLOAT, denominator)
    return pc.divide(pc.cast(merged[sum_col], pa.float64()), denominator)


def _finalize_pct(merged: "pa.Table", component_cols: Tuple[str, ...]):
    numerator_col, denominator_col = component_cols
    numerator = pc.cast(merged[numerator_col], pa.float64())
    denominator = pc.cast(merged[denominator_col], pa.float64())
    # Avoid dividing by zero, then null it out below.
    safe_denominator = pc.if_else(
        pc.greater(denominator, 0.0), denominator, pa.scalar(1.0)
    )
    pct = pc.multiply(pc.divide(numerator, safe_denominator), 100.0)
    return pc.if_else(pc.equal(denominator, 0.0), _NULL_FLOAT, pct)  # den 0 -> null


# --- spec builders (an AggregateFn picks one in its _arrow_agg_spec) ----------
def sum_spec() -> ArrowAggSpec:
    return ArrowAggSpec(
        components=("sum",),
        raw_agg_specs=lambda agg_index, source_col, options: [
            (source_col, "sum", options.value_opts)
        ],
        merge_specs=lambda input_cols, options: [
            (input_cols[0], "sum", options.value_opts)
        ],
        finalize=lambda merged, component_cols: merged[component_cols[0]],
    )


def count_spec() -> ArrowAggSpec:
    return ArrowAggSpec(
        components=("count",),
        # global Count() has no target column -> count_all (count rows).
        raw_agg_specs=lambda agg_index, source_col, options: [
            ([], "count_all")
            if source_col is None
            else (source_col, "count", options.count_opts)
        ],
        merge_specs=lambda input_cols, options: [
            (input_cols[0], "sum", options.zero_sum_opts)
        ],
        finalize=lambda merged, component_cols: pc.cast(
            merged[component_cols[0]], pa.int64()
        ),
    )


def minmax_spec(kind: str) -> ArrowAggSpec:
    return ArrowAggSpec(
        components=("minmax",),
        raw_agg_specs=lambda agg_index, source_col, options: [
            (source_col, kind, options.value_opts)
        ],
        merge_specs=lambda input_cols, options: [
            (input_cols[0], kind, options.value_opts)
        ],
        finalize=lambda merged, component_cols: merged[component_cols[0]],
    )


def mean_spec() -> ArrowAggSpec:
    return ArrowAggSpec(
        components=("sum", "count"),
        raw_agg_specs=lambda agg_index, source_col, options: [
            (source_col, "sum", options.value_opts),
            (source_col, "count", options.count_opts),
        ],
        merge_specs=lambda input_cols, options: [
            (input_cols[0], "sum", options.value_opts),
            (input_cols[1], "sum", options.zero_sum_opts),
        ],
        finalize=_finalize_mean,
    )


def missing_pct_spec() -> ArrowAggSpec:
    # numerator = #(null or nan) via a 0/1 indicator; denominator = #rows.
    return ArrowAggSpec(
        components=("numerator", "denominator"),
        prep=lambda agg_index, source_col, block: block.append_column(
            f"__d{agg_index}_miss",
            pc.cast(pc.is_null(block[source_col], nan_is_null=True), pa.int64()),
        ),
        raw_agg_specs=lambda agg_index, source_col, options: [
            (f"__d{agg_index}_miss", "sum", options.zero_sum_opts),
            ([], "count_all"),
        ],
        merge_specs=lambda input_cols, options: [
            (input_cols[0], "sum", options.zero_sum_opts),
            (input_cols[1], "sum", options.zero_sum_opts),
        ],
        finalize=_finalize_pct,
    )


def zero_pct_spec() -> ArrowAggSpec:
    # numerator = #zeros; denominator = #non-null (ignore_nulls) or #rows.
    return ArrowAggSpec(
        components=("numerator", "denominator"),
        prep=lambda agg_index, source_col, block: block.append_column(
            f"__d{agg_index}_zero", pc.cast(pc.equal(block[source_col], 0), pa.int64())
        ),
        raw_agg_specs=lambda agg_index, source_col, options: [
            (f"__d{agg_index}_zero", "sum", options.zero_sum_opts),
            (source_col, "count", options.count_opts)
            if options.ignore_nulls
            else ([], "count_all"),
        ],
        merge_specs=lambda input_cols, options: [
            (input_cols[0], "sum", options.zero_sum_opts),
            (input_cols[1], "sum", options.zero_sum_opts),
        ],
        finalize=_finalize_pct,
    )


def distinct_spec(kernel: str, finalize: Callable) -> ArrowAggSpec:
    """Collection agg: ``raw_agg_specs`` aggregates the raw source column with
    ``kernel`` (``distinct``/``count_distinct``) in one grouped pass.  Because it
    has no mergeable partial it is marked ``collection=True``, so the whole query
    runs single-phase and this runs in the reduce over co-located raw rows.  The
    mode (only_valid vs all, via count_opts) carries ``ignore_nulls`` in."""
    return ArrowAggSpec(
        collection=True,
        components=("distinct",),
        raw_agg_specs=lambda agg_index, source_col, options: [
            (source_col, kernel, options.count_opts)
        ],
        finalize=finalize,
    )
