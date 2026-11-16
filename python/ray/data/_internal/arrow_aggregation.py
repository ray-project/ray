from typing import Callable, List, NamedTuple, Optional, Tuple, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc


def is_boolean_arrow_type(t: "pa.DataType") -> bool:
    """Whether ``t`` is boolean, including dictionary/run-end encoded booleans."""
    if pa.types.is_dictionary(t) or pa.types.is_run_end_encoded(t):
        t = t.value_type
    return pa.types.is_boolean(t)


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
    # partial, so the query runs single-phase (see the hash_aggregate_v2
    # module docstring for the two shapes and the fallback rules).
    collection: bool = False
    # Reduction only: (component_cols, options) -> specs merging the map's
    # partial ``components`` across shards.  Unused (None) for collection aggs.
    merge_specs: Optional[Callable[[Tuple[str, ...], ArrowAggOptions], _Specs]] = None
    # (agg_index, source_col, block) -> block with a derived indicator column
    # appended (e.g. percentages).
    prep: Optional[Callable] = None
    # Reduction only: (prepped_block, agg_index, source_col, options) -> one
    # array per ``components`` entry, holding each row's state as if it were a
    # group of one.  Lets a reducer turn raw rows (shipped when a map task
    # skipped partial aggregation) into partial state without a group_by, so
    # one ``merge_specs`` pass covers raw and partial shards alike.  If any
    # agg in a query lacks it, map tasks always pre-aggregate.
    raw_to_partial: Optional[
        Callable[..., List[Union["pa.Array", "pa.ChunkedArray"]]]
    ] = None
    # Alternatives to ``raw_agg_specs`` / ``merge_specs`` for state that
    # Arrow's grouped functions can't build (lists, value counts, Std's
    # parallel merge).  Each gets the input table plus the ``Groups`` of the
    # same group_by and returns one array per component, one entry per group.
    # A spec with ``grouped_raw`` sets ``raw_agg_specs`` to ``_no_kernels``.
    #   grouped_raw: (prepped_table, groups, agg_index, source_col, options)
    #   grouped_merge: (partials_table, groups, component_cols, options)
    grouped_raw: Optional[Callable[..., List["pa.Array"]]] = None
    grouped_merge: Optional[Callable[..., List["pa.Array"]]] = None
    # (source column type) -> whether this spec can aggregate it.  None means
    # every type.  Unsupported columns are aggregated by the Python engine.
    supports_type: Optional[Callable[["pa.DataType"], bool]] = None


class Groups(NamedTuple):
    """Which group each input row of a ``group_by`` landed in.

    ``group_by`` returns one row per group but not the row -> group mapping
    (pandas' ``groupby().ngroup()``). Merges like Std's need it to broadcast a
    group's value back to its rows; ``ids[r]`` is the output row of input row
    ``r`` and ``count`` the number of groups.
    """

    ids: np.ndarray
    count: int


def _no_kernels(agg_index, source_col, options) -> "_Specs":
    return []


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


def _ones(block: "pa.Table") -> "pa.Array":
    return pa.array(np.ones(block.num_rows, dtype=np.int64))


def _count_state(
    block: "pa.Table", source_col: Optional[str], options: ArrowAggOptions
) -> Union["pa.Array", "pa.ChunkedArray"]:
    """Per-row ``count`` state: 1 per row, or 0 for nulls when they're ignored."""
    if source_col is None or not options.ignore_nulls:
        return _ones(block)
    return pc.cast(pc.is_valid(block[source_col]), pa.int64())


def _as_array(column: Union["pa.Array", "pa.ChunkedArray"]) -> "pa.Array":
    if isinstance(column, pa.ChunkedArray):
        return column.combine_chunks()
    return column


def _decode(column: Union["pa.Array", "pa.ChunkedArray"]):
    """Plain values for dictionary / run-end encoded columns."""
    if pa.types.is_dictionary(column.type):
        return pc.cast(column, column.type.value_type)
    if pa.types.is_run_end_encoded(column.type):
        return pc.run_end_decode(column)
    return column


def _nan_to_null(column):
    if pa.types.is_floating(column.type):
        return pc.if_else(pc.is_nan(column), pa.scalar(None, column.type), column)
    return column


def _list_offsets(lengths: np.ndarray) -> Tuple["pa.Array", type]:
    offsets = np.zeros(len(lengths) + 1, dtype=np.int64)
    np.cumsum(lengths, out=offsets[1:])
    if offsets[-1] > np.iinfo(np.int32).max:
        return pa.array(offsets), pa.LargeListArray
    return pa.array(offsets.astype(np.int32)), pa.ListArray


def lists_by_group(
    columns: List[Union["pa.Array", "pa.ChunkedArray"]],
    ids: np.ndarray,
    num_groups: int,
) -> List["pa.Array"]:
    """Gathers each column's values into one list per group (in group order),
    keeping row order within a group. Works for any value type."""
    order = pa.array(np.argsort(ids, kind="stable"))
    offsets, list_cls = _list_offsets(np.bincount(ids, minlength=num_groups))
    return [
        list_cls.from_arrays(offsets, _as_array(column).take(order))
        for column in columns
    ]


def flatten_lists(
    columns: List[Union["pa.Array", "pa.ChunkedArray"]], ids: np.ndarray
) -> Tuple[List["pa.Array"], np.ndarray]:
    """Inverse of ``lists_by_group`` for list columns with equal shapes: their
    flattened values, and the group of each value (``ids`` maps list rows to
    groups)."""
    lists = [_as_array(column) for column in columns]
    parents = pc.list_parent_indices(lists[0]).to_numpy()
    return [pc.list_flatten(column) for column in lists], ids[parents]


def _singleton_lists(
    column: Union["pa.Array", "pa.ChunkedArray"], keep: Optional[np.ndarray] = None
) -> "pa.Array":
    """One list per row holding that row's value, or nothing where ``keep`` is
    False."""
    column = _as_array(column)
    if keep is None:
        lengths = np.ones(len(column), dtype=np.int64)
    else:
        lengths = keep.astype(np.int64)
        column = column.filter(pa.array(keep))
    offsets, list_cls = _list_offsets(lengths)
    return list_cls.from_arrays(offsets, column)


def _is_valid_number(column) -> np.ndarray:
    """Non-null and, for floats, non-NaN."""
    valid = pc.is_valid(column)
    if pa.types.is_floating(column.type):
        valid = pc.and_(valid, pc.invert(pc.is_nan(column)))
    return _as_array(pc.fill_null(valid, False)).to_numpy(zero_copy_only=False)


def _count_values_by_group(
    values: Union["pa.Array", "pa.ChunkedArray"],
    ids: np.ndarray,
    num_groups: int,
    weights: Optional[Union["pa.Array", "pa.ChunkedArray"]] = None,
) -> List["pa.Array"]:
    """Per group, its distinct values and how often each occurs (summing
    ``weights`` instead of counting rows when given), as two list columns."""
    table = pa.table({"group": pa.array(ids), "value": values})
    if weights is None:
        counted = table.group_by(["group", "value"], use_threads=False).aggregate(
            [([], "count_all")]
        )
        counts = counted["count_all"]
    else:
        table = table.append_column("weight", weights)
        counted = table.group_by(["group", "value"], use_threads=False).aggregate(
            [("weight", "sum")]
        )
        counts = counted["weight_sum"]
    return lists_by_group(
        [counted["value"], pc.cast(counts, pa.int64())],
        _as_array(counted["group"]).to_numpy(),
        num_groups,
    )


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
        raw_to_partial=lambda block, agg_index, source_col, options: [
            block[source_col]
        ],
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
        raw_to_partial=lambda block, agg_index, source_col, options: [
            _count_state(block, source_col, options)
        ],
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
        raw_to_partial=lambda block, agg_index, source_col, options: [
            block[source_col]
        ],
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
        raw_to_partial=lambda block, agg_index, source_col, options: [
            block[source_col],
            _count_state(block, source_col, options),
        ],
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
        raw_to_partial=lambda block, agg_index, source_col, options: [
            block[f"__d{agg_index}_miss"],
            _ones(block),
        ],
    )


def _zero_indicator(column: "pa.ChunkedArray") -> "pa.ChunkedArray":
    # `equal(bool, int)` has no kernel; treat booleans as 0/1.
    if is_boolean_arrow_type(column.type):
        column = pc.cast(column, pa.int8())
    return pc.cast(pc.equal(column, 0), pa.int64())


def zero_pct_spec() -> ArrowAggSpec:
    # numerator = #zeros; denominator = #non-null (ignore_nulls) or #rows.
    return ArrowAggSpec(
        components=("numerator", "denominator"),
        prep=lambda agg_index, source_col, block: block.append_column(
            f"__d{agg_index}_zero", _zero_indicator(block[source_col])
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
        raw_to_partial=lambda block, agg_index, source_col, options: [
            block[f"__d{agg_index}_zero"],
            _count_state(block, source_col, options),
        ],
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
        supports_type=lambda t: not pa.types.is_nested(t),
    )


def _is_numeric(t: "pa.DataType") -> bool:
    if pa.types.is_dictionary(t) or pa.types.is_run_end_encoded(t):
        t = t.value_type
    return pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_null(t)


def _is_groupable(t: "pa.DataType") -> bool:
    """Whether Arrow can group by values of type ``t``."""
    if pa.types.is_dictionary(t) or pa.types.is_run_end_encoded(t):
        t = t.value_type
    return not (pa.types.is_nested(t) or isinstance(t, pa.ExtensionType))


def _is_numeric_or_bool(t: "pa.DataType") -> bool:
    return _is_numeric(t) or is_boolean_arrow_type(t)


def absmax_spec() -> ArrowAggSpec:
    """``max(abs(x))``: each shard keeps its largest absolute value, and the
    reducer keeps the largest of those."""
    return ArrowAggSpec(
        components=("absmax",),
        # abs_checked raises on abs(INT_MIN) instead of wrapping to INT_MIN.
        prep=lambda agg_index, source_col, block: block.append_column(
            f"__d{agg_index}_abs", pc.abs_checked(_decode(block[source_col]))
        ),
        raw_agg_specs=lambda agg_index, source_col, options: [
            (f"__d{agg_index}_abs", "max", options.value_opts)
        ],
        merge_specs=lambda input_cols, options: [
            (input_cols[0], "max", options.value_opts)
        ],
        finalize=lambda merged, component_cols: merged[component_cols[0]],
        raw_to_partial=lambda block, agg_index, source_col, options: [
            block[f"__d{agg_index}_abs"]
        ],
        supports_type=_is_numeric,
    )


def _std_input(column):
    """Std's input as float64 (booleans count as 0/1). NaN is a value, so it
    makes the result NaN, as with the vectorized Mean and Sum."""
    return pc.cast(_decode(column), pa.float64())


def _merge_std(
    table: "pa.Table",
    groups: Groups,
    component_cols: Tuple[str, ...],
    options: ArrowAggOptions,
) -> List["pa.Array"]:
    """Merges ``(count, mean, population variance)`` partials per group with
    Chan et al.'s parallel formula, for all groups at once:
    ``M2 = sum(n_i * (var_i + (mean_i - mean)^2))``."""
    count_col, mean_col, var_col = component_cols
    ids, num_groups = groups.ids, groups.count
    n = _as_array(pc.cast(table[count_col], pa.int64())).to_numpy(zero_copy_only=False)
    mean_arr = _as_array(pc.cast(table[mean_col], pa.float64()))
    mean_is_null = _as_array(pc.is_null(mean_arr)).to_numpy(zero_copy_only=False)
    mean = _as_array(pc.fill_null(mean_arr, 0.0)).to_numpy(zero_copy_only=False)
    var = _as_array(pc.fill_null(pc.cast(table[var_col], pa.float64()), 0.0)).to_numpy(
        zero_copy_only=False
    )

    has_rows = n > 0
    # With ignore_nulls=False a partial that saw a null has a null mean, and
    # so does the merged group.
    saw_null = np.bincount(ids, weights=has_rows & mean_is_null, minlength=num_groups)
    used = has_rows & ~mean_is_null
    weight = np.where(used, n, 0).astype(np.float64)
    total = np.bincount(ids, weights=weight, minlength=num_groups)
    with np.errstate(invalid="ignore", divide="ignore"):
        group_mean = (
            np.bincount(
                ids, weights=np.where(used, weight * mean, 0.0), minlength=num_groups
            )
            / total
        )
        delta = mean - group_mean[ids]
        m2 = np.bincount(
            ids,
            weights=np.where(used, weight * (var + delta * delta), 0.0),
            minlength=num_groups,
        )
        group_var = m2 / total
    null = (total == 0) | (saw_null > 0)
    return [
        pa.array(total.astype(np.int64)),
        pa.array(group_mean, mask=null),
        pa.array(group_var, mask=null),
    ]


def _finalize_std(ddof: int) -> Callable:
    def _finalize(merged: "pa.Table", component_cols: Tuple[str, ...]):
        count_col, _, var_col = component_cols
        var_arr = _as_array(pc.cast(merged[var_col], pa.float64()))
        null = _as_array(pc.is_null(var_arr)).to_numpy(zero_copy_only=False)
        n = _as_array(pc.cast(merged[count_col], pa.float64())).to_numpy(
            zero_copy_only=False
        )
        var = _as_array(pc.fill_null(var_arr, 0.0)).to_numpy(zero_copy_only=False)
        denominator = n - ddof
        with np.errstate(invalid="ignore", divide="ignore"):
            # count <= ddof leaves std undefined: NaN, like pandas.
            std = np.where(
                denominator > 0, np.sqrt(np.maximum(var * n, 0.0) / denominator), np.nan
            )
        return pa.array(std, mask=null)

    return _finalize


def std_spec(ddof: int) -> ArrowAggSpec:
    """State ``(count, mean, population variance)`` per group: Arrow's grouped
    ``variance`` computes it on the map, and ``_merge_std`` combines it on the
    reducer without the precision loss of sum / sum-of-squares."""
    return ArrowAggSpec(
        components=("count", "mean", "var"),
        prep=lambda agg_index, source_col, block: block.append_column(
            f"__d{agg_index}_std", _std_input(block[source_col])
        ),
        raw_agg_specs=lambda agg_index, source_col, options: [
            (f"__d{agg_index}_std", "count", options.count_opts),
            (f"__d{agg_index}_std", "mean", options.value_opts),
            (
                f"__d{agg_index}_std",
                "variance",
                pc.VarianceOptions(
                    ddof=0, skip_nulls=options.ignore_nulls, min_count=1
                ),
            ),
        ],
        grouped_merge=_merge_std,
        finalize=_finalize_std(ddof),
        raw_to_partial=lambda block, agg_index, source_col, options: [
            _count_state(block, f"__d{agg_index}_std", options),
            block[f"__d{agg_index}_std"],
            pa.array(np.zeros(block.num_rows)),
        ],
        supports_type=_is_numeric_or_bool,
    )


def list_spec(ignore_nulls: bool) -> ArrowAggSpec:
    """All of a group's values as one list; partials are lists, merged by
    concatenating a group's lists."""

    def _keep(column) -> Optional[np.ndarray]:
        if not ignore_nulls:
            return None
        return _as_array(pc.is_valid(column)).to_numpy(zero_copy_only=False)

    def _grouped_raw(table, groups, agg_index, source_col, options):
        column, ids = table[source_col], groups.ids
        keep = _keep(column)
        if keep is not None:
            column, ids = column.filter(pa.array(keep)), ids[keep]
        return lists_by_group([column], ids, groups.count)

    def _grouped_merge(table, groups, component_cols, options):
        (values,), ids = flatten_lists([table[component_cols[0]]], groups.ids)
        return lists_by_group([values], ids, groups.count)

    return ArrowAggSpec(
        components=("list",),
        raw_agg_specs=_no_kernels,
        grouped_raw=_grouped_raw,
        grouped_merge=_grouped_merge,
        finalize=lambda merged, component_cols: merged[component_cols[0]],
        raw_to_partial=lambda block, agg_index, source_col, options: [
            _singleton_lists(block[source_col], _keep(block[source_col]))
        ],
    )


def _value_counts_spec(
    prepare: Callable,
    keep_value: Callable,
    finalize: Callable,
    supports_type: Callable[["pa.DataType"], bool],
    count_nulls: bool = False,
) -> ArrowAggSpec:
    """State shared by ValueCounter, TopKUnique and Quantile: per group, its
    distinct values and their counts (Spark's ``mode``/``percentile`` state),
    as two list columns, plus a null count when ``count_nulls``. Merging adds
    up the counts of equal values.

    ``prepare(column) -> column`` normalizes the values (e.g. NaN -> null);
    ``keep_value(column) -> bool mask`` picks the values to count.
    """
    components = ("values", "counts") + (("nulls",) if count_nulls else ())

    def _prepped(agg_index: int) -> str:
        return f"__d{agg_index}_vc"

    def _null_count(column, ids, num_groups) -> "pa.Array":
        is_null = _as_array(pc.is_null(column)).to_numpy(zero_copy_only=False)
        return pa.array(
            np.bincount(ids, weights=is_null, minlength=num_groups).astype(np.int64)
        )

    def _grouped_raw(table, groups, agg_index, source_col, options):
        column = table[_prepped(agg_index)]
        keep = keep_value(column)
        states = _count_values_by_group(
            column.filter(pa.array(keep)), groups.ids[keep], groups.count
        )
        if count_nulls:
            states.append(_null_count(column, groups.ids, groups.count))
        return states

    def _grouped_merge(table, groups, component_cols, options):
        (values, counts), ids = flatten_lists(
            [table[component_cols[0]], table[component_cols[1]]], groups.ids
        )
        states = _count_values_by_group(values, ids, groups.count, weights=counts)
        if count_nulls:
            nulls = _as_array(table[component_cols[2]]).to_numpy(zero_copy_only=False)
            states.append(
                pa.array(
                    np.bincount(
                        groups.ids, weights=nulls, minlength=groups.count
                    ).astype(np.int64)
                )
            )
        return states

    def _raw_to_partial(block, agg_index, source_col, options):
        column = block[_prepped(agg_index)]
        keep = keep_value(column)
        states = [
            _singleton_lists(column, keep),
            _singleton_lists(pa.array(np.ones(len(column), dtype=np.int64)), keep),
        ]
        if count_nulls:
            states.append(pc.cast(pc.is_null(column), pa.int64()))
        return states

    return ArrowAggSpec(
        components=components,
        prep=lambda agg_index, source_col, block: block.append_column(
            _prepped(agg_index), prepare(_decode(block[source_col]))
        ),
        raw_agg_specs=_no_kernels,
        grouped_raw=_grouped_raw,
        grouped_merge=_grouped_merge,
        finalize=finalize,
        raw_to_partial=_raw_to_partial,
        supports_type=supports_type,
    )


def _keep_all(column) -> np.ndarray:
    return np.ones(len(column), dtype=bool)


def _keep_valid(column) -> np.ndarray:
    return _as_array(pc.is_valid(column)).to_numpy(zero_copy_only=False)


def value_counter_spec() -> ArrowAggSpec:
    """``{"values": [...], "counts": [...]}`` per group. Null and NaN are
    counted as values of their own, as Arrow's ``value_counts`` does."""
    return _value_counts_spec(
        prepare=lambda column: column,
        keep_value=_keep_all,
        finalize=lambda merged, component_cols: pa.StructArray.from_arrays(
            [
                _as_array(merged[component_cols[0]]),
                _as_array(merged[component_cols[1]]),
            ],
            names=["values", "counts"],
        ),
        supports_type=_is_groupable,
    )


def _finalize_topk(k: int) -> Callable:
    def _finalize(merged: "pa.Table", component_cols: Tuple[str, ...]):
        values_col, counts_col = component_cols[:2]
        num_groups = merged.num_rows
        (values, counts), parents = flatten_lists(
            [merged[values_col], merged[counts_col]], np.arange(num_groups)
        )
        # Most frequent first; ties by value, nulls last (the default).
        order = pc.sort_indices(
            pa.table({"group": parents, "count": counts, "value": values}),
            sort_keys=[
                ("group", "ascending"),
                ("count", "descending"),
                ("value", "ascending"),
            ],
        ).to_numpy()
        sorted_groups = parents[order]
        group_starts = np.searchsorted(sorted_groups, np.arange(num_groups))
        rank = np.arange(len(order)) - group_starts[sorted_groups]
        top = rank < k
        offsets, list_cls = _list_offsets(
            np.bincount(sorted_groups[top], minlength=num_groups)
        )
        return list_cls.from_arrays(offsets, values.take(pa.array(order[top])))

    return _finalize


def topk_unique_spec(k: int, ignore_nulls: bool) -> ArrowAggSpec:
    """The ``k`` most frequent values per group. NaN counts as null, as in
    the Python implementation."""
    return _value_counts_spec(
        prepare=_nan_to_null,
        keep_value=_keep_valid if ignore_nulls else _keep_all,
        finalize=_finalize_topk(k),
        supports_type=_is_groupable,
    )


def _finalize_quantile(q: float, ignore_nulls: bool) -> Callable:
    def _finalize(merged: "pa.Table", component_cols: Tuple[str, ...]):
        values_col, counts_col, nulls_col = component_cols
        num_groups = merged.num_rows
        (values, counts), parents = flatten_lists(
            [merged[values_col], merged[counts_col]], np.arange(num_groups)
        )
        values = pc.cast(values, pa.float64()).to_numpy(zero_copy_only=False)
        counts = counts.to_numpy(zero_copy_only=False)
        order = np.lexsort((values, parents))
        values, counts, parents = values[order], counts[order], parents[order]

        # The value at (0-based) rank r of group g is the first sorted entry
        # whose running count passes base[g] + r.
        group_sizes = np.bincount(parents, weights=counts, minlength=num_groups)
        group_sizes = group_sizes.astype(np.int64)
        base = np.concatenate([[0], np.cumsum(group_sizes)[:-1]])
        running = np.cumsum(counts)
        position = q * np.maximum(group_sizes - 1, 0)
        lower, upper = np.floor(position), np.ceil(position)
        last = max(len(values) - 1, 0)

        def _value_at(rank: np.ndarray) -> np.ndarray:
            if not len(values):
                return np.full(num_groups, np.nan)
            index = np.searchsorted(running, base + rank.astype(np.int64), "right")
            return values[np.minimum(index, last)]

        low_value, high_value = _value_at(lower), _value_at(upper)
        result = low_value + (high_value - low_value) * (position - lower)
        null = group_sizes == 0
        if not ignore_nulls:
            nulls = _as_array(merged[nulls_col]).to_numpy(zero_copy_only=False)
            null |= nulls > 0
        return pa.array(result, mask=null)

    return _finalize


def quantile_spec(q: float, ignore_nulls: bool) -> ArrowAggSpec:
    """Exact quantile with linear interpolation, like ``pc.quantile``, from
    each group's value counts. Null and NaN are skipped; a null makes the
    result null unless nulls are ignored."""
    return _value_counts_spec(
        prepare=lambda column: column,
        keep_value=_is_valid_number,
        finalize=_finalize_quantile(q, ignore_nulls),
        supports_type=_is_numeric,
        count_nulls=True,
    )
