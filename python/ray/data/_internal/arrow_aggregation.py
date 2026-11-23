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
    # parallel merge).  Each gets the input table plus a ``Groups`` saying
    # which group each of its rows belongs to, and returns one array per
    # component with one entry per group.  See ``Groups`` for a worked
    # example.  A spec with ``grouped_raw`` sets ``raw_agg_specs`` to
    # ``_no_kernels``.
    #   grouped_raw: (prepped_table, groups, agg_index, source_col, options)
    #     aggregates raw rows (map side, or raw shards on the reducer).
    #   grouped_merge: (partials_table, groups, component_cols, options)
    #     merges partial rows from different shards (reducer).
    grouped_raw: Optional[Callable[..., List["pa.Array"]]] = None
    grouped_merge: Optional[Callable[..., List["pa.Array"]]] = None
    # (source column type) -> whether this spec can aggregate it.  None means
    # every type.  Unsupported columns are aggregated by the Python engine.
    supports_type: Optional[Callable[["pa.DataType"], bool]] = None


class Groups(NamedTuple):
    """Which group each input row of a ``group_by`` landed in.

    ``ids[r]`` is the output row (group) of input row ``r``, and ``count`` is
    the number of groups: pandas' ``groupby().ngroup()``. Arrow's
    ``group_by(...).aggregate(...)`` only returns one row per group, so the
    hash aggregator gets this mapping by adding a ``("__ray_row", "list")``
    aggregation (each group's input row numbers) to the same ``group_by`` and
    scattering it back. Using the same ``group_by`` keeps ``ids`` aligned
    with the other aggregations' output rows, null keys included (a join
    would drop them).

    With ``ids``, per-group math is plain numpy over whole arrays:
    ``np.bincount(ids, weights=x)`` sums ``x`` per group, and
    ``per_group[ids]`` broadcasts a per-group value back to the rows.

    Example: merging Std partials (``n``, ``mean``) from two shards::

        partials   row  key  n  mean        group_by("key") output
                    0    a   2   10           group 0: key=a, __ray_row=[0, 2]
                    1    b   1    5           group 1: key=b, __ray_row=[1]
                    2    a   2   20
        ids   = [0, 1, 0]                   # row 0 -> group 0, ...
        count = 2

        total     = np.bincount(ids, weights=n)              # [4, 1]
        mean_all  = np.bincount(ids, weights=n * mean) / total  # [15, 5]
        deviation = mean - mean_all[ids]                     # [-5, 0, 5]

    ``deviation`` is each partial's distance from its group's overall mean,
    which Std's merge needs (see ``_merge_std``) and which no single Arrow
    grouped function can compute.
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


def _narrow_ids(ids: np.ndarray, num_groups: int) -> np.ndarray:
    """``ids`` in the smallest integer type that holds them. A stable sort on
    16-bit or smaller integers is a radix sort, ~10x faster than on int64."""
    for dtype in (np.uint8, np.uint16, np.uint32):
        # ids run from 0 to num_groups - 1.
        if num_groups <= np.iinfo(dtype).max + 1:
            return ids.astype(dtype, copy=False)
    return ids


# Arrow's ``sort_indices`` sorts integers whose values span a small range
# with a counting sort: one linear pass, no comparisons. Measured on 20M ids,
# it stays linear up to ~2K distinct values and is several times slower past
# ~10K.
_COUNTING_SORT_MAX_GROUPS = 2048
# Wider ids are sorted one 11-bit digit (2048 values) at a time.
_RADIX_DIGIT_BITS = 11


def _group_order(ids: np.ndarray, num_groups: int) -> np.ndarray:
    """Row indices ordered by group, keeping row order within a group.

    A stable sort of the group ids, done as an LSD radix sort: Arrow's
    counting sort orders the rows by the lowest 11 bits of their id, then
    (stably) by the next 11 bits, and so on. Each pass is linear, so this is
    O(rows * passes) with 1 pass for <= 2048 groups, 2 for <= 4M and 3
    beyond. On 20M rows it's 1.3-8x faster than numpy's stable argsort,
    which is a radix sort only for 8- and 16-bit ints and a merge sort
    otherwise.
    """
    ids = _narrow_ids(ids, num_groups)
    if num_groups <= _COUNTING_SORT_MAX_GROUPS:
        return pc.sort_indices(pa.array(ids)).to_numpy()
    digit_mask = (1 << _RADIX_DIGIT_BITS) - 1
    order: Optional[np.ndarray] = None
    for shift in range(0, int(num_groups - 1).bit_length(), _RADIX_DIGIT_BITS):
        current = ids if order is None else ids[order]
        digit = ((current >> shift) & digit_mask).astype(np.uint16)
        step = pc.sort_indices(pa.array(digit)).to_numpy()
        order = step if order is None else order[step]
    assert order is not None
    return order


def lists_by_group(
    columns: List[Union["pa.Array", "pa.ChunkedArray"]],
    ids: np.ndarray,
    num_groups: int,
) -> List["pa.Array"]:
    """Gathers each column's values into one list per group (in group order),
    keeping row order within a group. Works for any value type."""
    order = pa.array(_group_order(ids, num_groups))
    offsets, list_cls = _list_offsets(np.bincount(ids, minlength=num_groups))
    return [
        list_cls.from_arrays(offsets, _as_array(column).take(order))
        for column in columns
    ]


def _list_lengths(column: Union["pa.Array", "pa.ChunkedArray"]) -> np.ndarray:
    """Number of values in each list (0 for a null list)."""
    lengths = pc.fill_null(pc.list_value_length(column), 0)
    return _as_array(lengths).to_numpy(zero_copy_only=False).astype(np.int64)


def flatten_lists(
    columns: List[Union["pa.Array", "pa.ChunkedArray"]], ids: np.ndarray
) -> Tuple[List["pa.Array"], np.ndarray]:
    """Inverse of ``lists_by_group`` for list columns with equal shapes: their
    flattened values, and the group of each value (``ids`` maps list rows to
    groups). A row's values are contiguous, so their groups are ``ids``
    repeated by list length: a sequential pass, not a gather."""
    flat = [_as_array(pc.list_flatten(column)) for column in columns]
    return flat, np.repeat(ids, _list_lengths(columns[0]))


def concat_lists_by_group(
    column: Union["pa.Array", "pa.ChunkedArray"], ids: np.ndarray, num_groups: int
) -> "pa.Array":
    """One list per group: its rows' lists concatenated, in row order.

    Orders the rows (lists), not the values: a reducer merging k shards sorts
    about k rows per group, then copies every value once with ``take``.
    """
    order = pa.array(_group_order(ids, num_groups))
    values = _as_array(pc.list_flatten(column.take(order)))
    lengths = np.bincount(ids, weights=_list_lengths(column), minlength=num_groups)
    offsets, list_cls = _list_offsets(lengths.astype(np.int64))
    return list_cls.from_arrays(offsets, values)


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
    ``weights`` instead of counting rows when given), as two list columns.

    Values are equal as in a ``group_by``: NaN equals NaN, -0.0 and 0.0
    differ, and null is a value of its own. Rather than ``group_by`` on
    (group, value), which is slow for floating-point values (5.7s for 500K
    float64 rows), each value is replaced by a small integer code with
    ``dictionary_encode`` (a hash table pass), and each (group, code) pair by
    one int64, ``group * num_distinct + code``. Counting those pairs is a
    second hash pass plus ``np.bincount``: ~25x faster.
    """
    values = _as_array(values)
    encoded = pc.dictionary_encode(values, null_encoding="encode")
    distinct = encoded.dictionary
    codes = encoded.indices.to_numpy(zero_copy_only=False).astype(np.int64)
    pairs = pc.dictionary_encode(pa.array(ids.astype(np.int64) * len(distinct) + codes))
    pair_of_row = pairs.indices.to_numpy(zero_copy_only=False)
    pair_keys = pairs.dictionary.to_numpy()
    weight = (
        None if weights is None else _as_array(weights).to_numpy(zero_copy_only=False)
    )
    # bincount's weighted sums are float64: exact for counts below 2**53.
    counts = np.bincount(pair_of_row, weights=weight, minlength=len(pair_keys))
    num_distinct = max(len(distinct), 1)
    return lists_by_group(
        [
            distinct.take(pa.array(pair_keys % num_distinct)),
            pa.array(counts.astype(np.int64)),
        ],
        pair_keys // num_distinct,
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


def _widen_float16(column):
    """float16 as float32, exactly: Arrow has no ``dictionary_encode``,
    sort or ``abs`` kernel for float16. Applied to every block, so float16
    and float32 blocks of one column give partials of the same type."""
    if pa.types.is_float16(column.type):
        return pc.cast(column, pa.float32())
    return column


def _is_numeric_or_bool(t: "pa.DataType") -> bool:
    return _is_numeric(t) or is_boolean_arrow_type(t)


def absmax_input_type(t: "pa.DataType") -> "pa.DataType":
    """The type AbsMax takes ``abs`` in: signed integers narrower than 64 bits
    widen to int64, since ``abs(-128)`` doesn't fit in int8, and float16 to
    float32, which has an ``abs`` kernel. Other types are kept."""
    if pa.types.is_signed_integer(t) and t.bit_width < 64:
        return pa.int64()
    if pa.types.is_float16(t):
        return pa.float32()
    return t


def _abs_checked(column):
    """``abs`` that raises on ``abs(INT64_MIN)`` instead of wrapping. A
    null-typed (all-null) column passes through: older pyarrow has no ``abs``
    kernel for it."""
    column = _decode(column)
    if pa.types.is_null(column.type):
        return column
    return pc.abs_checked(pc.cast(column, absmax_input_type(column.type)))


def absmax_spec() -> ArrowAggSpec:
    """``max(abs(x))``: each shard keeps its largest absolute value, and the
    reducer keeps the largest of those."""
    return ArrowAggSpec(
        components=("absmax",),
        prep=lambda agg_index, source_col, block: block.append_column(
            f"__d{agg_index}_abs", _abs_checked(block[source_col])
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
    """Merges ``(count, mean, population variance)`` partials per group, for
    all groups at once, with the pairwise update of Chan, Golub and LeVeque,
    "Updating Formulae and a Pairwise Algorithm for Computing Sample
    Variances" (Stanford CS tech report STAN-CS-79-773, 1979), generalized
    to k partials:

        mean = sum(n_i * mean_i) / N,   N = sum(n_i)
        M2   = sum(n_i * (var_i + (mean_i - mean)^2))

    where ``M2`` is the sum of squared deviations from ``mean``. Unlike
    accumulating sum and sum of squares, this stays accurate when
    ``|mean| >> std``. See also
    https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    """
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
    # Work with each partial's mean relative to one of its group's partial
    # means. Averaging the raw means first rounds away the spread when
    # |mean| >> std: partials 1e16 and 1e16 + 2 have mean 1e16 + 1, which
    # rounds to 1e16, giving deviations 0 and 2 instead of -1 and 1. Any
    # partial of the group works as the reference.
    reference = np.zeros(num_groups)
    reference[ids[used]] = mean[used]
    offset = np.where(used, mean - reference[ids], 0.0)
    with np.errstate(invalid="ignore", divide="ignore"):
        mean_offset = (
            np.bincount(ids, weights=weight * offset, minlength=num_groups) / total
        )
        delta = np.where(used, offset - mean_offset[ids], 0.0)
        m2 = np.bincount(
            ids,
            weights=np.where(used, weight * (var + delta * delta), 0.0),
            minlength=num_groups,
        )
        group_mean = reference + mean_offset
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


def _std_grouped_raw(
    table: "pa.Table",
    groups: Groups,
    agg_index: int,
    source_col: Optional[str],
    options: ArrowAggOptions,
) -> List["pa.Array"]:
    """``(count, mean, population variance)`` of each group's raw values, as
    Arrow's grouped ``count``, ``mean`` and ``variance`` would give, but
    computed around one of the group's values (the "shifted data" method).
    Two reasons not to use Arrow's ``variance``:

    * It rounds the mean first, which loses the spread when |mean| >> std:
      [1e16, 1e16 + 2] has mean 1e16 + 1, which isn't a double, so it gives
      variance 2 instead of 1. Values relative to a group value are small and
      exact.
    * It slows down superlinearly with many groups: 25 s for 8M rows in ~6M
      groups, against 4 s for ``max`` (pyarrow 23). This is 5 s.
    """
    column = _as_array(table[f"__d{agg_index}_std"])
    ids, num_groups = groups.ids, groups.count
    valid = _as_array(pc.is_valid(column)).to_numpy(zero_copy_only=False)
    x = _as_array(pc.fill_null(column, 0.0)).to_numpy(zero_copy_only=False)
    num_valid = np.bincount(ids, weights=valid, minlength=num_groups)
    if options.ignore_nulls:
        count, saw_null = num_valid, np.zeros(num_groups, dtype=bool)
    else:
        count = np.bincount(ids, minlength=num_groups).astype(np.float64)
        saw_null = count > num_valid
    # Any finite value of the group works; inf - inf would be NaN.
    reference = np.zeros(num_groups)
    finite = valid & np.isfinite(x)
    reference[ids[finite]] = x[finite]
    offset = np.where(valid, x - reference[ids], 0.0)
    with np.errstate(invalid="ignore", divide="ignore"):
        mean_offset = np.bincount(ids, weights=offset, minlength=num_groups) / num_valid
        deviation = np.where(valid, offset - mean_offset[ids], 0.0)
        m2 = np.bincount(ids, weights=deviation * deviation, minlength=num_groups)
        mean = reference + mean_offset
        var = m2 / num_valid
    null = (num_valid == 0) | saw_null
    return [
        pa.array(count.astype(np.int64)),
        pa.array(mean, mask=null),
        pa.array(var, mask=null),
    ]


def std_spec(ddof: int) -> ArrowAggSpec:
    """State ``(count, mean, population variance)`` per group, computed from
    raw rows by ``_std_grouped_raw`` and merged by ``_merge_std``, both
    without the precision loss of sum / sum-of-squares."""
    return ArrowAggSpec(
        components=("count", "mean", "var"),
        prep=lambda agg_index, source_col, block: block.append_column(
            f"__d{agg_index}_std", _std_input(block[source_col])
        ),
        raw_agg_specs=_no_kernels,
        grouped_raw=_std_grouped_raw,
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
        return [
            concat_lists_by_group(table[component_cols[0]], groups.ids, groups.count)
        ]

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
) -> ArrowAggSpec:
    """State shared by ValueCounter and TopKUnique: per group, its distinct
    values and their counts (Spark's ``mode`` state), as two list columns.
    Merging adds up the counts of equal values.

    ``prepare(column) -> column`` normalizes the values (e.g. NaN -> null);
    ``keep_value(column) -> bool mask`` picks the values to count.
    """

    def _prepped(agg_index: int) -> str:
        return f"__d{agg_index}_vc"

    def _grouped_raw(table, groups, agg_index, source_col, options):
        column = table[_prepped(agg_index)]
        keep = keep_value(column)
        return _count_values_by_group(
            column.filter(pa.array(keep)), groups.ids[keep], groups.count
        )

    def _grouped_merge(table, groups, component_cols, options):
        (values, counts), ids = flatten_lists(
            [table[component_cols[0]], table[component_cols[1]]], groups.ids
        )
        return _count_values_by_group(values, ids, groups.count, weights=counts)

    def _raw_to_partial(block, agg_index, source_col, options):
        column = block[_prepped(agg_index)]
        keep = keep_value(column)
        return [
            _singleton_lists(column, keep),
            _singleton_lists(pa.array(np.ones(len(column), dtype=np.int64)), keep),
        ]

    return ArrowAggSpec(
        components=("values", "counts"),
        prep=lambda agg_index, source_col, block: block.append_column(
            _prepped(agg_index), prepare(_widen_float16(_decode(block[source_col])))
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


def _starts(sizes: np.ndarray) -> np.ndarray:
    """Where each group begins when groups of ``sizes`` are laid end to end."""
    starts = np.zeros(len(sizes), dtype=np.int64)
    np.cumsum(sizes[:-1], out=starts[1:])
    return starts


# Groups at least this large pick their quantile with np.partition (linear
# time) one group at a time; smaller ones are sorted together in one pass.
# The per-group Python loop then runs at most rows / 1024 times.
_PARTITION_MIN_GROUP_SIZE = 1024


def _values_at_ranks(
    values: np.ndarray,
    sizes: np.ndarray,
    lower: np.ndarray,
    upper: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """For each group g, its ``lower[g]``-th and ``upper[g]``-th smallest value
    (0-based ranks within the group). ``values`` holds the groups' values back
    to back, ``sizes[g]`` of them for group g. Empty groups get NaN.

    Sorting every value costs O(n log n), but only two ranks per group are
    needed. Each large group selects them with ``np.partition``, which is
    linear. Small groups are sorted together by (group, value) in one Arrow
    sort, so there's no Python loop over many groups.
    """
    num_groups = len(sizes)
    starts = _starts(sizes)
    low = np.full(num_groups, np.nan)
    high = np.full(num_groups, np.nan)
    large = sizes >= _PARTITION_MIN_GROUP_SIZE
    for group in np.flatnonzero(large):
        lo, hi = int(lower[group]), int(upper[group])
        start = starts[group]
        segment = np.partition(values[start : start + sizes[group]], [lo, hi])
        low[group], high[group] = segment[lo], segment[hi]

    small = (sizes > 0) & ~large
    if small.any():
        group_of_value = np.repeat(np.arange(num_groups), sizes)
        if large.any():
            keep = ~large[group_of_value]
            small_values, small_groups = values[keep], group_of_value[keep]
        else:
            small_values, small_groups = values, group_of_value
        order = pc.sort_indices(
            pa.table(
                {"group": _narrow_ids(small_groups, num_groups), "value": small_values}
            ),
            sort_keys=[("group", "ascending"), ("value", "ascending")],
        ).to_numpy()
        ordered = small_values[order]
        small_starts = _starts(np.where(large, 0, sizes))
        index = np.flatnonzero(small)
        low[index] = ordered[small_starts[index] + lower[index]]
        high[index] = ordered[small_starts[index] + upper[index]]
    return low, high


def _finalize_quantile(q: float, ignore_nulls: bool) -> Callable:
    if not 0 <= q <= 1:
        # pc.quantile's check; a negative q would index from the end.
        raise ValueError(f"Quantile must be between 0 and 1, got {q}")

    def _finalize(merged: "pa.Table", component_cols: Tuple[str, ...]):
        values_col, nulls_col = component_cols
        # One list per group, so the flattened values are already grouped.
        # Large integers (beyond 2^53) round to the nearest double, as in
        # pc.quantile. Rounding keeps the order, so the ranks pick the same
        # values as on the integers.
        values = _as_array(
            pc.cast(pc.list_flatten(merged[values_col]), pa.float64(), safe=False)
        )
        values = values.to_numpy(zero_copy_only=False)
        sizes = _list_lengths(merged[values_col])
        # Linear interpolation between the two ranks around q * (n - 1), as
        # pc.quantile and numpy's default method do.
        position = q * np.maximum(sizes - 1, 0)
        lower, upper = np.floor(position), np.ceil(position)
        low_value, high_value = _values_at_ranks(
            values, sizes, lower.astype(np.int64), upper.astype(np.int64)
        )
        # Linear interpolation written as pc.quantile computes it, so results
        # match the Python engine bit for bit (a + (b - a) * t rounds
        # differently, and turns (-inf, 1) into NaN instead of -inf).
        fraction = position - lower
        with np.errstate(invalid="ignore"):
            result = (1 - fraction) * low_value + fraction * high_value
        # Equal neighbors, e.g. a rank that lands exactly on a value, need no
        # interpolation: inf * 0 would give NaN.
        result = np.where(low_value == high_value, low_value, result)
        null = sizes == 0
        if not ignore_nulls:
            nulls = _as_array(merged[nulls_col]).to_numpy(zero_copy_only=False)
            null |= nulls > 0
        return pa.array(result, mask=null)

    return _finalize


def quantile_spec(q: float, ignore_nulls: bool) -> ArrowAggSpec:
    """Exact quantile with linear interpolation, like ``pc.quantile``. The
    state is each group's list of values (DataFusion's ``percentile_cont``)
    plus a null count; merging concatenates the lists. Null and NaN are
    skipped; a null makes the result null unless nulls are ignored.

    Value counts (Spark's ``percentile``) would compress repeated values, but
    on mostly-distinct values (e.g. prices) hashing every (group, value) pair
    costs far more than gathering lists."""

    def _null_count(column, ids, num_groups) -> "pa.Array":
        is_null = _as_array(pc.is_null(column)).to_numpy(zero_copy_only=False)
        return pa.array(
            np.bincount(ids, weights=is_null, minlength=num_groups).astype(np.int64)
        )

    def _grouped_raw(table, groups, agg_index, source_col, options):
        column = _decode(table[source_col])
        keep = _is_valid_number(column)
        return lists_by_group(
            [column.filter(pa.array(keep))], groups.ids[keep], groups.count
        ) + [_null_count(column, groups.ids, groups.count)]

    def _grouped_merge(table, groups, component_cols, options):
        values_col, nulls_col = component_cols
        nulls = _as_array(table[nulls_col]).to_numpy(zero_copy_only=False)
        return [concat_lists_by_group(table[values_col], groups.ids, groups.count)] + [
            pa.array(
                np.bincount(groups.ids, weights=nulls, minlength=groups.count).astype(
                    np.int64
                )
            )
        ]

    def _raw_to_partial(block, agg_index, source_col, options):
        column = _decode(block[source_col])
        return [
            _singleton_lists(column, _is_valid_number(column)),
            pc.cast(pc.is_null(column), pa.int64()),
        ]

    return ArrowAggSpec(
        components=("values", "nulls"),
        raw_agg_specs=_no_kernels,
        grouped_raw=_grouped_raw,
        grouped_merge=_grouped_merge,
        finalize=_finalize_quantile(q, ignore_nulls),
        raw_to_partial=_raw_to_partial,
        supports_type=_is_numeric,
    )
