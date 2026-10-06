from typing import Callable, List, NamedTuple, Optional, Tuple, Union

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
    # Integer "sum" inputs are accumulated in decimal128. PyArrow's sum kernel
    # otherwise wraps in int64. Non-integer columns are left alone.
    widen_integers: bool = False


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


# 38 decimal digits covers any sum of int64/uint64 values that can exist in
# memory: overflowing decimal128 would take on the order of 10**19 maximum-sized
# inputs. PyArrow's native sum kernel accumulates integers in int64 and wraps.
_INTEGER_SUM_TYPE = pa.decimal128(38, 0)
_INT64_MIN = -1 << 63
_INT64_MAX = (1 << 63) - 1
_INTEGER_SUM_METADATA_PREFIX = b"ray:integer_sum:"


def _integer_value_type(typ: "pa.DataType") -> "pa.DataType":
    if pa.types.is_dictionary(typ):
        return typ.value_type
    return typ


def integer_sum_type(typ: "pa.DataType") -> Optional["pa.DataType"]:
    """Return the native integer sum output type, or None for other inputs."""
    typ = _integer_value_type(typ)
    if pa.types.is_unsigned_integer(typ):
        return pa.uint64()
    if pa.types.is_signed_integer(typ):
        return pa.int64()
    return None


def integer_sum_metadata(column_name: str, input_type: "pa.DataType") -> dict:
    """Record which decimal accumulator came from an integer input.

    Arrow aggregation drops field metadata. Carry this provenance on the
    partial table's schema so the reducer can distinguish widened integers
    from original decimal columns, including scale-zero decimal columns.
    """
    output_type = integer_sum_type(input_type)
    if output_type is None:
        return {}
    key = _INTEGER_SUM_METADATA_PREFIX + column_name.encode()
    return {key: str(output_type).encode()}


def cast_column_for_exact_sum(column: Union["pa.Array", "pa.ChunkedArray"]):
    """Cast an integer column to decimal128 so ``sum`` cannot wrap.

    Returns ``None`` when ``column`` is not an integer type. The caller then
    uses PyArrow's sum kernel unchanged (floats, decimals, booleans).
    """
    if integer_sum_type(column.type) is None:
        return None
    return pc.cast(column, _INTEGER_SUM_TYPE)


def _native_integer_sum_is_safe(column: Union["pa.Array", "pa.ChunkedArray"]) -> bool:
    """Bound every possible partial integer sum before using a native kernel."""
    output_type = integer_sum_type(column.type)
    if output_type is not None and not pa.types.is_dictionary(column.type):
        # Bound every possible partial sum, including intermediate sums before
        # cancellation. Ordinary small integers can use the native fast kernel.
        bounds = pc.min_max(column).as_py()
        lower, upper = (
            (0, (1 << 64) - 1)
            if pa.types.is_unsigned_integer(output_type)
            else (_INT64_MIN, _INT64_MAX)
        )
        if bounds["min"] is None or (
            bounds["min"] * len(column) >= lower
            and bounds["max"] * len(column) <= upper
        ):
            return True
    return False


def sum_array(column: Union["pa.Array", "pa.ChunkedArray"], *, skip_nulls: bool):
    """Sum integers exactly, widening only when native accumulation may overflow."""
    if _native_integer_sum_is_safe(column):
        return pc.sum(column, skip_nulls=skip_nulls)
    widened = cast_column_for_exact_sum(column)
    return pc.sum(column if widened is None else widened, skip_nulls=skip_nulls)


def retarget_sum_column(block: "pa.Table", column_name: Optional[str], agg_index: int):
    """Append a decimal128 copy when an integer sum input may overflow.

    The original column is unchanged, so another aggregation on the same block
    still sees the source type. Returns ``(block, column_name)`` when the
    column is missing or not an integer.
    """
    if column_name is None or column_name not in block.column_names:
        return block, column_name
    if _native_integer_sum_is_safe(block[column_name]):
        return block, column_name
    casted = cast_column_for_exact_sum(block[column_name])
    if casted is None:
        return block, column_name
    side = f"__d{agg_index}_sumsrc"
    while side in block.column_names:
        side += "_"
    return block.append_column(side, casted), side


def widen_integer_sum_partials(block: "pa.Table") -> "pa.Table":
    """Normalize integer partials before concat and reduce-side summation.

    A map can safely emit native integer partials while another map emits
    decimals. Widen only the marked accumulators, keeping decimal source
    columns and other aggregation components unchanged.
    """
    for key in block.schema.metadata or {}:
        if not key.startswith(_INTEGER_SUM_METADATA_PREFIX):
            continue
        name = key[len(_INTEGER_SUM_METADATA_PREFIX) :].decode()
        widened = cast_column_for_exact_sum(block[name])
        if widened is not None:
            block = block.set_column(block.schema.get_field_index(name), name, widened)
    return block


def _finalize_sum(merged: "pa.Table", component_cols: Tuple[str, ...]):
    col = merged[component_cols[0]]
    # Reduced integer sums are decimal128. Cast back to the source's native
    # signed/unsigned sum type when possible; retain decimals on overflow.
    metadata = merged.schema.metadata or {}
    output_type = metadata.get(
        _INTEGER_SUM_METADATA_PREFIX + component_cols[0].encode()
    )
    if output_type is None:
        return col
    try:
        return pc.cast(col, pa.type_for_alias(output_type.decode()), safe=True)
    except pa.ArrowInvalid:
        return col


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
        finalize=_finalize_sum,
        widen_integers=True,
    )


def count_spec() -> ArrowAggSpec:
    return ArrowAggSpec(
        components=("count",),
        # global Count() has no target column -> count_all (count rows).
        raw_agg_specs=lambda agg_index, source_col, options: [
            (
                ([], "count_all")
                if source_col is None
                else (source_col, "count", options.count_opts)
            )
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
        widen_integers=True,
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
            (
                (source_col, "count", options.count_opts)
                if options.ignore_nulls
                else ([], "count_all")
            ),
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
