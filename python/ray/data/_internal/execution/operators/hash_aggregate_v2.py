from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    BlockTransformer,
    ReduceFn,
)
from ray.data.block import Block, BlockAccessor

if TYPE_CHECKING:
    from ray.data.aggregate import AggregateFn

# Scalar aggs decompose into mergeable scalar partials, so they run two-phase:
# a map-side combiner emits partial components, and the reduce merges them.
_SCALAR_AGG_KINDS = frozenset(
    {"sum", "count", "min", "max", "mean", "std", "missing_pct", "zero_pct"}
)
# Collection-valued aggs (distinct set / value list) are not scalar-mergeable,
# so a query made entirely of them runs single-phase instead: the map only
# prunes + partitions the raw rows, and the reduce does one full group-by
# (correct because the hash shuffle co-locates every key in a single reducer).
_LIST_AGG_KINDS = frozenset({"unique", "count_distinct", "list"})
# (kind, output_name, target_col, ignore_nulls, ddof); ddof is used only by std.
_AggMeta = Tuple[str, str, Optional[str], bool, Optional[int]]


def _component_names(i: int, kind: str) -> Tuple[str, ...]:
    """Names of the mergeable partial-component columns for aggregation ``i``.

    Single source of truth shared by the map (which produces these columns) and
    the reduce (which merges then finalizes them), so the two never drift.  The
    ``i`` index keeps components distinct even when two aggregations read the
    same source column (e.g. ``Mean`` and ``Std`` both need ``sum``).
    """
    if kind == "count":
        return (f"__agg{i}_cnt",)
    if kind == "sum":
        return (f"__agg{i}_sum",)
    if kind in ("min", "max"):
        return (f"__agg{i}_mm",)
    if kind == "mean":
        return (f"__agg{i}_sum", f"__agg{i}_cnt")
    if kind == "std":
        return (f"__agg{i}_sum", f"__agg{i}_cnt", f"__agg{i}_sq")
    # missing_pct / zero_pct: numerator + denominator
    return (f"__agg{i}_num", f"__agg{i}_den")


def _arrow_agg_meta(
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[List[_AggMeta]]:
    """Driver-side: per-agg meta for the vectorized path, or None if any
    aggregation isn't Arrow-vectorizable (caller falls back to Python)."""
    from ray.data.aggregate import (
        AsList,
        Count,
        CountDistinct,
        Max,
        Mean,
        Min,
        MissingValuePercentage,
        Std,
        Sum,
        Unique,
        ZeroPercentage,
    )

    # Resolve the kind by exact class, not get_agg_name() (which returns the
    # user alias for aliased aggs) and not isinstance (CountDistinct subclasses
    # Unique).  Only these built-in classes have a vectorized implementation.
    kind_by_type = {
        Sum: "sum",
        Count: "count",
        Min: "min",
        Max: "max",
        Mean: "mean",
        Std: "std",
        MissingValuePercentage: "missing_pct",
        ZeroPercentage: "zero_pct",
        AsList: "list",
        Unique: "unique",
        CountDistinct: "count_distinct",
    }

    meta: List[_AggMeta] = []
    for agg in aggregation_fns:
        kind = kind_by_type.get(type(agg))
        if kind is None:
            return None
        # Only the plain (scalar-column) forms of the collection aggs vectorize.
        if (
            kind in ("unique", "count_distinct")
            and getattr(agg, "_list_encoding_mode", None) is not None
        ):
            return None  # list-column encode (flatten/hash) -> Python
        if kind == "list" and agg._ignore_nulls:
            return None  # AsList(ignore_nulls=True): the list kernel keeps nulls
        meta.append(
            (
                kind,
                agg.name,
                agg.get_target_column(),
                agg._ignore_nulls,
                getattr(agg, "_ddof", None),
            )
        )
    # Scalar and collection aggs have different execution shapes (two-phase vs
    # single-phase); don't vectorize a query that mixes them.
    kinds = {m[0] for m in meta}
    if kinds & _LIST_AGG_KINDS and kinds & _SCALAR_AGG_KINDS:
        return None
    return meta


def _make_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> BlockTransformer:
    """Map-side partial aggregation applied to each block before shuffling.

    Uses the Arrow-vectorized combiner when every aggregation is vectorizable,
    otherwise the general (row-iterating) fallback engine.
    """
    return _make_vectorized_aggregating_transformer(
        key_columns, aggregation_fns
    ) or _fallback_aggregating_transformer(key_columns, aggregation_fns)


def _make_aggregating_reduce_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> ReduceFn:
    """Reduce-side merge + finalize of the map-side partial aggregates.

    Uses the Arrow-vectorized reducer when every aggregation is vectorizable,
    otherwise the general (row-iterating) fallback engine.
    """
    return _make_vectorized_aggregating_reduce_fn(
        key_columns, aggregation_fns
    ) or _fallback_aggregating_reduce_fn(key_columns, aggregation_fns)


def _make_vectorized_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[BlockTransformer]:
    """Arrow-vectorized map-side combiner, or None if any aggregation isn't
    Arrow-vectorizable (caller falls back to the Python engine)."""
    meta = _arrow_agg_meta(aggregation_fns)
    if meta is None:
        return None

    keys = list(key_columns)
    aggs = aggregation_fns  # captured only for schema validation (stable classes)

    if all(m[0] in _LIST_AGG_KINDS for m in meta):
        # Single-phase: the map only validates and prunes to keys + target
        # columns; ShuffleMapOp partitions the raw rows and the reduce does the
        # full distinct/list aggregation in one pass (see module note).
        keep = list(dict.fromkeys(keys + [m[2] for m in meta if m[2] is not None]))

        def _prune_transform(block: Block) -> Block:
            block_schema = BlockAccessor.for_block(block).schema()
            for agg in aggs:
                agg._validate(block_schema)
            if block.num_rows == 0:
                return block  # nothing to prune; skip the (possibly failing) select
            return block.select(keep)

        return _prune_transform

    # Columns the vectorized specs read.  A fully-empty map task can hand us a
    # schema-less block (e.g. an upstream filter that dropped every row); there
    # is nothing to aggregate, so such a block is returned unchanged below.
    needed = list(keys) + [m[2] for m in meta if m[2] is not None]

    def _arrow_transform(block: Block) -> Block:
        block_schema = BlockAccessor.for_block(block).schema()
        for agg in aggs:
            agg._validate(block_schema)

        if block.num_rows == 0 and not all(c in block.schema.names for c in needed):
            return block

        # Some aggregations aggregate a derived column: col**2 for std, and a
        # null/zero indicator for the percentages.  Append those first.
        for i, (kind, _out, col, _skip, _ddof) in enumerate(meta):
            if kind == "std":
                block = block.append_column(
                    f"__d{i}_sq", pc.multiply(block[col], block[col])
                )
            elif kind == "missing_pct":
                block = block.append_column(
                    f"__d{i}_miss",
                    pc.cast(pc.is_null(block[col], nan_is_null=True), pa.int64()),
                )
            elif kind == "zero_pct":
                block = block.append_column(
                    f"__d{i}_zero", pc.cast(pc.equal(block[col], 0), pa.int64())
                )

        # Build mergeable partial components per group.  min_count=1 makes an
        # all-null group yield null (not 0), matching the Python path.
        specs: List[tuple] = []
        names: List[str] = []
        for i, (kind, _out, col, skip, _ddof) in enumerate(meta):
            sopts = pc.ScalarAggregateOptions(skip_nulls=skip, min_count=1)
            copts = pc.CountOptions(mode="only_valid" if skip else "all")
            cnt0 = pc.ScalarAggregateOptions(min_count=0)
            names += _component_names(i, kind)
            if kind == "count":
                specs.append(
                    ([], "count_all") if col is None else (col, "count", copts)
                )
            elif kind == "sum":
                specs.append((col, "sum", sopts))
            elif kind in ("min", "max"):
                specs.append((col, kind, sopts))
            elif kind == "mean":
                specs += [(col, "sum", sopts), (col, "count", copts)]
            elif kind == "std":
                specs += [
                    (col, "sum", sopts),
                    (col, "count", copts),
                    (f"__d{i}_sq", "sum", sopts),
                ]
            elif kind == "missing_pct":
                # numerator = #(null or nan), denominator = #rows
                specs += [(f"__d{i}_miss", "sum", cnt0), ([], "count_all")]
            else:  # zero_pct: numerator = #zeros, denom = #non-null (or #rows)
                den = (col, "count", copts) if skip else ([], "count_all")
                specs += [(f"__d{i}_zero", "sum", cnt0), den]

        # use_threads=False: each shuffle task is allocated a single CPU, so
        # per-task multithreading would oversubscribe the cluster; parallelism
        # comes from running many map tasks concurrently.  (It also avoids an
        # Acero thread pool being torn down on max_calls-limited workers.)
        out = block.group_by(keys, use_threads=False).aggregate(specs)
        return out.rename_columns(keys + names)

    return _arrow_transform


def _make_vectorized_aggregating_reduce_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[ReduceFn]:
    """Arrow-vectorized reduce: merge the map-side partial components across
    shards, then finalize into output columns.  Returns None if any aggregation
    isn't Arrow-vectorizable (caller falls back to the Python engine).

    For an all-collection query this instead runs the single full group-by over
    the raw (pruned) rows the map partitioned here."""
    meta = _arrow_agg_meta(aggregation_fns)
    if meta is None:
        return None

    keys = list(key_columns)
    all_list = all(m[0] in _LIST_AGG_KINDS for m in meta)

    def _arrow_reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[Block]:
        # Drop empty shards: a fully-empty map task emits a 0-row (possibly
        # schema-less) shard that must not perturb the concat/group-by below.
        tables = [t for t in tables_by_input[0] if t.num_rows > 0]
        if not tables:
            return
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

        if all_list and not keys:
            # Global: a single group over every row.  The distinct/list hash
            # kernels aren't available with empty group keys, so compute each
            # column directly.
            cols = {}
            for i, (kind, out, col, skip, _ddof) in enumerate(meta):
                arr = combined[col].combine_chunks()
                if skip:
                    arr = arr.drop_null()
                if kind == "count_distinct":
                    cols[out] = pa.array([len(pc.unique(arr))], pa.int64())
                elif kind == "unique":
                    cols[out] = pa.array([pc.unique(arr).to_pylist()])
                else:  # list (AsList)
                    cols[out] = pa.array([arr.to_pylist()])
            yield pa.table(cols)
            return

        if all_list:
            # Single-phase: one full group-by with the distinct/list kernels.
            # mode "only_valid"/"all" carries ignore_nulls into the kernel.
            specs: List[tuple] = []
            names: List[str] = []
            for i, (kind, _out, col, skip, _ddof) in enumerate(meta):
                mode = "only_valid" if skip else "all"
                names.append(f"__agg{i}")
                if kind == "count_distinct":
                    specs.append((col, "count_distinct", pc.CountOptions(mode=mode)))
                elif kind == "unique":
                    specs.append((col, "distinct", pc.CountOptions(mode=mode)))
                else:  # list (AsList, ignore_nulls=False -> keeps nulls)
                    specs.append((col, "list"))
            merged = combined.group_by(keys, use_threads=False).aggregate(specs)
            merged = merged.rename_columns(keys + names)
            cols = {k: merged[k] for k in keys}
            for i, (kind, out, _col, _skip, _ddof) in enumerate(meta):
                col = merged[f"__agg{i}"]
                # count_distinct returns the count; unique/list return the list.
                cols[out] = (
                    pc.cast(col, pa.int64()) if kind == "count_distinct" else col
                )
            yield pa.table(cols)
            return

        # Merge partial components across shards (sum sums/counts, min mins, ...).
        specs: List[tuple] = []
        names: List[str] = []
        for i, (kind, _out, _col, skip, _ddof) in enumerate(meta):
            sopts = pc.ScalarAggregateOptions(skip_nulls=skip, min_count=1)
            cnt_opts = pc.ScalarAggregateOptions(min_count=0)
            comp = _component_names(i, kind)
            names += comp
            if kind in ("min", "max"):
                (mm,) = comp
                specs.append((mm, kind, sopts))
            elif kind == "count":
                (cnt,) = comp
                specs.append((cnt, "sum", cnt_opts))
            elif kind == "sum":
                (s,) = comp
                specs.append((s, "sum", sopts))
            elif kind == "mean":
                s, cnt = comp
                specs += [(s, "sum", sopts), (cnt, "sum", cnt_opts)]
            elif kind == "std":
                s, cnt, sq = comp
                specs += [
                    (s, "sum", sopts),
                    (cnt, "sum", cnt_opts),
                    (sq, "sum", sopts),
                ]
            else:  # missing_pct / zero_pct
                num, den = comp
                specs += [(num, "sum", cnt_opts), (den, "sum", cnt_opts)]

        # Single-threaded: one CPU per reduce task (see map note above).
        merged = combined.group_by(keys, use_threads=False).aggregate(specs)
        merged = merged.rename_columns(keys + names)

        # Finalize: turn merged partials into output columns.
        nan = pa.scalar(float("nan"), pa.float64())
        null_f = pa.scalar(None, pa.float64())
        one = pa.scalar(1.0)
        cols = {k: merged[k] for k in keys}
        for i, (kind, out, _col, _skip, ddof) in enumerate(meta):
            comp = _component_names(i, kind)
            if kind == "count":
                (cnt,) = comp
                cols[out] = pc.cast(merged[cnt], pa.int64())
            elif kind == "sum":
                (s,) = comp
                cols[out] = merged[s]
            elif kind in ("min", "max"):
                (mm,) = comp
                cols[out] = merged[mm]
            elif kind == "mean":  # sum / count (count == 0 -> null)
                s, cnt = comp
                denom = pc.cast(merged[cnt], pa.float64())
                denom = pc.if_else(pc.equal(denom, 0.0), null_f, denom)
                cols[out] = pc.divide(pc.cast(merged[s], pa.float64()), denom)
            elif kind == "std":
                # Two-pass variance from mergeable moments; NaN when count-ddof<=0
                # (matches the Welford fallback within float tolerance).
                s_n, cnt_n, sq_n = comp
                cnt = pc.cast(merged[cnt_n], pa.float64())
                s = pc.cast(merged[s_n], pa.float64())
                sq = pc.cast(merged[sq_n], pa.float64())
                denom = pc.subtract(cnt, float(ddof))
                safe_cnt = pc.if_else(pc.greater(cnt, 0.0), cnt, one)
                safe_den = pc.if_else(pc.greater(denom, 0.0), denom, one)
                m2 = pc.subtract(sq, pc.divide(pc.multiply(s, s), safe_cnt))
                std = pc.sqrt(pc.divide(m2, safe_den))
                # count == 0 (no non-null values) -> None; 0 < count <= ddof -> NaN;
                # else the std. Matches the Python path's null-vs-NaN distinction.
                std_or_nan = pc.if_else(pc.greater(denom, 0.0), std, nan)
                cols[out] = pc.if_else(pc.equal(cnt, 0.0), null_f, std_or_nan)
            else:  # missing_pct / zero_pct: num / den * 100 (den == 0 -> null)
                num_n, den_n = comp
                num = pc.cast(merged[num_n], pa.float64())
                den = pc.cast(merged[den_n], pa.float64())
                safe_den = pc.if_else(pc.greater(den, 0.0), den, one)
                pct = pc.multiply(pc.divide(num, safe_den), 100.0)
                cols[out] = pc.if_else(pc.equal(den, 0.0), null_f, pct)

        yield pa.table(cols)

    return _arrow_reduce


def _fallback_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> BlockTransformer:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    sort_key = SortKey(key=list(key_columns), descending=False)

    def _transform(block: Block) -> Block:
        from ray.data._internal.planner.exchange.aggregate_task_spec import (
            SortAggregateTaskSpec,
        )

        # TODO unify block schemas to avoid validating every block.
        block_schema = BlockAccessor.for_block(block).schema()
        for agg_fn in aggregation_fns:
            agg_fn._validate(block_schema)

        # Project down to only the key + aggregation-input columns.
        pruned_block = SortAggregateTaskSpec._prune_unused_columns(
            block, sort_key, aggregation_fns
        )

        # `_aggregate` assumes the block is sorted by key; skip when global.
        if sort_key.get_columns():
            target_block = BlockAccessor.for_block(pruned_block).sort(sort_key)
        else:
            target_block = pruned_block

        return BlockAccessor.for_block(target_block)._aggregate(
            sort_key, aggregation_fns
        )

    return _transform


def _fallback_aggregating_reduce_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> ReduceFn:
    """Reduce-side merge + finalize of the map-side partial aggregates."""
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    sort_key = SortKey(key=list(key_columns), descending=False)

    def _reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[Block]:
        # Aggregation is single-input, so there is exactly one shard list.
        tables = tables_by_input[0]
        if not tables:
            return
        combined_block, _ = BlockAccessor.for_block(
            tables[0]
        )._combine_aggregated_blocks(
            list(tables),
            sort_key=sort_key,
            aggs=aggregation_fns,
            finalize=True,
        )
        yield combined_block

    return _reduce
