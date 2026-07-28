from typing import TYPE_CHECKING, Iterable, List, NamedTuple, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    BlockTransformer,
    ReduceFn,
)
from ray.data.aggregate import (
    Count,
    Max,
    Mean,
    Min,
    MissingValuePercentage,
    Sum,
    ZeroPercentage,
)
from ray.data.block import Block, BlockAccessor

if TYPE_CHECKING:
    from ray.data.aggregate import AggregateFn


class _AggMeta(NamedTuple):
    kind: str  # sum | count | min | max | mean | missing_pct | zero_pct
    name: str  # output column name (e.g. "sum(x)")
    target_col: Optional[str]  # source column, or None (e.g. global Count)
    ignore_nulls: bool


def _component_names(i: int, kind: str) -> Tuple[str, ...]:
    """Names of the mergeable partial-component columns for aggregation ``i``."""
    if kind == "count":
        return (f"__agg{i}_cnt",)
    if kind == "sum":
        return (f"__agg{i}_sum",)
    if kind in ("min", "max"):
        return (f"__agg{i}_mm",)
    if kind == "mean":
        return (f"__agg{i}_sum", f"__agg{i}_cnt")
    # missing_pct / zero_pct: numerator + denominator
    return (f"__agg{i}_num", f"__agg{i}_den")


def _arrow_agg_meta(
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[List[_AggMeta]]:
    # TODO (you-cheng) we can support vectorized STD with some parallel algorithm
    # see https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    kind_by_type = {
        Sum: "sum",
        Count: "count",
        Min: "min",
        Max: "max",
        Mean: "mean",
        MissingValuePercentage: "missing_pct",
        ZeroPercentage: "zero_pct",
    }

    meta: List[_AggMeta] = []
    for agg in aggregation_fns:
        kind = kind_by_type.get(type(agg))
        if kind is None:
            return None
        target_col = agg.get_target_column()
        # Only Count accepts an optional (None) target column, Count() counts
        # all rows in a group.  Every other kind needs a real column; a missing
        # target falls back to the Python engine, which raises the proper ValueError.
        if kind != "count" and not isinstance(target_col, str):
            return None
        meta.append(
            _AggMeta(
                kind=kind,
                name=agg.name,
                target_col=target_col,
                ignore_nulls=agg._ignore_nulls,
            )
        )
    return meta


def _make_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> BlockTransformer:
    return _make_vectorized_aggregating_transformer(
        key_columns, aggregation_fns
    ) or _fallback_aggregating_transformer(key_columns, aggregation_fns)


def _make_aggregating_reduce_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> ReduceFn:
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

    def _arrow_transform(block: Block) -> Block:
        block_schema = BlockAccessor.for_block(block).schema()
        for agg in aggregation_fns:
            agg._validate(block_schema)

        if block.num_rows == 0:
            return block

        specs: List[tuple] = []
        names: List[str] = []
        for i, (kind, _out, col, ignore_nulls) in enumerate(meta):
            sopts = pc.ScalarAggregateOptions(skip_nulls=ignore_nulls, min_count=1)
            copts = pc.CountOptions(mode="only_valid" if ignore_nulls else "all")
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
            elif kind == "missing_pct":
                block = block.append_column(
                    f"__d{i}_miss",
                    pc.cast(pc.is_null(block[col], nan_is_null=True), pa.int64()),
                )
                specs += [(f"__d{i}_miss", "sum", cnt0), ([], "count_all")]
            else:
                block = block.append_column(
                    f"__d{i}_zero", pc.cast(pc.equal(block[col], 0), pa.int64())
                )
                den = (col, "count", copts) if ignore_nulls else ([], "count_all")
                specs += [(f"__d{i}_zero", "sum", cnt0), den]

        out = block.group_by(keys, use_threads=False).aggregate(specs)
        return out.rename_columns(keys + names)

    return _arrow_transform


def _make_vectorized_aggregating_reduce_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[ReduceFn]:
    """Arrow-vectorized reduce: merge the map-side partial components across
    shards, then finalize into output columns.  Returns None if any aggregation
    isn't Arrow-vectorizable (caller falls back to the Python engine)."""
    meta = _arrow_agg_meta(aggregation_fns)
    if meta is None:
        return None

    keys = list(key_columns)
    _fallback_map = _fallback_aggregating_transformer(key_columns, aggregation_fns)
    _fallback_reduce = _fallback_aggregating_reduce_fn(key_columns, aggregation_fns)

    def _arrow_reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[Block]:
        shards = tables_by_input[0]
        tables = [t for t in shards if t.num_rows > 0]
        if not tables:
            # Fallback when a global aggregation over an empty input because we need to build identical empty row.
            if shards and not keys:
                yield from _fallback_reduce(partition_id, [[_fallback_map(shards[0])]])
            return
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

        # Merge partial components across shards (sum sums/counts, min mins, ...).
        specs: List[tuple] = []
        names: List[str] = []
        for i, (kind, _out, _col, ignore_nulls) in enumerate(meta):
            sopts = pc.ScalarAggregateOptions(skip_nulls=ignore_nulls, min_count=1)
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
            else:  # missing_pct / zero_pct
                num, den = comp
                specs += [(num, "sum", cnt_opts), (den, "sum", cnt_opts)]

        merged = combined.group_by(keys, use_threads=False).aggregate(specs)
        merged = merged.rename_columns(keys + names)

        # Finalize: turn merged partials into output columns.
        null_f = pa.scalar(None, pa.float64())
        one = pa.scalar(1.0)
        cols = {k: merged[k] for k in keys}
        seen: dict = {}  # duplicate output names -> munged (name, name_2, ...),
        for i, (kind, out, _col, _ignore_nulls) in enumerate(meta):
            comp = _component_names(i, kind)
            if seen.get(out, 0) > 0:
                out = f"{out}_{seen[out] + 1}"
            seen[out] = seen.get(out, 0) + 1
            if kind == "count":
                (cnt,) = comp
                cols[out] = pc.cast(merged[cnt], pa.int64())
            elif kind == "sum":
                (s,) = comp
                cols[out] = merged[s]
            elif kind in ("min", "max"):
                (mm,) = comp
                cols[out] = merged[mm]
            elif kind == "mean":
                s, cnt = comp
                denom = pc.cast(merged[cnt], pa.float64())
                denom = pc.if_else(pc.equal(denom, 0.0), null_f, denom)
                cols[out] = pc.divide(pc.cast(merged[s], pa.float64()), denom)
            else:  # missing_pct / zero_pct
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
