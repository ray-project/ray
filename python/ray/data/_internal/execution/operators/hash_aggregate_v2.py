from typing import TYPE_CHECKING, Callable, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.types as pa_types

from ray.data._internal.arrow_aggregation import ArrowAggSpec, arrow_agg_options
from ray.data._internal.arrow_ops.transform_pyarrow import concat
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import (
    MapTransformCallable,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    BlockTransformer,
    ReduceFn,
)
from ray.data._internal.planner.exchange.aggregate_task_spec import (
    SortAggregateTaskSpec,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import Block, BlockAccessor, BlockType

if TYPE_CHECKING:
    from ray.data.aggregate import AggregateFn, AggregateFnV2


def _agg_specs(
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[Tuple[Tuple["AggregateFnV2", ...], List[ArrowAggSpec]]]:
    """Per-agg Arrow vectorization specs, each aggregation declares its own via
    ``AggregateFn._arrow_agg_spec``.  Returns None (caller falls back to the
    Python engine) if there are no aggregations or any one isn't vectorizable."""
    from ray.data.aggregate import AggregateFnV2

    if not aggregation_fns:
        return None
    aggs: List[AggregateFnV2] = []
    specs: List[ArrowAggSpec] = []
    for agg in aggregation_fns:
        spec = agg._arrow_agg_spec()
        if spec is None or not isinstance(agg, AggregateFnV2):
            return None
        aggs.append(agg)
        specs.append(spec)
    return tuple(aggs), specs


def _is_vectorizable(keys: List[str], specs: List[ArrowAggSpec]) -> bool:
    """Query-shape gate shared by the map and reduce builders, the two must
    decide identically (the reduce's expected input format depends on what the
    map shipped)."""
    has_collection = any(spec.collection for spec in specs)
    if has_collection and not all(spec.collection for spec in specs):
        # Mixed reduction + collection shapes need different map outputs
        # (bounded partials vs raw rows); Python handles both in one pass.
        return False
    if not keys:
        # Keyless (global) = one group: per-group vectorization buys nothing
        # (the Python path's intra-group aggregation is already vectorized).
        return False
    return True


def _has_nested_column(table: pa.Table, columns: List[Optional[str]]) -> bool:
    """The distinct/count_distinct kernels have no impl for a nested source
    column; callers delegate such a table to the Python engine."""
    return any(
        c is not None and pa_types.is_nested(table.schema.field(c).type)
        for c in columns
    )


def _aggregate_raw_rows(
    table: pa.Table,
    keys: List[str],
    aggs: Tuple["AggregateFnV2", ...],
    specs: List[ArrowAggSpec],
) -> Tuple[pa.Table, List[Tuple[str, ...]]]:
    """Group ``table``'s raw rows by ``keys`` and aggregate them via
    ``raw_agg_specs`` into each agg's ``__agg{i}_*`` component columns.
    Returns the grouped table and each agg's component column names."""
    agg_specs: List[tuple] = []
    comps: List[Tuple[str, ...]] = []
    for i, (agg, spec) in enumerate(zip(aggs, specs)):
        col = agg.get_target_column()
        opts = arrow_agg_options(agg._ignore_nulls)
        if spec.prep is not None:
            table = spec.prep(i, col, table)
        comps.append(tuple(f"__agg{i}_{c}" for c in spec.components))
        agg_specs += spec.raw_agg_specs(i, col, opts)

    out = table.group_by(keys, use_threads=False).aggregate(agg_specs)
    return out.rename_columns(keys + [c for cols in comps for c in cols]), comps


def _finalize_aggregates(
    merged: pa.Table,
    keys: List[str],
    aggs: Tuple["AggregateFnV2", ...],
    specs: List[ArrowAggSpec],
    comps: List[Tuple[str, ...]],
) -> pa.Table:
    """Finalize each agg's component columns into its output column."""
    # Duplicate output names are munged to name, name_2, name_3, ... (counting
    # against the original name).
    cols = {k: merged[k] for k in keys}
    seen: dict = {}
    for i, (agg, spec) in enumerate(zip(aggs, specs)):
        name = agg.name
        n = seen.get(name, 0)
        seen[name] = n + 1
        if n > 0:
            name = f"{name}_{n + 1}"
        cols[name] = spec.finalize(merged, comps[i])
    return pa.table(cols)


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


def _make_complete_aggregating_block_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> MapTransformCallable[Block, Block]:
    """One-phase ("complete") aggregation of a whole partition.

    Only correct if the input is hash-partitioned on a subset of
    ``key_columns``: every row of a group is then in this partition, so the
    partition is aggregated in a single pass, with no partial aggregates to
    merge.
    """

    sort_key = SortKey(key=list(key_columns), descending=False)
    aggregate = _make_vectorized_complete_aggregate_fn(
        key_columns, aggregation_fns
    ) or _fallback_complete_aggregate_fn(key_columns, aggregation_fns)

    def _aggregate_partition(
        blocks: Iterable[Block], ctx: TaskContext
    ) -> Iterable[Block]:
        # Gather the whole partition before aggregating, so each group is
        # aggregated once from all of its rows.
        tables: List[pa.Table] = []
        for block in blocks:
            table = TableBlockAccessor.try_convert_block_type(
                block, block_type=BlockType.ARROW
            )
            if table.num_rows == 0:
                continue
            block_schema = BlockAccessor.for_block(table).schema()
            for agg in aggregation_fns:
                agg._validate(block_schema)
            # Only buffer the columns the aggregation reads.
            tables.append(
                SortAggregateTaskSpec._prune_unused_columns(
                    table, sort_key, aggregation_fns
                )
            )
        if not tables:
            # Like the shuffle path, emit nothing for an empty partition.
            return
        yield aggregate(concat(tables, promote_types=True))

    return _aggregate_partition


def _make_vectorized_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[BlockTransformer]:
    """Arrow-vectorized map-side combiner, driven by each agg's own spec, or None
    if the query isn't vectorizable (caller falls back to the Python engine)."""
    unpacked = _agg_specs(aggregation_fns)
    if unpacked is None:
        return None
    aggs, specs = unpacked

    keys = list(key_columns)
    if not _is_vectorizable(keys, specs):
        return None
    has_collection = any(spec.collection for spec in specs)

    if has_collection:
        # All-collection query: the map prunes to keys + source columns;
        # the reduce aggregates the raw rows directly.
        keep = list(
            dict.fromkeys(
                keys
                + [
                    a.get_target_column()
                    for a in aggs
                    if a.get_target_column() is not None
                ]
            )
        )

        def _prune_transform(block: pa.Table) -> pa.Table:
            block_schema = BlockAccessor.for_block(block).schema()
            for agg in aggs:
                agg._validate(block_schema)
            if block.num_rows == 0:
                return block
            return block.select(keep)

        return _prune_transform

    def _arrow_transform(block: pa.Table) -> pa.Table:
        block_schema = BlockAccessor.for_block(block).schema()
        for agg in aggs:
            agg._validate(block_schema)

        if block.num_rows == 0:
            return block

        return _aggregate_raw_rows(block, keys, aggs, specs)[0]

    return _arrow_transform


def _make_vectorized_aggregating_reduce_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[ReduceFn]:
    unpacked = _agg_specs(aggregation_fns)
    if unpacked is None:
        return None
    aggs, specs = unpacked

    keys = list(key_columns)
    if not _is_vectorizable(keys, specs):
        return None
    has_collection = any(spec.collection for spec in specs)
    _fallback_map = _fallback_aggregating_transformer(key_columns, aggregation_fns)
    _fallback_reduce = _fallback_aggregating_reduce_fn(key_columns, aggregation_fns)
    src_cols = [a.get_target_column() for a in aggs] if has_collection else []

    def _arrow_reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[Block]:
        shards = tables_by_input[0]
        tables = [t for t in shards if t.num_rows > 0]
        if not tables:
            return
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

        # The map shipped raw rows for a collection query; a nested source
        # column has no distinct kernel, so delegate the partition to Python.
        if _has_nested_column(combined, src_cols):
            yield from _fallback_reduce(partition_id, [[_fallback_map(combined)]])
            return

        if has_collection:
            # Collection query: aggregate the raw rows (a group's rows are all
            # co-located here, so this yields the final value).
            merged, comps = _aggregate_raw_rows(combined, keys, aggs, specs)
        else:
            # Reduction: merge the map's partials via merge_specs.
            agg_specs: List[tuple] = []
            comps = []
            for i, (agg, spec) in enumerate(zip(aggs, specs)):
                out_cols = tuple(f"__agg{i}_{c}" for c in spec.components)
                comps.append(out_cols)
                assert spec.merge_specs is not None  # reduction specs define it
                agg_specs += spec.merge_specs(
                    out_cols, arrow_agg_options(agg._ignore_nulls)
                )
            merged = combined.group_by(keys, use_threads=False).aggregate(agg_specs)
            merged = merged.rename_columns(keys + [c for cols in comps for c in cols])

        yield _finalize_aggregates(merged, keys, aggs, specs, comps)

    return _arrow_reduce


def _make_vectorized_complete_aggregate_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[Callable[[pa.Table], Block]]:
    """Arrow-vectorized one-pass aggregation of a table that holds every row of
    each of its groups, or None if the query isn't vectorizable (caller falls
    back to the Python engine)."""
    unpacked = _agg_specs(aggregation_fns)
    if unpacked is None:
        return None
    aggs, specs = unpacked

    keys = list(key_columns)
    # Same engine choice as the shuffle path, so both plans give the same output.
    if not _is_vectorizable(keys, specs):
        return None
    has_collection = any(spec.collection for spec in specs)
    src_cols = [a.get_target_column() for a in aggs] if has_collection else []
    _fallback = _fallback_complete_aggregate_fn(key_columns, aggregation_fns)

    def _arrow_aggregate(table: pa.Table) -> Block:
        if _has_nested_column(table, src_cols):
            return _fallback(table)
        # Raw rows go straight to each group's final components; there are no
        # partials to merge.
        aggregated, comps = _aggregate_raw_rows(table, keys, aggs, specs)
        return _finalize_aggregates(aggregated, keys, aggs, specs, comps)

    return _arrow_aggregate


def _fallback_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> BlockTransformer:
    sort_key = SortKey(key=list(key_columns), descending=False)

    def _transform(block: pa.Table) -> pa.Table:
        # TODO unify block schemas to avoid validating every block.
        block_schema = BlockAccessor.for_block(block).schema()
        for agg_fn in aggregation_fns:
            agg_fn._validate(block_schema)

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


def _fallback_complete_aggregate_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Callable[[pa.Table], Block]:
    """Python-engine one-pass aggregation of a table that holds every row of
    each of its groups."""

    sort_key = SortKey(key=list(key_columns), descending=False)

    def _aggregate(table: pa.Table) -> Block:
        sorted_table = BlockAccessor.for_block(table).sort(sort_key)
        # One accumulator per group, built from all of the group's rows.
        accumulated = BlockAccessor.for_block(sorted_table)._aggregate(
            sort_key, aggregation_fns
        )
        # Each group has a single accumulator, so nothing is merged here; this
        # only finalizes them.
        finalized, _ = BlockAccessor.for_block(accumulated)._combine_aggregated_blocks(
            [accumulated],
            sort_key=sort_key,
            aggs=aggregation_fns,
            finalize=True,
        )
        return finalized

    return _aggregate
