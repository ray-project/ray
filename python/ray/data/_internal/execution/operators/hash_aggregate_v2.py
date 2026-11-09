from typing import TYPE_CHECKING, Callable, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.types as pa_types

from ray.data._internal.arrow_aggregation import ArrowAggSpec, arrow_agg_options
from ray.data._internal.arrow_ops.transform_pyarrow import concat
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    BlockTransformer,
    ReduceFn,
)
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import Block, BlockAccessor, BlockType

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import TaskContext
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


def _aggregate_raw_rows(
    table: pa.Table,
    keys: List[str],
    aggs: Tuple["AggregateFnV2", ...],
    specs: List[ArrowAggSpec],
) -> Tuple[pa.Table, List[Tuple[str, ...]]]:
    """Groups ``table``'s raw rows by ``keys`` into each agg's ``__agg{i}_*``
    columns (e.g. sum and count for Mean). Returns the table and those names."""
    agg_specs: List[tuple] = []
    comps: List[Tuple[str, ...]] = []
    names: List[str] = []
    for i, (agg, spec) in enumerate(zip(aggs, specs)):
        col = agg.get_target_column()
        opts = arrow_agg_options(agg._ignore_nulls)
        if spec.prep is not None:
            table = spec.prep(i, col, table)
        agg_comps = tuple(f"__agg{i}_{c}" for c in spec.components)
        comps.append(agg_comps)
        names.extend(agg_comps)
        agg_specs += spec.raw_agg_specs(i, col, opts)

    grouped = table.group_by(keys, use_threads=False).aggregate(agg_specs)
    grouped = grouped.rename_columns(keys + names)
    return grouped, comps


def _finalize_aggregates(
    merged: pa.Table,
    keys: List[str],
    aggs: Tuple["AggregateFnV2", ...],
    specs: List[ArrowAggSpec],
    comps: List[Tuple[str, ...]],
) -> pa.Table:
    """Turns each agg's ``__agg{i}_*`` columns into its output column."""
    # Repeated output names become name_2, name_3, ...
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


def _key_and_target_columns(
    keys: List[str], aggs: Tuple["AggregateFnV2", ...]
) -> List[str]:
    """The key columns, then the columns the aggs read, without duplicates."""
    targets = [agg.get_target_column() for agg in aggs]
    columns: List[str] = []
    for column in keys + targets:
        if column is not None and column not in columns:
            columns.append(column)
    return columns


# Peak heap of a one-phase aggregation task per byte it outputs. The task reads
# its input blocks zero-copy from the object store.
COMPLETE_AGGREGATION_PEAK_MEMORY_MULTIPLIER = 8


def _make_complete_aggregating_block_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[Callable[[Iterable[Block], "TaskContext"], Iterable[Block]]]:
    """One-pass ("complete") aggregation of a task's whole input, or None if
    the aggs aren't supported.

    Only correct when all rows of a group reach the same task, e.g. whole hash
    partitions. Only Arrow-vectorized, non-collection aggs are supported; the
    Python engine and collection aggs use more memory than the shuffle.
    """
    unpacked = _agg_specs(aggregation_fns)
    if unpacked is None:
        return None
    aggs, specs = unpacked
    keys = list(key_columns)
    if not _is_vectorizable(keys, specs) or any(spec.collection for spec in specs):
        return None
    # Buffer only the columns the aggregation needs.
    columns = _key_and_target_columns(keys, aggs)

    def _aggregate(blocks: Iterable[Block], ctx: "TaskContext") -> Iterable[Block]:
        tables: List[pa.Table] = []
        for block in blocks:
            table = TableBlockAccessor.try_convert_block_type(
                block, block_type=BlockType.ARROW
            )
            block_schema = BlockAccessor.for_block(table).schema()
            for agg in aggs:
                agg._validate(block_schema)
            if table.num_rows > 0:
                tables.append(table.select(columns))
        if not tables:
            # No rows, no groups: emit nothing, like the shuffle's reduce.
            return
        # concat copies only to promote types. Left unnamed so such a copy is
        # freed before finalizing.
        aggregated, comps = _aggregate_raw_rows(
            concat(tables, promote_types=True), keys, aggs, specs
        )
        yield _finalize_aggregates(aggregated, keys, aggs, specs, comps)

    return _aggregate


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
        keep = _key_and_target_columns(keys, aggs)

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

        partials, _ = _aggregate_raw_rows(block, keys, aggs, specs)
        return partials

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

        # The distinct/count_distinct kernels have no impl for a nested source
        # column (the map shipped raw rows); delegate the partition to Python.
        if has_collection and any(
            c is not None and pa_types.is_nested(combined.schema.field(c).type)
            for c in src_cols
        ):
            yield from _fallback_reduce(partition_id, [[_fallback_map(combined)]])
            return

        if has_collection:
            # Collection aggs: the map sent raw rows, so this gives final values.
            merged, comps = _aggregate_raw_rows(combined, keys, aggs, specs)
        else:
            # Other aggs: merge the map's partial results.
            agg_specs: List[tuple] = []
            comps = []
            names: List[str] = []
            for i, (agg, spec) in enumerate(zip(aggs, specs)):
                out_cols = tuple(f"__agg{i}_{c}" for c in spec.components)
                comps.append(out_cols)
                names.extend(out_cols)
                assert spec.merge_specs is not None  # reduction specs define it
                opts = arrow_agg_options(agg._ignore_nulls)
                agg_specs += spec.merge_specs(out_cols, opts)
            merged = combined.group_by(keys, use_threads=False).aggregate(agg_specs)
            merged = merged.rename_columns(keys + names)

        yield _finalize_aggregates(merged, keys, aggs, specs, comps)

    return _arrow_reduce


def _fallback_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> BlockTransformer:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    sort_key = SortKey(key=list(key_columns), descending=False)

    def _transform(block: pa.Table) -> pa.Table:
        from ray.data._internal.planner.exchange.aggregate_task_spec import (
            SortAggregateTaskSpec,
        )

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
