from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple

import pyarrow as pa

from ray.data._internal.arrow_aggregation import ArrowAggSpec, arrow_agg_options
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    BlockTransformer,
    ReduceFn,
)
from ray.data.block import Block, BlockAccessor

if TYPE_CHECKING:
    from ray.data.aggregate import AggregateFn


def _agg_specs(
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[List[ArrowAggSpec]]:
    """Per-agg Arrow vectorization specs -- each aggregation declares its own via
    ``AggregateFn._arrow_agg_spec``.  Returns None (caller falls back to the
    Python engine) if any aggregation isn't vectorizable, or if the query mixes
    reduction (two-phase) and collection (single-phase) aggs, which execute
    differently."""
    specs: List[ArrowAggSpec] = []
    for agg in aggregation_fns:
        spec = agg._arrow_agg_spec()
        if spec is None:
            return None
        specs.append(spec)
    if len({s.single_phase for s in specs}) > 1:
        return None
    return specs


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
    specs = _agg_specs(aggregation_fns)
    if specs is None:
        return None

    keys = list(key_columns)
    single_phase = specs[0].single_phase
    if single_phase and not keys:
        # Global collection agg: the distinct hash kernels need group keys, so we fallback to python
        return None

    if single_phase:
        # The map only validates + prunes to keys + source columns; the reduce
        # does the single grouped distinct/count_distinct pass over raw rows.
        keep = list(
            dict.fromkeys(
                keys
                + [
                    a.get_target_column()
                    for a in aggregation_fns
                    if a.get_target_column() is not None
                ]
            )
        )

        def _prune_transform(block: Block) -> Block:
            block_schema = BlockAccessor.for_block(block).schema()
            for agg in aggregation_fns:
                agg._validate(block_schema)
            if block.num_rows == 0:
                return block
            return block.select(keep)

        return _prune_transform

    def _arrow_transform(block: Block) -> Block:
        block_schema = BlockAccessor.for_block(block).schema()
        for agg in aggregation_fns:
            agg._validate(block_schema)

        if block.num_rows == 0:
            return block

        agg_specs: List[tuple] = []
        names: List[str] = []
        for i, (agg, spec) in enumerate(zip(aggregation_fns, specs)):
            col = agg.get_target_column()
            opts = arrow_agg_options(agg._ignore_nulls)
            if spec.prep is not None:
                block = spec.prep(i, col, block)
            names.extend(f"__agg{i}_{c}" for c in spec.components)
            agg_specs += spec.map_specs(i, col, opts)

        out = block.group_by(keys, use_threads=False).aggregate(agg_specs)
        return out.rename_columns(keys + names)

    return _arrow_transform


def _make_vectorized_aggregating_reduce_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> Optional[ReduceFn]:
    specs = _agg_specs(aggregation_fns)
    if specs is None:
        return None

    keys = list(key_columns)
    single_phase = specs[0].single_phase
    if single_phase and not keys:
        return None
    _fallback_map = _fallback_aggregating_transformer(key_columns, aggregation_fns)
    _fallback_reduce = _fallback_aggregating_reduce_fn(key_columns, aggregation_fns)
    # Source columns of collection aggs (only used to detect nested inputs).
    src_cols = [a.get_target_column() for a in aggregation_fns] if single_phase else []

    def _arrow_reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[Block]:
        shards = tables_by_input[0]
        tables = [t for t in shards if t.num_rows > 0]
        if not tables:
            # Empty global aggregation: delegate to the fallback for the same
            # identity row (Count->0, Sum->null, ...) it would emit.
            if shards and not keys:
                yield from _fallback_reduce(partition_id, [[_fallback_map(shards[0])]])
            return
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

        # The distinct/count_distinct kernels have no impl for a nested source
        # column (the map shipped raw rows); delegate the partition to Python.
        if single_phase and any(
            c is not None and pa.types.is_nested(combined.schema.field(c).type)
            for c in src_cols
        ):
            yield from _fallback_reduce(partition_id, [[_fallback_map(combined)]])
            return

        # Reduction: merge each agg's partial components (the map's output).
        # Collection: aggregate the raw source column in one grouped pass.
        merge_specs: List[tuple] = []
        names: List[str] = []
        comps: List[Tuple[str, ...]] = []
        for i, (agg, spec) in enumerate(zip(aggregation_fns, specs)):
            opts = arrow_agg_options(agg._ignore_nulls)
            out_cols = tuple(f"__agg{i}_{c}" for c in spec.components)
            input_cols = (agg.get_target_column(),) if single_phase else out_cols
            comps.append(out_cols)
            names += out_cols
            merge_specs += spec.merge_specs(input_cols, opts)

        merged = combined.group_by(keys, use_threads=False).aggregate(merge_specs)
        merged = merged.rename_columns(keys + names)

        # Finalize into output columns.  Duplicate output names are munged to
        # name, name_2, name_3, ... (counting against the original name).
        cols = {k: merged[k] for k in keys}
        seen: dict = {}
        for i, (agg, spec) in enumerate(zip(aggregation_fns, specs)):
            name = agg.name
            n = seen.get(name, 0)
            seen[name] = n + 1
            if n > 0:
                name = f"{name}_{n + 1}"
            cols[name] = spec.finalize(merged, comps[i])
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
