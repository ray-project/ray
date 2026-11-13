import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.types as pa_types

from ray.data._internal.arrow_aggregation import ArrowAggSpec, arrow_agg_options
from ray.data._internal.arrow_ops.transform_pyarrow import concat
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    BlockTransformer,
    ReduceFn,
)
from ray.data.block import Block, BlockAccessor

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import TaskContext
    from ray.data.aggregate import AggregateFn, AggregateFnV2


class PartialAggMode(str, enum.Enum):
    """Whether the map side of a hash aggregation pre-aggregates its input."""

    # Always pre-aggregate (the classic two-phase plan).
    ALWAYS = "always"
    # Never pre-aggregate: ship column-pruned rows, aggregate once on the reducer.
    NEVER = "never"
    # Decide per map task by counting distinct keys in a prefix of its input.
    PROBE = "probe"


@dataclass(frozen=True)
class PartialAggPolicy:
    """Map-side partial aggregation policy, resolved at planning time.

    Pre-aggregating pays off only when it collapses many rows per key. With
    nearly unique keys it costs a full hash aggregation (and its memory) on the
    mapper, shrinks nothing, and can even grow the shuffled bytes (e.g. Mean
    ships a sum and a count per row). Under ``PROBE`` a map task groups its
    first ``probe_rows`` rows by key and skips partial aggregation when
    distinct keys / probed rows exceeds ``unique_ratio_threshold``.
    """

    mode: PartialAggMode = PartialAggMode.PROBE
    probe_rows: int = 100_000
    unique_ratio_threshold: float = 0.8


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


def _raw_rows_to_partial_state(
    table: pa.Table,
    keys: List[str],
    aggs: Tuple["AggregateFnV2", ...],
    specs: List[ArrowAggSpec],
) -> pa.Table:
    """Turns raw rows into the partial-state layout ``_aggregate_raw_rows``
    produces, treating every row as its own group, so they can be merged with
    real partials in a single group_by. Uses each spec's ``raw_to_state``
    projection (no hashing); falls back to ``_aggregate_raw_rows`` if any spec
    lacks one."""
    if any(spec.raw_to_state is None for spec in specs):
        partials, _ = _aggregate_raw_rows(table, keys, aggs, specs)
        return partials

    # Cast each state column to the type the map-side group_by would produce
    # (e.g. int32 values -> int64 sums), so it concatenates with real partials.
    target_schema = _aggregate_raw_rows(table.slice(0, 0), keys, aggs, specs)[0].schema

    columns = {k: table[k] for k in keys}
    for i, (agg, spec) in enumerate(zip(aggs, specs)):
        col = agg.get_target_column()
        opts = arrow_agg_options(agg._ignore_nulls)
        if spec.prep is not None:
            table = spec.prep(i, col, table)
        raw_to_state = spec.raw_to_state
        assert raw_to_state is not None  # checked above
        states = raw_to_state(table, i, col, opts)
        for component, state in zip(spec.components, states):
            name = f"__agg{i}_{component}"
            columns[name] = pc.cast(state, target_schema.field(name).type)
    return pa.table(columns)


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


# A one-phase aggregation task peaks at ~3-7x the bytes it outputs (measured).
# Its input blocks are read zero-copy from the object store, so the heap is
# Arrow's group_by working set (hash table, per-group accumulators, result
# columns), which grows with the number of groups, i.e. with the output, not
# the input. 8x leaves headroom. The generic map estimate (one output block)
# under-requests it, overpacking tasks per node and risking OOM kills.
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
            table = BlockAccessor.for_block(block).to_arrow()
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
    policy: Optional[PartialAggPolicy] = None,
) -> BlockTransformer:
    """Map-side block transformer for a hash aggregation.

    ``policy`` only applies to the vectorized engine; ``None`` means always
    pre-aggregate. The Python fallback always pre-aggregates: its partial
    columns are named after the aggregations, so a reducer couldn't tell them
    apart from raw rows.
    """
    return _make_vectorized_aggregating_transformer(
        key_columns, aggregation_fns, policy
    ) or _fallback_aggregating_transformer(key_columns, aggregation_fns)


def _make_aggregating_reduce_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> ReduceFn:
    return _make_vectorized_aggregating_reduce_fn(
        key_columns, aggregation_fns
    ) or _fallback_aggregating_reduce_fn(key_columns, aggregation_fns)


def _should_skip_partial_aggregation(
    block: pa.Table, keys: List[str], policy: Optional[PartialAggPolicy]
) -> bool:
    """Whether a map task should ship ``block``'s rows without pre-aggregating."""
    if policy is None or policy.mode == PartialAggMode.ALWAYS:
        return False
    if policy.mode == PartialAggMode.NEVER:
        return True
    if block.num_rows < policy.probe_rows:
        # Small inputs are cheap to aggregate and can't blow up memory.
        return False
    # A prefix's distinct ratio is never below the whole block's, so this leans
    # toward skipping; the reducer still produces the same result either way.
    probe = block.select(keys).slice(0, policy.probe_rows)
    num_groups = probe.group_by(keys, use_threads=False).aggregate([]).num_rows
    return num_groups > policy.unique_ratio_threshold * probe.num_rows


def _make_vectorized_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
    policy: Optional[PartialAggPolicy] = None,
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
    keep = _key_and_target_columns(keys, aggs)

    if has_collection:
        # All-collection query: the map prunes to keys + source columns;
        # the reduce aggregates the raw rows directly.

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

        if _should_skip_partial_aggregation(block, keys, policy):
            # Raw rows, pruned like a collection query's; the reducer detects
            # them by the missing ``__agg0_*`` column (see _arrow_reduce).
            return block.select(keep)

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

    # Partial-state column layout (reduction queries only).
    comps: List[Tuple[str, ...]] = (
        []
        if has_collection
        else [
            tuple(f"__agg{i}_{c}" for c in spec.components)
            for i, spec in enumerate(specs)
        ]
    )
    merged_names = keys + [c for agg_comps in comps for c in agg_comps]
    # Every partial shard has this column; raw shards (from map tasks that
    # skipped partial aggregation) never do.
    partial_marker = comps[0][0] if comps else None

    def _merge(partials: List[pa.Table]) -> pa.Table:
        # Built per call: PyArrow aggregate options aren't picklable, so they
        # can't be captured in this closure (it ships to reduce tasks).
        merge_specs: List[tuple] = []
        for agg, spec, out_cols in zip(aggs, specs, comps):
            assert spec.merge_specs is not None  # reduction specs define it
            opts = arrow_agg_options(agg._ignore_nulls)
            merge_specs += spec.merge_specs(out_cols, opts)
        combined = concat(partials, promote_types=True)
        merged = combined.group_by(keys, use_threads=False).aggregate(merge_specs)
        return merged.rename_columns(merged_names)

    def _arrow_reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[Block]:
        shards = tables_by_input[0]
        tables = [t for t in shards if t.num_rows > 0]
        if not tables:
            return

        if not has_collection:
            partials = [t for t in tables if partial_marker in t.schema.names]
            raws = [t for t in tables if partial_marker not in t.schema.names]
            if not partials:
                # Every map task skipped partial aggregation: one raw pass.
                merged, _ = _aggregate_raw_rows(
                    concat(raws, promote_types=True), keys, aggs, specs
                )
            else:
                if raws:
                    partials.append(
                        _raw_rows_to_partial_state(
                            concat(raws, promote_types=True), keys, aggs, specs
                        )
                    )
                merged = _merge(partials)
            yield _finalize_aggregates(merged, keys, aggs, specs, comps)
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

        # Collection aggs: the map sent raw rows, so this gives final values.
        merged, collection_comps = _aggregate_raw_rows(combined, keys, aggs, specs)
        yield _finalize_aggregates(merged, keys, aggs, specs, collection_comps)

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
