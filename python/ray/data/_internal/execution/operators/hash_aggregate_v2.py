"""Map and reduce functions for SHUFFLE_V2 hash aggregations.

Two engines:

* Vectorized (Arrow): used when every aggregation has an ``ArrowAggSpec`` and
  there are group keys. Specs come in two shapes:

  - Reductions (Sum, Mean, Std, Quantile, ...): the map aggregates each
    block into partial state, one row per key in ``__agg{i}_*`` columns; the
    reducer merges those rows and finalizes. A map task may instead ship raw
    rows (adaptive partial aggregation), which the reducer turns into partial
    state with ``raw_to_partial`` before merging.
  - Collections (Unique, CountDistinct): no mergeable partial, so the map
    ships raw rows and the reducer aggregates them in one pass.

  A query mixing the two shapes runs on the Python engine. So do columns of
  a type some spec can't aggregate (``supports_type``, e.g. decimal for
  Std): a map task that sees one pre-aggregates its block on the Python
  engine and tags the output (``_PYTHON_PARTIALS_MARKER``), and the reducer
  merges that partition on the Python engine too. An all-null block (Arrow
  type ``null``) says nothing about the column's type, so it ships raw and
  the reducer decides once it has seen the other shards.
* Python: everything else (custom aggregations, keyless aggregations). The
  map sorts and accumulates each group; the reducer combines accumulators.
  Map tasks may skip that when every aggregation implements
  ``AggregateFnV2.raw_to_partial``.
"""
import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.types as pa_types

from ray.data._internal.arrow_aggregation import (
    ArrowAggSpec,
    Groups,
    arrow_agg_options,
)
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
    # Decide per map task by estimating its distinct keys from a sample of rows.
    ESTIMATE = "estimate"


@dataclass(frozen=True)
class PartialAggPolicy:
    """Map-side partial aggregation policy, resolved at planning time.

    Pre-aggregating pays off only when it collapses many rows per key (low cardinality). With
    nearly unique keys it costs a full hash aggregation (and its memory) on the
    mapper, shrinks nothing, and can even grow the shuffled bytes (e.g. Mean
    ships a sum and a count per row). Under ``ESTIMATE`` a map task samples
    ``sample_rows`` rows spread across its input, estimates the input's distinct
    keys from the sample, and skips partial aggregation when estimated distinct
    keys / input rows exceeds ``unique_ratio_threshold``.
    """

    mode: PartialAggMode = PartialAggMode.ESTIMATE
    sample_rows: int = 100_000
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


_ROW_INDEX = "__ray_row"


def _group_by(
    table: pa.Table,
    keys: List[str],
    kernel_specs: List[tuple],
    kernel_names: List[str],
    with_groups: bool,
) -> Tuple[Dict[str, "pa.ChunkedArray"], Optional[Groups]]:
    """One ``group_by`` over ``table``: the keys and kernel outputs by name,
    plus, if asked, the group of every input row (see ``Groups``).

    The row -> group mapping comes from the same call: each group also
    collects the indices of its rows (Arrow's ``list`` function), so null
    keys and the group order match the other outputs exactly.
    """
    if with_groups:
        table = table.append_column(
            _ROW_INDEX, pa.array(np.arange(table.num_rows, dtype=np.int64))
        )
        kernel_specs = kernel_specs + [(_ROW_INDEX, "list")]
    grouped = table.group_by(keys, use_threads=False).aggregate(kernel_specs)
    columns = dict(zip(keys + kernel_names, grouped.columns))
    if not with_groups:
        return columns, None
    rows = grouped.column(grouped.num_columns - 1).combine_chunks()
    # pyrefly: ignore[missing-attribute]
    row_indices = pc.list_flatten(rows).to_numpy()
    ids = np.empty(table.num_rows, dtype=np.int64)
    # pyrefly: ignore[missing-attribute]
    ids[row_indices] = pc.list_parent_indices(rows).to_numpy()
    return columns, Groups(ids=ids, count=grouped.num_rows)


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
    kernel_names: List[str] = []
    grouped_aggs = []
    for i, (agg, spec) in enumerate(zip(aggs, specs)):
        col = agg.get_target_column()
        opts = arrow_agg_options(agg._ignore_nulls)
        if spec.prep is not None:
            table = spec.prep(i, col, table)
        agg_comps = tuple(f"__agg{i}_{c}" for c in spec.components)
        comps.append(agg_comps)
        if spec.grouped_raw is not None:
            grouped_aggs.append((i, spec.grouped_raw, col, opts, agg_comps))
        else:
            kernel_names.extend(agg_comps)
            agg_specs += spec.raw_agg_specs(i, col, opts)

    columns, groups = _group_by(
        table, keys, agg_specs, kernel_names, with_groups=bool(grouped_aggs)
    )
    for i, grouped_raw, col, opts, agg_comps in grouped_aggs:
        columns.update(zip(agg_comps, grouped_raw(table, groups, i, col, opts)))
    names = keys + [c for agg_comps in comps for c in agg_comps]
    return pa.table({name: columns[name] for name in names}), comps


def _merge_partials(
    combined: pa.Table,
    keys: List[str],
    aggs: Tuple["AggregateFnV2", ...],
    specs: List[ArrowAggSpec],
    comps: List[Tuple[str, ...]],
) -> pa.Table:
    """Merges partial-state rows (several per key) into one row per key."""
    # Built per call: PyArrow aggregate options aren't picklable, so they
    # can't be captured in closures that ship to reduce tasks.
    merge_specs: List[tuple] = []
    kernel_names: List[str] = []
    grouped_aggs = []
    for agg, spec, agg_comps in zip(aggs, specs, comps):
        opts = arrow_agg_options(agg._ignore_nulls)
        if spec.grouped_merge is not None:
            grouped_aggs.append((spec.grouped_merge, agg_comps, opts))
        else:
            assert spec.merge_specs is not None  # reduction specs define one
            kernel_names.extend(agg_comps)
            merge_specs += spec.merge_specs(agg_comps, opts)

    columns, groups = _group_by(
        combined, keys, merge_specs, kernel_names, with_groups=bool(grouped_aggs)
    )
    for grouped_merge, agg_comps, opts in grouped_aggs:
        columns.update(zip(agg_comps, grouped_merge(combined, groups, agg_comps, opts)))
    names = keys + [c for agg_comps in comps for c in agg_comps]
    return pa.table({name: columns[name] for name in names})


def _supports_types(
    table: pa.Table,
    aggs: Tuple["AggregateFnV2", ...],
    specs: List[ArrowAggSpec],
) -> bool:
    """Whether every spec can aggregate its source column's type in
    ``table``; if not, the Python engine aggregates it instead."""
    for agg, spec in zip(aggs, specs):
        col = agg.get_target_column()
        if spec.supports_type is None or col is None:
            continue
        if not spec.supports_type(table.schema.field(col).type):
            return False
    return True


def _has_null_typed_target(
    table: pa.Table,
    aggs: Tuple["AggregateFnV2", ...],
    specs: List[ArrowAggSpec],
) -> bool:
    """Whether a type-checked spec's source column is entirely null here
    (Arrow type ``null``). Such a block says nothing about the column's real
    type, which another block may have and a spec may not support."""
    for agg, spec in zip(aggs, specs):
        col = agg.get_target_column()
        if spec.supports_type is not None and col is not None:
            if pa_types.is_null(table.schema.field(col).type):
                return True
    return False


def _raw_rows_to_partials(
    table: pa.Table,
    keys: List[str],
    aggs: Tuple["AggregateFnV2", ...],
    specs: List[ArrowAggSpec],
) -> pa.Table:
    """Turns raw rows into the partial-state layout ``_aggregate_raw_rows``
    produces, treating every row as its own group, so they can be merged with
    real partials in a single group_by. Uses each spec's ``raw_to_partial``
    projection (no hashing). Every spec must have one: map tasks only ship raw
    rows when they all do (see _make_vectorized_aggregating_transformer)."""

    # Cast every column to the type the map-side group_by would produce
    # (e.g. int32 values -> int64 sums), so it concatenates with real partials.
    # Keys normally keep their type through a group_by, which makes their cast
    # a no-op; it's there in case a pyarrow version re-encodes them.
    target_schema = _aggregate_raw_rows(table.slice(0, 0), keys, aggs, specs)[0].schema

    def _as_target(name: str, column):
        target_type = target_schema.field(name).type
        if pa_types.is_large_list(column.type) and pa_types.is_list(target_type):
            # List state past int32 offsets (see ``_list_offsets``): keep it
            # large; concat promotes the other shards to match.
            target_type = pa.large_list(target_type.value_field)
        return column if column.type == target_type else pc.cast(column, target_type)

    columns = {k: _as_target(k, table[k]) for k in keys}
    for i, (agg, spec) in enumerate(zip(aggs, specs)):
        col = agg.get_target_column()
        opts = arrow_agg_options(agg._ignore_nulls)
        if spec.prep is not None:
            table = spec.prep(i, col, table)
        raw_to_partial = spec.raw_to_partial
        assert raw_to_partial is not None, "map tasks only skip with raw_to_partial"
        states = raw_to_partial(table, i, col, opts)
        for component, state in zip(spec.components, states):
            name = f"__agg{i}_{component}"
            columns[name] = _as_target(name, state)
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
        combined = concat(tables, promote_types=True)
        if not _supports_types(combined, aggs, specs):
            yield from _python_aggregate(key_columns, aggregation_fns, combined)
            return
        aggregated, comps = _aggregate_raw_rows(combined, keys, aggs, specs)
        del combined
        yield _finalize_aggregates(aggregated, keys, aggs, specs, comps)

    return _aggregate


def _python_aggregate(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
    raw_rows: pa.Table,
) -> Iterable[Block]:
    """Aggregates raw rows holding every row of their groups on the Python
    engine, for columns whose type a vectorized spec can't handle."""
    partial = _fallback_aggregating_transformer(key_columns, aggregation_fns)(raw_rows)
    yield from _fallback_aggregating_reduce_fn(key_columns, aggregation_fns)(
        0, [[partial]]
    )


def _make_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
    policy: Optional[PartialAggPolicy] = None,
) -> BlockTransformer:
    """Map-side block transformer for a hash aggregation.

    ``policy`` decides whether map tasks may skip partial aggregation; ``None``
    means always pre-aggregate. Skipping needs a way to turn the shipped raw
    rows into partial state on the reducer: ``ArrowAggSpec.raw_to_partial`` on
    the vectorized engine, ``AggregateFnV2.raw_to_partial`` on the Python one.
    Queries where some aggregation has neither always pre-aggregate.
    """
    return _make_vectorized_aggregating_transformer(
        key_columns, aggregation_fns, policy
    ) or _fallback_aggregating_transformer(key_columns, aggregation_fns, policy)


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
    if block.num_rows < policy.sample_rows:
        # Small inputs are cheap to aggregate and can't blow up memory.
        return False
    num_distinct = _estimate_distinct_keys(block, keys, policy.sample_rows)
    return num_distinct > policy.unique_ratio_threshold * block.num_rows


# Keys seen at least this many times in the sample count as one distinct key
# each and are left out of the Duj1 scale-up (see _estimate_distinct_keys).
_HEAVY_KEY_SAMPLE_COUNT = 3


def _estimate_distinct_keys(
    block: pa.Table, keys: List[str], sample_rows: int
) -> float:
    """Estimates the number of distinct keys in ``block`` from a sample.

    Rows are sampled uniformly across the whole block, so a block whose key
    cardinality changes partway through (e.g. unique keys first, then a few
    repeated keys) is judged as a whole. The distinct count in the sample
    alone overstates uniqueness: if every key appears twice in the block, a
    small sample still sees each key about once. So it is scaled up with the
    Haas-Stokes "Duj1" estimator, which Postgres ``ANALYZE`` uses for
    ``n_distinct``::

        D = n * d / (n - f1 + f1 * n / N)

    where ``N`` is the block's rows, ``n`` the sampled rows, ``d`` the distinct
    keys in the sample and ``f1`` the keys seen exactly once in it.

    Duj1 runs low when a few heavy keys (e.g. null) sit next to many unique
    ones: the heavy keys' rows inflate ``n - f1``. So keys seen at least
    ``_HEAVY_KEY_SAMPLE_COUNT`` times in the sample count as one distinct key
    each, and Duj1 is applied only to the remaining rows, with ``N`` scaled by
    the same fraction.

    Reference: P. J. Haas and L. Stokes, "Estimating the number of classes in a
    finite population", Journal of the American Statistical Association 93(444),
    1998.
    """
    total = block.num_rows
    # Rename the keys so none can clash with the ``count_all`` output column.
    sample = block.select(keys).rename_columns([f"__key{i}" for i in range(len(keys))])
    if sample_rows < total:
        # Fixed seed: the same block always gets the same decision.
        rng = np.random.default_rng(0)
        indices = rng.choice(total, size=sample_rows, replace=False, shuffle=False)
        sample = sample.take(np.sort(indices))
    counts = (
        sample.group_by(sample.column_names, use_threads=False)
        .aggregate([([], "count_all")])
        .column("count_all")
        .to_numpy()
    )
    heavy = counts >= _HEAVY_KEY_SAMPLE_COUNT
    num_heavy = int(np.count_nonzero(heavy))
    # Sampled rows and distinct keys left after setting the heavy keys aside.
    n = sample.num_rows - int(counts[heavy].sum())
    d = len(counts) - num_heavy
    if n == 0:
        return float(num_heavy)
    f1 = int(np.count_nonzero(counts == 1))
    rest_total = total * n / sample.num_rows
    return num_heavy + n * d / (n - f1 + f1 * n / rest_total)


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

    # Blocks of a type some spec can't aggregate pre-aggregate on the Python
    # engine, which skips partial aggregation only when every aggregation
    # implements ``AggregateFnV2.raw_to_partial``.
    python_transform = _fallback_aggregating_transformer(
        key_columns, aggregation_fns, policy
    )
    if any(spec.raw_to_partial is None for spec in specs):
        # The reducer can only merge raw rows by projecting them with
        # ``raw_to_partial``; without it, always pre-aggregate.
        policy = None

    def _arrow_transform(block: pa.Table) -> pa.Table:
        block_schema = BlockAccessor.for_block(block).schema()
        for agg in aggs:
            agg._validate(block_schema)

        if block.num_rows == 0:
            return block

        if _has_null_typed_target(block, aggs, specs):
            # All-null block: its column may have an unsupported type in other
            # blocks, so ship it raw and let the reducer decide.
            return block.select(keep)

        if not _supports_types(block, aggs, specs):
            # A type some spec can't aggregate (e.g. decimal): pre-aggregate
            # on the Python engine, as a query with such a column always did.
            partials = python_transform(block)
            return partials.append_column(
                _PYTHON_PARTIALS_MARKER, pa.nulls(partials.num_rows, pa.null())
            )

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

    # Partial-state column layout (reduction queries only).
    comps: List[Tuple[str, ...]] = (
        []
        if has_collection
        else [
            tuple(f"__agg{i}_{c}" for c in spec.components)
            for i, spec in enumerate(specs)
        ]
    )
    # Every partial shard has this column; raw shards (from map tasks that
    # skipped partial aggregation) never do.
    partial_marker = comps[0][0] if comps else None

    def _arrow_reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[Block]:
        shards = tables_by_input[0]
        tables = [t for t in shards if t.num_rows > 0]
        if not tables:
            return

        if not has_collection:
            if any(_PYTHON_PARTIALS_MARKER in t.schema.names for t in tables):
                yield from _python_reduce(tables)
                return
            partials = [t for t in tables if partial_marker in t.schema.names]
            raws = [t for t in tables if partial_marker not in t.schema.names]
            raw = concat(raws, promote_types=True) if raws else None
            if raw is not None and not _supports_types(raw, aggs, specs):
                # Map tasks ship blocks of unsupported or unknown (all-null)
                # type raw, so partials here mean blocks disagree on the
                # column's type (e.g. int64 in some, a list in others).
                if partials:
                    raise ValueError(
                        "Blocks have different types for an aggregated "
                        f"column: {raw.schema} vs {partials[0].schema}"
                    )
                yield from _python_aggregate(key_columns, aggregation_fns, raw)
                return
            if not partials:
                # Every map task skipped partial aggregation: one raw pass.
                merged, _ = _aggregate_raw_rows(raw, keys, aggs, specs)
            else:
                if raw is not None:
                    partials.append(_raw_rows_to_partials(raw, keys, aggs, specs))
                del raw
                merged = _merge_partials(
                    concat(partials, promote_types=True), keys, aggs, specs, comps
                )
            yield _finalize_aggregates(merged, keys, aggs, specs, comps)
            return

        combined = concat(tables, promote_types=True)

        # E.g. distinct/count_distinct have no kernel for a nested source
        # column (the map shipped raw rows); delegate the partition to Python.
        if not _supports_types(combined, aggs, specs):
            yield from _python_aggregate(key_columns, aggregation_fns, combined)
            return

        # Collection aggs: the map sent raw rows, so this gives final values.
        merged, collection_comps = _aggregate_raw_rows(combined, keys, aggs, specs)
        yield _finalize_aggregates(merged, keys, aggs, specs, collection_comps)

    python_transform = _fallback_aggregating_transformer(key_columns, aggregation_fns)
    python_reduce = _fallback_aggregating_reduce_fn(key_columns, aggregation_fns)

    def _python_reduce(tables: List[pa.Table]) -> Iterable[Block]:
        """A partition where some map task pre-aggregated on the Python engine
        (an unsupported column type): merge it all on the Python engine."""
        shards = []
        for table in tables:
            if _PYTHON_PARTIALS_MARKER in table.schema.names:
                shards.append(table.drop_columns([_PYTHON_PARTIALS_MARKER]))
            elif partial_marker in table.schema.names:
                # Vectorized partials can't be turned into Python accumulators.
                raise ValueError(
                    "Blocks have different types for an aggregated column: "
                    f"{table.schema} vs Python-engine partials"
                )
            else:
                # Raw rows: all-null blocks, or blocks a map task skipped.
                shards.append(python_transform(table))
        yield from python_reduce(0, [shards])

    return _arrow_reduce


# Vectorized engine: marks map output pre-aggregated on the Python engine
# because a column's type isn't supported by some spec. Null-typed, like
# ``_RAW_ROWS_MARKER``.
_PYTHON_PARTIALS_MARKER = "__ray_python_partials"

# Python engine: marks shards of raw rows, shipped by map tasks that skipped
# partial aggregation. Partial shards can't be told apart by their columns
# (they're named after the aggregations, which users name freely). Null-typed,
# so it holds no data and survives slicing, concatenation and IPC.
_RAW_ROWS_MARKER = "__ray_raw_rows"


def _python_raw_to_partial_aggs(
    key_columns: Tuple[str, ...], aggregation_fns: Tuple["AggregateFn", ...]
) -> Optional[Tuple["AggregateFnV2", ...]]:
    """The aggregations, if map tasks may skip partial aggregation on the
    Python engine: there are keys, and every aggregation implements
    ``AggregateFnV2.raw_to_partial``."""
    from ray.data.aggregate import AggregateFnV2

    if not key_columns or not aggregation_fns:
        return None
    aggs = tuple(
        agg
        for agg in aggregation_fns
        if isinstance(agg, AggregateFnV2) and agg._supports_raw_to_partial()
    )
    return aggs if len(aggs) == len(aggregation_fns) else None


def _fallback_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
    policy: Optional[PartialAggPolicy] = None,
) -> BlockTransformer:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    sort_key = SortKey(key=list(key_columns), descending=False)
    keys = list(key_columns)
    raw_aggs = _python_raw_to_partial_aggs(key_columns, aggregation_fns)
    if raw_aggs is None:
        policy = None
    keep = _key_and_target_columns(keys, raw_aggs) if raw_aggs else []

    def _transform(block: pa.Table) -> pa.Table:
        from ray.data._internal.planner.exchange.aggregate_task_spec import (
            SortAggregateTaskSpec,
        )

        # TODO unify block schemas to avoid validating every block.
        block_schema = BlockAccessor.for_block(block).schema()
        for agg_fn in aggregation_fns:
            agg_fn._validate(block_schema)

        if (
            policy is not None
            and isinstance(block, pa.Table)
            and block.num_rows > 0
            and _should_skip_partial_aggregation(block, keys, policy)
        ):
            # The reducer turns these rows into accumulators with
            # ``raw_to_partial`` (see _fallback_aggregating_reduce_fn).
            raw = block.select(keep)
            return raw.append_column(
                _RAW_ROWS_MARKER, pa.nulls(raw.num_rows, pa.null())
            )

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
    from ray.data._internal.table_block import _resolve_aggregated_column_names

    sort_key = SortKey(key=list(key_columns), descending=False)

    def _python_raw_rows_to_partials(raw: pa.Table) -> pa.Table:
        """One partial row per raw row, laid out like ``_aggregate``'s output
        (keys, then one accumulator column per aggregation)."""
        raw_aggs = _python_raw_to_partial_aggs(key_columns, aggregation_fns)
        assert raw_aggs is not None, "map tasks only skip with raw_to_partial"
        names = _resolve_aggregated_column_names([agg.name for agg in raw_aggs])
        columns = {k: raw[k] for k in key_columns}
        for name, agg in zip(names, raw_aggs):
            accumulators = agg.raw_to_partial(raw)
            if not isinstance(accumulators, (pa.Array, pa.ChunkedArray)):
                accumulators = pa.array(list(accumulators))
            columns[name] = accumulators
        return pa.table(columns)

    def _reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[Block]:
        # Aggregation is single-input, so there is exactly one shard list.
        tables = tables_by_input[0]
        if not tables:
            return
        raws = [
            t
            for t in tables
            if isinstance(t, pa.Table) and _RAW_ROWS_MARKER in t.schema.names
        ]
        if raws:
            tables = [t for t in tables if not any(t is r for r in raws)]
            raw = concat(raws, promote_types=True).drop_columns([_RAW_ROWS_MARKER])
            if tables:
                # Merge one accumulator per raw row with the partials.
                tables.append(_python_raw_rows_to_partials(raw))
            else:
                # Every map task skipped: aggregate the raw rows directly.
                tables = [
                    _fallback_aggregating_transformer(key_columns, aggregation_fns)(raw)
                ]
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
