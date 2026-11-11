import dataclasses
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, cast

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.datasource_v2.common.pushdown_utils import (
    derive_list_files_pushdown,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
)
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
from ray.data._internal.datasource_v2.interfaces.supports_column_statistics import (
    ColumnStatistics,
    ColumnStatisticsState,
    SupportsColumnStatistics,
)
from ray.data._internal.logical.interfaces import LogicalPlan, Rule
from ray.data._internal.logical.operators.all_to_all_operator import Aggregate
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.logical.operators.read_operator import ListFiles, ReadFiles
from ray.data.aggregate import AggregateFn, Max, Min

logger = logging.getLogger(__name__)

# One footer-read task per this many files: the work is network-bound, so a
# task should amortize its scheduling cost over several footers. Kept equal to
# ``ParquetFileReader``'s count-rows batch size; both list manifest rows only.
_FILES_PER_TASK = 16
# A summary task may fall back to reading the files it was given, so reserve a
# whole CPU when the read did not pin its own.
_TASK_NUM_CPUS = 1.0

# Errors that make one footer pass untrustworthy. A short result, a missing
# column, an I/O error, or a bound that cannot be decoded all mean the same
# thing: read this batch instead. ``UNKNOWN`` is *not* an error -- it is a
# per-row instruction to read that row.
_TRUST_ERRORS = (
    AttributeError,
    KeyError,
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
    pa.ArrowException,
)


def _manifest_from_batch(batch: pa.Table) -> FileManifest:
    assert PATH_COLUMN_NAME in batch.column_names, batch.column_names
    return FileManifest(batch)


def _targets(
    aggs: Sequence[AggregateFn],
    schema: pa.Schema,
    excluded: Set[str],
    reader: SupportsColumnStatistics,
) -> Optional[Tuple[List[str], pa.Schema]]:
    """Target columns and their input types, or ``None`` to decline."""
    names: List[str] = []
    for fn in aggs:
        if type(fn) is not Min and type(fn) is not Max:
            # The rule substitutes the aggregate's input, so a subclass that
            # makes ``aggregate_block`` depend on more than the column's
            # extrema would silently answer differently. Only the stock
            # functions are provably safe to feed a summary.
            return None
        target = fn.get_target_column()
        if not isinstance(target, str) or target in excluded:
            return None
        if target not in schema.names:
            return None
        if not reader.supports_column_type(schema.field(target).type):
            # Declining at planning keeps the normal read, instead of decoding
            # the column inside a footer task. A per-file ``UNKNOWN`` is still
            # handled at execution.
            return None
        if target not in names:
            names.append(target)
    return names, pa.schema([(name, schema.field(name).type) for name in names])


def _bound(stats: ColumnStatistics, is_min: bool) -> Any:
    """One column's minimum or maximum, or ``None`` when it has no bound."""
    if stats.state is ColumnStatisticsState.BOUNDED:
        return stats.min if is_min else stats.max
    return None


def _has_null(stats: ColumnStatistics) -> bool:
    return stats.state is ColumnStatisticsState.ALL_NULL or bool(stats.null_count)


def _summary_table(
    per_file: Sequence[Mapping[str, ColumnStatistics]],
    target_schema: pa.Schema,
) -> pa.Table:
    """Rows whose extrema equal the files' extrema for every target column.

    For each non-empty file this emits the column-wise minimum row, the
    column-wise maximum row, and -- when some column has a null -- one row in
    which exactly those columns are null and the others carry a real bound.

    Nulls are therefore observed per ``(file, column)``: a null in one column
    never suppresses another, and an empty file contributes no observation at
    all, so the aggregate's own zero factory handles it. Reducing these rows
    with the original ``Min``/``Max`` equals reducing the file's rows, for
    either ``ignore_nulls`` setting: both bounds lie inside the file's value
    range, so adding the opposite bound cannot move the extremum.
    """
    names = target_schema.names
    rows: List[Dict[str, Any]] = []
    for stats in per_file:
        if all(s.state is ColumnStatisticsState.EMPTY for s in stats.values()):
            # A null row here would be read as an observed null by
            # ``Min/Max(ignore_nulls=False)`` and could null out a value from
            # another file.
            continue
        rows.append({name: _bound(stats[name], True) for name in names})
        rows.append({name: _bound(stats[name], False) for name in names})
        if any(_has_null(stats[name]) for name in names):
            rows.append(
                {
                    name: (
                        None if _has_null(stats[name]) else _bound(stats[name], True)
                    )
                    for name in names
                }
            )
    return pa.Table.from_pylist(rows, schema=target_schema)


def _read_statistics(
    table: pa.Table, target_schema: pa.Schema
) -> Dict[str, ColumnStatistics]:
    """Re-express one read batch as the same value object as a footer.

    The validating constructor is what makes the summary safe to build from
    either source: a batch's nulls and bounds obey exactly the invariants a
    footer's do.
    """
    out: Dict[str, ColumnStatistics] = {}
    for name in target_schema.names:
        if name in table.column_names:
            column = table.column(name).combine_chunks()
            null_count = column.null_count
        else:
            # A missing column is null-filled by the read, which is an
            # all-null observation for this batch.
            column = None
            null_count = table.num_rows

        if null_count == table.num_rows:
            out[name] = ColumnStatistics(
                num_rows=table.num_rows,
                null_count=null_count,
                min=None,
                max=None,
                state=ColumnStatisticsState.ALL_NULL,
            )
        else:
            assert column is not None
            bounds = pc.min_max(column)
            out[name] = ColumnStatistics(
                num_rows=table.num_rows,
                null_count=null_count,
                min=bounds["min"].as_py(),
                max=bounds["max"].as_py(),
                state=ColumnStatisticsState.BOUNDED,
            )
    return out


def _project(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """Project a read batch onto the target schema, null-filling absent columns."""
    columns = []
    for name in target_schema.names:
        if name in table.column_names:
            columns.append(table.column(name))
        else:
            columns.append(
                pa.nulls(table.num_rows, type=target_schema.field(name).type)
            )
    return pa.Table.from_arrays(columns, schema=target_schema)


def _read_projection(
    reader: Reader[FileManifest],
    manifest: FileManifest,
    target_schema: pa.Schema,
) -> pa.Table:
    """The identity fallback: the rows the replaced read would have produced."""
    tables = [
        _project(table, target_schema)
        for table in reader.read(manifest)
        if table.num_rows
    ]
    if not tables:
        return pa.Table.from_pylist([], schema=target_schema)
    return pa.concat_tables(tables)


def _footer_statistics(
    reader: SupportsColumnStatistics,
    manifest: FileManifest,
    targets: Sequence[str],
) -> Optional[List[Dict[str, ColumnStatistics]]]:
    """Per-row statistics in manifest order, or ``None`` if the pass is untrusted.

    ``UNKNOWN`` is a per-row answer and is returned as-is. ``None`` means the
    result cannot be aligned with the manifest -- short, malformed, or failed
    -- so every row in the batch must be read.
    """
    try:
        per_file = list(reader.read_column_statistics(manifest, targets))
    except _TRUST_ERRORS:
        logger.debug("column statistics unusable; reading the batch", exc_info=True)
        return None
    if len(per_file) != len(manifest):
        return None
    if any(target not in stats for stats in per_file for target in targets):
        return None
    return per_file


def _try_rewrite(agg: Aggregate) -> Optional[Aggregate]:
    """Feed a global Min/Max aggregate a summary of its files, or decline."""
    if agg.key is not None or not agg.aggs:
        return None
    if not all(type(fn) is Min or type(fn) is Max for fn in agg.aggs):
        return None
    if len(agg.input_dependencies) != 1:
        return None

    read_files = agg.input_dependencies[0]
    if not isinstance(read_files, ReadFiles) or read_files.block_udf is not None:
        return None
    if len(read_files.input_dependencies) != 1:
        return None

    scanner = read_files.scanner
    # Reuse the count-exactness gate: footer bounds, like footer row counts,
    # are only valid when no pushdown drops rows -- including filters that a
    # scanner applies from construction rather than through the pushdown API.
    if not scanner.metadata_row_count_is_exact():
        return None

    list_files = read_files.input_dependencies[0]
    if not isinstance(list_files, ListFiles):
        return None

    # Footer statistics are file-level, so every manifest row must be a whole
    # file. A chunking indexer would ask for the same file once per chunk.
    whole_file_indexer = list_files.file_indexer.as_whole_file_indexer()
    if whole_file_indexer is None:
        return None

    pushdown = derive_list_files_pushdown(scanner)
    if (
        pushdown.predicate is not None
        or pushdown.limit is not None
        or pushdown.partition_pruner is not None
    ):
        return None

    # Check before reading the schema: formats whose reader cannot answer
    # statistics should not pay for schema inference just to be declined.
    base_reader = scanner.create_reader()
    if not isinstance(base_reader, SupportsColumnStatistics):
        return None

    try:
        schema = scanner.read_schema()
    except Exception:
        return None

    # These attributes are format-specific. A synthesized or partition column
    # has no footer of its own, so it can never be answered from metadata.
    excluded_columns = set(getattr(scanner, "partition_columns", set())) | {
        column.name for column in getattr(scanner, "synthesized_columns", ())
    }
    targets = _targets(agg.aggs, schema, excluded_columns, base_reader)
    if targets is None:
        return None
    target_names, target_schema = targets

    read_scanner: Scanner = scanner
    if isinstance(scanner, SupportsColumnPruning):
        read_scanner = cast(Scanner, scanner.prune_columns(target_names))

    # Rebuild ``ListFiles`` so listing emits each file exactly once and does no
    # metadata IO of its own. Every pushdown is clear above; the replacement
    # only drops chunking/indexing behavior.
    list_files = dataclasses.replace(
        list_files,
        file_partitioner=None,
        file_indexer=whole_file_indexer,
        predicate=None,
        projected_columns=None,
        limit=None,
        partition_pruner=None,
        pushdown_is_final=True,
    )

    def summarize(batch: pa.Table) -> pa.Table:
        manifest = _manifest_from_batch(batch)
        # Readers are created on the worker, as in the normal read path;
        # serializing a pre-built reader does not preserve its scan state.
        reader = read_scanner.create_reader()
        per_file = (
            _footer_statistics(reader, manifest, target_names)
            if isinstance(reader, SupportsColumnStatistics)
            else None
        )
        if per_file is None:
            return _read_projection(
                cast(Reader[FileManifest], reader), manifest, target_schema
            )

        known: List[Dict[str, ColumnStatistics]] = []
        unknown: List[int] = []
        for idx, stats in enumerate(per_file):
            if any(
                stats[target].state is ColumnStatisticsState.UNKNOWN
                for target in target_names
            ):
                unknown.append(idx)
            else:
                known.append(stats)

        tables: List[pa.Table] = []
        if known:
            tables.append(_summary_table(known, target_schema))
        if unknown:
            # Read only the rows the footer could not settle. Their real rows
            # join the same aggregate, so a known file never causes a read.
            subset = FileManifest(
                manifest.as_block().take(pa.array(unknown, type=pa.int64()))
            )
            for table in reader.read(subset):
                if table.num_rows:
                    tables.append(
                        _summary_table(
                            [_read_statistics(table, target_schema)], target_schema
                        )
                    )
        if not tables:
            return pa.Table.from_pylist([], schema=target_schema)
        return pa.concat_tables(tables)

    # The summary task may read the files whose footer cannot answer, so it
    # inherits the read's compute strategy and remote args (resources,
    # runtime_env, GPUs, ...). Only the CPU share is defaulted here.
    remote_args = dict(read_files.ray_remote_args)
    remote_args.setdefault("num_cpus", _TASK_NUM_CPUS)

    map_op = MapBatches(
        fn=summarize,
        input_dependencies=[list_files],
        batch_format="pyarrow",
        batch_size=_FILES_PER_TASK,
        min_rows_per_bundled_input=_FILES_PER_TASK,
        zero_copy_batch=True,
        can_modify_num_rows=True,
        compute=read_files.compute,
        ray_remote_args=remote_args,
    )

    # Only the aggregate's *input* changes. Its functions, aliases,
    # ``ignore_nulls``, zero factories, and subclass behaviour are the user's
    # own, and stay exactly as written.
    return dataclasses.replace(agg, input_dependencies=[map_op])


class PushdownMinMaxFiles(Rule):
    """Answer a global ``min()``/``max()`` from footer statistics.

    The rule rewrites only the input of the user's ``Aggregate``: instead of
    reading the files, a ``MapBatches`` emits, per file, its column-wise
    minimum row, its column-wise maximum row, and one row whose nulls are
    exactly the columns that contain a null. Reducing those rows with the
    original aggregate equals reducing the file's rows, so ``ignore_nulls``,
    aliases, duplicate aggregates, the result type, and an empty input's zero
    factory all keep their stock behaviour.

    A file whose footer cannot answer is read and summarized the same way. The
    planning gate is deliberately fail-open: every condition that cannot be
    proven leaves the plan untouched, because a wrong bound is silent while
    declining only costs the read that would have happened anyway.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:  # pyrefly: ignore[bad-override]
        def transform(op):
            if isinstance(op, Aggregate):
                rewritten = _try_rewrite(op)
                if rewritten is not None:
                    return rewritten
            return op

        dag = plan.dag._apply_transform(transform)
        if dag is plan.dag:
            return plan
        return LogicalPlan(dag, plan.context)
