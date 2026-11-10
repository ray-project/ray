import dataclasses
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, cast

import pyarrow as pa

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
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
from ray.data._internal.datasource_v2.interfaces.supports_column_statistics import (
    ColumnStatistics,
    SupportsColumnStatistics,
)
from ray.data._internal.logical.interfaces import LogicalPlan, Rule
from ray.data._internal.logical.operators.all_to_all_operator import Aggregate
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.logical.operators.read_operator import ListFiles, ReadFiles
from ray.data.aggregate import AggregateFn, Max, Min

logger = logging.getLogger(__name__)

_PARTIAL_PREFIX = "__ray_meta_minmax_"
_BATCH_SIZE = 16
_TASK_NUM_CPUS = 1.0


@dataclass(frozen=True)
class _AggSpec:
    """How one original Min/Max maps onto a partial column and final reduce."""

    kind: str  # "min" or "max"
    target: str
    ignore_nulls: bool
    output_name: str
    partial_name: str
    field_type: Any


def _manifest_from_batch(batch: pa.Table) -> FileManifest:
    assert PATH_COLUMN_NAME in batch.column_names, batch.column_names
    return FileManifest(batch)


def _cast(value, field_type):
    return pa.scalar(value, type=field_type).as_py()


def _value_from_stats(
    spec: _AggSpec, stats: Sequence[ColumnStatistics]
) -> Optional[Any]:
    """Fold one file set's footer bounds into a single partial value."""
    value: Optional[Any] = None
    for column_stats in stats:
        if not column_stats.exact:
            raise ValueError("column statistics are not exact")
        if column_stats.num_rows == 0:
            continue
        if not spec.ignore_nulls and (
            column_stats.null_count is None or column_stats.null_count > 0
        ):
            return None
        bound = column_stats.min if spec.kind == "min" else column_stats.max
        if bound is None:
            continue
        bound = _cast(bound, spec.field_type)
        if value is None:
            value = bound
        elif spec.kind == "min":
            value = min(value, bound)
        else:
            value = max(value, bound)
    return value


def _values_from_read(
    reader_scanner: Scanner,
    manifest: FileManifest,
    specs: Sequence[_AggSpec],
) -> List[Any]:
    """Fallback: read the target columns and fold their min/max."""
    import pyarrow.compute as pc

    reader = reader_scanner.create_reader()
    values: List[Any] = [None] * len(specs)
    poisoned = [False] * len(specs)
    for table in reader.read(manifest):
        for idx, spec in enumerate(specs):
            if spec.target not in table.column_names:
                # A missing column is null-filled by the reader; for
                # ignore_nulls=False those nulls poison the aggregate.
                if not spec.ignore_nulls:
                    poisoned[idx] = True
                continue
            column = table.column(spec.target)
            if not spec.ignore_nulls and column.null_count > 0:
                poisoned[idx] = True
                continue
            if spec.kind == "min":
                scalar = pc.min(column, skip_nulls=spec.ignore_nulls)
            else:
                scalar = pc.max(column, skip_nulls=spec.ignore_nulls)
            value = scalar.as_py()
            if value is None:
                continue
            value = _cast(value, spec.field_type)
            if values[idx] is None:
                values[idx] = value
            elif spec.kind == "min":
                values[idx] = min(values[idx], value)
            else:
                values[idx] = max(values[idx], value)

    for idx in range(len(specs)):
        if poisoned[idx]:
            values[idx] = None
    return values


def _try_rewrite(agg: Aggregate) -> Optional[Aggregate]:
    """Rewrite a global Min/Max aggregate over an unfiltered file read."""
    if agg.key is not None or not agg.aggs:
        return None
    if not all(isinstance(fn, (Min, Max)) for fn in agg.aggs):
        return None

    read_files = agg.input_dependencies[0]
    if not isinstance(read_files, ReadFiles) or read_files.block_udf is not None:
        return None

    scanner = read_files.scanner
    if not scanner.metadata_row_count_is_exact():
        return None

    try:
        schema = scanner.read_schema()
    except Exception:
        return None

    partition_columns = set(getattr(scanner, "partition_columns", set()))
    synthesized_columns = {
        column.name for column in getattr(scanner, "synthesized_columns", ())
    }

    specs: List[_AggSpec] = []
    for idx, fn in enumerate(agg.aggs):
        if not isinstance(fn, (Min, Max)):
            return None
        target = fn.get_target_column()
        if not isinstance(target, str):
            return None
        if target in partition_columns or target in synthesized_columns:
            return None
        field = schema.field(target) if target in schema.names else None
        if field is None:
            return None
        if (
            pa.types.is_nested(field.type)
            or isinstance(field.type, pa.DictionaryType)
            or isinstance(field.type, pa.ExtensionType)
        ):
            return None
        specs.append(
            _AggSpec(
                kind="min" if isinstance(fn, Min) else "max",
                target=target,
                ignore_nulls=fn._ignore_nulls,
                output_name=fn.name,
                partial_name=f"{_PARTIAL_PREFIX}{idx}",
                field_type=field.type,
            )
        )

    reader = scanner.create_reader()
    if not isinstance(reader, SupportsColumnStatistics):
        return None

    list_files = read_files.input_dependencies[0]
    if not isinstance(list_files, ListFiles):
        return None

    whole_file_indexer = list_files.file_indexer.as_whole_file_indexer()
    if whole_file_indexer is None:
        return None

    pushdown = derive_list_files_pushdown(scanner)
    assert (
        pushdown.predicate is None
    ), "metadata_row_count_is_exact must refuse a filter"
    assert pushdown.limit is None, "metadata_row_count_is_exact must refuse a limit"
    assert pushdown.partition_pruner is None

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

    targets = list(dict.fromkeys(spec.target for spec in specs))
    reader_scanner: Scanner = scanner
    if isinstance(scanner, SupportsColumnPruning):
        reader_scanner = cast(Scanner, scanner.prune_columns(targets))

    def partials(batch: pa.Table) -> pa.Table:
        manifest = _manifest_from_batch(batch)
        stats_reader = reader_scanner.create_reader()
        assert isinstance(stats_reader, SupportsColumnStatistics)
        per_file = list(stats_reader.read_column_statistics(manifest, targets))
        assert len(per_file) == len(manifest)
        per_target: Dict[str, List[ColumnStatistics]] = {
            target: [stats[target] for stats in per_file] for target in targets
        }

        try:
            values = [
                _value_from_stats(spec, per_target[spec.target]) for spec in specs
            ]
        except Exception:
            # Any stat that cannot be decoded or cast is safe to re-read.
            values = _values_from_read(reader_scanner, manifest, specs)

        return pa.table(
            {
                spec.partial_name: pa.array([value], type=spec.field_type)
                for spec, value in zip(specs, values)
            }
        )

    map_op = MapBatches(
        fn=partials,
        input_dependencies=[list_files],
        batch_format="pyarrow",
        batch_size=_BATCH_SIZE,
        min_rows_per_bundled_input=_BATCH_SIZE,
        zero_copy_batch=True,
        can_modify_num_rows=True,
        ray_remote_args={"num_cpus": _TASK_NUM_CPUS},
    )

    final_aggs: List[AggregateFn] = []
    for spec in specs:
        kwargs = dict(
            on=spec.partial_name,
            ignore_nulls=spec.ignore_nulls,
            alias_name=spec.output_name,
        )
        final_aggs.append(Min(**kwargs) if spec.kind == "min" else Max(**kwargs))

    return dataclasses.replace(
        agg,
        aggs=final_aggs,
        input_dependencies=[map_op],
    )


class PushdownMinMaxFiles(Rule):
    """Answer global ``min()``/``max()`` from Parquet footer statistics.

    Only a global aggregate over an unfiltered, unpartitioned file read is
    rewritten. The rewritten plan keeps the original global ``Aggregate`` on
    top of per-batch partials, so multiple metadata batches cannot be mistaken
    for a single final value.
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
