import dataclasses
import logging
import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, List, Literal, Optional, Sequence, Set, cast

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

_PARTIAL_PREFIX = "__ray_meta_minmax_"
# One footer-read task per this many files: the work is network-bound, so a
# task should amortize its scheduling cost over several footers.
_FILES_PER_TASK = 16
# A partial batch may fall back to a full read, so reserve a whole CPU.
_TASK_NUM_CPUS = 1.0

_Extremum = Literal["min", "max"]


@dataclass(frozen=True)
class _AggSpec:
    """How one original Min/Max maps onto a partial column and final reduce."""

    kind: _Extremum
    target: str
    ignore_nulls: bool
    output_name: str
    partial_name: str
    field_type: pa.DataType
    # Reproduces the original aggregate's zero for an all-empty input. It is
    # only used by the final reduce when every manifest batch was empty.
    zero_factory: Callable[[], Any]


class _PartialState(Enum):
    UNSET = auto()
    VALUED = auto()
    POISONED = auto()


class _PartialExtremum:
    """Fold observations into one aggregate's partial min/max.

    The state machine is:

    - ``UNSET``: no candidate value has been seen. NaN observations are
      tracked separately, because Arrow ignores them when a real value exists
      but returns NaN for an all-NaN input.
    - ``VALUED``: ``_value`` holds the running extremum.
    - ``POISONED``: ``ignore_nulls=False`` and a real null was seen, so the
      result must be null regardless of later values. Terminal.

    ``result()`` is the running value only in ``VALUED``; ``UNSET`` after only
    NaN observations returns NaN, and every other unset/poisoned state reduces
    to null.
    """

    def __init__(self, kind: _Extremum, ignore_nulls: bool):
        self._kind = kind
        self._ignore_nulls = ignore_nulls
        self._state = _PartialState.UNSET
        self._value: Optional[Any] = None
        # Arrow's min/max kernel ignores NaN when any non-NaN value exists,
        # and returns NaN only for an all-NaN input. Track that separately
        # from a real null, which poisons ``ignore_nulls=False``.
        self._saw_nan = False

    def observe_null(self) -> None:
        """Record that this input contains at least one null value."""
        if not self._ignore_nulls:
            self._state = _PartialState.POISONED
            self._value = None

    def observe(self, value: Any) -> None:
        """Record one candidate value."""
        if self._state is _PartialState.POISONED:
            return
        if _is_nan(value):
            self._saw_nan = True
            return
        if self._state is _PartialState.UNSET:
            self._state = _PartialState.VALUED
            self._value = value
        else:
            current = cast(Any, self._value)
            self._value = (
                min(current, value) if self._kind == "min" else max(current, value)
            )

    def result(self) -> Optional[Any]:
        if self._state is _PartialState.VALUED:
            return self._value
        if self._state is _PartialState.UNSET and self._saw_nan:
            return float("nan")
        return None

    @property
    def is_unset(self) -> bool:
        """Whether this batch observed no rows at all."""
        return self._state is _PartialState.UNSET and not self._saw_nan


class _InexactStatistics(Exception):
    """Footer statistics cannot answer a column, so the manifest must be read."""


def _manifest_from_batch(batch: pa.Table) -> FileManifest:
    assert PATH_COLUMN_NAME in batch.column_names, batch.column_names
    return FileManifest(batch)


def _is_nan(value: Any) -> bool:
    try:
        return math.isnan(value)
    except TypeError:
        return False


def _cast(value: Any, field_type: pa.DataType) -> Any:
    return pa.scalar(value, type=field_type).as_py()


def _fold_footer_stats(
    spec: _AggSpec, stats: Sequence[ColumnStatistics]
) -> _PartialExtremum:
    """Fold one manifest batch's footer bounds into a partial accumulator."""
    partial = _PartialExtremum(spec.kind, spec.ignore_nulls)
    for column_stats in stats:
        state = column_stats.state
        if state is ColumnStatisticsState.UNKNOWN:
            raise _InexactStatistics(spec.target)
        if state is ColumnStatisticsState.EMPTY:
            continue
        if state is ColumnStatisticsState.ALL_NULL:
            partial.observe_null()
            continue

        bound = column_stats.min if spec.kind == "min" else column_stats.max
        if bound is None:
            # ``BOUNDED`` guarantees a bound; a fallback beats a crash if a
            # reader ever violates the contract.
            raise _InexactStatistics(spec.target)
        if column_stats.null_count:
            partial.observe_null()
        partial.observe(_cast(bound, spec.field_type))
    return partial


def _partials_from_stats(
    reader: SupportsColumnStatistics,
    manifest: FileManifest,
    specs: Sequence[_AggSpec],
) -> Optional[List[_PartialExtremum]]:
    """One partial per aggregate, or ``None`` when the footer cannot answer."""
    targets = list(dict.fromkeys(spec.target for spec in specs))
    try:
        per_file = list(reader.read_column_statistics(manifest, targets))
        if len(per_file) != len(manifest):
            return None
        per_target = {
            target: [stats[target] for stats in per_file] for target in targets
        }
        return [_fold_footer_stats(spec, per_target[spec.target]) for spec in specs]
    except (
        _InexactStatistics,
        AttributeError,
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        pa.ArrowException,
    ):
        # ``read_column_statistics`` is a trust boundary between the reader
        # and this rule: a short result, a missing column, an inexact file, an
        # I/O error, or a bound that cannot be decoded all mean "read this
        # manifest".
        return None


def _values_from_read(
    reader: Reader[FileManifest],
    manifest: FileManifest,
    specs: Sequence[_AggSpec],
) -> List[_PartialExtremum]:
    """Fallback: read the target columns and fold their min/max."""
    import pyarrow.compute as pc

    partials = [_PartialExtremum(spec.kind, spec.ignore_nulls) for spec in specs]
    for table in reader.read(manifest):
        if table.num_rows == 0:
            continue
        for partial, spec in zip(partials, specs):
            if spec.target not in table.column_names:
                # A missing column is null-filled by the reader.
                partial.observe_null()
                continue

            column = table.column(spec.target)
            if column.null_count:
                partial.observe_null()
            scalar = (
                pc.min(column, skip_nulls=spec.ignore_nulls)
                if spec.kind == "min"
                else pc.max(column, skip_nulls=spec.ignore_nulls)
            )
            value = scalar.as_py()
            if value is not None:
                partial.observe(_cast(value, spec.field_type))
    return partials


def _zero_factory(fn: AggregateFn) -> Callable[[], Any]:
    """The original aggregate's zero, for an all-empty input."""
    return lambda: fn.init(None)


def _build_specs(
    aggs: Sequence[AggregateFn],
    schema: pa.Schema,
    excluded_columns: Set[str],
) -> Optional[List[_AggSpec]]:
    """Translate Min/Max aggs into partial columns, or ``None`` to decline."""
    specs: List[_AggSpec] = []
    for idx, fn in enumerate(aggs):
        if not isinstance(fn, (Min, Max)):
            return None
        if type(fn) is not Min and type(fn) is not Max:
            # Subclasses may override aggregate_block/finalize/zero_factory;
            # rebuilding a plain Min/Max would silently discard those.
            return None
        target = fn.get_target_column()
        if not isinstance(target, str):
            return None
        if target in excluded_columns:
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
                # ``init`` is the null-safe zero wrapper. Reusing it preserves a
                # user-supplied ``zero_factory`` for an all-empty dataset.
                zero_factory=_zero_factory(fn),
            )
        )
    return specs


def _try_rewrite(agg: Aggregate) -> Optional[Aggregate]:
    """Rewrite a global Min/Max aggregate over an unfiltered file read."""
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
    specs = _build_specs(agg.aggs, schema, excluded_columns)
    if specs is None:
        return None
    # Bind a non-optional local so the closure below keeps the narrowed type.
    built_specs: List[_AggSpec] = specs

    targets = list(dict.fromkeys(spec.target for spec in built_specs))
    read_scanner: Scanner = scanner
    if isinstance(scanner, SupportsColumnPruning):
        read_scanner = cast(Scanner, scanner.prune_columns(targets))

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

    def partials(batch: pa.Table) -> pa.Table:
        manifest = _manifest_from_batch(batch)
        # Readers are created on the worker, as in the normal read path;
        # serializing a pre-built reader does not preserve its scan state.
        reader = read_scanner.create_reader()
        accumulators = (
            _partials_from_stats(reader, manifest, built_specs)
            if isinstance(reader, SupportsColumnStatistics)
            else None
        )
        if accumulators is None:
            logger.debug(
                "Reading %d file(s): footer statistics unavailable", len(manifest)
            )
            accumulators = _values_from_read(
                cast(Reader[FileManifest], reader), manifest, built_specs
            )

        # Empty files contribute no rows. Emitting a null row here would make
        # the final ``Min/Max(ignore_nulls=False)`` treat them as observed
        # nulls and could return null despite non-null values in other
        # batches, so an empty batch emits no partial row at all. The final
        # aggregate then falls back to its zero factory (``+inf``/``-inf``).
        if all(accumulator.is_unset for accumulator in accumulators):
            return pa.table(
                {
                    spec.partial_name: pa.array([], type=spec.field_type)
                    for spec in built_specs
                }
            )

        # One row per non-empty manifest batch. The final aggregate reduces
        # those rows across batches, so a multi-batch dataset cannot be
        # truncated by ``Dataset.min()``/``max()``'s final ``take(1)``.
        return pa.table(
            {
                spec.partial_name: pa.array(
                    [accumulator.result()], type=spec.field_type
                )
                for spec, accumulator in zip(built_specs, accumulators)
            }
        )

    map_op = MapBatches(
        fn=partials,
        input_dependencies=[list_files],
        batch_format="pyarrow",
        batch_size=_FILES_PER_TASK,
        min_rows_per_bundled_input=_FILES_PER_TASK,
        zero_copy_batch=True,
        can_modify_num_rows=True,
        ray_remote_args={"num_cpus": _TASK_NUM_CPUS},
    )

    final_aggs: List[AggregateFn] = []
    for spec in built_specs:
        kwargs = {
            "on": spec.partial_name,
            "ignore_nulls": spec.ignore_nulls,
            "alias_name": spec.output_name,
            # Preserve a user-supplied ``zero_factory``: an all-empty dataset
            # reduces to the rebuilt aggregate's zero, and the default would
            # silently replace a custom sentinel.
            "zero_factory": spec.zero_factory,
        }
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

    The rule is deliberately fail-open. Every applicability check that cannot
    be proven from the plan leaves the plan untouched, and a manifest batch
    whose statistics are unknown is read instead. A wrong bound is silent;
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
