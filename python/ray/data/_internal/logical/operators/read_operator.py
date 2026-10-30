import dataclasses
import functools
import logging
import math
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple, Union

from ray.data._internal.cbo_stats.operator_statistics import (
    ColumnStatistics,
    ConfidenceLevel,
    OperatorStatistics,
)
from ray.data._internal.cbo_stats.selectivity import filter_statistics
from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePushdown,
    LogicalOperatorSupportsProjectionPushdown,
    SourceOperator,
)
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.block import (
    Block,
    BlockMetadata,
    BlockMetadataWithSchema,
)
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, Reader
from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.interfaces.file_indexer import FileIndexer
    from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
        FilePartitioner,
    )
    from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
    from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.datasource.partitioning import PathPartitionFilter

__all__ = [
    "ListFiles",
    "Read",
    "ReadFiles",
]


@dataclass(frozen=True, repr=False, eq=False)
class Read(
    AbstractMap,
    SourceOperator,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalOperatorSupportsPredicatePushdown,
):
    """Logical operator for read."""

    datasource: Datasource
    datasource_or_legacy_reader: Union[Datasource, Reader]
    parallelism: int
    num_outputs: Optional[int] = None
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    compute: Optional[ComputeStrategy] = None
    detected_parallelism: Optional[int] = None
    can_modify_num_rows: bool = field(init=False, default=True)
    min_rows_per_bundled_input: Optional[int] = field(init=False, default=None)
    ray_remote_args_fn: None = field(init=False, default=None)
    per_block_limit: Optional[int] = None
    _input_dependencies: list = field(init=False, repr=False, default_factory=list)

    def __post_init__(self):
        if self.compute is None:
            from ray.data._internal.compute import TaskPoolStrategy

            object.__setattr__(self, "compute", TaskPoolStrategy())
        if self.ray_remote_args is None:
            object.__setattr__(self, "ray_remote_args", {})
        object.__setattr__(self, "_name", f"Read{self.datasource.get_name()}")
        object.__setattr__(self, "_input_dependencies", [])

    def output_data(self):
        return None

    def set_detected_parallelism(self, parallelism: int) -> "Read":
        """
        Set the true parallelism that should be used during execution. This
        should be specified by the user or detected by the optimizer.
        """
        object.__setattr__(self, "detected_parallelism", parallelism)
        return self

    def get_detected_parallelism(self) -> int:
        """
        Get the true parallelism that should be used during execution.
        """
        return self.detected_parallelism

    def estimated_num_outputs(self) -> Optional[int]:
        return self.num_outputs or self._estimate_num_outputs()

    def infer_metadata(self) -> BlockMetadata:
        """A ``BlockMetadata`` that represents the aggregate metadata of the outputs.

        This method gets metadata from the read tasks. It doesn't trigger any actual
        execution.
        """
        return self._cached_output_metadata.metadata

    def infer_statistics(self) -> Optional[OperatorStatistics]:
        """Infers output statistics from the datasource metadata.

        For Parquet, row counts and column statistics come from the footers that
        were already fetched while sampling files, so no extra I/O is done.
        """
        metadata = self.infer_metadata()
        num_rows, size_bytes = metadata.num_rows, metadata.size_bytes
        column_stats, footer_rows, complete = self._parquet_footer_statistics()
        if num_rows is None and footer_rows is not None:
            num_rows = footer_rows
        if num_rows is None and size_bytes is None:
            return None

        if metadata.num_rows is not None or complete:
            confidence = ConfidenceLevel.HIGH.value
        else:
            # Extrapolated from a sample of files, or a size-only estimate.
            confidence = ConfidenceLevel.MEDIUM.value
        stats = OperatorStatistics(
            num_rows=num_rows,
            size_bytes=size_bytes,
            # A complete footer scan measures the read; an extrapolation from a
            # sample of files is a point estimate without a proven bound.
            size_bytes_upper=(
                size_bytes if confidence >= ConfidenceLevel.HIGH.value else None
            ),
            num_blocks=self.estimated_num_outputs(),
            column_stats=column_stats,
            confidence=confidence,
        )

        # Metadata describes the data before the pushed-down predicate.
        predicate = (
            self.get_current_predicate() if self.supports_predicate_pushdown() else None
        )
        if predicate is not None:
            stats = filter_statistics(stats, predicate)

        if not self.supports_projection_pushdown():
            return stats

        # The size estimate of the datasource includes the pruned columns.
        projection = self.get_projection_map()
        if projection is not None:
            changes = {
                "column_stats": {
                    name: col
                    for name, col in stats.column_stats.items()
                    if name in projection
                }
            }
            projected_size = stats.projected_size_bytes(projection)
            if projected_size is not None:
                changes["size_bytes"] = projected_size
                changes["avg_row_bytes"] = (
                    projected_size / stats.num_rows if stats.num_rows else None
                )
            # A projection keeps a subset of the columns, so the input bound is
            # still valid even when the projected share isn't known.
            projected_upper = stats.projected_size_upper_bytes(projection)
            changes["size_bytes_upper"] = (
                projected_upper
                if projected_upper is not None
                else stats.size_bytes_upper
            )
            stats = dataclasses.replace(stats, **changes)

        # Upstream removed read-stage column renames (renaming is a ``Project``
        # above the read now), so there is nothing to remap here.
        return stats

    def _parquet_footer_statistics(
        self,
    ) -> Tuple[Dict[str, ColumnStatistics], Optional[int], bool]:
        """Returns ``(column_stats, num_rows, complete)`` from Parquet footers.

        ``complete`` is True if the footers of all files to read were sampled.
        Otherwise the row and null counts are extrapolated from the sample and
        the column statistics have ``MEDIUM`` confidence.
        """
        cache = self.__dict__.get("_footer_statistics_cache")
        if cache is not None and cache[0] is self.datasource:
            return cache[1]
        try:
            result = _parquet_footer_statistics(self.datasource)
        except Exception as e:
            logger.debug(f"Failed to extract Parquet footer statistics: {e}")
            result = ({}, None, False)
        # Keyed by datasource since projection and predicate pushdown clone this
        # operator with a new datasource. ``Read`` is a frozen dataclass, so the
        # cache is written straight into ``__dict__`` (as ``cached_property`` does).
        self.__dict__["_footer_statistics_cache"] = (self.datasource, result)
        return result

    def infer_schema(self):
        return self._cached_output_metadata.schema

    def _estimate_num_outputs(self) -> Optional[int]:
        metadata = self._cached_output_metadata.metadata

        # Handle edge-case of empty dataset
        if metadata.size_bytes == 0:
            return 0

        target_max_block_size = DataContext.get_current().target_max_block_size

        # In either case of
        #   - Total byte-size estimate not available
        #   - Target max-block-size not being configured
        #
        # We fallback to estimating number of outputs to be equivalent to the
        # number of input files being read (if any)
        if metadata.size_bytes is None or target_max_block_size is None:
            # NOTE: If there's no input files specified, return the count (could be 0)
            return (
                len(metadata.input_files) if metadata.input_files is not None else None
            )

        # Otherwise, estimate total number of blocks from estimated total
        # byte size
        return math.ceil(metadata.size_bytes / target_max_block_size)

    @functools.cached_property
    def _cached_output_metadata(self) -> "BlockMetadataWithSchema":
        # Legacy datasources might not implement `get_read_tasks`.
        if self.datasource.should_create_reader:
            empty_meta = BlockMetadata(None, None, None, None)
            return BlockMetadataWithSchema.from_metadata(empty_meta, schema=None)

        # HACK: Try to get a single read task to get the metadata.
        read_tasks = self.datasource.get_read_tasks(1)
        if len(read_tasks) == 0:
            # If there are no read tasks, the dataset is probably empty.
            empty_meta = BlockMetadata(
                num_rows=0,
                size_bytes=0,
                input_files=None,
                exec_stats=None,
            )
            return BlockMetadataWithSchema.from_metadata(empty_meta, schema=None)

        # `get_read_tasks` isn't guaranteed to return exactly one read task.
        metadata = [read_task.metadata for read_task in read_tasks]

        if all(meta.num_rows is not None for meta in metadata):
            num_rows = sum(meta.num_rows for meta in metadata)
            original_num_rows = num_rows
            # Apply per-block limit if set
            if self.per_block_limit is not None:
                num_rows = min(num_rows, self.per_block_limit)
        else:
            num_rows = None
            original_num_rows = None

        if all(meta.size_bytes is not None for meta in metadata):
            size_bytes = sum(meta.size_bytes for meta in metadata)
            # Pro-rate the byte size if we applied a row limit
            if (
                self.per_block_limit is not None
                and original_num_rows is not None
                and original_num_rows > 0
            ):
                size_bytes = int(size_bytes * (num_rows / original_num_rows))
        else:
            size_bytes = None

        input_files = []
        for meta in metadata:
            if meta.input_files is not None:
                input_files.extend(meta.input_files)

        meta = BlockMetadata(
            num_rows=num_rows,
            size_bytes=size_bytes,
            input_files=input_files,
            exec_stats=None,
        )
        schemas = [
            read_task.schema for read_task in read_tasks if read_task.schema is not None
        ]
        from ray.data._internal.util import unify_schemas_with_validation

        schema = None
        if schemas:
            schema = unify_schemas_with_validation(schemas)
        return BlockMetadataWithSchema.from_metadata(meta, schema=schema)

    def supports_projection_pushdown(self) -> bool:
        return self.datasource.supports_projection_pushdown()

    def get_projection_map(self) -> Optional[Dict[str, str]]:
        return self.datasource.get_projection_map()

    def apply_projection(
        self,
        projection_map: Optional[Dict[str, str]],
    ) -> "Read":
        projected_datasource = self.datasource.apply_projection(projection_map)
        return replace(
            self,
            datasource=projected_datasource,
            datasource_or_legacy_reader=projected_datasource,
            num_outputs=self.num_outputs,
        )

    def supports_predicate_pushdown(self) -> bool:
        return self.datasource.supports_predicate_pushdown()

    def get_current_predicate(self) -> Optional[Expr]:
        return self.datasource.get_current_predicate()

    def apply_predicate(self, predicate_expr: Expr) -> "Read":
        predicated_datasource = self.datasource.apply_predicate(predicate_expr)

        # A datasource returns its own instance to signal "no pushdown applied"
        # (e.g. ``ParquetDatasource`` does this when a mixed-column conjunct
        # leaves a residual). Preserve identity here so ``PredicatePushdown``'s
        # ``result_op is input_op`` no-op check keeps the ``Filter`` above.
        if self.datasource is predicated_datasource:
            return self

        return replace(
            self,
            datasource=predicated_datasource,
            datasource_or_legacy_reader=predicated_datasource,
            num_outputs=self.num_outputs,
        )


def _parquet_footer_statistics(
    datasource: Datasource,
) -> Tuple[Dict[str, ColumnStatistics], Optional[int], bool]:
    from ray.data._internal.datasource.parquet_stat_extractor import (
        extract_column_stats_from_parquet_metadata,
    )

    fragments = getattr(datasource, "_pq_fragments", None)
    sampled_fragments = getattr(datasource, "_sampled_fragments", None)
    file_infos = getattr(datasource, "_sampled_file_infos", None)
    if not fragments or not sampled_fragments or not file_infos:
        return {}, None, False

    # Partition pruning may have removed some of the sampled files.
    remaining = {id(fragment) for fragment in fragments}
    sampled = [
        file_info
        for fragment, file_info in zip(sampled_fragments, file_infos)
        if id(fragment) in remaining
        and file_info is not None
        and file_info.metadata is not None
    ]
    if not sampled:
        return {}, None, False
    footers = [file_info.metadata for file_info in sampled]

    complete = len(footers) == len(fragments)
    extrapolation = len(fragments) / len(footers)
    confidence = (
        ConfidenceLevel.HIGH.value if complete else ConfidenceLevel.MEDIUM.value
    )
    num_rows = round(sum(footer.num_rows for footer in footers) * extrapolation)

    per_file = [extract_column_stats_from_parquet_metadata(f) for f in footers]
    # Columns missing from some files (schema evolution) have no valid bounds.
    common_columns = set.intersection(*(set(stats) for stats in per_file))
    column_stats = {}
    for name in common_columns:
        merged = None
        for file_stats in per_file:
            stat = file_stats[name]
            column = ColumnStatistics(
                name=name,
                min_value=stat["min"],
                max_value=stat["max"],
                null_count=stat["null_count"],
                distinct_count=stat["distinct_count"],
                confidence=confidence,
            )
            merged = column if merged is None else merged.merge(column)
        if merged.null_count is not None:
            merged.null_count = round(merged.null_count * extrapolation)
        column_stats[name] = merged

    # In-memory column sizes, used to estimate the size of projections.
    for name, size in _avg_column_sizes(sampled).items():
        if name in column_stats:
            column_stats[name].avg_size_bytes = size
        else:
            column_stats[name] = ColumnStatistics(
                name=name, avg_size_bytes=size, confidence=confidence
            )
    return column_stats, num_rows, complete


def _avg_column_sizes(file_infos) -> Dict[str, float]:
    """Row-weighted average in-memory size of the columns of all sampled files."""
    per_file = [getattr(fi, "avg_column_in_mem_bytes", None) for fi in file_infos]
    if any(sizes is None for sizes in per_file):
        return {}
    weights = [fi.metadata.num_rows for fi in file_infos]
    total_rows = sum(weights)
    if total_rows <= 0:
        return {}
    common_columns = set.intersection(*(set(sizes) for sizes in per_file))
    return {
        name: sum(w * sizes[name] for w, sizes in zip(weights, per_file)) / total_rows
        for name in common_columns
    }


@dataclass(frozen=True, repr=False, eq=False)
class ReadFiles(
    AbstractMap,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalOperatorSupportsPredicatePushdown,
):
    """Logical operator for DataSourceV2 reads.

    Consumes ``FileManifest`` blocks produced by a :class:`ListFiles`
    source operator upstream. Owns the :class:`Scanner` (with any pushed
    column/predicate/limit state) and the post-pushdown schema. Listing,
    shuffling, and size-balanced bucketing happen in the upstream op;
    this op's physical planner just reads each manifest bucket via
    ``scanner.create_reader().read(manifest)``.

    V2 reads never rename columns at the read stage — column renaming
    is always handled by a ``Project`` operator above ``ReadFiles``.
    This simplifies projection and predicate pushdown by eliminating
    the "predicate above uses new names, predicate below uses old
    names" rebinding dance.
    """

    datasource_name: str
    scanner: "Scanner"
    schema: "pa.Schema"
    parallelism: int
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    compute: Optional[ComputeStrategy] = None
    # Optional post-read block transform. Used by ``read_parquet``'s
    # ``_block_udf`` and ``tensor_column_schema`` (the latter is folded
    # into a ``_block_udf`` by ``_resolve_parquet_args`` before it gets
    # here). Applied in ``plan_read_files_op.do_read`` after each
    # table is read.
    block_udf: Optional[Callable[[Block], Block]] = None
    # Columns that a known post-read transform may modify. ``None`` means
    # that the transform's effects are unknown, so predicate pushdown must
    # stay disabled. Currently populated only for tensor-column decoding.
    block_udf_modified_columns: Optional[Set[str]] = field(default=None, kw_only=True)
    input_dependencies: List[LogicalOperator] = field(repr=False, kw_only=True)
    can_modify_num_rows: bool = field(init=False, default=True)
    min_rows_per_bundled_input: Optional[int] = field(init=False, default=None)
    ray_remote_args_fn: None = field(init=False, default=None)
    # Declared so the inherited ``AbstractMap._get_args`` can resolve it; V2
    # limit pushdown is applied via ``scanner.push_limit`` (see
    # ``LimitPushdownRule._apply_per_block_limit_if_supported``), not this field.
    per_block_limit: Optional[int] = field(init=False, default=None)
    num_outputs: Optional[int] = None
    _name: str = field(init=False, repr=False)

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        assert isinstance(
            self.input_dependencies[0], LogicalOperator
        ), self.input_dependencies[0]
        if self.compute is None:
            from ray.data._internal.compute import TaskPoolStrategy

            object.__setattr__(self, "compute", TaskPoolStrategy())
        if self.ray_remote_args is None:
            object.__setattr__(self, "ray_remote_args", {})
        object.__setattr__(self, "_name", f"ReadFiles{self.datasource_name}")

    def infer_schema(self) -> "pa.Schema":
        # Scanner schema reflects any applied projection pushdown
        # (``scanner.prune_columns`` / empty projection from
        # ``select_columns([])``); the stored ``self.schema`` is the
        # unprojected one and only used for construction.
        schema = self.scanner.read_schema()
        # When a ``block_udf`` is attached (e.g. ``read_parquet`` was
        # called with ``tensor_column_schema`` or ``_block_udf``), probe
        # its effect on the schema so downstream consumers see the
        # post-transform column types. Mirrors V1 ``ParquetDatasource``'s
        # dummy-table trick. Falls back to the scanner schema if the
        # probe fails — the UDF may require a non-empty input.
        if self.block_udf is not None:
            try:
                transformed = self.block_udf(schema.empty_table()).schema
                schema = transformed.with_metadata(schema.metadata)
            except Exception:
                pass
        return schema

    def infer_metadata(self) -> BlockMetadata:
        """Return empty metadata; downstream callers fall back to materialization.

        Prior ``ReadFiles`` versions reached into a driver-side file cache to
        compute size hints. With listing owned by an upstream
        ``ListFiles`` op, metadata-for-sizing is computed from the
        materialized manifest at execution time — the logical op doesn't
        try to pre-estimate.
        """
        return BlockMetadata(None, None, None, None)

    def supports_projection_pushdown(self) -> bool:
        from ray.data._internal.datasource_v2.interfaces.pushdown import (
            SupportsColumnPruning,
        )

        return isinstance(self.scanner, SupportsColumnPruning)

    def get_projection_map(self) -> Optional[Dict[str, str]]:
        if not self.supports_projection_pushdown():
            return None
        columns = self.scanner.pruned_column_names()
        if columns is None:
            return None
        # The read stage never renames at the read layer; the projection
        # map is always an identity (original name -> original name).
        # Renaming is always carried by an ``AliasExpr`` in a ``Project``
        # operator above the read.
        return {name: name for name in columns}

    def apply_projection(
        self,
        projection_map: Optional[Dict[str, str]],
    ) -> "ReadFiles":
        if projection_map is None:
            return self
        from ray.data._internal.datasource_v2.interfaces.pushdown import (
            SupportsColumnPruning,
        )

        assert isinstance(self.scanner, SupportsColumnPruning)

        # V2 reads only prune columns at the read stage. Any rename info
        # in ``projection_map`` is dropped here; the optimizer rule keeps
        # a ``Project`` op on top of ``ReadFiles`` to carry rename
        # ``AliasExpr`` instances. Only the keys (column names to keep)
        # are used.
        new_scanner = self.scanner.prune_columns(list(projection_map.keys()))
        return replace(self, scanner=new_scanner)

    def supports_predicate_pushdown(self) -> bool:
        if self.block_udf is not None and self.block_udf_modified_columns is None:
            # An arbitrary post-read transform can change any field's values.
            return False

        from ray.data._internal.datasource_v2.interfaces.pushdown import (
            SupportsFilterPushdown,
            SupportsPartitionPruning,
        )

        # Either mixin is enough: a scanner that only prunes partitions still
        # takes the partition-column conjuncts, and ``apply_predicate`` keeps
        # the rest in a ``Filter``.
        return isinstance(
            self.scanner, (SupportsFilterPushdown, SupportsPartitionPruning)
        )

    def get_current_predicate(self) -> Optional[Expr]:
        return getattr(self.scanner, "predicate", None)

    def apply_predicate(self, predicate_expr: Expr) -> LogicalOperator:
        if self.block_udf is not None:
            if self.block_udf_modified_columns is None:
                # The transform's effects are unknown; keep every predicate
                # above it, including callers that skip the capability check.
                return self

            from ray.data._internal.planner.plan_expression.expression_visitors import (
                get_column_references,
            )

            if (
                set(get_column_references(predicate_expr))
                & self.block_udf_modified_columns
            ):
                # Predicates on transformed fields must observe post-UDF values.
                # Keep mixed predicates intact too; splitting them could change
                # the result when the expression contains OR or other nesting.
                return self

        from ray.data._internal.datasource_v2.common.pushdown_utils import (
            _split_predicate_by_columns,
            combine_predicates,
        )
        from ray.data._internal.datasource_v2.interfaces.pushdown import (
            SupportsFilterPushdown,
            SupportsPartitionPruning,
        )
        from ray.data._internal.logical.operators.map_operator import Filter

        pushes_filters = isinstance(self.scanner, SupportsFilterPushdown)
        partition_cols: Set[str] = (
            self.scanner.partition_columns
            if isinstance(self.scanner, SupportsPartitionPruning)
            else set()
        )

        if not partition_cols:
            if not pushes_filters:
                # Prunes partitions but has no partition columns to prune on
                # (no ``Partitioning`` spec): nothing to push. Returning
                # ``self`` keeps the ``Filter`` above us.
                return self
            new_scanner, residual_unpushed = self.scanner.push_filters(predicate_expr)
            new_op = replace(self, scanner=new_scanner)
            if residual_unpushed is None:
                return new_op
            # Our caller replaces the whole ``Filter`` -> ``ReadFiles`` subtree
            # with what we return, so the leftover needs a ``Filter`` of its own
            # or it never runs.
            return Filter(predicate_expr=residual_unpushed, input_dependencies=[new_op])

        # Cuts the top-level ``AND`` chain into three buckets by referenced
        # columns: partition-only, data-only, and mixed (bindable by neither).
        split = _split_predicate_by_columns(predicate_expr, partition_cols)

        if split.data_predicate is None and split.partition_predicate is None:
            # Entire predicate is residual (e.g. a single mixed-column
            # ``OR``); nothing safe to push. Returning ``self`` tells
            # ``PredicatePushdown`` to keep the ``Filter`` above us.
            return self

        new_scanner = self.scanner
        # Only an ``AND`` chain is splittable: each conjunct must hold on its
        # own, so where each one is applied doesn't change the result.
        residual_unpushed = split.residual_predicate
        if split.partition_predicate is not None:
            new_scanner = new_scanner.prune_partitions(split.partition_predicate)
        if split.data_predicate is not None:
            if pushes_filters:
                # ``push_filters`` may decline part of what it was offered; that
                # leftover is another conjunct of the same chain, so it goes back
                # into the residual.
                new_scanner, declined = new_scanner.push_filters(split.data_predicate)
            else:
                # No filter pushdown: the data-column conjuncts stay in a ``Filter``
                # above the read, and only the partition ones move.
                declined = split.data_predicate
            residual_unpushed = combine_predicates(residual_unpushed, declined)

        if new_scanner is self.scanner:
            # Nothing was pushed (e.g. a data-only predicate on a scanner that
            # only prunes partitions); keep the original ``Filter``.
            return self

        new_op = replace(self, scanner=new_scanner)

        if residual_unpushed is None:
            return new_op

        # Residual conjuncts can't be pushed through either ``push_filters``
        # (pyarrow only binds data columns) or ``prune_partitions`` (path
        # parser only binds partition columns), so re-emit them as a
        # ``Filter`` above the new ``ReadFiles``. Without this, we'd keep
        # the splittable parts and silently drop the residual — letting
        # rows through that the original predicate would have rejected.
        return Filter(predicate_expr=residual_unpushed, input_dependencies=[new_op])


@dataclass(frozen=True, repr=False, eq=False)
class ListFiles(LogicalOperator, SourceOperator):
    """Logical source op that lists files and yields ``FileManifest`` blocks.

    Extracted from the prior monolithic ``ReadFiles`` so listing, file shuffle
    (applied by the ``FileIndexer`` after path discovery and before metadata
    fetch), and size-balanced bucketing live in one place (see
    :func:`ray.data._internal.planner.plan_list_files_op.plan_list_files_op`).
    Downstream, ``ReadFiles`` consumes the manifest blocks produced here.
    """

    paths: List[str]
    file_indexer: "FileIndexer"
    filesystem: Optional["FileSystem"]
    # Original user-supplied paths. Lineage-tracking pins this to the
    # caller's intent rather than the resolved absolute paths.
    source_paths: List[str]
    # Path and size blocks discovered during full schema inference. When set,
    # execution processes this candidate set instead of walking ``paths`` again.
    prelisted_file_infos: Optional[List["pa.Table"]] = None
    file_partitioner: Optional["FilePartitioner"] = None
    file_extensions: Optional[List[str]] = None
    partition_filter: Optional["PathPartitionFilter"] = None
    # A factory (not a stored config) so the shuffle seed is re-sampled
    # per execution when the config asks for it.
    shuffle_config_factory: Callable[[], Optional["FileShuffleConfig"]] = field(
        default=lambda: None
    )
    # Pushed-down read constraints, populated by the optimizer rules
    # (``predicate_pushdown`` / ``projection_pushdown`` / ``limit_pushdown``).
    # A metadata-aware indexer (the Parquet ``FooterFileIndexer``) uses them to
    # prune row groups, size only projected columns, and stop listing early;
    # the per-file listing path ignores them. Whether footer reads happen is
    # decided by the indexer type, not a flag here -- this op stays
    # format-agnostic.
    predicate: Optional[Expr] = None
    projected_columns: Optional[List[str]] = None
    limit: Optional[int] = None
    # Drops whole files by path. Unlike ``predicate`` above (row-group stats,
    # blind to partition columns), this is what makes ``limit`` safe here.
    partition_pruner: Optional["FilePruner"] = None
    _name: str = field(init=False, repr=False)
    _input_dependencies: List[LogicalOperator] = field(
        init=False, repr=False, default_factory=list
    )

    def __post_init__(self):
        object.__setattr__(self, "_name", self.__class__.__name__)

    def output_data(self) -> Optional[list]:
        return None

    def infer_schema(self) -> "pa.Schema":
        # ``FileManifest`` columns are fixed: __path, __file_size.
        import pyarrow as pa

        from ray.data._internal.datasource_v2.interfaces.file_manifest import (
            FILE_SIZE_COLUMN_NAME,
            PATH_COLUMN_NAME,
        )

        return pa.schema(
            [
                pa.field(PATH_COLUMN_NAME, pa.string()),
                pa.field(FILE_SIZE_COLUMN_NAME, pa.int64()),
            ]
        )
