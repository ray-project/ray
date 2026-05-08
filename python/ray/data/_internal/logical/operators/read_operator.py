import functools
import math
from dataclasses import InitVar, dataclass, field, replace
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePushdown,
    LogicalOperatorSupportsProjectionPushdown,
    SourceOperator,
)
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.block import (
    BlockMetadata,
    BlockMetadataWithSchema,
)
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, Reader
from ray.data.expressions import Expr

if TYPE_CHECKING:
    import pyarrow as pa
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.listing.file_indexer import FileIndexer
    from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
        FilePartitioner,
    )
    from ray.data._internal.datasource_v2.scanners.scanner import Scanner
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
    num_outputs: InitVar[Optional[int]] = None
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    compute: Optional[ComputeStrategy] = None
    detected_parallelism: Optional[int] = None
    can_modify_num_rows: bool = field(init=False, default=True)
    min_rows_per_bundled_input: Optional[int] = field(init=False, default=None)
    ray_remote_args_fn: None = field(init=False, default=None)
    per_block_limit: Optional[int] = None
    _input_dependencies: list = field(init=False, repr=False, default_factory=list)
    _num_outputs: Optional[int] = field(init=False, repr=False)

    def __post_init__(self, num_outputs: Optional[int]):
        if self.compute is None:
            from ray.data._internal.compute import TaskPoolStrategy

            object.__setattr__(self, "compute", TaskPoolStrategy())
        if self.ray_remote_args is None:
            object.__setattr__(self, "ray_remote_args", {})
        object.__setattr__(self, "_name", f"Read{self.datasource.get_name()}")
        object.__setattr__(self, "_input_dependencies", [])
        object.__setattr__(self, "_num_outputs", num_outputs)

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
        return self._num_outputs or self._estimate_num_outputs()

    def infer_metadata(self) -> BlockMetadata:
        """A ``BlockMetadata`` that represents the aggregate metadata of the outputs.

        This method gets metadata from the read tasks. It doesn't trigger any actual
        execution.
        """
        return self._cached_output_metadata.metadata

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
            num_outputs=self._num_outputs,
        )

    def get_column_renames(self) -> Optional[Dict[str, str]]:
        return self.datasource.get_column_renames()

    def supports_predicate_pushdown(self) -> bool:
        return self.datasource.supports_predicate_pushdown()

    def get_current_predicate(self) -> Optional[Expr]:
        return self.datasource.get_current_predicate()

    def apply_predicate(self, predicate_expr: Expr) -> "Read":
        predicated_datasource = self.datasource.apply_predicate(predicate_expr)

        return replace(
            self,
            datasource=predicated_datasource,
            datasource_or_legacy_reader=predicated_datasource,
            num_outputs=self._num_outputs,
        )


@dataclass(frozen=True, repr=False, eq=False)
class ReadFiles(
    AbstractMap,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalOperatorSupportsPredicatePushdown,
):
    """Logical operator for DataSourceV2 reads.

    Consumes ``FileManifest`` blocks produced by a :class:`ListFiles`
    source operator upstream. Owns the :class:`Scanner` (with any pushed
    column/predicate/limit state), schema, and ``column_renames`` map.
    Listing, shuffling, and size-balanced bucketing happen in the
    upstream op; this op's physical planner just reads each manifest
    bucket via ``scanner.create_reader().read(manifest)``.
    """

    input_op: InitVar[LogicalOperator]
    datasource_name: str
    scanner: "Scanner"
    schema: "pa.Schema"
    parallelism: int
    ray_remote_args: Dict[str, Any] = field(default_factory=dict)
    compute: Optional[ComputeStrategy] = None
    detected_parallelism: Optional[int] = None
    # ``old_name → new_name`` for any columns the projection pushdown rule
    # renamed. The scanner only knows original names; renames are applied
    # in ``plan_read_files_op`` after each block is read.
    column_renames: Optional[Dict[str, str]] = None
    can_modify_num_rows: bool = field(init=False, default=True)
    min_rows_per_bundled_input: Optional[int] = field(init=False, default=None)
    ray_remote_args_fn: None = field(init=False, default=None)
    _name: str = field(init=False, repr=False)
    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, repr=False, default=None)

    def __post_init__(self, input_op: LogicalOperator):
        assert isinstance(input_op, LogicalOperator), input_op
        if self.compute is None:
            from ray.data._internal.compute import TaskPoolStrategy

            object.__setattr__(self, "compute", TaskPoolStrategy())
        if self.ray_remote_args is None:
            object.__setattr__(self, "ray_remote_args", {})
        object.__setattr__(self, "_name", f"ReadFiles{self.datasource_name}")
        object.__setattr__(self, "_input_dependencies", [input_op])
        object.__setattr__(self, "_num_outputs", None)

    @property
    def input_dependency(self) -> LogicalOperator:
        return self.input_dependencies[0]

    def _apply_transform(
        self, transform: "Callable[[LogicalOperator], LogicalOperator]"
    ) -> LogicalOperator:
        transformed_input = self.input_dependency._apply_transform(transform)
        target: LogicalOperator
        if transformed_input is self.input_dependency:
            target = self
        else:
            target = replace(self, input_op=transformed_input)
        return transform(target)

    def set_detected_parallelism(self, parallelism: int) -> "ReadFiles":
        return replace(
            self, input_op=self.input_dependency, detected_parallelism=parallelism
        )

    def get_detected_parallelism(self) -> Optional[int]:
        return self.detected_parallelism

    def infer_schema(self) -> "pa.Schema":
        # Scanner schema reflects any applied projection pushdown
        # (``scanner.prune_columns`` / empty projection from
        # ``select_columns([])``); the stored ``self.schema`` is the
        # unprojected one and only used for construction.
        schema = self.scanner.read_schema()
        if self.column_renames:
            import pyarrow as pa

            renamed_fields = [
                pa.field(self.column_renames.get(f.name, f.name), f.type, f.nullable)
                for f in schema
            ]
            schema = pa.schema(renamed_fields)
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

    def estimate_in_memory_size(self) -> Optional[int]:
        """Used by ``SetReadParallelismRule``. Returns ``None`` now — the
        upstream ``ListFiles`` op computes balanced buckets via
        ``RoundRobinPartitioner`` at execution time, so the rule's
        size-based parallelism heuristic is no longer load-bearing."""
        return None

    def supports_projection_pushdown(self) -> bool:
        from ray.data._internal.datasource_v2.logical_optimizers import (
            SupportsColumnPruning,
        )

        return isinstance(self.scanner, SupportsColumnPruning)

    def get_projection_map(self) -> Optional[Dict[str, str]]:
        if not self.supports_projection_pushdown():
            return None
        columns = self.scanner.pruned_column_names()
        if columns is None:
            return None
        renames = self.column_renames or {}
        return {name: renames.get(name, name) for name in columns}

    def get_column_renames(self) -> Optional[Dict[str, str]]:
        return self.column_renames

    def apply_projection(
        self,
        projection_map: Optional[Dict[str, str]],
    ) -> "ReadFiles":
        if projection_map is None:
            return self
        from ray.data._internal.datasource_v2.logical_optimizers import (
            SupportsColumnPruning,
        )

        assert isinstance(self.scanner, SupportsColumnPruning)

        # ``projection_map`` is ``{input_name: output_name}`` where the
        # input names are what this op's current output produces — which
        # includes any renames applied on a prior pushdown. Translate
        # input names back to the ORIGINAL on-disk column names via the
        # existing ``column_renames`` map, then hand those originals to
        # the scanner. The new rename map is composed on top of the old
        # so a chain like ``sepal.length → a → b`` collapses to
        # ``sepal.length → b``.
        existing = self.column_renames or {}
        reverse = {out: orig for orig, out in existing.items()}

        original_to_new: Dict[str, str] = {}
        for input_name, output_name in projection_map.items():
            original = reverse.get(input_name, input_name)
            original_to_new[original] = output_name

        new_scanner = self.scanner.prune_columns(list(original_to_new.keys()))
        merged = {orig: out for orig, out in original_to_new.items() if orig != out}
        return replace(
            self,
            input_op=self.input_dependency,
            scanner=new_scanner,
            column_renames=merged or None,
        )

    def supports_predicate_pushdown(self) -> bool:
        from ray.data._internal.datasource_v2.logical_optimizers import (
            SupportsFilterPushdown,
        )

        return isinstance(self.scanner, SupportsFilterPushdown)

    def get_current_predicate(self) -> Optional[Expr]:
        return getattr(self.scanner, "predicate", None)

    def apply_predicate(self, predicate_expr: Expr) -> "ReadFiles":
        from ray.data._internal.datasource_v2.logical_optimizers import (
            SupportsFilterPushdown,
            SupportsPartitionPruning,
        )
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            get_column_references,
        )

        assert isinstance(self.scanner, SupportsFilterPushdown)

        # Skip pushdown if the predicate references any partition column:
        # ``push_filters`` passes the expression to pyarrow's scanner, which
        # can only bind on-disk columns. Splitting the predicate into
        # data- and partition-column conjuncts (then routing each through
        # ``push_filters`` / ``prune_partitions``) is a follow-up; for
        # now the rule keeps the ``Filter`` above ``ReadFiles`` when we
        # return ``self`` unchanged, so correctness is preserved.
        if isinstance(self.scanner, SupportsPartitionPruning):
            referenced = set(get_column_references(predicate_expr))
            if referenced & self.scanner.partition_columns:
                return self

        new_scanner, _residual = self.scanner.push_filters(predicate_expr)
        return replace(self, input_op=self.input_dependency, scanner=new_scanner)


@dataclass(frozen=True, repr=False, eq=False)
class ListFiles(LogicalOperator, SourceOperator):
    """Logical source op that lists files and yields ``FileManifest`` blocks.

    Extracted from the prior monolithic ``ReadFiles`` so listing, shuffling,
    and size-balanced bucketing live in one place (see
    :func:`ray.data._internal.planner.plan_list_files_op.plan_list_files_op`).
    Downstream, ``ReadFiles`` consumes the manifest blocks produced here.
    """

    paths: List[str]
    file_indexer: "FileIndexer"
    filesystem: "FileSystem"
    # Original user-supplied paths. Lineage-tracking pins this to the
    # caller's intent rather than the resolved absolute paths.
    source_paths: List[str]
    file_partitioner: Optional["FilePartitioner"] = None
    file_extensions: Optional[List[str]] = None
    partition_filter: Optional["PathPartitionFilter"] = None
    # A factory (not a stored config) so the shuffle seed is re-sampled
    # per execution when the config asks for it.
    shuffle_config_factory: Callable[[], Optional["FileShuffleConfig"]] = field(
        default=lambda: None
    )
    _name: str = field(init=False, repr=False)
    _input_dependencies: List[LogicalOperator] = field(
        init=False, repr=False, default_factory=list
    )
    _num_outputs: Optional[int] = field(init=False, repr=False, default=None)

    def __post_init__(self):
        object.__setattr__(self, "_name", self.__class__.__name__)

    def output_data(self) -> Optional[list]:
        return None

    @property
    def num_outputs(self) -> Optional[int]:
        return None

    def infer_schema(self) -> "pa.Schema":
        # ``FileManifest`` columns are fixed: __path, __file_size.
        import pyarrow as pa

        from ray.data._internal.datasource_v2.listing.file_manifest import (
            FILE_SIZE_COLUMN_NAME,
            PATH_COLUMN_NAME,
        )

        return pa.schema(
            [
                pa.field(PATH_COLUMN_NAME, pa.string()),
                pa.field(FILE_SIZE_COLUMN_NAME, pa.int64()),
            ]
        )
