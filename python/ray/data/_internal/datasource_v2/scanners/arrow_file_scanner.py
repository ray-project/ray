import logging
from dataclasses import dataclass, replace
from typing import List, Optional, Set, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.file_pruners import (
    FilePruner,
    PartitionPredicatePruner,
)
from ray.data._internal.datasource_v2.logical_optimizers import (
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
    SupportsPartitionPruning,
)
from ray.data._internal.datasource_v2.scanners.file_scanner import FileScanner
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
@dataclass(frozen=True)
class ArrowFileScanner(
    FileScanner,
    SupportsFilterPushdown,
    SupportsColumnPruning,
    SupportsLimitPushdown,
    SupportsPartitionPruning,
):
    """Base scanner for file-based datasources that use PyArrow's Dataset API.

    Holds shared Arrow types and options (schema, projection, filesystem,
    partitioning, etc.). Subclasses set the file format in :meth:`create_reader`.

    Provides default implementations of filter pushdown, column pruning,
    limit pushdown, and partition pruning that work for all Arrow-backed
    formats.

    Non-Arrow file formats should subclass :class:`FileScanner` directly.
    """

    schema: pa.Schema
    batch_size: Optional[int] = None
    columns: Optional[Tuple[str, ...]] = None
    predicate: Optional[Expr] = None
    partition_predicate: Optional[Expr] = None
    limit: Optional[int] = None
    filesystem: Optional[FileSystem] = None
    partitioning: Optional[Partitioning] = None
    ignore_prefixes: Optional[List[str]] = None

    @property
    def partition_columns(self) -> Set[str]:
        """Return the set of partition column names, or empty if unpartitioned."""
        if self.partitioning is None:
            return set()
        return set(self.partitioning.field_names or [])

    @override
    def metadata_row_count_is_exact(self) -> bool:
        """``True`` when no row-reducing pushdown is set on this scanner.

        A Parquet footer's ``num_rows`` is the file's total, with nothing in it
        to say how many rows survive a filter, so for this scanner the question
        collapses to "is anything reducing rows?". Column projection is
        deliberately not consulted: it changes the width of the output, never
        the row count.
        """
        return (
            self.predicate is None
            and self.partition_predicate is None
            and self.limit is None
        )

    def read_schema(self) -> pa.Schema:
        """Return the logical schema after column pruning.

        ``columns is None`` → no projection applied, return the full schema.
        ``columns = ()`` → empty projection (``ds.select_columns([])``),
        return an empty schema.

        The physical read may still inject a stub column (see
        ``_BATCH_SIZE_PRESERVING_STUB_COL_NAME``) so that row counts
        survive a zero-column scan; that stub is an execution-layer detail
        and is deliberately not reflected in this logical schema.
        """
        if self.columns is None:
            return self.schema
        fields = []
        for name in self.columns:
            idx = self.schema.get_field_index(name)
            assert idx >= 0, f"Column {name} not found in schema"
            fields.append(self.schema.field(idx))
        return pa.schema(fields)

    @override
    def push_filters(
        self, predicate: "Expr"
    ) -> Tuple["ArrowFileScanner", Optional["Expr"]]:
        """Push filter predicate down to the scanner.

        ANDs the predicate with any existing predicate. The Ray ``Expr`` is
        retained as the source of truth so the reader can introspect filter
        columns; conversion to a PyArrow expression happens at the
        scanner-kwargs boundary in :class:`FileReader`.

        This method handles data-column predicates only. Partition predicates
        should be pushed via :meth:`prune_partitions` instead; the optimizer
        is responsible for splitting them before calling either method.

        Args:
            predicate: Ray Data expression to push down.

        Returns:
            A pair ``(scanner, residual)`` where ``scanner`` has the predicate
            merged into its PyArrow filter. ``residual`` is ``None`` because
            PyArrow handles the full filter at scan time.
        """
        if self.predicate is not None:
            combined = self.predicate & predicate
        else:
            combined = predicate

        return replace(self, predicate=combined), None

    @override
    def pushed_predicate(self) -> Optional["Expr"]:
        return self.predicate

    @override
    def prune_columns(self, columns: List[str]) -> "ArrowFileScanner":
        """Prune to only the specified columns.

        Args:
            columns: List of column names to keep.

        Returns:
            New scanner with column pruning applied.
        """
        if self.columns:
            existing = set(self.columns)
            columns = [c for c in columns if c in existing]

        return replace(self, columns=tuple(columns))

    @override
    def pruned_column_names(self) -> Optional[Tuple[str, ...]]:
        return self.columns

    @override
    def push_limit(self, limit: int) -> "ArrowFileScanner":
        """Push row limit down to the scanner.

        Args:
            limit: Maximum number of rows to read.

        Returns:
            New scanner with limit applied.
        """
        current = self.limit
        new_limit = min(current, limit) if current is not None else limit
        return replace(self, limit=new_limit)

    @override
    def pushed_limit(self) -> Optional[int]:
        return self.limit

    @override
    def prune_partitions(self, predicate: "Expr") -> "ArrowFileScanner":
        """Store a partition predicate for file-level pruning during plan().

        The predicate is ANDed with any existing partition predicate. Actual
        file pruning happens in :meth:`plan` when the manifest is available,
        using :class:`PathPartitionParser` to evaluate partition values from
        file paths.

        Args:
            predicate: Expression referencing only partition columns.

        Returns:
            New scanner with partition predicate stored.
        """
        if self.partition_predicate is not None:
            combined = self.partition_predicate & predicate
        else:
            combined = predicate

        return replace(self, partition_predicate=combined)

    @override
    def pushed_partition_predicate(self) -> Optional["Expr"]:
        return self.partition_predicate

    @override
    def pushed_partition_pruner(self) -> Optional["FilePruner"]:
        if self.partition_predicate is None or self.partitioning is None:
            # No spec, no partition values -- same guard as ``prune_input_split``.
            return None
        return PartitionPredicatePruner(self.partitioning, self.partition_predicate)

    @override
    def prune_input_split(self, input_split: FileManifest) -> FileManifest:
        """Keep only the files matching ``self.partition_predicate``.

        No-op when either the predicate or the partitioning spec is absent.
        Partition values are parsed out of each file path by
        :class:`PathPartitionParser`.
        """
        if self.partition_predicate is None or self.partitioning is None:
            return input_split

        parser = PathPartitionParser(self.partitioning)
        keep_indices = []

        for i, path in enumerate(input_split.paths):
            if parser.evaluate_predicate_on_partition(path, self.partition_predicate):
                keep_indices.append(i)

        if len(keep_indices) == len(input_split):
            return input_split

        pruned_count = len(input_split) - len(keep_indices)
        logger.debug(
            "Partition pruning removed %d of %d files",
            pruned_count,
            len(input_split),
        )

        block = input_split.as_block()
        # An untyped empty list infers null indices: ArrowNotImplementedError.
        pruned_block = block.take(pa.array(keep_indices, type=pa.int64()))
        return FileManifest(pruned_block)
