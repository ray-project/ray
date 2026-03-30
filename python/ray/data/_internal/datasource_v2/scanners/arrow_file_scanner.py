import logging
from dataclasses import dataclass, replace
from typing import List, Optional, Set, Tuple

import pyarrow as pa
from pyarrow import compute as pc
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.logical_optimizers import (
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
    SupportsPartitionPruning,
)
from ray.data._internal.datasource_v2.scanners.file_scanner import FileScanner
from ray.data.context import DataContext
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
    batch_size: int
    columns: Optional[Tuple[str, ...]] = None
    predicate: Optional[pc.Expression] = None
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

    def read_schema(self) -> pa.Schema:
        """Return schema after column pruning."""
        if not self.columns:
            return self.schema
        fields = []
        for name in self.columns:
            idx = self.schema.get_field_index(name)
            if idx != -1:
                fields.append(self.schema.field(idx))
        return pa.schema(fields)

    @override
    def push_filters(
        self, predicate: "Expr"
    ) -> Tuple["ArrowFileScanner", Optional["Expr"]]:
        """Push filter predicate down to the scanner.

        Converts the Ray Data expression to a PyArrow expression and ANDs it
        with any existing predicate. PyArrow applies the filter at scan time
        for all supported formats.

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
        pa_predicate = predicate.to_pyarrow()

        if self.predicate is not None:
            combined = self.predicate & pa_predicate
        else:
            combined = pa_predicate

        return replace(self, predicate=combined), None

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
    def plan(
        self,
        manifest: FileManifest,
        parallelism: int,
        data_context: Optional["DataContext"] = None,
    ) -> List[FileManifest]:
        """Plan parallel work units, pruning files by partition predicate first.

        If a partition predicate is set, files whose partition values do not
        satisfy the predicate are removed from the manifest before splitting.

        Args:
            manifest: FileManifest to partition.
            parallelism: Target number of parallel tasks.
            data_context: Optional data context.

        Returns:
            List of FileManifest objects for parallel execution.
        """
        if self.partition_predicate is not None and self.partitioning is not None:
            manifest = self._prune_manifest(manifest)
        return super().plan(manifest, parallelism, data_context)

    def _prune_manifest(self, manifest: FileManifest) -> FileManifest:
        """Filter manifest to only files matching the partition predicate.

        Uses :class:`PathPartitionParser` to parse partition values from each
        file path and evaluate the partition predicate against them.

        Args:
            manifest: The full file manifest.

        Returns:
            A new FileManifest containing only matching files.
        """
        parser = PathPartitionParser(self.partitioning)
        keep_indices = []

        for i, path in enumerate(manifest.paths):
            if parser.evaluate_predicate_on_partition(
                str(path), self.partition_predicate
            ):
                keep_indices.append(i)

        if len(keep_indices) == len(manifest):
            return manifest

        pruned_count = len(manifest) - len(keep_indices)
        logger.debug(
            "Partition pruning removed %d of %d files",
            pruned_count,
            len(manifest),
        )

        block = manifest.as_block()
        pruned_block = block.take(keep_indices)
        return FileManifest(pruned_block)
