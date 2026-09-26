from dataclasses import dataclass, replace
from typing import List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.logical_optimizers import (
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
)
from ray.data._internal.datasource_v2.scanners.file_scanner import FileScanner
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class ArrowFileScanner(
    FileScanner,
    SupportsFilterPushdown,
    SupportsColumnPruning,
    SupportsLimitPushdown,
):
    """Base scanner for file-based datasources that use PyArrow's Dataset API.

    Holds shared Arrow types and options (schema, projection, filesystem,
    etc.). Subclasses set the file format in :meth:`create_reader`.

    Provides default implementations of filter pushdown, column pruning and
    limit pushdown that work for all Arrow-backed formats. Partition pruning
    comes from :class:`FileScanner`.

    Non-Arrow file formats should subclass :class:`FileScanner` directly.
    """

    schema: pa.Schema
    batch_size: Optional[int] = None
    columns: Optional[Tuple[str, ...]] = None
    predicate: Optional[Expr] = None
    limit: Optional[int] = None
    filesystem: Optional[FileSystem] = None
    ignore_prefixes: Optional[List[str]] = None

    @override
    def metadata_row_count_is_exact(self) -> bool:
        """``True`` when nothing reduces rows, or only whole files are dropped.

        A Parquet footer's ``num_rows`` is the file's total, with nothing in it
        to say how many rows survive a filter, so a data predicate or a limit
        rules the count out. A partition predicate is different: it drops whole
        files, and ``prune_manifest`` drops them before any footer is read, so
        the surviving footers still sum exactly. That needs a partitioning
        spec -- without one ``prune_manifest`` no-ops and would silently sum
        the files it should have dropped.

        Column projection is deliberately not consulted: it changes the width
        of the output, never the row count.
        """
        return (
            self.predicate is None
            and (self.partition_predicate is None or self.partitioning is not None)
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
