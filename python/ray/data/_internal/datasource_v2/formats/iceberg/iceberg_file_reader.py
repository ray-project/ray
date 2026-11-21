"""``Reader`` that decodes an Iceberg read unit through PyIceberg.

A read unit is a ``FileManifest`` of whole data files. Each row's pickled
``FileScanTask`` goes to PyIceberg's ``ArrowScan`` unchanged, one file at a
time, so deletes, schema evolution and partition backfill stay PyIceberg's job:

    manifest row -> FileScanTask -> ArrowScan (one file) -> record batches
                 -> pickle check -> cast to the projected schema -> block

The reader yields a block per record batch and stops once the pushed limit is
reached. Filter parts PyArrow's statistics could get wrong (see
``iceberg_filter_pushdown``) are applied after ``ArrowScan``, with Ray's own
evaluator, and the columns read only for them are then dropped.

One file per call bounds memory but re-reads a delete file shared by several
data files once per file; ``IcebergConfig.read_file_tasks_sequentially =
False`` trades the other way, passing the whole unit in one call as V1 does.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator, List, Optional, Set

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.arrow_block import (
    _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
    ArrowBlockAccessor,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
    scan_tasks_from_manifest,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.supports_metadata import (
    MetadataType,
    SupportsMetadata,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data.block import BlockMetadata
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.io.pyarrow import ArrowScan
    from pyiceberg.schema import Schema
    from pyiceberg.table import FileScanTask
    from pyiceberg.table.metadata import TableMetadata

    from ray.data.expressions import Expr


@DeveloperAPI
class IcebergFileReader(Reader[FileManifest], SupportsMetadata):
    """Decodes a manifest's files with PyIceberg's ``ArrowScan``, one at a time.

    ``ArrowScan`` reads Parquet and ORC data files; Avro ones raise
    ``ValueError``, as in V1. It holds every batch of the files it is given, so
    one file per call keeps memory to one file. With
    ``DataContext.iceberg_config.read_file_tasks_sequentially = False`` the
    whole unit goes in one call, as in V1.
    """

    def __init__(
        self,
        *,
        table_metadata: "TableMetadata",
        io: "FileIO",
        projected_schema: "Schema",
        row_filter: "BooleanExpression",
        case_sensitive: bool = True,
        limit: Optional[int] = None,
        drop_all_columns: bool = False,
        after_read_filter: Optional["Expr"] = None,
        output_columns: Optional[List[str]] = None,
    ):
        self._table_metadata = table_metadata
        self._io = io
        self._projected_schema = projected_schema
        self._row_filter = row_filter
        self._case_sensitive = case_sensitive
        self._limit = limit
        # Zero columns were asked for; see ``IcebergScanner.create_reader``.
        self._drop_all_columns = drop_all_columns
        # Applied by Ray after ``ArrowScan``; see ``iceberg_filter_pushdown``.
        self._after_read_filter = after_read_filter
        # The columns to keep after that filter, ``None`` for all of them.
        self._output_columns = output_columns

    def _scan(self, limit: Optional[int]) -> "ArrowScan":
        from pyiceberg.io.pyarrow import ArrowScan

        return ArrowScan(
            table_metadata=self._table_metadata,
            io=self._io,
            projected_schema=self._projected_schema,
            row_filter=self._row_filter,
            case_sensitive=self._case_sensitive,
            limit=limit,
        )

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        # Files can disagree on ``string`` vs ``large_string``, so every batch is
        # cast to the declared schema, as V1 does.
        target_schema = schema_to_pyarrow(
            self._projected_schema, include_field_ids=False
        )

        rows_yielded = 0
        for record_batch in self._record_batches(input_split):
            if self._limit is not None and rows_yielded >= self._limit:
                return
            table = pa.Table.from_batches([record_batch])
            # Before the cast, which fails opaquely on a pickled column.
            raise_on_pickle_object_columns(table)
            table = table.cast(target_schema)
            table = self._apply_after_read_filter(table)
            if self._limit is not None:
                table = table.slice(0, self._limit - rows_yielded)
            rows_yielded += table.num_rows
            if self._drop_all_columns:
                # Zero-column tables lose their length on concat, so use the
                # shared ``FileReader``'s null stub column.
                table = table.select([]).append_column(
                    _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
                    pa.nulls(table.num_rows),
                )
            yield table

    def _apply_after_read_filter(self, table: pa.Table) -> pa.Table:
        if self._after_read_filter is None:
            return table
        table = ArrowBlockAccessor(table).filter(self._after_read_filter)
        if self._output_columns is not None:
            table = table.select(self._output_columns)
        return table

    def _record_batches(self, input_split: FileManifest) -> Iterator[pa.RecordBatch]:
        from ray.data.context import DataContext

        # ``ArrowScan`` counts rows before the after-read filter, so it gets no
        # limit then; ``read`` stops instead.
        limit = self._limit if self._after_read_filter is None else None
        tasks = scan_tasks_from_manifest(manifest=input_split)
        if not DataContext.get_current().iceberg_config.read_file_tasks_sequentially:
            # One call reads a delete file shared by several files once, but
            # holds every file's batches. A list, since PyIceberg iterates twice.
            yield from self._scan(limit=limit).to_record_batches(list(tasks))
            return
        rows_read = 0
        for task in tasks:
            remaining = None if limit is None else limit - rows_read
            if remaining is not None and remaining <= 0:
                return
            for record_batch in self._scan(limit=remaining).to_record_batches([task]):
                rows_read += record_batch.num_rows
                yield record_batch

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Yield one ``BlockMetadata`` per file, with ``num_rows`` from Iceberg.

        A file's ``record_count`` is written into Iceberg's own manifests, so
        for most files this touches no data at all -- not even a Parquet
        footer, unlike the equivalent path for a plain Parquet read.

        It is an *upper* bound on two kinds of file, and each is handled rather
        than assumed away. A file with delete files has rows that are still
        counted but must not be returned, and a file whose per-file residual is
        not ``AlwaysTrue`` has rows the filter rejects. Either way the count
        comes from decoding the file, which is what PyIceberg's own ``count()``
        does. So does every file under an after-read filter, whose residual
        was planned with ``| is_nan`` and so can keep rows Ray drops.

        The residual is what makes a filtered ``count()`` worth attempting at
        all: a filter on a partition column leaves every listed file with an
        ``AlwaysTrue`` residual, so the whole count stays free. A filter on a
        data column leaves none of them, and every file is decoded -- the same
        work the ordinary read path would have done. Residuals are trusted only
        for filters ``_residuals_match_arrow`` accepts.

        Args:
            file_manifest: The read unit's manifest, as the listing wrote it.

        Yields:
            BlockMetadata: One per file in ``file_manifest``, in order.
        """
        from pyiceberg.expressions import AlwaysTrue

        trust_residuals = _residuals_match_arrow(self._row_filter)
        for task in scan_tasks_from_manifest(manifest=file_manifest):
            if (
                trust_residuals
                and isinstance(task.residual, AlwaysTrue)
                and not task.delete_files
                and self._after_read_filter is None
            ):
                num_rows = task.file.record_count
            else:
                num_rows = self._count_by_reading(task)
            yield BlockMetadata(
                num_rows=num_rows,
                size_bytes=None,
                exec_stats=None,
                input_files=None,
            )

    def _count_by_reading(self, task: "FileScanTask") -> int:
        """Rows ``task`` actually yields, by decoding it.

        ``self._projected_schema`` is whatever the scanner asked for, which on
        the count path is a stub field no file has (see
        ``IcebergScanner.create_reader``), so the only columns decoded are the
        ones the row filter names. PyIceberg reads those alongside the
        projection, so the filter still binds. ``limit`` is deliberately not
        passed: a partial read would undercount, and ``available_metadata``
        already declines when a limit is set.

        No ``raise_on_pickle_object_columns`` here, unlike ``read``: the batches
        are counted and dropped, never handed to anyone and never unpickled, so
        there is nothing to protect. A caller who then reads the data gets the
        check from ``read``.

        Args:
            task: One Iceberg scan task, naming a single data file.

        Returns:
            The number of rows the task yields after filters and deletes.
        """
        return sum(
            self._apply_after_read_filter(pa.Table.from_batches([batch])).num_rows
            for batch in self._scan(None).to_record_batches([task])
        )

    @override
    def available_metadata(self) -> Set[MetadataType]:
        # A limit would make the answer a partial count, and unlike a row
        # filter there is nothing in the metadata that could account for it.
        # Only ``NUM_ROWS``: Iceberg records a file's *compressed* size, which
        # is not what a byte-metadata consumer means.
        if self._limit is not None:
            return set()
        return {MetadataType.NUM_ROWS}

    @override
    def get_target_metadata_batch_size(self) -> Optional[int]:
        # ``None`` means one task per manifest, and a manifest is already one
        # bin-packed read unit. The Parquet reader batches to bound how many
        # footers one task fetches; here the counts are in the manifest, so
        # there is no per-file IO to spread out.
        return None


def _residuals_match_arrow(row_filter: "BooleanExpression") -> bool:
    """Whether an ``AlwaysTrue`` residual means ``ArrowScan`` keeps every row.

    Iceberg plans residuals from partition values with ``null != 3`` true, but
    PyArrow evaluates it as null and drops the row. So only filters built from
    operators that treat null the same way in both qualify.
    """
    from pyiceberg.expressions import (
        AlwaysFalse,
        AlwaysTrue,
        And,
        EqualTo,
        GreaterThan,
        GreaterThanOrEqual,
        In,
        IsNaN,
        IsNull,
        LessThan,
        LessThanOrEqual,
        NotNull,
        Or,
    )
    from pyiceberg.expressions.visitors import rewrite_not

    def agrees(expr: "BooleanExpression") -> bool:
        if isinstance(expr, (And, Or)):
            return agrees(expr.left) and agrees(expr.right)
        return isinstance(
            expr,
            (
                AlwaysTrue,
                AlwaysFalse,
                EqualTo,
                GreaterThan,
                GreaterThanOrEqual,
                In,
                IsNaN,
                IsNull,
                LessThan,
                LessThanOrEqual,
                NotNull,
            ),
        )

    return agrees(rewrite_not(row_filter))
